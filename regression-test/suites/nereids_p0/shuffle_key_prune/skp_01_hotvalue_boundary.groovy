// ===========================================================================================
// 分类 01：热点值阈值与边界 — A01, S01, S02, FC1, B01-B04
// ===========================================================================================
//
// 【测试目的】
//   验证 Agg Shuffle Key Pruning 对 hot_values 的处理逻辑：
//   1. hot_values='' (空字符串) → 表示"已采样但无热点" → isBalanced()=true → 允许被选为 shuffle key
//   2. hot_values=null → 表示"未采样" → 保守回退，不 prune
//   3. hot_values='value :ratio' → ratio >= 5% → isBalanced()=false → 跳过该列（视为倾斜列）
//   4. null 比例 >= 5% → isBalanced()=false → 跳过该列（视为倾斜列）
//   5. max(null比例, hot_ratio) >= 5% → 综合判断为倾斜列
//
// 【Test Case 列表】
//   A01: hot_key 30% skew + good_key 无热点 → good_key 被选中（正例）
//   S01: hot_values='' 空串 → 视为已采样无热点 → prune 生效（正例）
//   S02: hot_values=null → 视为未采样 → 保守回退不 prune（负例）
//   FC1: full analyze 产生 hot_values=null → 不 prune（负例）
//   B01: hot_ratio=4.9% (刚好低于 5% 阈值) → 列保留，被选为 shuffle key
//   B02: hot_ratio=5.0% (等于阈值) → 列被跳过
//   B03: null 占比 10% → 视为 skew → 列被跳过
//   B04: max(null占比 10%, hot_ratio 4.9%) = 10% → 视为 skew → 列被跳过
//
// 【验证方法】
//   每个 Case 对比 OFF/ON 的 explain physical plan 中 orderedShuffledColumns 的变化，
//   并验证 OFF/ON 的执行结果一致。
// ===========================================================================================
suite("skp_01_hotvalue_boundary", "agg_shuffle_prune_func") {

    sql """set enable_nereids_planner=true;"""
    sql """set enable_fallback_to_original_planner=false;"""
    sql """set enable_sql_cache=false;"""
    sql """set enable_query_cache=false;"""

    def dbName = context.config.getDbNameByFile(context.file)
    sql "create database if not exists ${dbName};"
    sql "use ${dbName};"

    // 固定 8 个 pipeline task，确保 NDV 阈值计算一致
    def tptn = 8
    sql "set parallel_fragment_exec_instance_num=1;"
    sql "set parallel_pipeline_task_num=${tptn};"
    sql "set be_number_for_test=1;"

    // ======== 公共工具函数 ========

    // R: 手动注入的 row_count（48 万行，远大于 NDV 阈值，确保 optimizer 认为有 shuffle 必要）
    def R = "480000"

    // esc: 转义字符串中的反斜杠和单引号，用于拼接 SQL
    def esc = { Object v -> v==null?"null":v.toString().replace("\\","\\\\").replace("'","\\'") }

    // ss: 手动注入某列的统计信息（alter table ... set stats）
    //   t: 表名, c: 列名, n: NDV, nn: null 数量, ds: data_size
    //   mi/ma: min/max value, hv: hot_values 字符串
    //   当 hv=null 时不注入 hot_values 字段（模拟 full analyze 的行为）
    //   当 hv="" 时注入 hot_values=''（表示"已采样但无热点"）
    //   当 hv="val :ratio" 时注入指定热点值
    def ss = { String t,String c,String n,String nn,String ds,String mi,String ma,String hv ->
        def hvc = hv==null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql "alter table ${t} modify column ${c} set stats ('row_count'='${R}','ndv'='${esc(n)}','num_nulls'='${esc(nn)}','data_size'='${esc(ds)}','min_value'='${esc(mi)}','max_value'='${esc(ma)}'${hvc});"
    }

    // extractSigns: 从 explain 输出中提取所有 orderedShuffledColumns 列表
    //   返回如: ["[1,2,3]", "[4]"]，用于对比 OFF/ON 的 Plan 变化
    def extractSigns = { String e ->
        def s=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/); while(m.find()){s<<"["+m.group(1).replaceAll("\\s+","")+"]"}; s
    }

    // extractSingle: 提取只有单列的 orderedShuffledColumns（即被裁剪到 1 列的 shuffle key）
    //   返回 exprId 列表，如 [4] 表示只有 exprId=4 的列被选为唯一 shuffle key
    def extractSingle = { String e ->
        def ids=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while(m.find()){def r=m.group(1).trim(); if(r.length()==0)continue; def p=r.split(",").collect{it.trim()}.findAll{it.length()>0}; if(p.size()==1)ids<<Integer.parseInt(p[0])}; ids
    }

    // exprId: 在 explain 输出中查找列名对应的 exprId（如 "good_key#7" → 返回 7）
    def exprId = { String e,String c -> def m=(e=~("\\b"+java.util.regex.Pattern.quote(c)+"#(\\d+)\\b")); m.find()?Integer.parseInt(m.group(1)):null }

    // norm: 将查询结果标准化为可比较的字符串列表（处理 null 值，排序后对比）
    def norm = { rows -> rows.collect{r->r.collect{v->v==null?"NULL":v.toString()}.join("||")}.sort() }

    // showRow: 获取某列的 show column stats 结果
    def showRow = { String t,String c -> def r=sql("show column stats ${t}(${c})"); assertEquals(1,r.size()); r[0] }

    // run: 统一测试执行函数
    //   id: Case 编号（如 "A01"）
    //   q: 待测 SQL
    //   chg: 预期 Plan 是否变化（true=正例/prune 生效，false=负例/不 prune）
    //   sel: 预期被选为 shuffle key 的列名（null 则不检查）
    //   exc: 预期被排除的列名列表
    //   single: 是否预期裁剪到单列
    def run = { String id,String q,boolean chg,String sel,List<String> exc,boolean single ->
        sql "set agg_phase=0;"; sql "set disable_nereids_rules='';"
        // 先关闭 prune 获取原始 plan
        sql "set enable_agg_shuffle_key_prune=false;"
        def eOff=(sql "explain physical plan "+q).toString(); def oS=extractSigns(eOff)
        // 再开启 prune 获取优化后 plan
        sql "set enable_agg_shuffle_key_prune=true;"
        def eOn=(sql "explain physical plan "+q).toString(); def nS=extractSigns(eOn); def si=extractSingle(eOn)
        // 验证 Plan 变化/不变化
        if(chg){assertTrue(oS.toString()!=nS.toString(),"${id}: signs should change"); if(single)assertTrue(si.size()>0,"${id}: expect single")}
        else{assertTrue(oS.toString()==nS.toString(),"${id}: should stay unchanged")}
        // 验证被选中的 shuffle key 是否正确
        if(sel!=null){def eid=exprId(eOn,sel); assertTrue(eid!=null&&si.contains(eid),"${id}: ${sel} not selected")}
        // 验证被排除的列确实不在 shuffle key 中
        exc.each{c->def eid=exprId(eOn,c); if(eid!=null)assertTrue(!si.contains(eid),"${id}: ${c} excluded")}
        // 验证 OFF/ON 的查询结果完全一致（功能正确性）
        sql "set enable_agg_shuffle_key_prune=false;"; def rOff=sql q
        sql "set enable_agg_shuffle_key_prune=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
        logger.info("${id}: oS=${oS}, nS=${nS}, si=${si}")
    }

    // ===== 建表 =====
    // t_01_simple: 简单表，用于 A01/S01/S02
    //   hot_key: 会被注入 skew 的列
    //   good_key: 无热点的“好”列，预期被选为 shuffle key
    sql "drop table if exists t_01_simple;"
    sql """create table t_01_simple (x bigint, hot_key int, good_key bigint, v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_01_simple select number*100000+1, if(number<600,1,cast(number%2000 as int)+2),
        (number%6000)*100000+7, number from numbers("number"="2000");"""

    // t_01_boundary: 边界条件表，用于 B01-B04
    //   hot_below_key:    hot_ratio=4.9% (刚好低于 5% 阈值)
    //   hot_boundary_key: hot_ratio=5.0% (等于 5% 阈值)
    //   hot_above_key:    hot_ratio=5.1% (刚好超过 5% 阈值)
    //   null10_key:       null 占比=10% (超过 5% 阈值)
    //   maxmix_key:       null占比 10% + hot_ratio 4.9%, 取 max=10% (超过 5% 阈值)
    //   bi_good:          bigint 类型的安全列
    //   vc16_good:        varchar(16) 类型的安全列
    sql "drop table if exists t_01_boundary;"
    sql """create table t_01_boundary (x bigint, hot_below_key int, hot_boundary_key int, hot_above_key int,
        null10_key int, maxmix_key int, bi_good bigint, vc16_good varchar(16), v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_01_boundary select number*100000+1,
        cast(number%2000 as int)+2, cast(number%2000 as int)+2, cast(number%2000 as int)+2,
        cast(number%2000 as int)+2, cast(number%2000 as int)+2,
        (number%6000)*100000+7, concat('vc_',lpad(cast(number%7000 as string),4,'0')),
        number from numbers("number"="2000");"""

    // ===== A01: hot_key 30% skew, good_key 无热点 → good_key 被选中 =====
    // hot_key: NDV=7000, hot_values='1 :0.30' → 值 1 占比 30%, isBalanced()=false → 被跳过
    // good_key: NDV=9000, hot_values='' → 无热点, isBalanced()=true → 被选中
    ss("t_01_simple","x","60000","0","1600000","1","199900001","")
    ss("t_01_simple","hot_key","7000","0","400000","1","7000","1 :0.30")
    ss("t_01_simple","good_key","9000","0","800000","7","599900007","")
    ss("t_01_simple","v","60000","0","1600000","0","1999","")

    run("A01","""select count(hot_key),count(good_key),count(sum_v) from (
        select hot_key,good_key,sum(v) as sum_v from t_01_simple group by hot_key,good_key) t""",
        true,"good_key",["hot_key"],true)

    // ===== S01: hot_values='' → 表示"已采样但无热点" → prune 生效 =====
    // hot_key: hot='1 :0.10' → 刚好达到阈值 → skew → 被跳过
    // good_key: hot='' → 已采样无热点 → balanced → 被选中
    ss("t_01_simple","hot_key","7000","0","400000","1","7000","1 :0.10")
    ss("t_01_simple","good_key","9000","0","400000","1","9000","")
    run("S01","""select count(hot_key),count(good_key),count(sum_v) from (
        select hot_key,good_key,sum(v) as sum_v from t_01_simple group by hot_key,good_key) t""",
        true,"good_key",["hot_key"],true)

    // ===== S02: hot_values=null → 表示"未采样" → 保守回退不 prune =====
    // good_key 的 hot_values 传 null(不注入 hot_values 字段) → isBalanced() 返回 false
    // 导致 optimizer 无法确认 good_key 是否安全 → 不优化
    ss("t_01_simple","hot_key","7000","0","400000","1","7000","1 :0.10")
    ss("t_01_simple","good_key","9000","0","400000","1","9000",null)
    run("S02","""select count(hot_key),count(good_key),count(sum_v) from (
        select hot_key,good_key,sum(v) as sum_v from t_01_simple group by hot_key,good_key) t""",
        false,null,[],false)

    // ===== FC1: full analyze 产生的统计信息 hot_values=null → 不 prune =====
    // 其体 analyze 不会生成 hot_values，因此 show column stats 中 hot_values 字段为 null
    // 这就是 "S02 场景的真实复现"：full analyze 后的表无法被 prune
    sql "drop table if exists t_01_full;"
    sql """create table t_01_full (x bigint, skew_key int, good_key int, v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_01_full select number*100000+1, if(number<200,1,cast(number%2000 as int)+2),
        cast(number%9000 as int)+1, number from numbers("number"="2000");"""
    sql "analyze table t_01_full with sync;"
    // 验证: full analyze 后 hot_values 确实为 null
    def fStats = showRow("t_01_full","skew_key")
    assertEquals("null", fStats[17].toString())
    run("FC1","""select count(skew_key),count(good_key),count(sum_v) from (
        select skew_key,good_key,sum(v) as sum_v from t_01_full group by skew_key,good_key) t""",
        false,null,[],false)

    // ===== B 组：hot_values 边界值测试 =====
    // 倾斜判断：skew_ratio = max(null_ratio, max_hot_ratio)
    // skew_ratio >= 5% 即视为倾斜列
    def nullTen = "48000" // 480000*0.10 = 48000 → null 占比=10%
    ss("t_01_boundary","x","60000","0","1600000","1","199900001","")
    ss("t_01_boundary","v","60000","0","1600000","0","1999","")
    // hot_below_key:    hot_ratio=4.9% < 5.0% 阈值 → balanced=true
    ss("t_01_boundary","hot_below_key","7000","0","420000","1","7000","1 :0.049")
    // hot_boundary_key: hot_ratio=5.0% = 5.0% 阈值 → balanced=false (等于也不通过)
    ss("t_01_boundary","hot_boundary_key","7000","0","420000","1","7000","1 :0.05")
    // hot_above_key:    hot_ratio=5.1% > 5.0% 阈值 → balanced=false
    ss("t_01_boundary","hot_above_key","7000","0","420000","1","7000","1 :0.051")
    // null10_key:       null数=48000, 占比 10% → skew
    ss("t_01_boundary","null10_key","7000",nullTen,"420000","1","7000","")
    // maxmix_key:       null 10% + hot 4.9% → max(10%, 4.9%) = 10% → skew
    ss("t_01_boundary","maxmix_key","7000",nullTen,"420000","1","7000","1 :0.049")
    // bi_good:          bigint, 无热点, NDV=9200 > 阈值 → 安全列
    ss("t_01_boundary","bi_good","9200","0","840000","1","9200","")
    // vc16_good:        varchar(16), 无热点 → 安全列（但优先级低于 bigint）
    ss("t_01_boundary","vc16_good","8100","0","1680000","vc_0000","vc_6999","")

    // B01: hot_below(4.9%) balanced + hot_above(5.1%) skew → 选 hot_below_key
    //   hot_below_key balanced=true, hot_above_key balanced=false
    //   bigint 优先级: hot_below_key(int) < bi_good(bigint)，但 int 也是 balanced 且被先遇到
    run("B01","""select hot_below_key,hot_above_key,vc16_good,sum(v) from t_01_boundary
        group by hot_below_key,hot_above_key,vc16_good""",true,"hot_below_key",["hot_above_key"],true)

    // B02: hot_boundary(5.0%) skew → 跳过，选 bi_good
    //   5.0% 等于阈值，isBalanced() 返回 false
    run("B02","""select hot_boundary_key,bi_good,vc16_good,sum(v) from t_01_boundary
        group by hot_boundary_key,bi_good,vc16_good""",true,"bi_good",["hot_boundary_key"],true)

    // B03: null 占比 10% → 视为 skew → 跳过，选 bi_good
    //   null_ratio = 48000/480000 = 10% >= 5% → skew
    run("B03","""select null10_key,bi_good,vc16_good,sum(v) from t_01_boundary
        group by null10_key,bi_good,vc16_good""",true,"bi_good",["null10_key"],true)

    // B04: max(null 10%, hot 4.9%) = 10% → 综合判断为 skew → 跳过，选 bi_good
    //   即使 hot_ratio 未超过阈值，null_ratio 已经超过 → skew
    run("B04","""select maxmix_key,bi_good,vc16_good,sum(v) from t_01_boundary
        group by maxmix_key,bi_good,vc16_good""",true,"bi_good",["maxmix_key"],true)

}
