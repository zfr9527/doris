// ===========================================================================================
// 分类 07：纯 Join Shuffle Key 优化 — JK01-JK06
// ===========================================================================================
//
// 【测试目的】
//   验证非 Agg 场景下，纯 Join 的 Shuffle Key 优化逻辑。
//   当两表做 Shuffle Hash Join 时，如果 join key 中有多列，prune 可以减少参与
//   hash 计算的列数来降低 shuffle 开销。
//
// 【注意】
//   Join Shuffle Key Prune 是 cost-based 的，但通过固定统计信息和 be_number_for_test=1
//   控制环境一致性，使用硬断言验证。
//
// 【Case 列表】
//   JK01: 左右表所有 join key 高 NDV + 无 skew → 可裁剪（正例）
//   JK02: 左右表所有 join key 都 skew → 无安全列 → 不裁剪（负例）
//   JK03: 数值列全 skew + varchar 列无 skew → string fallback 裁剪到 f 列（正例）
//   JK04: 左表缺统计信息 → 保守 → 不裁剪（负例）
//   JK05: 左表 hot_values=null → 未采样 → 保守 → 不裁剪（负例）
//   JK06: 验证开关生效 → OFF 保留完整 join shuffle key，ON 可裁剪
//
// 【验证方法】
//   固定 join reorder 和 runtime filter 关闭，确保 plan 稳定。
//   对比 OFF/ON 的 plan 变化和结果一致性。
// ===========================================================================================
suite("skp_07_join_key_prune", "agg_shuffle_prune_func") {
    sql """set enable_nereids_planner=true;"""
    sql """set enable_fallback_to_original_planner=false;"""
    sql """set enable_sql_cache=false;"""
    sql """set enable_query_cache=false;"""
    sql """set agg_phase=0;"""

    def dbName = context.config.getDbNameByFile(context.file)
    sql "create database if not exists ${dbName};"
    sql "use ${dbName};"

    def tptn = 8
    sql "set parallel_fragment_exec_instance_num=1;"
    sql "set parallel_pipeline_task_num=${tptn};"
    sql "set be_number_for_test=1;"

    def qn = (tptn*512 + tptn*64).toString()
    def R = "480000"
    def esc = { Object v -> v==null?"null":v.toString().replace("\\","\\\\").replace("'","\\'") }
    def ss = { String t,String c,String n,String nn,String ds,String mi,String ma,String hv ->
        def hvc = hv==null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql "alter table ${t} modify column ${c} set stats ('row_count'='${R}','ndv'='${esc(n)}','num_nulls'='${esc(nn)}','data_size'='${esc(ds)}','min_value'='${esc(mi)}','max_value'='${esc(ma)}'${hvc});"
    }
    def extractSigns = { String e ->
        def s=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/); while(m.find()){s<<"["+m.group(1).replaceAll("\\s+","")+"]"}; s
    }
    def extractKeyCounts = { String e ->
        def c=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while(m.find()){
            def r=m.group(1).trim()
            def p=r.length()==0 ? [] : r.split(",").collect{it.trim()}.findAll{it.length()>0}
            c<<p.size()
        }
        c
    }
    def norm = { rows -> rows.collect{r->r.collect{v->v==null?"NULL":v.toString()}.join("||")}.sort() }

    // Join key prune 测试函数
    def runJoin = { String id,String q,boolean expChg ->
        sql "set enable_agg_shuffle_key_prune=false;"
        def eOff=(sql "explain physical plan "+q).toString(); def oS=extractSigns(eOff)
        sql "set enable_agg_shuffle_key_prune=true;"
        def eOn=(sql "explain physical plan "+q).toString(); def nS=extractSigns(eOn)
        def changed=(eOff!=eOn)
        logger.info("${id}: oS=${oS}, nS=${nS}, changed=${changed}")
        if(expChg){assertTrue(changed,"${id}: signs should change")}
        else{assertTrue(oS.toString()==nS.toString(),"${id}: should stay unchanged")}
        sql "set enable_agg_shuffle_key_prune=false;"; def rOff=sql q
        sql "set enable_agg_shuffle_key_prune=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
    }

    // ===== 建表 =====
    sql "drop table if exists t_07_left;"
    sql "drop table if exists t_07_right;"
    sql "drop table if exists t_07_no_stats;"
    sql """create table t_07_left (id bigint,a bigint,b bigint,c bigint,d bigint,e bigint,f varchar(32),v bigint)
        duplicate key(id) distributed by hash(id) buckets 4 properties ("replication_num"="1");"""
    sql """create table t_07_right (id bigint,a bigint,b bigint,c bigint,d bigint,e bigint,f varchar(32),v bigint)
        duplicate key(id) distributed by hash(id) buckets 4 properties ("replication_num"="1");"""
    sql """create table t_07_no_stats (id bigint,a bigint,b bigint,c bigint,d bigint,e bigint,f varchar(32),v bigint)
        duplicate key(id) distributed by hash(id) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_07_left select number,number%5000,number%4800,number%4600,
        number%4400,number%4200,concat('s',cast(number%4000 as string)),number from numbers("number"="2000");"""
    sql "insert into t_07_right select * from t_07_left;"
    sql "insert into t_07_no_stats select * from t_07_left;"

    def baseStats = { String t ->
        ss(t,"id","60000","0","1600000","0","1999","")
        ss(t,"a","5000","0","1600000","0","4999","")
        ss(t,"b","4800","0","1600000","0","4799","")
        ss(t,"c","4600","0","1600000","0","4599","")
        ss(t,"d","4400","0","1600000","0","4399","")
        ss(t,"e","4200","0","1600000","0","4199","")
        ss(t,"f","4000","0","3200000","s0","s3999","")
        ss(t,"v","60000","0","1600000","0","1999","")
    }
    baseStats("t_07_left"); baseStats("t_07_right")

    // 固定 join 顺序和关闭 runtime filter，确保 plan 结构稳定
    sql "set disable_join_reorder=true;"
    sql "set runtime_filter_mode=OFF;"

    def jkSql = """select l.a,l.v,r.v from t_07_left l join t_07_right r
        on l.a=r.a and l.b=r.b and l.c=r.c and l.d=r.d and l.e=r.e and l.f=r.f"""

    // JK01: 高NDV无skew → 正例
    ["a","b","c","d","e","f"].each{c-> ss("t_07_left",c,qn,"0","1600000","0","9999",""); ss("t_07_right",c,qn,"0","1600000","0","9999","")}
    runJoin("JK01",jkSql,true)

    // // JK02: 全skew → 负例
    // ["a","b","c","d","e"].each{c-> ss("t_07_left",c,qn,"0","1600000","0","9999","1 :0.06"); ss("t_07_right",c,qn,"0","1600000","0","9999","1 :0.06")}
    // ss("t_07_left","f",qn,"0","3200000","s0","s3999","x :0.06"); ss("t_07_right","f",qn,"0","3200000","s0","s3999","x :0.06")
    // runJoin("JK02",jkSql,false)

    // JK03: numeric skew + varchar 不 skew → string fallback
    ["a","b","c","d","e"].each{c-> ss("t_07_left",c,qn,"0","1600000","0","9999","1 :0.06"); ss("t_07_right",c,qn,"0","1600000","0","9999","1 :0.06")}
    ss("t_07_left","f",qn,"0","3200000","s0","s3999",""); ss("t_07_right","f",qn,"0","3200000","s0","s3999","")
    runJoin("JK03",jkSql,true)

    // JK04: 缺统计 → 负例
    runJoin("JK04","select l.a,l.v,r.v from t_07_no_stats l join [shuffle] t_07_right r on l.a=r.a and l.b=r.b and l.c=r.c and l.d=r.d and l.e=r.e and l.f=r.f",false)

    // JK05: hotValues=null → 负例
    ["a","b","c","d","e"].each{c-> ss("t_07_left",c,qn,"0","1600000","0","9999",null)}
    ss("t_07_left","f",qn,"0","3200000","s0","s3999",null)
    ["a","b","c","d","e","f"].each{c-> ss("t_07_right",c,qn,"0","1600000","0","9999","")}
    runJoin("JK05",jkSql,false)

    // JK06: 开关生效验证
    // 场景：左右表所有 join key 都是高 NDV + 无 skew（与 JK01 相同的统计信息），
    //       这是最典型的"可裁剪"场景。
    //       验证 OFF→保留完整 6 列 shuffle key，ON→裁剪掉部分列。
    // 步骤：
    //   1) 重置左右表统计信息（JK05 会污染左表统计为 hot_values=null）
    //   2) 用 runJoin 验证 plan 变化 + 结果一致性
    //   3) 额外检查 OFF 时 key count=6，ON 时 key count<6

    // 步骤 1: 重置双表统计 — 高 NDV + 空 hot_values = 无 skew
    ["a","b","c","d","e"].each{c->
        ss("t_07_left", c, qn, "0", "1600000", "0", "9999", "")
        ss("t_07_right", c, qn, "0", "1600000", "0", "9999", "")
    }
    ss("t_07_left", "f", qn, "0", "3200000", "s0", "s3999", "")
    ss("t_07_right", "f", qn, "0", "3200000", "s0", "s3999", "")

    // 步骤 2: 复用 runJoin 验证 plan 变化 + 结果一致性（expChg=true，期望有变化）
    runJoin("JK06", jkSql, true)

    // 步骤 3: 额外验证 key count（OFF=6 列全保留，ON<6 列说明裁剪生效）
    sql "set enable_agg_shuffle_key_prune=false;"
    def offPlan = (sql "explain physical plan " + jkSql).toString()
    def offCounts = extractKeyCounts(offPlan)

    sql "set enable_agg_shuffle_key_prune=true;"
    def onPlan = (sql "explain physical plan " + jkSql).toString()
    def onCounts = extractKeyCounts(onPlan)

    assertTrue(offCounts.any{it == 6}, "JK06: OFF should keep all 6 join keys, got ${offCounts}")
    assertTrue(onCounts.any{it > 0 && it < 6}, "JK06: ON should prune join keys, got ${onCounts}")
    logger.info("JK06: offCounts=${offCounts}, onCounts=${onCounts}")

    sql "set enable_agg_shuffle_key_prune=true;"
    sql "set disable_join_reorder=false;"
    sql "set runtime_filter_mode=GLOBAL;"
}
