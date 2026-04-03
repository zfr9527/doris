// ===========================================================================================
// 分类 02：NDV 门槛 — C01-C03
// ===========================================================================================
//
// 【测试目的】
//   验证 Shuffle Key Pruning 的 NDV（Number of Distinct Values）门槛逻辑：
//   只有 NDV 足够大的列才能被选为 shuffle key，否则数据分布太集中会导致 hash 倾斜。
//
// 【NDV 阈值计算公式】
//   ndvThreshold = totalPipelineTaskNum * 512
//   其中 totalPipelineTaskNum = parallel_pipeline_task_num (这里固定为 8)
//   因此 ndvThreshold = 8 * 512 = 4096
//
// 【Case 列表】
//   C01: NDV = threshold - 1（刚好低于阈值） → 列被跳过，选 bi_good（负例）
//   C02: NDV = threshold（等于阈值）         → 列被跳过，选 bi_good（负例，等于也不通过）
//   C03: NDV > threshold（高于阈值）          → 列可被选中（正例）
//
// 【验证方法】
//   注入不同的 NDV 值到待测列，检查 orderedShuffledColumns 是否变化，
//   以及被跳过列和被选中列是否符合预期。
// ===========================================================================================
suite("skp_02_ndv_threshold", "agg_shuffle_prune_func") {
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

    // NDV 阈值 = pipeline_task_num * 512 = 4096
    // 只有 NDV > ndvThreshold 的列才允许被选为 shuffle key
    def ndvThreshold = tptn * 512
    def R = "480000"
    def esc = { Object v -> v==null?"null":v.toString().replace("\\","\\\\").replace("'","\\'") }
    def ss = { String t,String c,String n,String nn,String ds,String mi,String ma,String hv ->
        def hvc = hv==null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql "alter table ${t} modify column ${c} set stats ('row_count'='${R}','ndv'='${esc(n)}','num_nulls'='${esc(nn)}','data_size'='${esc(ds)}','min_value'='${esc(mi)}','max_value'='${esc(ma)}'${hvc});"
    }
    def extractSigns = { String e ->
        def s=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/); while(m.find()){s<<"["+m.group(1).replaceAll("\\s+","")+"]"}; s
    }
    def extractSingle = { String e ->
        def ids=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while(m.find()){def r=m.group(1).trim(); if(r.length()==0)continue; def p=r.split(",").collect{it.trim()}.findAll{it.length()>0}; if(p.size()==1)ids<<Integer.parseInt(p[0])}; ids
    }
    def exprId = { String e,String c -> def m=(e=~("\\b"+java.util.regex.Pattern.quote(c)+"#(\\d+)\\b")); m.find()?Integer.parseInt(m.group(1)):null }
    def norm = { rows -> rows.collect{r->r.collect{v->v==null?"NULL":v.toString()}.join("||")}.sort() }
    def run = { String id,String q,boolean chg,String sel,List<String> exc,boolean single ->
        sql "set agg_phase=0;"; sql "set disable_nereids_rules='';"
        sql "set enable_agg_shuffle_key_prune=false;"
        def eOff=(sql "explain physical plan "+q).toString(); def oS=extractSigns(eOff)
        sql "set enable_agg_shuffle_key_prune=true;"
        def eOn=(sql "explain physical plan "+q).toString(); def nS=extractSigns(eOn); def si=extractSingle(eOn)
        if(chg){assertTrue(oS.toString()!=nS.toString(),"${id}: signs should change"); if(single)assertTrue(si.size()>0,"${id}: expect single")}
        else{assertTrue(oS.toString()==nS.toString(),"${id}: should stay unchanged")}
        if(sel!=null){def eid=exprId(eOn,sel); assertTrue(eid!=null&&si.contains(eid),"${id}: ${sel} not selected")}
        exc.each{c->def eid=exprId(eOn,c); if(eid!=null)assertTrue(!si.contains(eid),"${id}: ${c} excluded")}
        sql "set enable_agg_shuffle_key_prune=false;"; def rOff=sql q
        sql "set enable_agg_shuffle_key_prune=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
        logger.info("${id}: oS=${oS}, nS=${nS}, si=${si}")
    }

    // ===== 建表 =====
    // 表包含 3 种 NDV 级别的列：below / eq / above threshold，以及一个稳定 balanced 的 bi_good 列
    sql "drop table if exists t_02_ndv;"
    sql """create table t_02_ndv (x bigint, ndv_below_key int, ndv_eq_key int, ndv_above_key int,
        ratio8_key int, bi_good bigint, v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_02_ndv select number*100000+1,
        cast(number%5000 as int)+1, cast(number%5000 as int)+1, cast(number%6000 as int)+1,
        cast(number%9000 as int)+1, (number%6000)*100000+7,
        number from numbers("number"="2000");"""

    // ndvBelow = 4095 (刚好低于 4096 阈值)
    def ndvBelow = Math.max(1, ndvThreshold - 1)
    // ndvAbove = 4096 + 1024 = 5120 (明显超过阈值)
    def ndvAbove = ndvThreshold + tptn * 128

    ss("t_02_ndv","x","60000","0","1600000","1","199900001","")
    ss("t_02_ndv","v","60000","0","1600000","0","1999","")
    ss("t_02_ndv","ndv_below_key","${ndvBelow}","0","420000","1","${ndvBelow}","")
    ss("t_02_ndv","ndv_eq_key","${ndvThreshold}","0","420000","1","${ndvThreshold}","")
    ss("t_02_ndv","ndv_above_key","${ndvAbove}","0","420000","1","${ndvAbove}","")
    ss("t_02_ndv","ratio8_key","3000","0","420000","1","3000","")
    ss("t_02_ndv","bi_good","9200","0","840000","1","9200","")

    // C01: ndv_below_key 的 NDV=4095 < 阈值 4096 → 不满足 NDV 门槛 → 被跳过
    //   bi_good 的 NDV=9200 > 阈值 → 满足条件 → 被选为 shuffle key
    //   预期: Plan 变化, bi_good 被选中, ndv_below_key 被排除
    run("C01","select ndv_below_key,bi_good,sum(v) from t_02_ndv group by ndv_below_key,bi_good",
        true,"bi_good",["ndv_below_key"],true)

    // C02: ndv_eq_key 的 NDV=4096 = 阈值 → 不满足（等于也不通过，必须严格大于）
    //   预期: Plan 变化, bi_good 被选中, ndv_eq_key 被排除
    run("C02","select ndv_eq_key,bi_good,sum(v) from t_02_ndv group by ndv_eq_key,bi_good",
        true,"bi_good",["ndv_eq_key"],true)

    // C03: ndv_above_key 的 NDV=5120 > 阈值 → 满足 NDV 门槛
    //   ratio8_key 的 NDV=3000 < 阈值 4096 → NDV 不足被跳过
    //   预期: Plan 变化, ndv_above_key 被选中, ratio8_key 被排除
    run("C03","select ndv_above_key,ratio8_key,sum(v) from t_02_ndv group by ndv_above_key,ratio8_key",
        true,"ndv_above_key",["ratio8_key"],true)

}
