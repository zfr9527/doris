// ===========================================================================================
// 分类 08：Distinct Partition 与 Mixed Join — DA01-DA04, M01-M03
// ===========================================================================================
//
// 【测试目的】
//   验证两类特殊场景：
//   A. COUNT(DISTINCT) 场景：DISTINCT 产生额外的 partition 算子，prune 需要同时
//      优化 partition 和 agg 两层的 shuffle key
//   B. Mixed Join：Agg 的 hot_values 与 Join probe 端的 hot_values=null 混合时
//      的 prune 行为
//
// 【DA 组 Case 列表】
//   DA01: 高 NDV + COUNT(DISTINCT g) + agg_phase=2 → prune 生效（正例）
//   DA02: 低 NDV + COUNT(DISTINCT g) → NDV 不足 → 不 prune（负例）
//   DA03: COUNT(DISTINCT g) + AVG(v) 混合聚合 → prune 仍生效（正例）
//   DA04: agg_phase=4 (2+2 模式) → prune 仍生效（正例）
//
// 【M 组 Case 列表】
//   M01: t_08_m_agg (hot_values='') JOIN t_08_m_probe (hot_values=null)
//        join on 6列 = group by 6列 → Plan 变化（正例）
//   M02: join on 3列 < group by 6列 → subset 优化 → Plan 变化（正例）
//   M03: probe 端用 hot_values=null 的表做 agg → 保守 → Plan 不变（负例）
//
// 【验证方法】
//   DA 组和 M 组均使用硬断言。
//   所有 Case 都验证 OFF/ON 结果一致。
// ===========================================================================================
suite("skp_08_distinct_mixed", "agg_shuffle_prune_func") {
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
    def lowNdv = "4"
    def daQn = "60000"
    def esc = { Object v -> v==null?"null":v.toString().replace("\\","\\\\").replace("'","\\'") }
    def ss = { String t,String c,String n,String nn,String ds,String mi,String ma,String hv ->
        def hvc = hv==null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql "alter table ${t} modify column ${c} set stats ('row_count'='${R}','ndv'='${esc(n)}','num_nulls'='${esc(nn)}','data_size'='${esc(ds)}','min_value'='${esc(mi)}','max_value'='${esc(ma)}'${hvc});"
    }
    def extractSigns = { String e ->
        def s=[]; def m=(e=~/orderedShuffledColumns=\[([0-9,\s-]*)\]/); while(m.find()){s<<"["+m.group(1).replaceAll("\\s+","")+"]"}; s
    }
    def norm = { rows -> rows.collect{r->r.collect{v->v==null?"NULL":v.toString()}.join("||")}.sort() }

    // DA 组测试函数
    def runDA = { String id,String q,boolean expChg,int ap ->
        sql "set agg_phase=${ap};"; sql "set disable_nereids_rules='';"
        sql "set enable_agg_shuffle_key_prune=false;"
        def eOff=(sql "explain physical plan "+q).toString(); def oS=extractSigns(eOff)
        sql "set enable_agg_shuffle_key_prune=true;"
        def eOn=(sql "explain physical plan "+q).toString(); def nS=extractSigns(eOn)
        def chg=(eOff!=eOn)
        logger.info("${id}: oS=${oS}, nS=${nS}, changed=${chg}")
        if(expChg){assertTrue(chg,"${id}: signs should change")}
        else{assertTrue(oS.toString()==nS.toString(),"${id}: should stay unchanged")}
        sql "set enable_agg_shuffle_key_prune=false;"; def rOff=sql q
        sql "set enable_agg_shuffle_key_prune=true;"; def rOn=sql q
        assertTrue(norm(rOff)==norm(rOn),"${id}: results differ")
    }

    // ===== DA 组建表 =====
    sql "drop table if exists t_08_da;"
    sql """create table t_08_da (id bigint,a bigint,b bigint,c bigint,d bigint,e bigint,f bigint,g bigint,v bigint)
        duplicate key(id) distributed by hash(id) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_08_da select number,number%5000,number%4800,number%4600,
        number%4400,number%4200,number%4000,number%3800,number from numbers("number"="2000");"""

    def daBase = { String ndv ->
        ss("t_08_da","id","60000","0","1600000","0","1999","")
        ["a","b","c","d","e","f","g","v"].each{c-> ss("t_08_da",c,ndv,"0","1600000","0","9999","")}
    }

    // DA01: 高NDV → prune
    daBase(daQn)
    runDA("DA01","select a,b,c,d,e,f,count(distinct g) from t_08_da group by a,b,c,d,e,f",true,2)

    // DA02: 低NDV → 不prune
    daBase(lowNdv)
    runDA("DA02","select a,b,c,d,e,f,count(distinct g) from t_08_da group by a,b,c,d,e,f",false,2)

    // DA03: distinct + avg 混合 → prune
    daBase(daQn)
    runDA("DA03","select a,b,c,d,e,f,count(distinct g),avg(v) from t_08_da group by a,b,c,d,e,f",true,2)

    // DA04: agg_phase=4 (2+2)
    daBase(daQn)
    runDA("DA04","select a,b,c,d,e,f,count(distinct g) from t_08_da group by a,b,c,d,e,f",true,4)

    // ===== M 组建表 =====
    sql "drop table if exists t_08_m_agg;"
    sql "drop table if exists t_08_m_probe;"
    sql """create table t_08_m_agg (x bigint,b bigint,c bigint,d bigint,e bigint,f bigint,g bigint,v bigint)
        duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """create table t_08_m_probe (b bigint,c bigint,d bigint,e bigint,f bigint,g bigint,v bigint)
        duplicate key(b) distributed by hash(c) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_08_m_agg select number,number%50,number%800,number%700,number%600,number%500,number%400,number from numbers("number"="1000");"""
    sql "insert into t_08_m_probe select b,c,d,e,f,g,v from t_08_m_agg;"

    // t_08_m_agg: sample 语义 → hot_values=''
    ss("t_08_m_agg","x","1000","0","800000","0","999","")
    ss("t_08_m_agg","b","50","0","800000","0","49","")
    ss("t_08_m_agg","c","10000","0","800000","0","799","")
    ss("t_08_m_agg","d","10000","0","800000","0","699","")
    ss("t_08_m_agg","e","10000","0","800000","0","599","")
    ss("t_08_m_agg","f","500","0","800000","0","499","")
    ss("t_08_m_agg","g","400","0","800000","0","399","")
    ss("t_08_m_agg","v","1000","0","800000","0","999","")
    // t_08_m_probe: full 语义 → hot_values=null（保守）
    ss("t_08_m_probe","b","50","0","800000","0","49","")
    ss("t_08_m_probe","c","800","0","800000","0","799","")
    ss("t_08_m_probe","d","700","0","800000","0","699","")
    ss("t_08_m_probe","e","600","0","800000","0","599","")
    ss("t_08_m_probe","f","500","0","800000","0","499","")
    ss("t_08_m_probe","g","400","0","800000","0","399","")
    ss("t_08_m_probe","v","1000","0","800000","0","999","")

    // M01: join on 6 = gby 6 → plan 变化
    sql "set enable_agg_shuffle_key_prune=false;"
    def offM01=(sql "explain physical plan select s.b,s.c,s.v,a.sv from t_08_m_probe s join (select b,c,d,e,f,g,sum(v) as sv from t_08_m_agg group by b,c,d,e,f,g) a on s.b=a.b and s.c=a.c and s.d=a.d and s.e=a.e and s.f=a.f and s.g=a.g").toString()

    // explain physical plan 
    // select s.b,s.c,s.v,a.sv from t_08_m_probe s 
    // join (
    //     select b,c,d,e,f,g,sum(v) as sv 
    //     from t_08_m_agg 
    //     group by b,c,d,e,f,g) a 
    // on s.b=a.b and s.c=a.c and s.d=a.d and s.e=a.e and s.f=a.f and s.g=a.g

    sql "set enable_agg_shuffle_key_prune=true;"
    def onM01=(sql "explain physical plan select s.b,s.c,s.v,a.sv from t_08_m_probe s join (select b,c,d,e,f,g,sum(v) as sv from t_08_m_agg group by b,c,d,e,f,g) a on s.b=a.b and s.c=a.c and s.d=a.d and s.e=a.e and s.f=a.f and s.g=a.g").toString()
    logger.info("M01 changed: ${offM01!=onM01}")
    assertTrue(offM01!=onM01,"M01: plans should differ")

    // M02: join on 3 < gby 6 → subset
    sql "set enable_agg_shuffle_key_prune=false;"
    def offM02=(sql "explain physical plan select s.c,s.d,s.v,a.sv from t_08_m_probe s join (select b,c,d,e,f,g,sum(v) as sv from t_08_m_agg group by b,c,d,e,f,g) a on s.c=a.c and s.d=a.d and s.e=a.e").toString()
    sql "set enable_agg_shuffle_key_prune=true;"
    def onM02=(sql "explain physical plan select s.c,s.d,s.v,a.sv from t_08_m_probe s join (select b,c,d,e,f,g,sum(v) as sv from t_08_m_agg group by b,c,d,e,f,g) a on s.c=a.c and s.d=a.d and s.e=a.e").toString()
    logger.info("M02 changed: ${offM02!=onM02}")
    assertTrue(offM02!=onM02,"M02: plans should differ")

    // M03: probe 用 agg 表(hot_values=null) → 保守
    sql "set enable_agg_shuffle_key_prune=false;"
    def offM03=(sql "explain physical plan select s.c,s.d,s.v,a.sv from t_08_m_agg s join (select b,c,d,e,f,g,sum(v) as sv from t_08_m_probe group by b,c,d,e,f,g) a on s.c=a.c and s.d=a.d and s.e=a.e").toString()
    sql "set enable_agg_shuffle_key_prune=true;"
    def onM03=(sql "explain physical plan select s.c,s.d,s.v,a.sv from t_08_m_agg s join (select b,c,d,e,f,g,sum(v) as sv from t_08_m_probe group by b,c,d,e,f,g) a on s.c=a.c and s.d=a.d and s.e=a.e").toString()
    logger.info("M03 changed: ${offM03!=onM03}")
    assertTrue(offM03==onM03,"M03: plans should be same (hot_values=null)")

    sql "set agg_phase=0;"
    sql "set enable_agg_shuffle_key_prune=true;"
}
