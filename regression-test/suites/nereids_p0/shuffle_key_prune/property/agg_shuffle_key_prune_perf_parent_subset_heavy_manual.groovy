// ===========================================================================================
// 性能实验 D：Parent Subset — parent 下推后的性能影响
// ===========================================================================================
// 核心问题：join parent 下推缩窄 key 后，是优化还是分布变差？
//
// 设计：
//   - fact 表 1 亿行（group by 6 列）
//   - probe 表做 join，join key 是 fact group-by key 的子集
//   - parent request 下推使 agg 侧 shuffle key 缩窄
//   - both-side agg join 场景
//
// Case：
//   PS01: probe join on a,b,c → 3 列 subset（NDV 高）→ 稳定优化
//   PS02: probe join on a → 1 列 subset（NDV 高）→ 继续优化
//   PS03: probe join on a → 1 列 subset（NDV≈阈值）→ 劣化观察
//   PS04: both-side agg join on a,b → 两侧 agg 都受益
// ===========================================================================================
suite("agg_shuffle_key_prune_perf_parent_subset_heavy_manual") {

    sql """set global enable_auto_analyze=false;"""

    sql """set enable_nereids_planner=true;"""
    sql """set enable_sql_cache=false;"""
    sql """set enable_query_cache=false;"""
    sql """set parallel_fragment_exec_instance_num=8;"""
    sql """set parallel_pipeline_task_num=8;"""
    // sql """set agg_phase=2;"""
    sql """set disable_join_reorder=true;"""
    sql """set runtime_filter_mode=OFF;"""

    def dbName = context.config.getDbNameByFile(context.file)
    sql """create database if not exists ${dbName};"""
    sql """use ${dbName};"""

    def rowCount = 100000000L
    def repeatTimes = 5
    def warmupTimes = 1

    def backendCount = Math.max(1, sql("""show backends""").size())
    def totalInstanceNum = (int) (8 * backendCount)
    def ndvThreshold = totalInstanceNum * 512

    // ---- 建表 ----
    sql """drop table if exists t_perf_fact;"""
    sql """drop table if exists t_perf_probe;"""
    sql """drop table if exists t_perf_fact2;"""

    sql """create table t_perf_fact (
            id bigint, a bigint, b bigint, c bigint,
            d bigint, e bigint, f bigint, v bigint
        ) duplicate key(id)
        distributed by hash(id) buckets 32
        properties ("replication_num" = "1");"""

    sql """create table t_perf_probe (
            a bigint, b bigint, c bigint, rv bigint
        ) duplicate key(a)
        distributed by hash(rv) buckets 32
        properties ("replication_num" = "1");"""

    sql """create table t_perf_fact2 (
            id bigint, a bigint, b bigint, c bigint,
            d bigint, e bigint, f bigint, v bigint
        ) duplicate key(id)
        distributed by hash(id) buckets 32
        properties ("replication_num" = "1");"""

    // ---- 造数据 ----
    sql """insert into t_perf_fact
        select number,
            number % 5000000, number % 4800000, number % 4600000,
            number % 4400000, number % 4200000, number % 4000000,
            number
        from numbers("number" = "${rowCount}");"""

    sql """insert into t_perf_probe
        select number % 5000000, number % 4800000, number % 4600000, number
        from numbers("number" = "${rowCount}");"""

    sql """insert into t_perf_fact2 select * from t_perf_fact;"""

    // ---- 手动注入统计信息 ----
    def statsRowCount = "100000000"
    def esc = { Object v -> v==null?"null":v.toString().replace("\\","\\\\").replace("'","\\'") }
    def ss = { String tbl, String col, String ndv, String numNulls, String dataSize, String minVal, String maxVal, String hv ->
        def hvClause = hv == null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql """alter table ${tbl} modify column ${col} set stats (
            'row_count'='${statsRowCount}', 'ndv'='${esc(ndv)}', 'num_nulls'='${esc(numNulls)}',
            'data_size'='${esc(dataSize)}', 'min_value'='${esc(minVal)}', 'max_value'='${esc(maxVal)}'${hvClause});"""
    }

    // fact 表: 所有列高 NDV 无 skew
    ["t_perf_fact", "t_perf_fact2"].each { tbl ->
        ss(tbl, "id", "100000000", "0", "800000000", "0", "99999999", "")
        ss(tbl, "a",  "5000000",   "0", "800000000", "0", "4999999",  "")
        ss(tbl, "b",  "4800000",   "0", "800000000", "0", "4799999",  "")
        ss(tbl, "c",  "4600000",   "0", "800000000", "0", "4599999",  "")
        ss(tbl, "d",  "4400000",   "0", "800000000", "0", "4399999",  "")
        ss(tbl, "e",  "4200000",   "0", "800000000", "0", "4199999",  "")
        ss(tbl, "f",  "4000000",   "0", "800000000", "0", "3999999",  "")
        ss(tbl, "v",  "100000000", "0", "800000000", "0", "99999999", "")
    }
    // probe 表
    ss("t_perf_probe", "a",  "5000000", "0", "800000000", "0", "4999999", "")
    ss("t_perf_probe", "b",  "4800000", "0", "800000000", "0", "4799999", "")
    ss("t_perf_probe", "c",  "4600000", "0", "800000000", "0", "4599999", "")
    ss("t_perf_probe", "rv", "100000000", "0", "800000000", "0", "99999999", "")

    // ---- helper ----
    def normalizeRows = { def rows ->
        rows.collect { row -> row.collect { v -> v == null ? "NULL" : v.toString() }.join("||") }.sort()
    }

    def runPerfCase = { String id, String sqlStr ->
        sql """set enable_agg_shuffle_key_prune=false;"""
        def explainOff = (sql "explain physical plan " + sqlStr).toString()
        sql """set enable_agg_shuffle_key_prune=true;"""
        def explainOn = (sql "explain physical plan " + sqlStr).toString()
        logger.info("${id}: planChanged=${explainOff != explainOn}")

        for (int i = 0; i < warmupTimes; i++) {
            sql """set enable_agg_shuffle_key_prune=false;"""
            sql sqlStr
            sql """set enable_agg_shuffle_key_prune=true;"""
            sql sqlStr
        }

        def offTimes = []
        def onTimes = []
        for (int i = 0; i < repeatTimes; i++) {
            sql """set enable_agg_shuffle_key_prune=false;"""
            def t0 = System.currentTimeMillis()
            sql sqlStr
            offTimes << (System.currentTimeMillis() - t0)

            sql """set enable_agg_shuffle_key_prune=true;"""
            t0 = System.currentTimeMillis()
            sql sqlStr
            onTimes << (System.currentTimeMillis() - t0)
        }

        offTimes.sort()
        onTimes.sort()
        def offMedian = offTimes[(int) (repeatTimes / 2)]
        def onMedian = onTimes[(int) (repeatTimes / 2)]
        def speedup = offMedian > 0 ? String.format("%.2f", offMedian / (double) onMedian) : "N/A"

        logger.info("${id}: offTimes=${offTimes}, onTimes=${onTimes}")
        logger.info("${id}: offMedian=${offMedian}ms, onMedian=${onMedian}ms, speedup=${speedup}x")

        sql """set enable_agg_shuffle_key_prune=false;"""
        def resultOff = sql sqlStr
        sql """set enable_agg_shuffle_key_prune=true;"""
        def resultOn = sql sqlStr
        assertTrue(normalizeRows(resultOff) == normalizeRows(resultOn),
                "${id}: OFF/ON results must be identical")
    }

    // ====================================================
    // PS01: probe join on a,b,c → 3 列 subset（NDV 高）
    // ====================================================
    runPerfCase("PS01", """
        select count(*), max(sum_v) from (
            select s.a, s.b, s.rv, a.sum_v
            from t_perf_probe s
            join [shuffle] (
                select a, b, c, d, e, f, sum(v) as sum_v
                from t_perf_fact
                group by a, b, c, d, e, f
            ) a on s.a = a.a and s.b = a.b and s.c = a.c
        ) t
    """)

    // ====================================================
    // PS02: probe join on a → 1 列 subset（NDV 高）
    // ====================================================
    runPerfCase("PS02", """
        select count(*), max(sum_v) from (
            select s.a, s.rv, a.sum_v
            from t_perf_probe s
            join [shuffle] (
                select a, b, c, d, e, f, sum(v) as sum_v
                from t_perf_fact
                group by a, b, c, d, e, f
            ) a on s.a = a.a
        ) t
    """)

    // ====================================================
    // PS03: probe join on a → 1 列 subset（NDV≈阈值）→ 劣化观察
    // ====================================================
    def borderNdv = ndvThreshold + 100
    ss("t_perf_fact",  "a", borderNdv.toString(), "0", "800000000", "0", "4999999", "1 :0.049")
    ss("t_perf_probe", "a", borderNdv.toString(), "0", "800000000", "0", "4999999", "1 :0.049")

    runPerfCase("PS03", """
        select count(*), max(sum_v) from (
            select s.a, s.rv, a.sum_v
            from t_perf_probe s
            join [shuffle] (
                select a, b, c, d, e, f, sum(v) as sum_v
                from t_perf_fact
                group by a, b, c, d, e, f
            ) a on s.a = a.a
        ) t
    """)

    // ====================================================
    // PS04: both-side agg join on a,b → 两侧 agg 都受益
    // ====================================================
    ss("t_perf_fact",  "a", "5000000", "0", "800000000", "0", "4999999", "")
    ss("t_perf_fact2", "a", "5000000", "0", "800000000", "0", "4999999", "")

    runPerfCase("PS04", """
        select count(*), max(lv + rv) from (
            select l.a, l.b, l.sum_v as lv, r.sum_v as rv
            from (
                select a, b, c, d, e, f, sum(v) as sum_v
                from t_perf_fact
                group by a, b, c, d, e, f
            ) l
            join [shuffle] (
                select a, b, c, d, e, f, sum(v) as sum_v
                from t_perf_fact2
                group by a, b, c, d, e, f
            ) r on l.a = r.a and l.b = r.b
        ) t
    """)

    sql """set enable_agg_shuffle_key_prune=true;"""
    sql """set disable_join_reorder=false;"""
    sql """set runtime_filter_mode=GLOBAL;"""
}
