// ===========================================================================================
// 性能实验 B：Valid Margin — 安全裕量对性能的影响
// ===========================================================================================
// 核心问题：同样合法 prune，"明显安全" vs "勉强安全" 的 key 收益差多少？
//
// 设计：
//   - 固定 SQL 形态（group by 6 列，裁到 1 列）
//   - 只改变 good_key 的 stats：hot ratio 和 NDV
//   - k2~k6 注入 hot_values 使其 skew → 确保 good_key 被选中
//
// Case：
//   VM01: hot=0, NDV=60000 → 高裕量（远超阈值）
//   VM02: hot=0.02, NDV=20000 → 中裕量
//   VM03: hot=0.049, NDV≈阈值+100 → 低裕量（勉强通过 isBalanced）
// ===========================================================================================
suite("agg_shuffle_key_prune_perf_valid_margin_heavy_manual") {

    sql """set global enable_auto_analyze=false;"""

    sql """set enable_nereids_planner=true;"""
    sql """set enable_sql_cache=false;"""
    sql """set enable_query_cache=false;"""
    sql """set parallel_fragment_exec_instance_num=8;"""
    sql """set parallel_pipeline_task_num=8;"""
    // sql """set agg_phase=2;"""

    def dbName = context.config.getDbNameByFile(context.file)
    sql """create database if not exists ${dbName};"""
    sql """use ${dbName};"""

    def rowCount = 100000000L
    def repeatTimes = 5
    def warmupTimes = 1

    def backendCount = Math.max(1, sql("""show backends""").size())
    def totalInstanceNum = (int) (8 * backendCount)
    def ndvThreshold = totalInstanceNum * 512

    logger.info("valid_margin: backendCount=${backendCount}, totalInstanceNum=${totalInstanceNum}, ndvThreshold=${ndvThreshold}")

    // ---- 建表 ----
    sql """drop table if exists t_perf_vm;"""
    sql """create table t_perf_vm (
            id bigint,
            good_key bigint,
            k2 bigint, k3 bigint, k4 bigint, k5 bigint, k6 bigint,
            v bigint
        ) duplicate key(id)
        distributed by hash(id) buckets 32
        properties ("replication_num" = "1");"""

    // ---- 造数据 ----
    sql """insert into t_perf_vm
        select number,
            number % 8000000,
            number % 7500000,
            number % 7000000,
            number % 6500000,
            number % 6000000,
            number % 5500000,
            number
        from numbers("number" = "${rowCount}");"""

    // ---- 手动注入统计信息 ----
    def statsRowCount = "100000000"
    def esc = { Object v -> v==null?"null":v.toString().replace("\\","\\\\").replace("'","\\'") }
    def ssBase = { String col, String ndv, String numNulls, String dataSize, String minVal, String maxVal, String hv ->
        def hvClause = hv == null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql """alter table t_perf_vm modify column ${col} set stats (
            'row_count'='${statsRowCount}', 'ndv'='${esc(ndv)}', 'num_nulls'='${esc(numNulls)}',
            'data_size'='${esc(dataSize)}', 'min_value'='${esc(minVal)}', 'max_value'='${esc(maxVal)}'${hvClause});"""
    }

    // 基础 stats
    ssBase("id",       "100000000", "0", "800000000", "0", "99999999", "")
    ssBase("good_key", "8000000",   "0", "800000000", "0", "7999999",  "")
    // k2~k6: 有 skew → 确保 good_key 被选中
    ssBase("k2", "7500000", "0", "800000000", "0", "7499999", "1 :0.10")
    ssBase("k3", "7000000", "0", "800000000", "0", "6999999", "1 :0.10")
    ssBase("k4", "6500000", "0", "800000000", "0", "6499999", "1 :0.10")
    ssBase("k5", "6000000", "0", "800000000", "0", "5999999", "1 :0.10")
    ssBase("k6", "5500000", "0", "800000000", "0", "5499999", "1 :0.10")
    ssBase("v",  "100000000", "0", "800000000", "0", "99999999", "")

    // ---- helper ----
    def extractSingleShuffleIds = { String explainStr ->
        def ids = []
        def matcher = (explainStr =~ /orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while (matcher.find()) {
            def raw = matcher.group(1).trim()
            if (raw.length() == 0) continue
            def parts = raw.split(",").collect { it.trim() }.findAll { it.length() > 0 }
            if (parts.size() == 1) ids << Integer.parseInt(parts[0])
        }
        return ids
    }

    def extractExprIdByColumn = { String explainStr, String colName ->
        def m = (explainStr =~ ("\\b" + java.util.regex.Pattern.quote(colName) + "#(\\d+)\\b"))
        if (m.find()) return Integer.parseInt(m.group(1))
        return null
    }

    def assertSelectedKey = { String id, String explainOn, String targetCol ->
        def singleIds = extractSingleShuffleIds(explainOn)
        def targetExprId = extractExprIdByColumn(explainOn, targetCol)
        assertTrue(targetExprId != null, "${id}: cannot find exprId for column '${targetCol}' in plan")
        assertTrue(singleIds.contains(targetExprId),
                "${id}: expected '${targetCol}' (exprId=${targetExprId}) as shuffle key, but singleIds=${singleIds}")
        logger.info("${id}: confirmed '${targetCol}' (exprId=${targetExprId}) is the selected shuffle key")
    }

    def normalizeRows = { def rows ->
        rows.collect { row -> row.collect { v -> v == null ? "NULL" : v.toString() }.join("||") }.sort()
    }

    def runPerfCase = { String id, String sqlStr ->
        sql """set enable_agg_shuffle_key_prune=false;"""
        def explainOff = (sql "explain physical plan " + sqlStr).toString()
        sql """set enable_agg_shuffle_key_prune=true;"""
        def explainOn = (sql "explain physical plan " + sqlStr).toString()
        logger.info("${id}: planChanged=${explainOff != explainOn}")

        assertSelectedKey(id, explainOn, "good_key")

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

    def baseSql = """
        select count(*), max(sum_v) from (
            select good_key, k2, k3, k4, k5, k6, sum(v) as sum_v
            from t_perf_vm
            group by good_key, k2, k3, k4, k5, k6
        ) t
    """

    // ====================================================
    // VM01: 高裕量 — hot=0, NDV=60000（远超阈值）
    // ====================================================
    ssBase("good_key", "60000", "0", "800000000", "0", "7999999", "")
    runPerfCase("VM01", baseSql)

    // ====================================================
    // VM02: 中裕量 — hot=0.02, NDV=20000
    // ====================================================
    ssBase("good_key", "20000", "0", "800000000", "0", "7999999", "1 :0.02")
    runPerfCase("VM02", baseSql)

    // ====================================================
    // VM03: 低裕量 — hot=0.049, NDV≈阈值+100（勉强通过 isBalanced）
    //   这是劣化观察点：prune 合法但选中的 key 处于边界
    // ====================================================
    def borderNdv = ndvThreshold + 100
    ssBase("good_key", borderNdv.toString(), "0", "800000000", "0", "7999999", "1 :0.049")
    runPerfCase("VM03", baseSql)

    sql """set enable_agg_shuffle_key_prune=true;"""
}
