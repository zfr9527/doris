// ===========================================================================================
// 性能实验 A：Key Width — 被裁掉的列数对性能的影响
// ===========================================================================================
// 核心问题：从 6→1 列 vs 4→1 列 vs 2→1 列，收益差距多大？
//
// 设计：
//   - 1 张 1 亿行表，只造一次
//   - good_key 高 NDV 无 skew → 每次都被选为最终 shuffle key
//   - k2~k6 注入 hot_values 使其 skew → 被跳过
//   - 改变 group by 的列数驱动不同 case
//   - 外层 count(*) 消除结果集回传干扰
//
// Case：
//   KW00: group by 1 列 → 无裁剪（基线）
//   KW01: group by 6 列 → 裁到 1 列（收益最大）
//   KW02: group by 4 列 → 裁到 1 列
//   KW03: group by 2 列 → 裁到 1 列（收益最弱）
// ===========================================================================================
suite("agg_shuffle_key_prune_perf_key_width_heavy_manual") {

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

    // ---- 建表 ----
    sql """drop table if exists t_perf_kw;"""
    sql """create table t_perf_kw (
            id bigint,
            good_key bigint,
            k2 bigint,
            k3 bigint,
            k4 bigint,
            k5 bigint,
            k6 bigint,
            v bigint
        ) duplicate key(id)
        distributed by hash(id) buckets 32
        properties ("replication_num" = "1");"""

    // ---- 造数据 ----
    sql """insert into t_perf_kw
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
        sql """alter table t_perf_kw modify column ${col} set stats (
            'row_count'='${statsRowCount}', 'ndv'='${esc(ndv)}', 'num_nulls'='${esc(numNulls)}',
            'data_size'='${esc(dataSize)}', 'min_value'='${esc(minVal)}', 'max_value'='${esc(maxVal)}'${hvClause});"""
    }

    // good_key: 高 NDV + 无 skew → 被选为 shuffle key
    ssBase("id",       "100000000", "0", "800000000", "0", "99999999", "")
    ssBase("good_key", "8000000",   "0", "800000000", "0", "7999999",  "")
    // k2~k6: 高 NDV + 有 skew → 被跳过
    ssBase("k2", "7500000", "0", "800000000", "0", "7499999", "1 :0.10")
    ssBase("k3", "7000000", "0", "800000000", "0", "6999999", "1 :0.10")
    ssBase("k4", "6500000", "0", "800000000", "0", "6499999", "1 :0.10")
    ssBase("k5", "6000000", "0", "800000000", "0", "5999999", "1 :0.10")
    ssBase("k6", "5500000", "0", "800000000", "0", "5499999", "1 :0.10")
    ssBase("v",  "100000000", "0", "800000000", "0", "99999999", "")

    // ---- helper ----
    def extractShuffleSigns = { String explainStr ->
        def signs = []
        def matcher = (explainStr =~ /orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while (matcher.find()) {
            signs << "[" + matcher.group(1).replaceAll("\\s+", "") + "]"
        }
        return signs
    }

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

    def runPerfCase = { String id, String sqlStr, boolean expectPrune ->
        sql """set enable_agg_shuffle_key_prune=false;"""
        def explainOff = (sql "explain physical plan " + sqlStr).toString()
        sql """set enable_agg_shuffle_key_prune=true;"""
        def explainOn = (sql "explain physical plan " + sqlStr).toString()
        logger.info("${id}: explainOff signs=${extractShuffleSigns(explainOff)}")
        logger.info("${id}: explainOn signs=${extractShuffleSigns(explainOn)}")
        logger.info("${id}: planChanged=${explainOff != explainOn}")

        // 验证是否选中了 good_key（仅裁剪场景）
        if (expectPrune) {
            assertSelectedKey(id, explainOn, "good_key")
        }

        // warmup
        for (int i = 0; i < warmupTimes; i++) {
            sql """set enable_agg_shuffle_key_prune=false;"""
            sql sqlStr
            sql """set enable_agg_shuffle_key_prune=true;"""
            sql sqlStr
        }

        // 正式测量
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
        def offMin = offTimes[0]
        def onMin = onTimes[0]
        def speedup = offMedian > 0 ? String.format("%.2f", offMedian / (double) onMedian) : "N/A"

        logger.info("${id}: offTimes=${offTimes}, onTimes=${onTimes}")
        logger.info("${id}: offMedian=${offMedian}ms, onMedian=${onMedian}ms, speedup=${speedup}x")
        logger.info("${id}: offMin=${offMin}ms, onMin=${onMin}ms")

        // 结果一致性验证
        sql """set enable_agg_shuffle_key_prune=false;"""
        def resultOff = sql sqlStr
        sql """set enable_agg_shuffle_key_prune=true;"""
        def resultOn = sql sqlStr
        assertTrue(normalizeRows(resultOff) == normalizeRows(resultOn),
                "${id}: OFF/ON results must be identical")
    }

    // ====================================================
    // KW00: group by 1 列 → 无裁剪（基线）
    // ====================================================
    runPerfCase("KW00", """
        select count(*), max(sum_v) from (
            select good_key, sum(v) as sum_v
            from t_perf_kw
            group by good_key
        ) t
    """, false)

    // ====================================================
    // KW01: group by 6 列 → 裁到 1 列（收益最大）
    // ====================================================
    runPerfCase("KW01", """
        select count(*), max(sum_v) from (
            select good_key, k2, k3, k4, k5, k6, sum(v) as sum_v
            from t_perf_kw
            group by good_key, k2, k3, k4, k5, k6
        ) t
    """, true)

    // ====================================================
    // KW02: group by 4 列 → 裁到 1 列
    // ====================================================
    runPerfCase("KW02", """
        select count(*), max(sum_v) from (
            select good_key, k2, k3, k4, sum(v) as sum_v
            from t_perf_kw
            group by good_key, k2, k3, k4
        ) t
    """, true)

    // ====================================================
    // KW03: group by 2 列 → 裁到 1 列（收益最弱）
    // ====================================================
    runPerfCase("KW03", """
        select count(*), max(sum_v) from (
            select good_key, k2, sum(v) as sum_v
            from t_perf_kw
            group by good_key, k2
        ) t
    """, true)

    sql """set enable_agg_shuffle_key_prune=true;"""
}
