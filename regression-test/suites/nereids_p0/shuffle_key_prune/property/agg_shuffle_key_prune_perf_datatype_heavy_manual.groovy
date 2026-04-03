// ===========================================================================================
// 性能实验 C：Datatype — 最终 key 的数据类型对性能的影响
// ===========================================================================================
// 核心问题：bigint vs datev2 vs char(8) vs varchar(16) vs varchar(128) 作为最终 shuffle key，
//           hash/compare 开销差异对实际查询性能的影响有多大？
//
// 设计：
//   - 1 张 1 亿行宽表，包含 5 种不同类型的列
//   - **同一条 SQL**：select count(*), max(sum_v) from (
//       select ki, kd, kc8, kv16, kv128, guard, sum(v) as sum_v
//       from t_perf_dt group by ki, kd, kc8, kv16, kv128, guard) t
//   - 通过改变统计信息（hot_values）控制哪个列被选为 shuffle key
//     * 目标列：高 NDV + 无 skew → 被选中
//     * 其他列：注入 hot_values 使其 skew → 被跳过
//   - 对比 OFF（全部 6 列 shuffle）vs ON（裁到目标单列）的性能
//
// Case：
//   DT01: bigint (ki) 作为最终 key → hash 最快
//   DT02: datev2 (kd) 作为最终 key → 与 bigint 接近
//   DT03: char(8) (kc8) 作为最终 key → 略弱
//   DT04: varchar(16) (kv16) 作为最终 key → 中等
//   DT05: varchar(128) (kv128) 作为最终 key → hash 开销大
// ===========================================================================================
suite("agg_shuffle_key_prune_perf_datatype_heavy_manual") {

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
    sql """drop table if exists t_perf_dt;"""
    sql """create table t_perf_dt (
            id bigint,
            ki bigint,
            kd datev2,
            kc8 char(8),
            kv16 varchar(16),
            kv128 varchar(128),
            guard bigint,
            v bigint
        ) duplicate key(id)
        distributed by hash(id) buckets 32
        properties ("replication_num" = "1");"""

    // ---- 造数据 ----
    sql """insert into t_perf_dt
        select number,
            number % 8000000,
            date_add('2020-01-01', interval (number % 3650) day),
            lpad(cast(number % 5000000 as string), 8, '0'),
            concat('v', cast(number % 6000000 as string)),
            concat(repeat('x', 64), cast(number % 7000000 as string)),
            number % 3,
            number
        from numbers("number" = "${rowCount}");"""

    // 手动注入统计信息（避免 analyze 设置表级 row_count=2000 导致 1-phase agg）
    def statsRowCount = "100000000"
    def esc = { Object v -> v==null?"null":v.toString().replace("\\","\\\\").replace("'","\\'") }
    def ssBase = { String col, String ndv, String numNulls, String dataSize, String minVal, String maxVal, String hv ->
        def hvClause = hv == null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql """alter table t_perf_dt modify column ${col} set stats (
            'row_count'='${statsRowCount}', 'ndv'='${esc(ndv)}', 'num_nulls'='${esc(numNulls)}',
            'data_size'='${esc(dataSize)}', 'min_value'='${esc(minVal)}', 'max_value'='${esc(maxVal)}'${hvClause});"""
    }
    ssBase("id", "60000", "0", "1600000", "0", "1999", "")
    ssBase("ki", "8000000", "0", "1600000", "0", "7999999", "")
    ssBase("kd", "3650", "0", "800000", "2020-01-01", "2030-01-01", "")
    ssBase("kc8", "5000000", "0", "3200000", "00000000", "04999999", "")
    ssBase("kv16", "6000000", "0", "3200000", "v0", "v5999999", "")
    ssBase("kv128", "7000000", "0", "25600000", "x0", "x6999999", "")
    ssBase("guard", "3", "0", "1600000", "0", "2", "")
    ssBase("v", "60000", "0", "1600000", "0", "1999", "")

    // ---- helper ----
    def cleanBoundValue = { Object val ->
        if (val == null) return ""
        def s = val.toString()
        if (s.length() >= 2 && s.startsWith("'") && s.endsWith("'")) {
            s = s.substring(1, s.length() - 1)
        }
        return s.replace("'", "\\'")
    }

    def injectStats = { String col, String ndv, String hv ->
        def row = sql("show column stats t_perf_dt(${col})")[0]
        def hvSql = hv == null ? "'null'" : "'${hv}'"
        def minVal = cleanBoundValue(row[7])
        def maxVal = cleanBoundValue(row[8])
        sql """alter table t_perf_dt modify column ${col}
               set stats ('row_count'='${row[2]}', 'ndv'='${ndv}',
                          'num_nulls'='${row[4]}', 'data_size'='${row[5]}',
                          'min_value'='${minVal}', 'max_value'='${maxVal}',
                          'hot_values'=${hvSql});"""
    }

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

    // 断言：ON plan 中目标列确实被选为单列 shuffle key
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

    def runPerfCase = { String id, String sqlStr, String targetCol ->
        sql """set enable_agg_shuffle_key_prune=false;"""
        def explainOff = (sql "explain physical plan " + sqlStr).toString()
        sql """set enable_agg_shuffle_key_prune=true;"""
        def explainOn = (sql "explain physical plan " + sqlStr).toString()
        logger.info("${id}: explainOff signs=${extractShuffleSigns(explainOff)}")
        logger.info("${id}: explainOn signs=${extractShuffleSigns(explainOn)}")
        logger.info("${id}: planChanged=${explainOff != explainOn}")

        // 关键断言：确认目标列确实被选为 shuffle key
        assertSelectedKey(id, explainOn, targetCol)

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
        def offMin = offTimes[0]
        def onMin = onTimes[0]
        def speedup = offMedian > 0 ? String.format("%.2f", offMedian / (double) onMedian) : "N/A"

        logger.info("${id}: offTimes=${offTimes}, onTimes=${onTimes}")
        logger.info("${id}: offMedian=${offMedian}ms, onMedian=${onMedian}ms, speedup=${speedup}x")
        logger.info("${id}: offMin=${offMin}ms, onMin=${onMin}ms")

        sql """set enable_agg_shuffle_key_prune=false;"""
        def resultOff = sql sqlStr
        sql """set enable_agg_shuffle_key_prune=true;"""
        def resultOn = sql sqlStr
        assertTrue(normalizeRows(resultOff) == normalizeRows(resultOn),
                "${id}: OFF/ON results must be identical")
    }

    // ---- 统一 SQL（所有 case 完全相同） ----
    def perfSql = """
        select count(*), max(sum_v) from (
            select ki, kd, kc8, kv16, kv128, guard, sum(v) as sum_v
            from t_perf_dt
            group by ki, kd, kc8, kv16, kv128, guard
        ) t
    """

    // 所有 5 种类型的列名
    def allCols = ["ki", "kd", "kc8", "kv16", "kv128"]
    // guard 列固定 skew
    def guardSkew = "0 :0.34;1 :0.33;2 :0.33"

    // ---- helper: 设置统计信息让指定列成为唯一 balanced key ----
    // 每列的固定元数据（numNulls, dataSize, minVal, maxVal, skewHotValue）
    // skewHotValue: 当该列需要被标记为 skew 时使用的 hot_values（必须匹配列类型）
    def colMeta = [
        "ki":    ["0", "1600000", "0", "7999999", "1 :0.10"],
        "kd":    ["0", "800000", "2020-01-01", "2030-01-01", "2020-01-01 :0.10"],
        "kc8":   ["0", "3200000", "00000000", "04999999", "00000001 :0.10"],
        "kv16":  ["0", "3200000", "v0", "v5999999", "v1 :0.10"],
        "kv128": ["0", "25600000", "x0", "x6999999", "x1 :0.10"],
        "guard": ["0", "1600000", "0", "2"]
    ]
    def setupTarget = { String targetCol ->
        // guard 始终 skew（单个 hot value，避免 ';' 被 SQL 框架截断）
        ssBase("guard", "3", "0", "1600000", "0", "2", "0 :0.40")
        // 目标列：高 NDV + 无 skew → 被选为 shuffle key
        def tm = colMeta[targetCol]
        ssBase(targetCol, "60000", tm[0], tm[1], tm[2], tm[3], "")
        // 其他列：注入类型匹配的 hot_values 使其 skew → 被跳过
        allCols.findAll { it != targetCol }.each { col ->
            def cm = colMeta[col]
            ssBase(col, "60000", cm[0], cm[1], cm[2], cm[3], cm[4])
        }
    }

    // ====================================================
    // DT01: bigint (ki) 作为最终 shuffle key
    // ====================================================
    setupTarget("ki")
    runPerfCase("DT01", perfSql, "ki")

    // ====================================================
    // DT02: datev2 (kd) 作为最终 shuffle key
    // ====================================================
    setupTarget("kd")
    runPerfCase("DT02", perfSql, "kd")

    // ====================================================
    // DT03: char(8) (kc8) 作为最终 shuffle key
    // ====================================================
    setupTarget("kc8")
    runPerfCase("DT03", perfSql, "kc8")

    // ====================================================
    // DT04: varchar(16) (kv16) 作为最终 shuffle key
    // ====================================================
    setupTarget("kv16")
    runPerfCase("DT04", perfSql, "kv16")

    // ====================================================
    // DT05: varchar(128) (kv128) 作为最终 shuffle key
    // ====================================================
    setupTarget("kv128")
    runPerfCase("DT05", perfSql, "kv128")

    sql """set enable_agg_shuffle_key_prune=true;"""
    sql """set agg_phase=0;"""
}
