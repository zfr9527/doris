suite("agg_shuffle_key_prune_functional_full") {

    // 中文：功能测试只关注计划形态（shuffle key 是否符合预期），关闭 cache 降低噪声。
    // English: Functional tests focus on plan shapes (expected shuffle-key behavior); disable cache to reduce noise.
    sql """set enable_sql_cache=false;"""
    sql """create database if not exists regression_test_stress_agg_shuffle;"""
    sql """use regression_test_stress_agg_shuffle;"""

    // 中文：清理历史对象。
    // English: Clean historical objects.
    sql """drop table if exists t_case_by_x;"""
    sql """drop table if exists t_case_by_b;"""
    sql """drop table if exists t_case_by_a;"""
    sql """drop table if exists t_case_by_b_max;"""
    sql """drop table if exists t_case_random;"""
    sql """drop table if exists t_case_no_stats;"""
    sql """drop table if exists t_case_parent_scan;"""
    sql """drop table if exists t_case_type_unsatisfied;"""
    sql """drop table if exists t_case_type_satisfied;"""
    sql """drop table if exists t_case_type_string_dom;"""
    sql """drop table if exists t_case_type_numtxt_unsatisfied;"""
    sql """drop table if exists t_case_type_numtxt_satisfied;"""

    // 中文：按 x 分布（通常不满足上层 gby，倾向两阶段）。
    // English: Distributed by x (usually mismatch with upper gby, tends to two-phase).
    sql """create table t_case_by_x (
            x bigint,
            a bigint,
            b bigint,
            c bigint,
            d bigint,
            e bigint,
            f bigint,
            g bigint,
            v bigint,
            hot_k bigint,
            null_k bigint,
            str_k string
        )
        duplicate key(x, a, b, c)
        distributed by hash(x) buckets 8
        properties ("replication_num" = "1");"""

    // 中文：按 b 分布（用于“满足 gby 分布”路径，且 b NDV 不是最高）。
    // English: Distributed by b (distribution-satisfied path, with b not being max-NDV).
    sql """create table t_case_by_b (
            x bigint,
            a bigint,
            b bigint,
            c bigint,
            d bigint,
            e bigint,
            f bigint,
            g bigint,
            v bigint,
            hot_k bigint,
            null_k bigint,
            str_k string
        )
        duplicate key(x, a, b, c)
        distributed by hash(b) buckets 8
        properties ("replication_num" = "1");"""

    // 中文：按 a 分布（用于“满足 distinct key 分布”路径）。
    // English: Distributed by a (distribution-satisfied path for distinct key).
    sql """create table t_case_by_a (
            x bigint,
            a bigint,
            b bigint,
            c bigint,
            d bigint,
            e bigint,
            f bigint,
            g bigint,
            v bigint,
            hot_k bigint,
            null_k bigint,
            str_k string
        )
        duplicate key(x, a, b, c)
        distributed by hash(a) buckets 8
        properties ("replication_num" = "1");"""

    // 中文：按 b 分布且 b NDV 更高（用于“b 是最大 NDV”验证）。
    // English: Distributed by b with higher NDV on b (for "b is max-NDV" checks).
    sql """create table t_case_by_b_max (
            x bigint,
            a bigint,
            b bigint,
            c bigint,
            d bigint,
            e bigint,
            f bigint,
            g bigint,
            v bigint,
            hot_k bigint,
            null_k bigint,
            str_k string
        )
        duplicate key(x, a, b, c)
        distributed by hash(b) buckets 8
        properties ("replication_num" = "1");"""

    // 中文：随机分布（参考 gby6 压测用例，验证始终不满足分布时的路径）。
    // English: Random distribution (aligned with gby6 repeat tests) to validate always-unsatisfied distribution paths.
    sql """create table t_case_random (
            x bigint,
            a bigint,
            b bigint,
            c bigint,
            d bigint,
            e bigint,
            f bigint,
            g bigint,
            v bigint,
            hot_k bigint,
            null_k bigint,
            str_k string
        )
        duplicate key(x, a, b, c)
        distributed by random buckets 8
        properties ("replication_num" = "1");"""

    // 中文：无统计信息表（用于回退反例）。
    // English: No-stats table for fallback negatives.
    sql """create table t_case_no_stats (
            x bigint,
            a bigint,
            b bigint,
            c bigint,
            d bigint,
            e bigint,
            f bigint,
            g bigint,
            v bigint,
            hot_k bigint,
            null_k bigint,
            str_k string
        )
        duplicate key(x, a, b, c)
        distributed by hash(x) buckets 8
        properties ("replication_num" = "1");"""

    // 中文：join 场景的 scan 侧（b..g 六列 join key）。
    // English: Scan side for JOIN scenarios (six join keys: b..g).
    sql """create table t_case_parent_scan (
            b bigint,
            c bigint,
            d bigint,
            e bigint,
            f bigint,
            g bigint,
            rv bigint
        )
        duplicate key(b, c, d)
        distributed by hash(b) buckets 8
        properties ("replication_num" = "1");"""

    // 中文：类型专项表（不满足分布）：用于验证 int/bigint/decimal/string 候选列行为。
    // English: Type-focused table (distribution unsatisfied) for int/bigint/decimal/string candidate behavior.
    sql """create table t_case_type_unsatisfied (
            x bigint,
            i_key int,
            bi_key bigint,
            de_key decimal(20, 4),
            s_key string,
            l1 bigint,
            l2 bigint,
            v bigint
        )
        duplicate key(x, i_key, bi_key)
        distributed by hash(x) buckets 8
        properties ("replication_num" = "1");"""

    // 中文：类型专项表（满足分布）：按 i_key 分布，用于“不可优化时 OFF/ON 相同”验证。
    // English: Type-focused table (distribution satisfied): hash by i_key to validate OFF/ON equality in non-optimizable paths.
    sql """create table t_case_type_satisfied (
            x bigint,
            i_key int,
            bi_key bigint,
            de_key decimal(20, 4),
            s_key string,
            l1 bigint,
            l2 bigint,
            v bigint
        )
        duplicate key(x, i_key, bi_key)
        distributed by hash(i_key) buckets 8
        properties ("replication_num" = "1");"""

    // 中文：字符串主导表：string NDV 显著更高，验证 string 仍可被选为 shuffle key。
    // English: String-dominant table: much higher string NDV to verify string can still be selected as shuffle key.
    sql """create table t_case_type_string_dom (
            x bigint,
            i_key int,
            bi_key bigint,
            de_key decimal(20, 4),
            s_key string,
            l1 bigint,
            l2 bigint,
            v bigint
        )
        duplicate key(x, i_key, bi_key)
        distributed by hash(x) buckets 8
        properties ("replication_num" = "1");"""

    // 中文：数值+文本混合类型表（不满足分布），增加 float/double/varchar 覆盖。
    // English: Mixed numeric+text table (distribution unsatisfied), adding float/double/varchar coverage.
    sql """create table t_case_type_numtxt_unsatisfied (
            x bigint,
            i_key int,
            bi_key bigint,
            de_key decimal(20, 4),
            fl_key float,
            db_key double,
            vc_key varchar(64),
            s_key string,
            l1 bigint,
            v bigint
        )
        duplicate key(x, i_key, bi_key)
        distributed by hash(x) buckets 8
        properties ("replication_num" = "1");"""

    // 中文：数值+文本混合类型表（满足分布）：按 vc_key 分布。
    // English: Mixed numeric+text table (distribution satisfied): hash by vc_key.
    sql """create table t_case_type_numtxt_satisfied (
            x bigint,
            i_key int,
            bi_key bigint,
            de_key decimal(20, 4),
            fl_key float,
            db_key double,
            vc_key varchar(64),
            s_key string,
            l1 bigint,
            v bigint
        )
        duplicate key(x, i_key, bi_key)
        distributed by hash(vc_key) buckets 8
        properties ("replication_num" = "1");"""

    // 中文：5000 行 hash 造数（b NDV 低，c/d/e/f NDV 高；带热点/null/string 风险列）。
    // English: 5000-row hash data (low NDV on b, high NDV on c/d/e/f, plus hotspot/null/string risk columns).
    sql """insert into t_case_by_x
        select
            (bitand(murmur_hash3_32(concat(cast(number as string), '_x_4500')), 2147483647) % 4500) * 100000 + 1 as x,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_a_4200')), 2147483647) % 4200) * 100000 + 2 as a,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_b_220')), 2147483647) % 220) * 100000 + 3 as b,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_c_3900')), 2147483647) % 3900) * 100000 + 4 as c,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_d_3600')), 2147483647) % 3600) * 100000 + 5 as d,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_e_4000')), 2147483647) % 4000) * 100000 + 6 as e,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_f_3400')), 2147483647) % 3400) * 100000 + 7 as f,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_g_120')), 2147483647) % 120) * 100000 + 8 as g,
            bitand(murmur_hash3_32(concat(cast(number as string), '_v')), 2147483647) as v,
            if(number % 10 < 9, 1, (bitand(murmur_hash3_32(concat(cast(number as string), '_hot_4000')), 2147483647) % 4000) + 2) as hot_k,
            if(number % 10 < 8, null, (bitand(murmur_hash3_32(concat(cast(number as string), '_null_3000')), 2147483647) % 3000) + 1) as null_k,
            concat('s_', cast((bitand(murmur_hash3_32(concat(cast(number as string), '_str_4200')), 2147483647) % 4200) as string)) as str_k
        from numbers("number" = "5000");"""

    sql """insert into t_case_by_b select * from t_case_by_x;"""
    sql """insert into t_case_by_a select * from t_case_by_x;"""
    sql """insert into t_case_random select * from t_case_by_x;"""
    sql """insert into t_case_no_stats select * from t_case_by_x;"""

    // 中文：再造一份 b NDV 更高的数据，验证“b最大NDV”与“b非最大NDV”两种分支。
    // English: Build another dataset where b has higher NDV, covering both "b-max" and "b-not-max" branches.
    sql """insert into t_case_by_b_max
        select
            (bitand(murmur_hash3_32(concat(cast(number as string), '_x2_4500')), 2147483647) % 4500) * 100000 + 1 as x,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_a2_4200')), 2147483647) % 4200) * 100000 + 2 as a,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_b2_4200')), 2147483647) % 4200) * 100000 + 3 as b,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_c2_220')), 2147483647) % 220) * 100000 + 4 as c,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_d2_2600')), 2147483647) % 2600) * 100000 + 5 as d,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_e2_2800')), 2147483647) % 2800) * 100000 + 6 as e,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_f2_3000')), 2147483647) % 3000) * 100000 + 7 as f,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_g2_120')), 2147483647) % 120) * 100000 + 8 as g,
            bitand(murmur_hash3_32(concat(cast(number as string), '_v2')), 2147483647) as v,
            if(number % 10 < 9, 1, (bitand(murmur_hash3_32(concat(cast(number as string), '_hot2_4000')), 2147483647) % 4000) + 2) as hot_k,
            if(number % 10 < 8, null, (bitand(murmur_hash3_32(concat(cast(number as string), '_null2_3000')), 2147483647) % 3000) + 1) as null_k,
            concat('s_', cast((bitand(murmur_hash3_32(concat(cast(number as string), '_str2_4200')), 2147483647) % 4200) as string)) as str_k
        from numbers("number" = "5000");"""

    sql """insert into t_case_parent_scan
        select b, c, d, e, f, g, max(v) as rv
        from t_case_by_x
        group by b, c, d, e, f, g;"""

    // 中文：类型专项造数（混合类型 NDV 接近，字符串不占优势）。
    // English: Type-focused data (mixed types with similar NDV; string is not dominant).
    sql """insert into t_case_type_unsatisfied
        select
            (bitand(murmur_hash3_32(concat(cast(number as string), '_tx_4500')), 2147483647) % 4500) * 100000 + 1 as x,
            cast((bitand(murmur_hash3_32(concat(cast(number as string), '_ti_3200')), 2147483647) % 3200) as int) as i_key,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_tbi_3200')), 2147483647) % 3200) * 100000 + 2 as bi_key,
            cast((bitand(murmur_hash3_32(concat(cast(number as string), '_tde_3200')), 2147483647) % 3200) as decimal(20,4)) / 10 as de_key,
            concat('ks_', cast((bitand(murmur_hash3_32(concat(cast(number as string), '_ts_2800')), 2147483647) % 2800) as string)) as s_key,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_tl1_200')), 2147483647) % 200) + 1 as l1,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_tl2_180')), 2147483647) % 180) + 1 as l2,
            bitand(murmur_hash3_32(concat(cast(number as string), '_tv')), 2147483647) as v
        from numbers("number" = "5000");"""

    sql """insert into t_case_type_satisfied select * from t_case_type_unsatisfied;"""

    // 中文：字符串主导造数（s_key NDV 高，数值列 NDV 低）。
    // English: String-dominant data (high NDV on s_key, low NDV on numeric columns).
    sql """insert into t_case_type_string_dom
        select
            (bitand(murmur_hash3_32(concat(cast(number as string), '_sx_4500')), 2147483647) % 4500) * 100000 + 1 as x,
            cast((bitand(murmur_hash3_32(concat(cast(number as string), '_si_200')), 2147483647) % 200) as int) as i_key,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_sbi_220')), 2147483647) % 220) * 100000 + 2 as bi_key,
            cast((bitand(murmur_hash3_32(concat(cast(number as string), '_sde_240')), 2147483647) % 240) as decimal(20,4)) / 10 as de_key,
            concat('dom_', cast((bitand(murmur_hash3_32(concat(cast(number as string), '_ss_3800')), 2147483647) % 3800) as string)) as s_key,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_sl1_200')), 2147483647) % 200) + 1 as l1,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_sl2_180')), 2147483647) % 180) + 1 as l2,
            bitand(murmur_hash3_32(concat(cast(number as string), '_sv')), 2147483647) as v
        from numbers("number" = "5000");"""

    // 中文：数值+文本混合类型造数（vc_key NDV 高，验证 varchar 相关路径）。
    // English: Mixed numeric+text data (high NDV on vc_key) to validate varchar-related paths.
    sql """insert into t_case_type_numtxt_unsatisfied
        select
            (bitand(murmur_hash3_32(concat(cast(number as string), '_nx_4500')), 2147483647) % 4500) * 100000 + 1 as x,
            cast((bitand(murmur_hash3_32(concat(cast(number as string), '_ni_2800')), 2147483647) % 2800) as int) as i_key,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_nbi_3000')), 2147483647) % 3000) * 100000 + 2 as bi_key,
            cast((bitand(murmur_hash3_32(concat(cast(number as string), '_nde_2600')), 2147483647) % 2600) as decimal(20,4)) / 10 as de_key,
            cast((bitand(murmur_hash3_32(concat(cast(number as string), '_nfl_2400')), 2147483647) % 2400) as float) / 10 as fl_key,
            cast((bitand(murmur_hash3_32(concat(cast(number as string), '_ndb_2300')), 2147483647) % 2300) as double) / 10 as db_key,
            concat('vc_', cast((bitand(murmur_hash3_32(concat(cast(number as string), '_nvc_3400')), 2147483647) % 3400) as string)) as vc_key,
            concat('ns_', cast((bitand(murmur_hash3_32(concat(cast(number as string), '_ns_1600')), 2147483647) % 1600) as string)) as s_key,
            (bitand(murmur_hash3_32(concat(cast(number as string), '_nl1_160')), 2147483647) % 160) + 1 as l1,
            bitand(murmur_hash3_32(concat(cast(number as string), '_nv')), 2147483647) as v
        from numbers("number" = "5000");"""

    sql """insert into t_case_type_numtxt_satisfied select * from t_case_type_numtxt_unsatisfied;"""

    // 中文：收集统计信息（no_stats 表故意不 analyze）。
    // English: Collect stats (no_stats table is intentionally not analyzed).
    sql """analyze table t_case_by_x with sync;"""
    sql """analyze table t_case_by_b with sync;"""
    sql """analyze table t_case_by_a with sync;"""
    sql """analyze table t_case_by_b_max with sync;"""
    sql """analyze table t_case_random with sync;"""
    sql """analyze table t_case_parent_scan with sync;"""
    sql """analyze table t_case_type_unsatisfied with sync;"""
    sql """analyze table t_case_type_satisfied with sync;"""
    sql """analyze table t_case_type_string_dom with sync;"""
    sql """analyze table t_case_type_numtxt_unsatisfied with sync;"""
    sql """analyze table t_case_type_numtxt_satisfied with sync;"""

    // 中文：解析 orderedShuffledColumns，统计每个 distribute 节点的 key 数量。
    // English: Parse orderedShuffledColumns and count key size for each distribute node.
    def extractShuffleKeyCounts = { String explainStr ->
        def counts = []
        def matcher = (explainStr =~ /orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while (matcher.find()) {
            def raw = matcher.group(1).trim()
            if (raw.length() == 0) {
                counts << 0
            } else {
                def parts = raw.split(",").collect { it.trim() }.findAll { it.length() > 0 }
                counts << parts.size()
            }
        }
        return counts
    }

    // 中文：提取 orderedShuffledColumns 签名（用于判断 OFF/ON 是否使用相同 shuffle key）。
    // English: Extract orderedShuffledColumns signatures to check OFF/ON shuffle-key equality.
    def extractShuffleSigns = { String explainStr ->
        def signs = []
        def matcher = (explainStr =~ /orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while (matcher.find()) {
            signs << "[" + matcher.group(1).replaceAll("\\s+", "") + "]"
        }
        return signs
    }

    // 中文：标准化结果集（排序后比较），用于 OFF/ON 结果一致性断言。
    // English: Canonicalize results (sorted) for OFF/ON result-equality assertions.
    def normalizeRows = { def rows ->
        def normalized = rows.collect { row ->
            row.collect { v -> v == null ? "NULL" : v.toString() }.join("||")
        }
        normalized.sort()
        return normalized
    }

    // 中文：按列名提取 explain 中的 ExprId（例如 s_key#12 -> 12）。
    // English: Extract ExprId by column name from explain text (e.g., s_key#12 -> 12).
    def extractExprIdByColumn = { String explainStr, String columnName ->
        def matcher = (explainStr =~ ("\\b" + java.util.regex.Pattern.quote(columnName) + "#(\\d+)\\b"))
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1))
        }
        return null
    }

    // 中文：提取 explain 中“单列 shuffle key”的 ExprId 列表。
    // English: Extract ExprIds for single-key shuffle entries from explain text.
    def extractSingleShuffleIds = { String explainStr ->
        def ids = []
        def matcher = (explainStr =~ /orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while (matcher.find()) {
            def raw = matcher.group(1).trim()
            if (raw.length() == 0) {
                continue
            }
            def parts = raw.split(",").collect { it.trim() }.findAll { it.length() > 0 }
            if (parts.size() == 1) {
                ids << Integer.parseInt(parts[0])
            }
        }
        return ids
    }

    // 中文：针对 no-stats 用例，执行前清空该表统计信息（不依赖全局开关）。
    // English: For no-stats cases, clear table stats right before execution (without global switches).
    def resetNoStatsTableStats = {
        sql """drop stats t_case_no_stats;"""
        def noStatsSnapshot = sql """show table stats t_case_no_stats;"""
        logger.info("t_case_no_stats stats after drop: " + noStatsSnapshot)
    }

    // 中文：提取 explain 中 equivalenceExprIds 原文签名（参考 gby6 压测用例）。
    // English: Extract raw equivalenceExprIds signatures from explain text (aligned with gby6 repeat tests).
    def extractEquivalenceExprIdsRaw = { String explainStr ->
        def raws = []
        def matcher = (explainStr =~ /(?s)equivalenceExprIds=\[(.*?)\],\s*exprIdToEquivalenceSet=/)
        while (matcher.find()) {
            raws << "equivalenceExprIds=[" + matcher.group(1).replaceAll("\\s+", "") + "]"
        }
        return raws
    }

    // 中文：统一执行 explain（开关关闭/开启）+ 结果一致性对比，并按场景断言。
    // English: Unified OFF/ON explain + result-equality runner with scenario assertions.
    def runCase = { String id, String category, String sqlStr, int aggPhase, boolean expectSingleKey, boolean requireShuffle,
                    Boolean expectPlanChanged = null, Boolean expectEquivalenceChanged = null ->
        sql """set agg_phase=${aggPhase};"""
        sql """set disable_nereids_rules='';"""

        // 中文：A 组：关闭优化开关。
        // English: Group A: optimization switch OFF.
        sql """set choose_one_agg_shuffle_key=false;"""
        def explainOff = (sql "explain physical plan " + sqlStr).toString()
        def offCounts = extractShuffleKeyCounts(explainOff)
        def offSigns = extractShuffleSigns(explainOff)
        def offEquivalence = extractEquivalenceExprIdsRaw(explainOff)

        // 中文：B 组：开启优化开关（默认开启，这里显式设置）。
        // English: Group B: optimization switch ON (explicitly set although default is ON).
        sql """set choose_one_agg_shuffle_key=true;"""
        def explainOn = (sql "explain physical plan " + sqlStr).toString()
        def onCounts = extractShuffleKeyCounts(explainOn)
        def onSigns = extractShuffleSigns(explainOn)
        def onEquivalence = extractEquivalenceExprIdsRaw(explainOn)
        def onHasSingle = onCounts.any { it == 1 }
        def shouldChangePlan = (expectPlanChanged == null ? expectSingleKey : expectPlanChanged)

        if (shouldChangePlan) {
            // 中文：计划应变化：OFF/ON explain 必须不同，且 shuffle key 签名不同。
            // English: Plan-change expected: OFF/ON explain and shuffle-key signatures must differ.
            assertTrue(explainOff != explainOn)
            assertTrue(offSigns.toString() != onSigns.toString())
            if (expectSingleKey) {
                assertTrue(onHasSingle)
            }
        } else {
            // 中文：计划应稳定：OFF/ON 使用的 shuffle key 必须一致。
            // English: Plan-stable expected: OFF/ON must use the same shuffle keys.
            assertTrue(offSigns.toString() == onSigns.toString())
            if (requireShuffle) {
                assertTrue(onSigns.size() > 0)
            }
        }

        if (expectEquivalenceChanged != null && offEquivalence.size() > 0 && onEquivalence.size() > 0) {
            if (expectEquivalenceChanged) {
                assertTrue(offEquivalence.toString() != onEquivalence.toString())
            } else {
                assertTrue(offEquivalence.toString() == onEquivalence.toString())
            }
        }

        // 中文：SQL 结果一致性（OFF vs ON）。
        // English: SQL result consistency (OFF vs ON).
        sql """set choose_one_agg_shuffle_key=false;"""
        def resultOff = sql sqlStr
        sql """set choose_one_agg_shuffle_key=true;"""
        def resultOn = sql sqlStr
        assertTrue(normalizeRows(resultOff) == normalizeRows(resultOn))

        logger.info("${id} [${category}] offCounts=${offCounts}, onCounts=${onCounts}, offSigns=${offSigns}, onSigns=${onSigns}, " +
                "offEquivalence=${offEquivalence}, onEquivalence=${onEquivalence}, expectSingleKey=${expectSingleKey}, " +
                "requireShuffle=${requireShuffle}, expectPlanChanged=${shouldChangePlan}, " +
                "expectEquivalenceChanged=${expectEquivalenceChanged}, resultRows=" + resultOn.size())
    }

    // =========================
    // 中文：1) simple agg（对齐 gby6 case1 的 baseline 结构）
    // English: 1) simple agg (aligned with gby6 case1 baseline patterns)
    // =========================

    // S01 gby6 baseline（不满足分布） -> 预期优化
    // S01 gby6 baseline (distribution unsatisfied) -> optimization expected
    runCase("S01", "gby6_baseline_unsatisfied", """
        select a, b, c, d, e, f, count(*) as c1
        from t_case_by_x
        group by a, b, c, d, e, f
    """, 0, true, true)

    // S02 gby6 baseline（满足分布：group by 包含 x） -> 预期不优化
    // S02 gby6 baseline (distribution satisfied: group by includes x) -> expected no optimization
    runCase("S02", "gby6_baseline_satisfied", """
        select x, a, b, c, d, e, f, count(*) as c1
        from t_case_by_x
        group by x, a, b, c, d, e, f
    """, 0, false, false)

    // S03 gby6 baseline（随机分布） -> 预期优化
    // S03 gby6 baseline (random distribution) -> optimization expected
    runCase("S03", "gby6_baseline_random", """
        select a, b, c, d, e, f, count(*) as c1
        from t_case_random
        group by a, b, c, d, e, f
    """, 0, true, true)

    // S04 gby6 + count/sum/min/max（不满足分布） -> 预期优化
    // S04 gby6 + count/sum/min/max (distribution unsatisfied) -> optimization expected
    runCase("S04", "gby6_csmm_unsatisfied", """
        select a, b, c, d, e, f, count(*) as c1, sum(v) as s1, min(v) as mn, max(v) as mx
        from t_case_by_x
        group by a, b, c, d, e, f
    """, 0, true, true)

    // S05 gby6 + count/sum/min/max（满足分布：group by 包含 x） -> 预期不优化
    // S05 gby6 + count/sum/min/max (distribution satisfied: group by includes x) -> expected no optimization
    runCase("S05", "gby6_csmm_satisfied", """
        select x, a, b, c, d, e, f, count(*) as c1, sum(v) as s1, min(v) as mn, max(v) as mx
        from t_case_by_x
        group by x, a, b, c, d, e, f
    """, 0, false, false)

    // =========================
    // 中文：2) agg 收到父节点 hash shuffle 请求（父: agg/window/join）
    // English: 2) agg receives parent hash-shuffle request (parent: agg/window/join)
    // =========================

    // P01 父=agg，满足分布 -> 预期不优化
    // P01 parent=agg, satisfied -> no optimization
    runCase("P01", "parent_agg_satisfied", """
        select t.b, t.c, t.d, t.e, t.f, t.g, sum(t.sv), max(t.cnt_v)
        from (
            select a, b, c, d, e, f, g, sum(v) as sv, count(*) as cnt_v
            from t_case_by_b
            group by a, b, c, d, e, f, g
        ) t
        group by t.b, t.c, t.d, t.e, t.f, t.g
    """, 0, false, false)

    // P02 父=agg，不满足分布 -> 预期优化
    // P02 parent=agg, unsatisfied -> optimization expected
    runCase("P02", "parent_agg_unsatisfied", """
        select t.b, t.c, t.d, t.e, t.f, t.g, sum(t.sv), max(t.cnt_v)
        from (
            select a, b, c, d, e, f, g, sum(v) as sv, count(*) as cnt_v
            from t_case_by_x
            group by a, b, c, d, e, f, g
        ) t
        group by t.b, t.c, t.d, t.e, t.f, t.g
    """, 0, true, true)

    // P03 父=window，满足分布 -> 预期不优化
    // P03 parent=window, satisfied -> no optimization
    runCase("P03", "parent_window_satisfied", """
        select t.a, t.b, t.c, t.d, t.e, t.f, t.g, t.sv,
               row_number() over (
                   partition by t.b, t.c, t.d, t.e, t.f, t.g
                   order by t.sv desc, t.a
               ) as rn
        from (
            select a, b, c, d, e, f, g, sum(v) as sv
            from t_case_by_b
            group by a, b, c, d, e, f, g
        ) t
    """, 0, false, false)

    // P04 父=window，不满足分布 -> 预期优化
    // P04 parent=window, unsatisfied -> optimization expected
    runCase("P04", "parent_window_unsatisfied", """
        select t.a, t.b, t.c, t.d, t.e, t.f, t.g, t.sv,
               row_number() over (
                   partition by t.b, t.c, t.d, t.e, t.f, t.g
                   order by t.sv desc, t.a
               ) as rn
        from (
            select a, b, c, d, e, f, g, sum(v) as sv
            from t_case_by_x
            group by a, b, c, d, e, f, g
        ) t
    """, 0, true, true)

    // P05 父=join(agg,scan)，满足分布 -> 预期不优化
    // P05 parent=join(agg,scan), satisfied -> no optimization
    runCase("P05", "parent_join_satisfied", """
        select l.b, l.c, l.d, l.e, l.f, l.g, l.sv, r.rv
        from (
            select a, b, c, d, e, f, g, sum(v) as sv
            from t_case_by_b
            group by a, b, c, d, e, f, g
        ) l
        join t_case_parent_scan r
          on l.b = r.b and l.c = r.c and l.d = r.d and l.e = r.e and l.f = r.f and l.g = r.g
    """, 0, false, false)

    // P06 父=join(agg,scan)，不满足分布 -> 预期优化
    // P06 parent=join(agg,scan), unsatisfied -> optimization expected
    runCase("P06", "parent_join_unsatisfied", """
        select l.b, l.c, l.d, l.e, l.f, l.g, l.sv, r.rv
        from (
            select a, b, c, d, e, f, g, sum(v) as sv
            from t_case_by_x
            group by a, b, c, d, e, f, g
        ) l
        join t_case_parent_scan r
          on l.b = r.b and l.c = r.c and l.d = r.d and l.e = r.e and l.f = r.f and l.g = r.g
    """, 0, true, true)

    // =========================
    // 中文：3) agg_with_distinct_func（count/sum/min/max）
    // English: 3) agg_with_distinct_func (count/sum/min/max)
    // =========================

    // D01 不满足分布 -> 2+1
    // D01 distribution not satisfied -> 2+1
    runCase("D01", "distinct_csmm_unsatisfied_2plus1", """
        select b, c, d, e, f, g,
               count(distinct a) as cda,
               sum(v) as sv,
               min(v) as mn,
               max(v) as mx
        from t_case_by_x
        group by b, c, d, e, f, g
    """, 0, true, true)

    // D02 不满足分布 -> 2+2 (agg_phase=4)，有可能出现冗余agg
    // D02 distribution not satisfied -> 2+2 (agg_phase=4), possible redundant agg
    runCase("D02", "distinct_csmm_unsatisfied_2plus2_redundant", """
        select b, c, d, e, f, g,
               count(distinct d, e, f) as cddef,
               sum(v) as sv,
               min(v) as mn,
               max(v) as mx
        from t_case_by_x
        group by b, c, d, e, f, g
    """, 4, true, true)

    // D03 不满足分布 -> 2+2 (agg_phase=4)，无冗余agg候选
    // D03 distribution not satisfied -> 2+2 (agg_phase=4), non-redundant agg candidate
    runCase("D03", "distinct_csmm_unsatisfied_2plus2_nonredundant", """
        select b, c, d, e, f, g,
               count(distinct a, x) as cdax,
               sum(v) as sv,
               min(v) as mn,
               max(v) as mx
        from t_case_by_x
        group by b, c, d, e, f, g
    """, 4, true, true)

    // D04 满足分布 -> 1+1，b为最大NDV（按b分布）
    // D04 distribution satisfied -> 1+1, b is max NDV (distributed by b)
    runCase("D04", "distinct_csmm_satisfied_1plus1_b_max", """
        select b, c, d, e, f, g,
               count(distinct a) as cda,
               sum(v) as sv,
               min(v) as mn,
               max(v) as mx
        from t_case_by_b_max
        group by b, c, d, e, f, g
    """, 0, false, false)

    // D05 满足分布 -> 1+1，b不是最大NDV（c更高）仍优先满足分布
    // D05 distribution satisfied -> 1+1, b not max NDV (c higher) but distribution-first still applies
    runCase("D05", "distinct_csmm_satisfied_1plus1_b_notmax", """
        select b, c, d, e, f, g,
               count(distinct a) as cda,
               sum(v) as sv,
               min(v) as mn,
               max(v) as mx
        from t_case_by_b
        group by b, c, d, e, f, g
    """, 0, false, false)

    // D06 满足 distinct_key -> 1+2 (agg_phase=4) 分支验证（按a分布）
    // D06 satisfy distinct_key -> validate 1+2 (agg_phase=4) branch (distributed by a)
    runCase("D06", "distinct_csmm_satisfied_1plus2_distinct_key", """
        select b, c, d, e, f, g,
               count(distinct a) as cda,
               sum(v) as sv,
               min(v) as mn,
               max(v) as mx
        from t_case_by_a
        group by b, c, d, e, f, g
    """, 4, true, true)

    // D07 满足 gby key -> 1+2(agg_phase=4) 候选，但预期1+1更优（按b分布，b最大NDV）
    // D07 satisfy gby key -> 1+2(agg_phase=4) candidate, but 1+1 expected better (distributed by b, b is max NDV)
    runCase("D07", "distinct_csmm_satisfied_1plus2_gby_key_b_max", """
        select b, c, d, e, f, g,
               count(distinct a) as cda,
               sum(v) as sv,
               min(v) as mn,
               max(v) as mx
        from t_case_by_b_max
        group by b, c, d, e, f, g
    """, 4, false, false)

    // D08 满足 gby key -> 1+2(agg_phase=4) 候选，b非最大NDV仍应分布优先（最终1+1）
    // D08 satisfy gby key -> 1+2(agg_phase=4) candidate, b not max NDV but distribution-first (final 1+1)
    runCase("D08", "distinct_csmm_satisfied_1plus2_gby_key_b_notmax", """
        select b, c, d, e, f, g,
               count(distinct a) as cda,
               sum(v) as sv,
               min(v) as mn,
               max(v) as mx
        from t_case_by_b
        group by b, c, d, e, f, g
    """, 4, false, false)

    // =========================
    // 中文：4) agg_with_distinct_func + avg()
    // English: 4) agg_with_distinct_func + avg()
    // =========================

    // A01 不满足分布 -> 2+1
    // A01 distribution not satisfied -> 2+1
    runCase("A01", "distinct_avg_unsatisfied_2plus1", """
        select b, c, d, e, f, g,
               count(distinct a) as cda,
               avg(v) as av
        from t_case_by_x
        group by b, c, d, e, f, g
    """, 0, true, true)

    // A02 不满足分布 -> 2+2
    // A02 distribution not satisfied -> 2+2
    runCase("A02", "distinct_avg_unsatisfied_2plus2", """
        select b, c, d, e, f, g,
               count(distinct a, x) as cdax,
               avg(v) as av
        from t_case_by_x
        group by b, c, d, e, f, g
    """, 4, true, true)

    // A03 满足分布 -> 1+1
    // A03 distribution satisfied -> 1+1
    runCase("A03", "distinct_avg_satisfied_1plus1", """
        select b, c, d, e, f, g,
               count(distinct a) as cda,
               avg(v) as av
        from t_case_by_b
        group by b, c, d, e, f, g
    """, 0, false, false)

    // A04 满足分布 -> 1+2 分支（按a分布，满足distinct key）
    // A04 distribution satisfied -> 1+2 branch (distributed by a, satisfy distinct key)
    runCase("A04", "distinct_avg_satisfied_1plus2", """
        select b, c, d, e, f, g,
               count(distinct a) as cda,
               avg(v) as av
        from t_case_by_a
        group by b, c, d, e, f, g
    """, 4, true, true)

    // =========================
    // 中文：5) no stats 回退（不应触发单列 shuffle key 选择）
    // English: 5) No-stats fallback (should not trigger single-key shuffle selection)
    // =========================

    // N01 无统计信息 + simple agg（不满足分布） -> 预期不优化
    // N01 no stats + simple agg (distribution unsatisfied) -> expected no optimization
    resetNoStatsTableStats()
    runCase("N01", "no_stats_simple_agg_unsatisfied", """
        select a, b, c, d, e, f, count(*) as c1, sum(v) as s1, min(v) as mn, max(v) as mx
        from t_case_no_stats
        group by a, b, c, d, e, f
    """, 0, false, false)

    // N02 无统计信息 + distinct + avg（不满足分布，agg_phase=4） -> 预期不优化
    // N02 no stats + distinct + avg (distribution unsatisfied, agg_phase=4) -> expected no optimization
    resetNoStatsTableStats()
    runCase("N02", "no_stats_distinct_avg_unsatisfied", """
        select b, c, d, e, f, g,
               count(distinct a, x) as cdax,
               avg(v) as av
        from t_case_no_stats
        group by b, c, d, e, f, g
    """, 4, false, false)

    // =========================
    // 中文：6) agg and join（补充 case1 的双 agg 子节点 join）
    // English: 6) agg and join (including both-agg-child join cases from case1)
    // =========================

    // J01 双子节点均为 agg（满足分布） -> 预期不优化
    // J01 both join children are agg (distribution satisfied) -> expected no optimization
    runCase("J01", "agg_join_both_agg_satisfied", """
        select l.b, l.c, l.d, l.e, l.f, l.g, l.sum_v, r.cnt_v
        from (
            select b, c, d, e, f, g, sum(v) as sum_v
            from t_case_by_b
            group by b, c, d, e, f, g
        ) l
        join (
            select b, c, d, e, f, g, count(*) as cnt_v
            from t_case_by_b
            group by b, c, d, e, f, g
        ) r
          on l.b = r.b and l.c = r.c and l.d = r.d and l.e = r.e and l.f = r.f and l.g = r.g
    """, 0, false, false)

    // J02 双子节点均为 agg（不满足分布） -> 预期优化
    // J02 both join children are agg (distribution unsatisfied) -> optimization expected
    runCase("J02", "agg_join_both_agg_unsatisfied", """
        select l.b, l.c, l.d, l.e, l.f, l.g, l.sum_v, r.cnt_v
        from (
            select b, c, d, e, f, g, sum(v) as sum_v
            from t_case_by_x
            group by b, c, d, e, f, g
        ) l
        join (
            select b, c, d, e, f, g, count(*) as cnt_v
            from t_case_by_x
            group by b, c, d, e, f, g
        ) r
          on l.b = r.b and l.c = r.c and l.d = r.d and l.e = r.e and l.f = r.f and l.g = r.g
    """, 0, true, true)

    // =========================
    // 中文：7) shuffle key 数据类型专项（int/bigint/decimal/string/varchar/float/double）
    // English: 7) Shuffle-key data type focused tests (int/bigint/decimal/string/varchar/float/double)
    // =========================

    // T01 不满足分布 + 混合类型：预期优化生效，且优先不选 string 候选。
    // T01 Unsatisfied distribution + mixed types: optimization should trigger and should prefer non-string candidate.
    sql """set agg_phase=0;"""
    sql """set disable_nereids_rules='';"""
    sql """set choose_one_agg_shuffle_key=false;"""
    def t01OffExplain = (sql """explain physical plan
        select i_key, bi_key, de_key, s_key, l1, l2, sum(v)
        from t_case_type_unsatisfied
        group by i_key, bi_key, de_key, s_key, l1, l2
    """).toString()
    sql """set choose_one_agg_shuffle_key=true;"""
    def t01OnExplain = (sql """explain physical plan
        select i_key, bi_key, de_key, s_key, l1, l2, sum(v)
        from t_case_type_unsatisfied
        group by i_key, bi_key, de_key, s_key, l1, l2
    """).toString()
    assertTrue(t01OffExplain != t01OnExplain)
    def t01OnSingleIds = extractSingleShuffleIds(t01OnExplain)
    assertTrue(t01OnSingleIds.size() > 0)
    def t01StrId = extractExprIdByColumn(t01OnExplain, "s_key")
    assertTrue(t01StrId != null)
    assertTrue(!t01OnSingleIds.contains(t01StrId))
    sql """set choose_one_agg_shuffle_key=false;"""
    def t01OffRows = sql """select i_key, bi_key, de_key, s_key, l1, l2, sum(v)
        from t_case_type_unsatisfied
        group by i_key, bi_key, de_key, s_key, l1, l2"""
    sql """set choose_one_agg_shuffle_key=true;"""
    def t01OnRows = sql """select i_key, bi_key, de_key, s_key, l1, l2, sum(v)
        from t_case_type_unsatisfied
        group by i_key, bi_key, de_key, s_key, l1, l2"""
    assertTrue(normalizeRows(t01OffRows) == normalizeRows(t01OnRows))

    // T02 满足分布 + 混合类型：预期不开优化（OFF/ON shuffle key 相同）。
    // T02 Satisfied distribution + mixed types: expected non-optimizable (same shuffle keys OFF/ON).
    sql """set choose_one_agg_shuffle_key=false;"""
    def t02OffExplain = (sql """explain physical plan
        select i_key, bi_key, de_key, s_key, l1, l2, sum(v)
        from t_case_type_satisfied
        group by i_key, bi_key, de_key, s_key, l1, l2
    """).toString()
    sql """set choose_one_agg_shuffle_key=true;"""
    def t02OnExplain = (sql """explain physical plan
        select i_key, bi_key, de_key, s_key, l1, l2, sum(v)
        from t_case_type_satisfied
        group by i_key, bi_key, de_key, s_key, l1, l2
    """).toString()
    assertTrue(extractShuffleSigns(t02OffExplain).toString() == extractShuffleSigns(t02OnExplain).toString())
    sql """set choose_one_agg_shuffle_key=false;"""
    def t02OffRows = sql """select i_key, bi_key, de_key, s_key, l1, l2, sum(v)
        from t_case_type_satisfied
        group by i_key, bi_key, de_key, s_key, l1, l2"""
    sql """set choose_one_agg_shuffle_key=true;"""
    def t02OnRows = sql """select i_key, bi_key, de_key, s_key, l1, l2, sum(v)
        from t_case_type_satisfied
        group by i_key, bi_key, de_key, s_key, l1, l2"""
    assertTrue(normalizeRows(t02OffRows) == normalizeRows(t02OnRows))

    // T03 不满足分布 + string 主导：预期优化生效，且 string 可以被选中。
    // T03 Unsatisfied distribution + string dominant: optimization should trigger and string may be selected.
    sql """set choose_one_agg_shuffle_key=false;"""
    def t03OffExplain = (sql """explain physical plan
        select i_key, bi_key, de_key, s_key, l1, l2, sum(v)
        from t_case_type_string_dom
        group by i_key, bi_key, de_key, s_key, l1, l2
    """).toString()
    sql """set choose_one_agg_shuffle_key=true;"""
    def t03OnExplain = (sql """explain physical plan
        select i_key, bi_key, de_key, s_key, l1, l2, sum(v)
        from t_case_type_string_dom
        group by i_key, bi_key, de_key, s_key, l1, l2
    """).toString()
    assertTrue(t03OffExplain != t03OnExplain)
    def t03OnSingleIds = extractSingleShuffleIds(t03OnExplain)
    assertTrue(t03OnSingleIds.size() > 0)
    def t03StrId = extractExprIdByColumn(t03OnExplain, "s_key")
    assertTrue(t03StrId != null)
    assertTrue(t03OnSingleIds.contains(t03StrId))
    sql """set choose_one_agg_shuffle_key=false;"""
    def t03OffRows = sql """select i_key, bi_key, de_key, s_key, l1, l2, sum(v)
        from t_case_type_string_dom
        group by i_key, bi_key, de_key, s_key, l1, l2"""
    sql """set choose_one_agg_shuffle_key=true;"""
    def t03OnRows = sql """select i_key, bi_key, de_key, s_key, l1, l2, sum(v)
        from t_case_type_string_dom
        group by i_key, bi_key, de_key, s_key, l1, l2"""
    assertTrue(normalizeRows(t03OffRows) == normalizeRows(t03OnRows))

    // T04 不满足分布 + float/double/varchar 混合类型：预期优化生效（OFF/ON explain 不同）。
    // T04 Unsatisfied + float/double/varchar mixed types: optimization expected (OFF/ON explain should differ).
    sql """set choose_one_agg_shuffle_key=false;"""
    def t04OffExplain = (sql """explain physical plan
        select i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1, sum(v)
        from t_case_type_numtxt_unsatisfied
        group by i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1
    """).toString()
    sql """set choose_one_agg_shuffle_key=true;"""
    def t04OnExplain = (sql """explain physical plan
        select i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1, sum(v)
        from t_case_type_numtxt_unsatisfied
        group by i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1
    """).toString()
    assertTrue(t04OffExplain != t04OnExplain)
    assertTrue(extractSingleShuffleIds(t04OnExplain).size() > 0)
    sql """set choose_one_agg_shuffle_key=false;"""
    def t04OffRows = sql """select i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1, sum(v)
        from t_case_type_numtxt_unsatisfied
        group by i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1"""
    sql """set choose_one_agg_shuffle_key=true;"""
    def t04OnRows = sql """select i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1, sum(v)
        from t_case_type_numtxt_unsatisfied
        group by i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1"""
    assertTrue(normalizeRows(t04OffRows) == normalizeRows(t04OnRows))

    // T05 满足分布 + float/double/varchar 混合类型：预期不可优化（OFF/ON shuffle key 相同）。
    // T05 Satisfied + float/double/varchar mixed types: expected non-optimizable (same shuffle keys OFF/ON).
    sql """set choose_one_agg_shuffle_key=false;"""
    def t05OffExplain = (sql """explain physical plan
        select i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1, sum(v)
        from t_case_type_numtxt_satisfied
        group by i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1
    """).toString()
    sql """set choose_one_agg_shuffle_key=true;"""
    def t05OnExplain = (sql """explain physical plan
        select i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1, sum(v)
        from t_case_type_numtxt_satisfied
        group by i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1
    """).toString()
    assertTrue(extractShuffleSigns(t05OffExplain).toString() == extractShuffleSigns(t05OnExplain).toString())
    sql """set choose_one_agg_shuffle_key=false;"""
    def t05OffRows = sql """select i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1, sum(v)
        from t_case_type_numtxt_satisfied
        group by i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1"""
    sql """set choose_one_agg_shuffle_key=true;"""
    def t05OnRows = sql """select i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1, sum(v)
        from t_case_type_numtxt_satisfied
        group by i_key, bi_key, de_key, fl_key, db_key, vc_key, s_key, l1"""
    assertTrue(normalizeRows(t05OffRows) == normalizeRows(t05OnRows))

    // 中文：恢复会话设置，避免影响其他 suite。
    // English: Restore session settings to avoid affecting other suites.
    sql """set agg_phase=0;"""
    sql """set choose_one_agg_shuffle_key=true;"""
    sql """set disable_nereids_rules='';"""
}
