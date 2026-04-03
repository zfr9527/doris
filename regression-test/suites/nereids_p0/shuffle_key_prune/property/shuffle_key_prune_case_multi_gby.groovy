// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// 思路说明：
// 1. 这份文件把原来的 gby6/gby5/gby4/gby3/gby2 五份性能回归合并到一个 suite 里。
// 2. 目标是复用同一套建表、造数和 analyze，避免每个 gby 版本都重复创建 1e8 行数据。
// 3. 不改变压测结构本身：仍然比较 explain、记录 OFF/ON 最小耗时，并保留原来的场景骨架。
// 4. 差异只集中在各个 gby 版本的 key 组合上，通过 profile 配置生成查询。

suite("shuffle_key_prune_case_multi_gby") {

    sql "set global enable_auto_analyze=false;"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_sql_cache=false"
    sql "set enable_query_cache=false"
    sql "set agg_phase=4"
    sql "set parallel_pipeline_task_num=8"

    def db_name = context.config.getDbNameByFile(context.file)
    def dist_ndv_low_tb = "dist_ndv_low_tb"
    def dist_ndv_high_tb = "dist_ndv_high_tb"
    def random_ndv_low_tb = "random_ndv_low_tb"
    def random_ndv_high_tb = "random_ndv_high_tb"

    sql """set enable_sql_cache=false;"""
    sql """create database if not exists ${db_name}"""
    sql """use ${db_name}"""

    sql """DROP TABLE IF EXISTS ${dist_ndv_low_tb};"""

    sql """CREATE TABLE ${dist_ndv_low_tb} (
            dist_key BIGINT,
            a BIGINT,
            b BIGINT,
            c BIGINT,
            d BIGINT,
            e BIGINT,
            f BIGINT,
            g bigint,
            h bigint,
            i bigint,
            j bigint,
            v BIGINT,
        )
        DUPLICATE KEY(dist_key, a, b, c)
        DISTRIBUTED BY HASH(dist_key) BUCKETS 32
        PROPERTIES (
            "replication_num" = "1"
        );"""

    sql """INSERT INTO ${dist_ndv_low_tb}
        SELECT
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_dist_10000000')), 2147483647) % 10000000) * 100000 + 1 AS dist_key,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_a_1000')), 2147483647) % 1000) * 100000 + 1 AS a,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_b_5000')), 2147483647) % 5000) * 100000 + 2 AS b,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_c_40000000')), 2147483647) % 40000000) * 100000 + 3 AS c,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_d_45000000')), 2147483647) % 45000000) * 100000 + 4 AS d,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_e_50000000')), 2147483647) % 50000000) * 100000 + 5 AS e,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_f_55000000')), 2147483647) % 55000000) * 100000 + 6 AS f,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_g_60000000')), 2147483647) % 60000000) * 100000 + 7 AS g,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_h_65000000')), 2147483647) % 65000000) * 100000 + 8 AS h,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_i_70000000')), 2147483647) % 70000000) * 100000 + 9 AS i,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_j_75000000')), 2147483647) % 75000000) * 100000 + 10 AS j,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_v_80000000')), 2147483647) % 80000000) * 100000 + 11 AS v
        FROM numbers("number" = "100000000") AS tmp;"""


    sql """DROP TABLE IF EXISTS ${dist_ndv_high_tb};"""

    sql """CREATE TABLE ${dist_ndv_high_tb} (
            dist_key BIGINT,
            a BIGINT,
            b BIGINT,
            c BIGINT,
            d BIGINT,
            e BIGINT,
            f BIGINT,
            g bigint,
            h bigint,
            i bigint,
            j bigint,
            v BIGINT,
        )
        DUPLICATE KEY(dist_key, a, b, c)
        DISTRIBUTED BY HASH(dist_key) BUCKETS 32
        PROPERTIES (
            "replication_num" = "1"
        );"""

    sql """INSERT INTO ${dist_ndv_high_tb}
        SELECT
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_dist_85000000')), 2147483647) % 85000000) * 100000 + 12 AS dist_key,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_a_1000')), 2147483647) % 1000) * 100000 + 1 AS a,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_b_5000')), 2147483647) % 5000) * 100000 + 2 AS b,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_c_40000000')), 2147483647) % 40000000) * 100000 + 3 AS c,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_d_45000000')), 2147483647) % 45000000) * 100000 + 4 AS d,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_e_50000000')), 2147483647) % 50000000) * 100000 + 5 AS e,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_f_55000000')), 2147483647) % 55000000) * 100000 + 6 AS f,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_g_60000000')), 2147483647) % 60000000) * 100000 + 7 AS g,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_h_65000000')), 2147483647) % 65000000) * 100000 + 8 AS h,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_i_70000000')), 2147483647) % 70000000) * 100000 + 9 AS i,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_j_75000000')), 2147483647) % 75000000) * 100000 + 10 AS j,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_v_80000000')), 2147483647) % 80000000) * 100000 + 11 AS v
        FROM numbers("number" = "100000000") AS tmp;"""


    sql """DROP TABLE IF EXISTS ${random_ndv_low_tb};"""

    sql """CREATE TABLE ${random_ndv_low_tb} (
            dist_key BIGINT,
            a BIGINT,
            b BIGINT,
            c BIGINT,
            d BIGINT,
            e BIGINT,
            f BIGINT,
            g bigint,
            h bigint,
            i bigint,
            j bigint,
            v BIGINT,
        )
        DUPLICATE KEY(dist_key, a, b, c)
        DISTRIBUTED BY random BUCKETS 32
        PROPERTIES (
            "replication_num" = "1"
        );"""

    sql """INSERT INTO ${random_ndv_low_tb}
        SELECT
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_dist_10000000')), 2147483647) % 10000000) * 100000 + 1 AS dist_key,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_a_1000')), 2147483647) % 1000) * 100000 + 1 AS a,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_b_5000')), 2147483647) % 5000) * 100000 + 2 AS b,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_c_40000000')), 2147483647) % 40000000) * 100000 + 3 AS c,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_d_45000000')), 2147483647) % 45000000) * 100000 + 4 AS d,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_e_50000000')), 2147483647) % 50000000) * 100000 + 5 AS e,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_f_55000000')), 2147483647) % 55000000) * 100000 + 6 AS f,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_g_60000000')), 2147483647) % 60000000) * 100000 + 7 AS g,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_h_65000000')), 2147483647) % 65000000) * 100000 + 8 AS h,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_i_70000000')), 2147483647) % 70000000) * 100000 + 9 AS i,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_j_75000000')), 2147483647) % 75000000) * 100000 + 10 AS j,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_v_80000000')), 2147483647) % 80000000) * 100000 + 11 AS v
        FROM numbers("number" = "100000000") AS tmp;"""


    sql """DROP TABLE IF EXISTS ${random_ndv_high_tb};"""

    sql """CREATE TABLE ${random_ndv_high_tb} (
            dist_key BIGINT,
            a BIGINT,
            b BIGINT,
            c BIGINT,
            d BIGINT,
            e BIGINT,
            f BIGINT,
            g bigint,
            h bigint,
            i bigint,
            j bigint,
            v BIGINT,
        )
        DUPLICATE KEY(dist_key, a, b, c)
        DISTRIBUTED BY random BUCKETS 32
        PROPERTIES (
            "replication_num" = "1"
        );"""

    sql """INSERT INTO ${random_ndv_high_tb}
        SELECT
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_dist_85000000')), 2147483647) % 85000000) * 100000 + 12 AS dist_key,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_a_1000')), 2147483647) % 1000) * 100000 + 1 AS a,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_b_5000')), 2147483647) % 5000) * 100000 + 2 AS b,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_c_40000000')), 2147483647) % 40000000) * 100000 + 3 AS c,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_d_45000000')), 2147483647) % 45000000) * 100000 + 4 AS d,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_e_50000000')), 2147483647) % 50000000) * 100000 + 5 AS e,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_f_55000000')), 2147483647) % 55000000) * 100000 + 6 AS f,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_g_60000000')), 2147483647) % 60000000) * 100000 + 7 AS g,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_h_65000000')), 2147483647) % 65000000) * 100000 + 8 AS h,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_i_70000000')), 2147483647) % 70000000) * 100000 + 9 AS i,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_j_75000000')), 2147483647) % 75000000) * 100000 + 10 AS j,
            (bitand(murmur_hash3_32(concat(cast(tmp.number AS STRING), '_v_80000000')), 2147483647) % 80000000) * 100000 + 11 AS v
        FROM numbers("number" = "100000000") AS tmp;"""


    sql """ANALYZE TABLE ${dist_ndv_low_tb} WITH SYNC WITH SAMPLE ROWS 24000000;"""
    sql """ANALYZE TABLE ${dist_ndv_high_tb} WITH SYNC WITH SAMPLE ROWS 24000000;"""
    sql """ANALYZE TABLE ${random_ndv_low_tb} WITH SYNC WITH SAMPLE ROWS 24000000;"""
    sql """ANALYZE TABLE ${random_ndv_high_tb} WITH SYNC WITH SAMPLE ROWS 24000000;"""

    def dist_ndv_low_tb_ndvRes = sql """SELECT
                    CAST(NDV(dist_key) * 1.0 / COUNT(*) < 0.3 AS INT),
                    CAST(NDV(a) * 1.0 / COUNT(*) < 0.3 AS INT),
                    CAST(NDV(b) * 1.0 / COUNT(*) < 0.3 AS INT),
                    CAST(NDV(c) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(d) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(e) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(f) * 1.0 / COUNT(*) >= 0.3 AS INT)
                FROM ${dist_ndv_low_tb};"""
    assertTrue(dist_ndv_low_tb_ndvRes[0][0] == 1)
    assertTrue(dist_ndv_low_tb_ndvRes[0][1] == 1)
    assertTrue(dist_ndv_low_tb_ndvRes[0][2] == 1)
    assertTrue(dist_ndv_low_tb_ndvRes[0][3] == 1)
    assertTrue(dist_ndv_low_tb_ndvRes[0][4] == 1)
    assertTrue(dist_ndv_low_tb_ndvRes[0][5] == 1)
    assertTrue(dist_ndv_low_tb_ndvRes[0][6] == 1)

    def dist_ndv_high_tb_ndvRes = sql """SELECT
                    CAST(NDV(dist_key) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(a) * 1.0 / COUNT(*) < 0.3 AS INT),
                    CAST(NDV(b) * 1.0 / COUNT(*) < 0.3 AS INT),
                    CAST(NDV(c) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(d) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(e) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(f) * 1.0 / COUNT(*) >= 0.3 AS INT)
                FROM ${dist_ndv_high_tb};"""
    assertTrue(dist_ndv_high_tb_ndvRes[0][0] == 1)
    assertTrue(dist_ndv_high_tb_ndvRes[0][1] == 1)
    assertTrue(dist_ndv_high_tb_ndvRes[0][2] == 1)
    assertTrue(dist_ndv_high_tb_ndvRes[0][3] == 1)
    assertTrue(dist_ndv_high_tb_ndvRes[0][4] == 1)
    assertTrue(dist_ndv_high_tb_ndvRes[0][5] == 1)
    assertTrue(dist_ndv_high_tb_ndvRes[0][6] == 1)

    def random_ndv_low_tb_ndvRes = sql """SELECT
                    CAST(NDV(dist_key) * 1.0 / COUNT(*) < 0.3 AS INT),
                    CAST(NDV(a) * 1.0 / COUNT(*) < 0.3 AS INT),
                    CAST(NDV(b) * 1.0 / COUNT(*) < 0.3 AS INT),
                    CAST(NDV(c) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(d) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(e) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(f) * 1.0 / COUNT(*) >= 0.3 AS INT)
                FROM ${random_ndv_low_tb};"""
    assertTrue(random_ndv_low_tb_ndvRes[0][0] == 1)
    assertTrue(random_ndv_low_tb_ndvRes[0][1] == 1)
    assertTrue(random_ndv_low_tb_ndvRes[0][2] == 1)
    assertTrue(random_ndv_low_tb_ndvRes[0][3] == 1)
    assertTrue(random_ndv_low_tb_ndvRes[0][4] == 1)
    assertTrue(random_ndv_low_tb_ndvRes[0][5] == 1)
    assertTrue(random_ndv_low_tb_ndvRes[0][6] == 1)

    def random_ndv_high_tb_ndvRes = sql """SELECT
                    CAST(NDV(dist_key) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(a) * 1.0 / COUNT(*) < 0.3 AS INT),
                    CAST(NDV(b) * 1.0 / COUNT(*) < 0.3 AS INT),
                    CAST(NDV(c) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(d) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(e) * 1.0 / COUNT(*) >= 0.3 AS INT),
                    CAST(NDV(f) * 1.0 / COUNT(*) >= 0.3 AS INT)
                FROM ${random_ndv_high_tb};"""
    assertTrue(random_ndv_high_tb_ndvRes[0][0] == 1)
    assertTrue(random_ndv_high_tb_ndvRes[0][1] == 1)
    assertTrue(random_ndv_high_tb_ndvRes[0][2] == 1)
    assertTrue(random_ndv_high_tb_ndvRes[0][3] == 1)
    assertTrue(random_ndv_high_tb_ndvRes[0][4] == 1)
    assertTrue(random_ndv_high_tb_ndvRes[0][5] == 1)
    assertTrue(random_ndv_high_tb_ndvRes[0][6] == 1)

    def joinSql = { List<String> cols ->
        cols.join(", ")
    }

    def extractEquivalenceExprIdsRaw = { String explainStr ->
        def raws = []
        def matcher = (explainStr =~ /(?s)equivalenceExprIds=\[(.*?)\],\s*exprIdToEquivalenceSet=/)
        while (matcher.find()) {
            raws << "equivalenceExprIds=[" + matcher.group(1).replaceAll("\\s+", "") + "]"
        }
        return raws
    }

    def wrapCountQuery = { List<String> selectExprs, String fromSql ->
        def aliasedExprs = []
        for (int i = 0; i < selectExprs.size(); i++) {
            aliasedExprs << "${selectExprs[i]} AS col${i + 1}"
        }
        def countExprs = (1..selectExprs.size()).collect { "count(col${it})" }.join(", ")
        return """select ${countExprs} from (
               SELECT ${aliasedExprs.join(",\n                      ")}
               ${fromSql}
           ) z"""
    }

    def buildSimpleAggQuery = { String tableName, List<String> groupKeys, boolean withCsmm ->
        def selectExprs = []
        selectExprs.addAll(groupKeys)
        selectExprs << "COUNT(*)"
        if (withCsmm) {
            selectExprs << "SUM(v)"
            selectExprs << "MIN(v)"
            selectExprs << "MAX(v)"
        }
        return wrapCountQuery(selectExprs, """
               FROM ${tableName}
               GROUP BY ${joinSql(groupKeys)}""")
    }

    def buildDistinctCountQuery = { String tableName, List<String> distinctKeys, List<String> groupKeys ->
        return """select count(cd_dist) from (
               SELECT COUNT(DISTINCT ${joinSql(distinctKeys)}) AS cd_dist
               FROM ${tableName}
               GROUP BY ${joinSql(groupKeys)}
           ) z"""
    }

    def buildWindowQuery = { String tableName, List<String> innerKeys, List<String> displayExprs,
                             List<String> partitionKeys, List<String> rowNumberOrderExprs,
                             List<String> outerOrderExprs ->
        def selectExprs = []
        selectExprs.addAll(displayExprs)
        selectExprs << """ROW_NUMBER() OVER (
                          PARTITION BY ${partitionKeys.collect { "t.${it}" }.join(", ")}
                          ORDER BY ${rowNumberOrderExprs.join(", ")}
                      )"""
        return wrapCountQuery(selectExprs, """
               FROM (
                   SELECT ${joinSql(innerKeys)}, SUM(v) AS sum_v
                   FROM ${tableName}
                   GROUP BY ${joinSql(innerKeys)}
               ) t
               ORDER BY ${outerOrderExprs.join(", ")}""")
    }

    def buildParentAggQuery = { String tableName, List<String> innerKeys, List<String> outerGroupExprs,
                                List<String> selectExprs ->
        return wrapCountQuery(selectExprs, """
               FROM (
                   SELECT ${joinSql(innerKeys)}, SUM(v) AS sum_v, COUNT(*) AS cnt_v
                   FROM ${tableName}
                   GROUP BY ${joinSql(innerKeys)}
               ) t
               GROUP BY ${outerGroupExprs.join(", ")}""")
    }

    def buildAggJoinScanQuery = { String tableName, List<String> innerKeys, List<String> joinKeys,
                                  List<String> selectExprs ->
        def joinCondition = joinKeys.collect { "l.${it} = r.${it}" }.join(" AND ")
        return wrapCountQuery(selectExprs, """
               FROM (
                   SELECT ${joinSql(innerKeys)}, SUM(v) AS sum_v
                   FROM ${tableName}
                   GROUP BY ${joinSql(innerKeys)}
               ) l
               JOIN ${tableName} r
                 ON ${joinCondition}""")
    }

    def buildScanJoinScanQuery = { String tableName, List<String> scanKeys, List<String> joinKeys,
                                   List<String> selectExprs ->
        def joinCondition = joinKeys.collect { "l.${it} = r.${it}" }.join(" AND ")
        return wrapCountQuery(selectExprs, """
               FROM (
                   SELECT ${joinSql(scanKeys)}
                   FROM ${tableName}
               ) l
               JOIN ${tableName} r
                 ON ${joinCondition}""")
    }

    def buildBothAggQuery = { String tableName, List<String> innerKeys, List<String> joinKeys,
                              List<String> selectExprs ->
        def joinCondition = joinKeys.collect { "l.${it} = r.${it}" }.join(" AND ")
        return wrapCountQuery(selectExprs, """
               FROM (
                   SELECT ${joinSql(innerKeys)}, SUM(v) AS sum_v
                   FROM ${tableName}
                   GROUP BY ${joinSql(innerKeys)}
               ) l
               JOIN (
                   SELECT ${joinSql(innerKeys)}, COUNT(*) AS cnt_v
                   FROM ${tableName}
                   GROUP BY ${joinSql(innerKeys)}
               ) r
                 ON ${joinCondition}""")
    }

    def buildNestedJoinQuery = { String tableName, List<String> scanKeys, List<String> joinKeys,
                                 List<String> selectExprs ->
        def leftJoinCondition = joinKeys.collect { "l.${it} = r.${it}" }.join(" AND ")
        def rightJoinCondition = joinKeys.collect { "j.${it} = s.${it}" }.join(" AND ")
        return wrapCountQuery(selectExprs, """
               FROM (
                   SELECT ${joinSql(scanKeys.collect { "l.${it}" })}, r.v AS left_v
                   FROM (
                       SELECT ${joinSql(scanKeys)}
                       FROM ${tableName}
                   ) l
                   JOIN ${tableName} r
                     ON ${leftJoinCondition}
               ) j
               JOIN ${tableName} s
                 ON ${rightJoinCondition}""")
    }

    def compareExplain = { String sqlStr, String label, int sqlIndex, def equivalenceExprIdsClose, def equivalenceExprIdsOpen ->
        sql """Set enable_agg_shuffle_key_prune=true;"""
        def explainClose = sql "explain physical plan " + sqlStr

        sql """Set enable_agg_shuffle_key_prune=false;"""
        def explainOpen = sql "explain physical plan " + sqlStr

        logger.info("${label}, sql_index=${sqlIndex}, explain_close: " + explainClose)
        logger.info("${label}, sql_index=${sqlIndex}, explain_open: " + explainOpen)

        def rawsClose = extractEquivalenceExprIdsRaw(explainClose.toString())
        def rawsOpen = extractEquivalenceExprIdsRaw(explainOpen.toString())

        equivalenceExprIdsClose.add(rawsClose.toString())
        equivalenceExprIdsOpen.add(rawsOpen.toString())
    }

    def runTimes = { String sqlStr, String label, int sqlIndex ->
        def times = []
        int runCount = 3
        for (int round = 1; round <= runCount; round++) {
            long start = System.currentTimeMillis()
            try {
                sql sqlStr
            } catch (Exception e1) {
                def msg = e1.getMessage()
                logger.info("${label}, sql_index=${sqlIndex} run error: ${msg}")
                if (msg == null || (!msg.contains("MEM_LIMIT_EXCEEDED") && !msg.contains("MEM_ALLOC_FAILED"))) {
                    throw e1
                }
                times << -1
                break
            }
            long duration = System.currentTimeMillis() - start
            times << duration
            println "${label} SQL ${sqlIndex}, 第 ${round} 轮耗时: ${duration} ms"
        }
        return times.min()
    }

    def runTimeGroup = { String label, List<String> sqlArr ->
        def timesClose = []
        def timesOpen = []
        def equivalenceExprIdsClose = []
        def equivalenceExprIdsOpen = []
        def splitBlockHashComputeTimeCloseArr = []
        def splitBlockHashComputeTimeOpenArr = []

        for (int i = 0; i < sqlArr.size(); i++) {
            sql sqlArr[i]
        }

        for (int i = 0; i < sqlArr.size(); i++) {
            compareExplain(sqlArr[i], label, i, equivalenceExprIdsClose, equivalenceExprIdsOpen)
            sql """Set enable_agg_shuffle_key_prune=true;"""
            timesClose << runTimes(sqlArr[i], label, i)
            sql """Set enable_agg_shuffle_key_prune=false;"""
            timesOpen << runTimes(sqlArr[i], label, i)
        }

        logger.info("${label} times_close=" + timesClose)
        logger.info("${label} times_open=" + timesOpen)
        logger.info("${label} equivalenceExprIdsClose=" + equivalenceExprIdsClose)
        logger.info("${label} equivalenceExprIdsOpen=" + equivalenceExprIdsOpen)
        logger.info("${label} SplitBlockHashComputeTimeCloseArr=" + splitBlockHashComputeTimeCloseArr)
        logger.info("${label} SplitBlockHashComputeTimeOpenArr=" + splitBlockHashComputeTimeOpenArr)
    }

    def makeGenericProfile = { String id, List<String> unsatKeys, List<String> satKeys, List<String> distinctGroupKeys ->
        return [
                id: id,
                unsatKeys: unsatKeys,
                satKeys: satKeys,
                distinctCountKeys: satKeys,
                distinctGroupKeys: distinctGroupKeys,
                shuffleGroupKeys: ["dist_key"] + distinctGroupKeys.subList(1, distinctGroupKeys.size()),
                multiDistinctKeys: unsatKeys,
                distKeyGroupKeys: ["a"] + distinctGroupKeys.subList(1, distinctGroupKeys.size())
        ]
    }

    def profiles = [
            [
                    id: "gby6",
                    unsatKeys: ["a", "b", "c", "d", "e", "f"],
                    satKeys: ["dist_key", "a", "b", "c", "d", "e", "f"],
                    distinctCountKeys: ["dist_key", "a", "b", "c", "d", "e"],
                    distinctGroupKeys: ["f", "g", "h", "i", "j", "v"],
                    shuffleGroupKeys: ["dist_key", "g", "h", "i", "j", "v"],
                    multiDistinctKeys: ["a", "b", "c", "d", "e", "f"],
                    distKeyGroupKeys: ["a", "g", "h", "i", "j", "v"],
                    windowDisplayExprs: ["t.a", "t.b", "t.c", "t.d", "t.e", "t.f", "t.sum_v"],
                    windowRowNumberOrderExprs: ["t.sum_v DESC", "t.a", "t.d", "t.e", "t.f"],
                    windowOuterOrderExprs: ["t.b", "t.c", "col8", "t.a", "t.d", "t.e", "t.f"],
                    aggSelectExprs: ["t.dist_key", "t.b", "t.c", "SUM(t.sum_v)", "MAX(t.cnt_v)"],
                    aggOuterGroupExprs: ["t.dist_key", "t.a", "t.b", "t.c", "t.d", "t.e", "t.f"],
                    aggJoinSelectExprs: ["l.a", "l.b", "l.sum_v", "r.v"],
                    scanJoinSelectExprs: ["l.a", "l.b", "l.c", "r.v"],
                    bothAggSelectExprs: ["l.a", "l.b", "l.sum_v", "r.cnt_v"],
                    nestedJoinSelectExprs: ["j.a", "j.b", "j.c", "j.left_v", "s.v"]
            ],
            makeGenericProfile("gby5", ["a", "b", "c", "d", "e"], ["dist_key", "a", "b", "c", "d"], ["f", "g", "h", "i", "j"]),
            makeGenericProfile("gby4", ["a", "b", "c", "d"], ["dist_key", "a", "b", "c"], ["f", "g", "h", "i"]),
            makeGenericProfile("gby3", ["a", "b", "c"], ["dist_key", "a", "b"], ["f", "g", "h"]),
            makeGenericProfile("gby2", ["a", "c"], ["dist_key", "a"], ["f", "g"])
    ]

    def buildQueriesForTable = { String tableName, Map profile ->
        def nonDistSatKeys = profile.satKeys.findAll { it != "dist_key" }
        def windowDisplayExprs = profile.windowDisplayExprs ?: (nonDistSatKeys.collect { "t.${it}" } + ["t.sum_v"])
        def windowRowNumberOrderExprs = profile.windowRowNumberOrderExprs ?: (["t.sum_v DESC"] + nonDistSatKeys.collect { "t.${it}" })
        def windowOuterOrderExprs = profile.windowOuterOrderExprs ?: (nonDistSatKeys.collect { "t.${it}" } + ["col${nonDistSatKeys.size() + 2}"])
        def aggOuterGroupExprs = profile.aggOuterGroupExprs ?: profile.satKeys.collect { "t.${it}" }
        def aggSelectExprs = profile.aggSelectExprs ?: (profile.satKeys.collect { "t.${it}" } + ["SUM(t.sum_v)", "MAX(t.cnt_v)"])
        def aggJoinSelectExprs = profile.aggJoinSelectExprs ?: (profile.satKeys.collect { "l.${it}" } + ["l.sum_v", "r.v"])
        def scanJoinSelectExprs = profile.scanJoinSelectExprs ?: (profile.satKeys.collect { "l.${it}" } + ["r.v"])
        def bothAggSelectExprs = profile.bothAggSelectExprs ?: (profile.satKeys.collect { "l.${it}" } + ["l.sum_v", "r.cnt_v"])
        def nestedJoinSelectExprs = profile.nestedJoinSelectExprs ?: (profile.satKeys.collect { "j.${it}" } + ["j.left_v", "s.v"])

        return [
                buildSimpleAggQuery(tableName, profile.unsatKeys, false),
                buildSimpleAggQuery(tableName, profile.satKeys, false),
                buildSimpleAggQuery(tableName, profile.unsatKeys, true),
                buildSimpleAggQuery(tableName, profile.satKeys, true),
                buildDistinctCountQuery(tableName, profile.distinctCountKeys, profile.distinctGroupKeys),
                buildDistinctCountQuery(tableName, ["a"], profile.shuffleGroupKeys),
                buildDistinctCountQuery(tableName, profile.multiDistinctKeys, profile.shuffleGroupKeys),
                buildDistinctCountQuery(tableName, ["dist_key"], profile.distKeyGroupKeys),
                buildWindowQuery(tableName, profile.satKeys, windowDisplayExprs, profile.satKeys,
                        windowRowNumberOrderExprs, windowOuterOrderExprs),
                buildParentAggQuery(tableName, profile.satKeys, aggOuterGroupExprs, aggSelectExprs),
                buildAggJoinScanQuery(tableName, profile.satKeys, profile.satKeys, aggJoinSelectExprs),
                buildScanJoinScanQuery(tableName, profile.satKeys, profile.satKeys, scanJoinSelectExprs),
                buildBothAggQuery(tableName, profile.satKeys, profile.satKeys, bothAggSelectExprs),
                buildNestedJoinQuery(tableName, profile.satKeys, profile.satKeys, nestedJoinSelectExprs)
        ]
    }

    def tableProfiles = [
            [id: "dist_ndv_low_tb", tableName: dist_ndv_low_tb],
            [id: "dist_ndv_high_tb", tableName: dist_ndv_high_tb],
            [id: "random_ndv_low_tb", tableName: random_ndv_low_tb],
            [id: "random_ndv_high_tb", tableName: random_ndv_high_tb]
    ]

    profiles.each { profile ->
        tableProfiles.each { tableProfile ->
            def label = "${profile.id}_${tableProfile.id}"
            def queries = buildQueriesForTable(tableProfile.tableName, profile)
            runTimeGroup(label, queries)
        }
    }
}
