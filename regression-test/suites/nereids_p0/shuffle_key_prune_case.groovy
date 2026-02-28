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


suite("shuffle_key_prune_case") {

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_sql_cache=false"
    sql "set enable_query_cache=false"
    sql "set agg_phase=4"

    def getSplitBlockHashComputeTime = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        def splitBlockHashComputeTimes = []
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                if (exception != null) {
                    throw exception
                }
                if (profileString == null) {
                    return
                }
                def matcher = (profileString =~ /(?m)^\s*(?:-\s*)?SplitBlockHashComputeTime:\s*(.+)$/)
                while (matcher.find()) {
                    splitBlockHashComputeTimes << matcher.group(1).trim()
                }
                logger.info("SplitBlockHashComputeTime values: " + splitBlockHashComputeTimes)
            }
        }
        return splitBlockHashComputeTimes
    }

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


    sql """ANALYZE TABLE ${dist_ndv_low_tb} WITH SYNC;"""
    sql """ANALYZE TABLE ${dist_ndv_high_tb} WITH SYNC;"""
    sql """ANALYZE TABLE ${random_ndv_low_tb} WITH SYNC;"""
    sql """ANALYZE TABLE ${random_ndv_high_tb} WITH SYNC;"""

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

    // 提取所有 equivalenceExprIds=[...] 的原文片段
    def extractEquivalenceExprIdsRaw = { String explainStr ->
        def raws = []
        def matcher = (explainStr =~ /(?s)equivalenceExprIds=\[(.*?)\],\s*exprIdToEquivalenceSet=/)
        while (matcher.find()) {
            raws << "equivalenceExprIds=[" + matcher.group(1).replaceAll("\\s+", "") + "]"
        }
        return raws
    }

    def compare_explain = { String sqlStr, int i, def equivalenceExprIdsClose, def equivalenceExprIdsOpen ->
        sql """Set choose_one_agg_shuffle_key=true;"""
        def explainClose = sql "explain physical plan " + sqlStr

        sql """Set choose_one_agg_shuffle_key=false;"""
        def explainOpen = sql "explain physical plan " + sqlStr

        logger.info("sql_index=" + i + ", explain_close: " + explainClose)
        logger.info("sql_index=" + i + ", explain_open: " + explainOpen)

        def rawsClose = extractEquivalenceExprIdsRaw(explainClose.toString())
        def rawsOpen = extractEquivalenceExprIdsRaw(explainOpen.toString())

        equivalenceExprIdsClose.add(rawsClose.toString())
        equivalenceExprIdsOpen.add(rawsOpen.toString())
    }

    def run_times = { String sqlStr, def resArr, int i ->
        def times = []
        def runCount = 3
        for (int r = 1; r <= runCount; r++) {
            long start = System.currentTimeMillis()
            try {
                sql sqlStr
            } catch (Exception e1) {
                def msg = e1.getMessage()
                logger.info("sql_index=" + i + " run error: ${msg}")
                if (msg == null || (!msg.contains("MEM_LIMIT_EXCEEDED") && !msg.contains("MEM_ALLOC_FAILED"))) {
                    throw e1
                }
                times << -1
                break
            }
            long duration = System.currentTimeMillis() - start
            times << duration
            println "SQL ${i}, 第 ${r} 轮耗时: ${duration} ms"
        }
        resArr.add(times.min())
    }


    def runTimeFunc = { List<String> cur_sql_arr, def timesClose, def timesOpen,  def equivalenceExprIdsClose,
                        def equivalenceExprIdsOpen, def SplitBlockHashComputeTimeCloseArr, def SplitBlockHashComputeTimeOpenArr ->
        for (int i = 0; i < cur_sql_arr.size(); i++) {
            sql cur_sql_arr[i]
        }

        for (int i = 0; i < cur_sql_arr.size(); i++) {
            compare_explain(cur_sql_arr[i], i, equivalenceExprIdsClose, equivalenceExprIdsOpen)
            sql """Set choose_one_agg_shuffle_key=true;"""
            run_times(cur_sql_arr[i], timesClose, i)
            SplitBlockHashComputeTimeCloseArr.add(getSplitBlockHashComputeTime(cur_sql_arr[i]))
            sql """Set choose_one_agg_shuffle_key=false;"""
            run_times(cur_sql_arr[i], timesOpen, i)
            SplitBlockHashComputeTimeOpenArr.add(getSplitBlockHashComputeTime(cur_sql_arr[i]))
        }

    }

    def dist_ndv_low_tb_sql = [
            // baseline gby6
            // sql本身不满足分布，需要优化
            """select count(a), count(b), count(c), count(d), count(e), count(f), count(c1) from (
               SELECT a, b, c, d, e, f, COUNT(*) AS c1
               FROM ${dist_ndv_low_tb}
               GROUP BY a, b, c, d, e, f) t""",

            // sql本身满足分布
            """select count(dist_key), count(a), count(b), count(c), count(d), count(e), count(f), count(c1) from (
               SELECT dist_key,a, b, c, d, e, f, COUNT(*) AS c1
               FROM ${dist_ndv_low_tb}
               GROUP BY dist_key, a, b, c, d, e, f) t""",

            // sql本身不满足分布
            """select count(a), count(b), count(c), count(d), count(e), count(f), count(c1), count(s1), count(mn), count(mx) from (
               SELECT a, b, c, d, e, f, COUNT(*) AS c1, SUM(v) AS s1, MIN(v) AS mn, MAX(v) AS mx
               FROM ${dist_ndv_low_tb}
               GROUP BY a, b, c, d, e, f) t""",

            // sql本身满足分布
            """select count(dist_key), count(a), count(b), count(c), count(d), count(e), count(f), count(c1), count(s1), count(mn), count(mx) from (
               SELECT dist_key, a, b, c, d, e, f, COUNT(*) AS c1, SUM(v) AS s1, MIN(v) AS mn, MAX(v) AS mx
               FROM ${dist_ndv_low_tb}
               GROUP BY dist_key, a, b, c, d, e, f) t""",

            // scene2: multi distinct + multi group by (SplitAggMultiPhase)
            // set agg_phase=4
            """select count(cd_dist) from (
               SELECT COUNT(DISTINCT dist_key, a, b, c, d, e) AS cd_dist 
               FROM ${dist_ndv_low_tb}
               GROUP BY f, g, h, i, j, v) z""",

            // shuffle
            """select count(cd_dist) from (SELECT COUNT(DISTINCT a) AS cd_dist 
               FROM ${dist_ndv_low_tb}
               GROUP BY dist_key, g, h, i, j, v) z""",

            """select count(cd_dist) from (SELECT COUNT(DISTINCT a, b, c, d, e, f) AS cd_dist 
               FROM ${dist_ndv_low_tb}
               GROUP BY dist_key, g, h, i, j, v) z""",

            // shuffle
            """select count(cd_dist) from (SELECT COUNT(DISTINCT dist_key) AS cd_dist 
               FROM ${dist_ndv_low_tb}
               GROUP BY a, g, h, i, j, v) z""",


            // scene3: parent has hash request
            // parent = window
            """select count(col1), count(col2), count(col3), count(col4), count(col5), count(col6), count(col7), count(rn) from (
               SELECT t.a as col1, t.b as col2, t.c as col3, t.d as col4, t.e as col5, t.f as col6, t.sum_v as col7,
                      ROW_NUMBER() OVER (
                          PARTITION BY t.dist_key, t.a, t.b, t.c, t.d, t.e, t.f
                          ORDER BY t.sum_v DESC, t.a, t.d, t.e, t.f
                      ) AS rn
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v
                   FROM ${dist_ndv_low_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) t
               ORDER BY t.b, t.c, rn, t.a, t.d, t.e, t.f) z""",

            // parent = agg
            """select count(col1), count(col2), count(col3), count(total_sum_v), count(max_cnt_v) from (
               SELECT t.dist_key as col1, t.b as col2, t.c as col3, SUM(t.sum_v) AS total_sum_v, MAX(t.cnt_v) AS max_cnt_v
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v, COUNT(*) AS cnt_v
                   FROM ${dist_ndv_low_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) t
               GROUP BY t.dist_key, t.a, t.b, t.c, t.d, t.e, t.f) z""",

            // parent = join (agg + scan)
            """select count(col1), count(col2), count(col3), count(col4) from (
               SELECT l.a as col1, l.b as col2, l.sum_v as col3, r.v as col4
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v
                   FROM ${dist_ndv_low_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) l
               JOIN ${dist_ndv_low_tb} r
                 ON l.dist_key and r.dist_key and l.a = r.a AND l.b = r.b and l.c = r.c AND l.d = r.d and l.e = r.e AND l.f = r.f) z""",


            // scene5: both children are agg
            // 换一下表名就可以构造满足分布和不满足分布的分支用例
            """select count(col1), count(col2), count(col3), count(col4) from (
               SELECT l.a as col1, l.b as col2, l.sum_v as col3, r.cnt_v as col4
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v
                   FROM ${dist_ndv_low_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) l
               JOIN (
                   SELECT dist_key, a, b, c, d, e, f, COUNT(*) AS cnt_v
                   FROM ${dist_ndv_low_tb}
                   GROUP BY dist_key,a, b, c, d, e, f
               ) r
                 ON l.dist_key = r.dist_key and l.a = r.a AND l.b = r.b and l.c = r.c AND l.d = r.d and l.e = r.e and l.f = r.f) z""",
    ]

    def timesClose1 = []
    def timesOpen1 = []

    def equivalenceExprIdsClose1 = []
    def equivalenceExprIdsOpen1 = []

    def SplitBlockHashComputeTimeCloseArr1 = []
    def SplitBlockHashComputeTimeOpenArr1 = []

    runTimeFunc(dist_ndv_low_tb_sql, timesClose1, timesOpen1, equivalenceExprIdsClose1, equivalenceExprIdsOpen1, SplitBlockHashComputeTimeCloseArr1, SplitBlockHashComputeTimeOpenArr1)
    logger.info("times_close1=" + timesClose1)
    logger.info("times_open1=" + timesOpen1)

    logger.info("equivalenceExprIdsClose1=" + equivalenceExprIdsClose1)
    logger.info("equivalenceExprIdsOpen1=" + equivalenceExprIdsOpen1)

    logger.info("SplitBlockHashComputeTimeCloseArr1=" + SplitBlockHashComputeTimeCloseArr1)
    logger.info("SplitBlockHashComputeTimeOpenArr1=" + SplitBlockHashComputeTimeOpenArr1)


    def dist_ndv_high_tb_sql = [
            // baseline gby6
            // sql本身不满足分布，需要优化
            """select count(a), count(b), count(c), count(d), count(e), count(f), count(c1) from (
               SELECT a, b, c, d, e, f, COUNT(*) AS c1
               FROM ${dist_ndv_high_tb}
               GROUP BY a, b, c, d, e, f) t""",

            // sql本身满足分布
            """select count(dist_key), count(a), count(b), count(c), count(d), count(e), count(f), count(c1) from (
               SELECT dist_key,a, b, c, d, e, f, COUNT(*) AS c1
               FROM ${dist_ndv_high_tb}
               GROUP BY dist_key, a, b, c, d, e, f) t""",

            // sql本身不满足分布
            """select count(a), count(b), count(c), count(d), count(e), count(f), count(c1), count(s1), count(mn), count(mx) from (
               SELECT a, b, c, d, e, f, COUNT(*) AS c1, SUM(v) AS s1, MIN(v) AS mn, MAX(v) AS mx
               FROM ${dist_ndv_high_tb}
               GROUP BY a, b, c, d, e, f) t""",

            // sql本身满足分布
            """select count(dist_key), count(a), count(b), count(c), count(d), count(e), count(f), count(c1), count(s1), count(mn), count(mx) from (
               SELECT dist_key, a, b, c, d, e, f, COUNT(*) AS c1, SUM(v) AS s1, MIN(v) AS mn, MAX(v) AS mx
               FROM ${dist_ndv_high_tb}
               GROUP BY dist_key, a, b, c, d, e, f) t""",

            // scene2: multi distinct + multi group by (SplitAggMultiPhase)
            // set agg_phase=4
            """select count(cd_dist) from (SELECT COUNT(DISTINCT dist_key, a, b, c, d, e) AS cd_dist 
               FROM ${dist_ndv_high_tb}
               GROUP BY f, g, h, i, j, v) z""",

            // shuffle
            """select count(cd_dist) from (SELECT COUNT(DISTINCT a) AS cd_dist 
               FROM ${dist_ndv_high_tb}
               GROUP BY dist_key, g, h, i, j, v) z""",

            """select count(cd_dist) from (SELECT COUNT(DISTINCT a, b, c, d, e, f) AS cd_dist 
               FROM ${dist_ndv_high_tb}
               GROUP BY dist_key, g, h, i, j, v) z""",

            // shuffle
            """select count(cd_dist) from (SELECT COUNT(DISTINCT dist_key) AS cd_dist 
               FROM ${dist_ndv_high_tb}
               GROUP BY a, g, h, i, j, v) z""",


            // scene3: parent has hash request
            // parent = window
            """select count(col1), count(col2), count(col3), count(col4), count(col5), count(col6), count(col7), count(rn) from (
               SELECT t.a as col1, t.b as col2, t.c as col3, t.d as col4, t.e as col5, t.f as col6, t.sum_v as col7,
                      ROW_NUMBER() OVER (
                          PARTITION BY t.dist_key, t.a, t.b, t.c, t.d, t.e, t.f
                          ORDER BY t.sum_v DESC, t.a, t.d, t.e, t.f
                      ) AS rn
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v
                   FROM ${dist_ndv_high_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) t
               ORDER BY t.b, t.c, rn, t.a, t.d, t.e, t.f) z""",

            // parent = agg
            """select count(col1), count(col2), count(col3), count(total_sum_v), count(max_cnt_v) from (
               SELECT t.dist_key as col1, t.b as col2, t.c as col3, SUM(t.sum_v) AS total_sum_v, MAX(t.cnt_v) AS max_cnt_v
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v, COUNT(*) AS cnt_v
                   FROM ${dist_ndv_high_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) t
               GROUP BY t.dist_key, t.a, t.b, t.c, t.d, t.e, t.f) z""",

            // parent = join (agg + scan)
            """select count(col1), count(col2), count(col3), count(col4) from (
               SELECT l.a as col1, l.b as col2, l.sum_v as col3, r.v as col4
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v
                   FROM ${dist_ndv_high_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) l
               JOIN ${dist_ndv_high_tb} r
                 ON l.dist_key and r.dist_key and l.a = r.a AND l.b = r.b and l.c = r.c AND l.d = r.d and l.e = r.e AND l.f = r.f) z""",


            // scene5: both children are agg
            // 换一下表名就可以构造满足分布和不满足分布的分支用例
            """select count(col1), count(col2), count(col3), count(col4) from (
               SELECT l.a as col1, l.b as col2, l.sum_v as col3, r.cnt_v as col4
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v
                   FROM ${dist_ndv_high_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) l
               JOIN (
                   SELECT dist_key, a, b, c, d, e, f, COUNT(*) AS cnt_v
                   FROM ${dist_ndv_high_tb}
                   GROUP BY dist_key,a, b, c, d, e, f
               ) r
                 ON l.dist_key = r.dist_key and l.a = r.a AND l.b = r.b and l.c = r.c AND l.d = r.d and l.e = r.e and l.f = r.f) z""",

    ]

    def timesClose2 = []
    def timesOpen2 = []

    def equivalenceExprIdsClose2 = []
    def equivalenceExprIdsOpen2 = []

    def SplitBlockHashComputeTimeCloseArr2 = []
    def SplitBlockHashComputeTimeOpenArr2 = []

    runTimeFunc(dist_ndv_high_tb_sql, timesClose2, timesOpen2, equivalenceExprIdsClose2, equivalenceExprIdsOpen2, SplitBlockHashComputeTimeCloseArr2, SplitBlockHashComputeTimeOpenArr2)
    logger.info("times_close2=" + timesClose2)
    logger.info("times_open2=" + timesOpen2)

    logger.info("equivalenceExprIdsClose2=" + equivalenceExprIdsClose2)
    logger.info("equivalenceExprIdsOpen2=" + equivalenceExprIdsOpen2)

    logger.info("SplitBlockHashComputeTimeCloseArr2=" + SplitBlockHashComputeTimeCloseArr2)
    logger.info("SplitBlockHashComputeTimeOpenArr2=" + SplitBlockHashComputeTimeOpenArr2)


    def random_ndv_low_tb_sql = [

            // baseline gby6
            // sql本身不满足分布，需要优化
            """select count(a), count(b), count(c), count(d), count(e), count(f), count(c1) from (
               SELECT a, b, c, d, e, f, COUNT(*) AS c1
               FROM ${random_ndv_low_tb}
               GROUP BY a, b, c, d, e, f) t""",

            // sql本身满足分布
            """select count(dist_key), count(a), count(b), count(c), count(d), count(e), count(f), count(c1) from (
               SELECT dist_key,a, b, c, d, e, f, COUNT(*) AS c1
               FROM ${random_ndv_low_tb}
               GROUP BY dist_key, a, b, c, d, e, f) t""",

            // sql本身不满足分布
            """select count(a), count(b), count(c), count(d), count(e), count(f), count(c1), count(s1), count(mn), count(mx) from (
               SELECT a, b, c, d, e, f, COUNT(*) AS c1, SUM(v) AS s1, MIN(v) AS mn, MAX(v) AS mx
               FROM ${random_ndv_low_tb}
               GROUP BY a, b, c, d, e, f) t""",

            // sql本身满足分布
            """select count(dist_key), count(a), count(b), count(c), count(d), count(e), count(f), count(c1), count(s1), count(mn), count(mx) from (
               SELECT dist_key, a, b, c, d, e, f, COUNT(*) AS c1, SUM(v) AS s1, MIN(v) AS mn, MAX(v) AS mx
               FROM ${random_ndv_low_tb}
               GROUP BY dist_key, a, b, c, d, e, f) t""",

            // scene2: multi distinct + multi group by (SplitAggMultiPhase)
            // set agg_phase=4
            """select count(cd_dist) from (SELECT COUNT(DISTINCT dist_key, a, b, c, d, e) AS cd_dist 
               FROM ${random_ndv_low_tb}
               GROUP BY f, g, h, i, j, v) z""",

            // shuffle
            """select count(cd_dist) from (SELECT COUNT(DISTINCT a) AS cd_dist 
               FROM ${random_ndv_low_tb}
               GROUP BY dist_key, g, h, i, j, v) z""",

            """select count(cd_dist) from (SELECT COUNT(DISTINCT a, b, c, d, e, f) AS cd_dist 
               FROM ${random_ndv_low_tb}
               GROUP BY dist_key, g, h, i, j, v) z""",

            // shuffle
            """select count(cd_dist) from (SELECT COUNT(DISTINCT dist_key) AS cd_dist 
               FROM ${random_ndv_low_tb}
               GROUP BY a, g, h, i, j, v) z""",


            // scene3: parent has hash request
            // parent = window
            """select count(col1), count(col2), count(col3), count(col4), count(col5), count(col6), count(col7), count(rn) from (
               SELECT t.a as col1, t.b as col2, t.c as col3, t.d as col4, t.e as col5, t.f as col6, t.sum_v as col7,
                      ROW_NUMBER() OVER (
                          PARTITION BY t.dist_key, t.a, t.b, t.c, t.d, t.e, t.f
                          ORDER BY t.sum_v DESC, t.a, t.d, t.e, t.f
                      ) AS rn
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v
                   FROM ${random_ndv_low_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) t
               ORDER BY t.b, t.c, rn, t.a, t.d, t.e, t.f) z""",

            // parent = agg
            """select count(col1), count(col2), count(col3), count(total_sum_v), count(max_cnt_v) from (
               SELECT t.dist_key as col1, t.b as col2, t.c as col3, SUM(t.sum_v) AS total_sum_v, MAX(t.cnt_v) AS max_cnt_v
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v, COUNT(*) AS cnt_v
                   FROM ${random_ndv_low_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) t
               GROUP BY t.dist_key, t.a, t.b, t.c, t.d, t.e, t.f) z""",

            // parent = join (agg + scan)
            """select count(col1), count(col2), count(col3), count(col4) from (
               SELECT l.a as col1, l.b as col2, l.sum_v as col3, r.v as col4
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v
                   FROM ${random_ndv_low_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) l
               JOIN ${random_ndv_low_tb} r
                 ON l.dist_key and r.dist_key and l.a = r.a AND l.b = r.b and l.c = r.c AND l.d = r.d and l.e = r.e AND l.f = r.f) z""",


            // scene5: both children are agg
            // 换一下表名就可以构造满足分布和不满足分布的分支用例
            """select count(col1), count(col2), count(col3), count(col4) from (
               SELECT l.a as col1, l.b as col2, l.sum_v as col3, r.cnt_v as col4
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v
                   FROM ${random_ndv_low_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) l
               JOIN (
                   SELECT dist_key, a, b, c, d, e, f, COUNT(*) AS cnt_v
                   FROM ${random_ndv_low_tb}
                   GROUP BY dist_key,a, b, c, d, e, f
               ) r
                 ON l.dist_key = r.dist_key and l.a = r.a AND l.b = r.b and l.c = r.c AND l.d = r.d and l.e = r.e and l.f = r.f) z""",
    ]

    def timesClose3 = []
    def timesOpen3 = []

    def equivalenceExprIdsClose3 = []
    def equivalenceExprIdsOpen3 = []

    def SplitBlockHashComputeTimeCloseArr3 = []
    def SplitBlockHashComputeTimeOpenArr3 = []

    runTimeFunc(random_ndv_low_tb_sql, timesClose3, timesOpen3, equivalenceExprIdsClose3, equivalenceExprIdsOpen3, SplitBlockHashComputeTimeCloseArr3, SplitBlockHashComputeTimeOpenArr3)
    logger.info("times_close3=" + timesClose3)
    logger.info("times_open3=" + timesOpen3)

    logger.info("equivalenceExprIdsClose3=" + equivalenceExprIdsClose3)
    logger.info("equivalenceExprIdsOpen3=" + equivalenceExprIdsOpen3)

    logger.info("SplitBlockHashComputeTimeCloseArr3=" + SplitBlockHashComputeTimeCloseArr3)
    logger.info("SplitBlockHashComputeTimeOpenArr3=" + SplitBlockHashComputeTimeOpenArr3)

    def random_ndv_high_tb_sql = [
            // baseline gby6
            // sql本身不满足分布，需要优化
            """select count(a), count(b), count(c), count(d), count(e), count(f), count(c1) from (
               SELECT a, b, c, d, e, f, COUNT(*) AS c1
               FROM ${random_ndv_high_tb}
               GROUP BY a, b, c, d, e, f) t""",

            // sql本身满足分布
            """select count(dist_key), count(a), count(b), count(c), count(d), count(e), count(f), count(c1) from (
               SELECT dist_key,a, b, c, d, e, f, COUNT(*) AS c1
               FROM ${random_ndv_high_tb}
               GROUP BY dist_key, a, b, c, d, e, f) t""",

            // sql本身不满足分布
            """select count(a), count(b), count(c), count(d), count(e), count(f), count(c1), count(s1), count(mn), count(mx) from (
               SELECT a, b, c, d, e, f, COUNT(*) AS c1, SUM(v) AS s1, MIN(v) AS mn, MAX(v) AS mx
               FROM ${random_ndv_high_tb}
               GROUP BY a, b, c, d, e, f) t""",

            // sql本身满足分布
            """select count(dist_key), count(a), count(b), count(c), count(d), count(e), count(f), count(c1), count(s1), count(mn), count(mx) from (
               SELECT dist_key, a, b, c, d, e, f, COUNT(*) AS c1, SUM(v) AS s1, MIN(v) AS mn, MAX(v) AS mx
               FROM ${random_ndv_high_tb}
               GROUP BY dist_key, a, b, c, d, e, f) t""",

            // scene2: multi distinct + multi group by (SplitAggMultiPhase)
            // set agg_phase=4
            """select count(cd_dist) from (SELECT COUNT(DISTINCT dist_key, a, b, c, d, e) AS cd_dist 
               FROM ${random_ndv_high_tb}
               GROUP BY f, g, h, i, j, v) z""",

            // shuffle
            """select count(cd_dist) from (SELECT COUNT(DISTINCT a) AS cd_dist 
               FROM ${random_ndv_high_tb}
               GROUP BY dist_key, g, h, i, j, v) z""",

            """select count(cd_dist) from (SELECT COUNT(DISTINCT a, b, c, d, e, f) AS cd_dist 
               FROM ${random_ndv_high_tb}
               GROUP BY dist_key, g, h, i, j, v) z""",

            // shuffle
            """select count(cd_dist) from (SELECT COUNT(DISTINCT dist_key) AS cd_dist 
               FROM ${random_ndv_high_tb}
               GROUP BY a, g, h, i, j, v) z""",


            // scene3: parent has hash request
            // parent = window
            """select count(col1), count(col2), count(col3), count(col4), count(col5), count(col6), count(col7), count(rn) from (
               SELECT t.a as col1, t.b as col2, t.c as col3, t.d as col4, t.e as col5, t.f as col6, t.sum_v as col7,
                      ROW_NUMBER() OVER (
                          PARTITION BY t.dist_key, t.a, t.b, t.c, t.d, t.e, t.f
                          ORDER BY t.sum_v DESC, t.a, t.d, t.e, t.f
                      ) AS rn
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v
                   FROM ${random_ndv_high_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) t
               ORDER BY t.b, t.c, rn, t.a, t.d, t.e, t.f) z""",

            // parent = agg
            """select count(col1), count(col2), count(col3), count(total_sum_v), count(max_cnt_v) from (
               SELECT t.dist_key as col1, t.b as col2, t.c as col3, SUM(t.sum_v) AS total_sum_v, MAX(t.cnt_v) AS max_cnt_v
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v, COUNT(*) AS cnt_v
                   FROM ${random_ndv_high_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) t
               GROUP BY t.dist_key, t.a, t.b, t.c, t.d, t.e, t.f) z""",

            // parent = join (agg + scan)
            """select count(col1), count(col2), count(col3), count(col4) from (
               SELECT l.a as col1, l.b as col2, l.sum_v as col3, r.v as col4
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v
                   FROM ${random_ndv_high_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) l
               JOIN ${random_ndv_high_tb} r
                 ON l.dist_key and r.dist_key and l.a = r.a AND l.b = r.b and l.c = r.c AND l.d = r.d and l.e = r.e AND l.f = r.f) z""",


            // scene5: both children are agg
            // 换一下表名就可以构造满足分布和不满足分布的分支用例
            """select count(col1), count(col2), count(col3), count(col4) from (
               SELECT l.a as col1, l.b as col2, l.sum_v as col3, r.cnt_v as col4
               FROM (
                   SELECT dist_key, a, b, c, d, e, f, SUM(v) AS sum_v
                   FROM ${random_ndv_high_tb}
                   GROUP BY dist_key, a, b, c, d, e, f
               ) l
               JOIN (
                   SELECT dist_key, a, b, c, d, e, f, COUNT(*) AS cnt_v
                   FROM ${random_ndv_high_tb}
                   GROUP BY dist_key,a, b, c, d, e, f
               ) r
                 ON l.dist_key = r.dist_key and l.a = r.a AND l.b = r.b and l.c = r.c AND l.d = r.d and l.e = r.e and l.f = r.f) z""",
    ]

    def timesClose4 = []
    def timesOpen4 = []

    def equivalenceExprIdsClose4 = []
    def equivalenceExprIdsOpen4 = []

    def SplitBlockHashComputeTimeCloseArr4 = []
    def SplitBlockHashComputeTimeOpenArr4 = []

    runTimeFunc(random_ndv_high_tb_sql, timesClose4, timesOpen4, equivalenceExprIdsClose4, equivalenceExprIdsOpen4, SplitBlockHashComputeTimeCloseArr4, SplitBlockHashComputeTimeOpenArr4)
    logger.info("times_close4=" + timesClose4)
    logger.info("times_open4=" + timesOpen4)

    logger.info("equivalenceExprIdsClose4=" + equivalenceExprIdsClose4)
    logger.info("equivalenceExprIdsOpen4=" + equivalenceExprIdsOpen4)

    logger.info("SplitBlockHashComputeTimeCloseArr4=" + SplitBlockHashComputeTimeCloseArr4)
    logger.info("SplitBlockHashComputeTimeOpenArr4=" + SplitBlockHashComputeTimeOpenArr4)

}
