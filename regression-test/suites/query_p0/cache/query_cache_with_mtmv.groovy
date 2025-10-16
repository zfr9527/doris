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

import java.util.stream.Collectors

suite("query_cache_with_mtmv") {
    def setSessionVariables = {
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set enable_sql_cache=false"
        sql "set enable_query_cache=true"
        sql """set enable_materialized_view_nest_rewrite=true;"""
    }

    def assertHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
            }

            check { profileString, exception ->
                assertTrue(profileString.contains("HitCache:  1")) && assertFalse(profileString.contains("HitCache:  0"))
            }
        }
    }

    def assertPartHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
            }

            check { profileString, exception ->
                assertTrue(profileString.contains("HitCache:  1")) && assertTrue(profileString.contains("HitCache:  0"))
            }
        }
    }

    def assertNoCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
            }

            check { profileString, exception ->
                assertTrue(profileString.contains("HitCache:  0")) && assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    def noQueryCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
            }

            check { profileString, exception ->
                assertFalse(profileString.contains("HitCache:  0")) && assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    def cur_create_async_partition_mv = { def db, def mv_name, def mv_sql, def partition_col ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${db}.${mv_name}"""
        sql """
                CREATE MATERIALIZED VIEW ${db}.${mv_name} 
                BUILD IMMEDIATE REFRESH auto ON MANUAL 
                -- PARTITION BY ${partition_col} 
                DISTRIBUTED BY RANDOM BUCKETS 2 
                PROPERTIES ('replication_num' = '1')  
                AS ${mv_sql}
                """
        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        sql "analyze table ${db}.${mv_name} with sync;"
        // force meta sync to avoid stale meta data on follower fe
        sql """sync;"""
    }

    def judge_res = { def res1, def res2 ->

        assertTrue(res1.size() == res2.size())
        for (int i = 0; i < res1.size(); i++) {
            assertTrue(res1[i].size() == res2[i].size())
            for (int j = 0; j < res1[i].size(); j++) {
                assertTrue(res1[i][j] == res2[i][j])
            }
        }
    }

    String dbName = context.config.getDbNameByFile(context.file)
    sql "ADMIN SET FRONTEND CONFIG ('cache_last_version_interval_second' = '0')"

    def create_table_and_insert = { def table_name ->
        sql """drop table if exists ${table_name}"""
        sql """CREATE TABLE ${table_name} (
                product_id INT NOT NULL,
                city VARCHAR(50) NOT NULL,
                sale_date DATE NOT NULL,
                amount DECIMAL(18, 2) NOT NULL
            )
            DUPLICATE KEY(product_id, city, sale_date)
            PARTITION BY RANGE(sale_date) (
                PARTITION p20251001 VALUES [('2025-10-01'), ('2025-10-02')),
                PARTITION p20251002 VALUES [('2025-10-02'), ('2025-10-03')),
                PARTITION p20251003 VALUES [('2025-10-03'), ('2025-10-04')),
                PARTITION p_other VALUES [('2025-10-04'), ('2025-11-01'))
            )
            DISTRIBUTED BY HASH(product_id) BUCKETS 10
            PROPERTIES (
                "replication_num" = "1"
            );"""
        sql """INSERT INTO ${table_name} (product_id, city, sale_date, amount) VALUES
            (101, 'Beijing', '2025-10-01', 100.00), -- p20251001
            (101, 'Shanghai', '2025-10-01', 150.00), -- p20251001
            (102, 'Beijing', '2025-10-02', 200.00), -- p20251002
            (102, 'Shanghai', '2025-10-02', 250.00), -- p20251002
            (101, 'Beijing', '2025-10-03', 120.00), -- p20251003
            (102, 'Shanghai', '2025-10-03', 300.00); -- p20251003
            """
    }

    combineFutures(
            /*
            extraThread("testRenameMtmv", {
                def prefix_str = "qc_rename_mtmv_"

                def tb_name = prefix_str + "table1"

                def mv_name1 = prefix_str + "mtmv1"
                def mv_name2 = prefix_str + "mtmv2"
                def mv_name3 = prefix_str + "mtmv3"
                def nested_mv_name1 = prefix_str + "nested_mtmv1"

                create_table_and_insert(tb_name)
                setSessionVariables()

                def mtmv_sql = """
                    SELECT city,sale_date,SUM(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date;"""
                def nested_mtmv_sql = """
                    SELECT city,date_trunc(sale_date, 'MONTH') AS sale_date, SUM(daily_city_amount) AS monthly_city_amount FROM ${mv_name1} GROUP BY city, date_trunc(sale_date, 'MONTH');"""
                // 直查表，不改写mtmv1
                def select_sql = """
                    SELECT product_id, SUM(amount) AS total_city_amount FROM ${tb_name} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY product_id;"""
                // 直查表，改写mtmv1
                def mtmv_select_sql = """
                    SELECT city, SUM(amount) AS total_city_amount FROM ${tb_name} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY city;"""
                // 直查nested_mtmv1，不改写
                def nested_mtmv_select_sql = """
                    select city, sum(monthly_city_amount) from ${nested_mv_name1} group by city;"""
                // 直查mtmv1，改写nested_mtmv1
                def nested_mtmv_select_sql1 = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM ${mv_name1} GROUP BY date_trunc(sale_date, 'MONTH');"""
                // 直查表，改写nested_mtmv1
                def nested_mtmv_select_sql2 = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM (SELECT city, sale_date, SUM(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date) as t GROUP BY date_trunc(sale_date, 'MONTH');"""
                // 直查mtmv1，不改写nested_mtmv1
                def nested_mtmv_select_sql3 = """
                    select city, avg(daily_city_amount) from ${mv_name1} group by city;"""
                def nested_mtmv_select_sql3_new = """
                    select city, avg(daily_city_amount) from ${mv_name2} group by city;"""


                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                create_async_mv(dbName, mv_name1, mtmv_sql)
                create_async_mv(dbName, nested_mv_name1, nested_mtmv_sql)

                assertNoCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertNoCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                assertHasCache select_sql // 直查表，不改写mtmv1
                assertHasCache mtmv_select_sql  // 直查表，改写mtmv1
                assertHasCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                sql """ALTER MATERIALIZED VIEW ${mv_name1} rename ${mv_name2};"""
                assertHasCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertNoCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                test {
                    sql nested_mtmv_select_sql1   // 直查mtmv1，改写nested_mtmv1
                    exception "does not exist"
                }
                test {
                    sql nested_mtmv_select_sql3
                    exception "does not exist"
                }
                assertNoCache nested_mtmv_select_sql3_new // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                sql """ALTER MATERIALIZED VIEW ${mv_name2} rename ${mv_name1};"""
                assertHasCache select_sql // 直查表，不改写mtmv1
                assertHasCache mtmv_select_sql  // 直查表，改写mtmv1
                assertHasCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

            }),

            extraThread("testReplaceMtmv", {
                def prefix_str = "qc_replace_mtmv_"

                def tb_name = prefix_str + "table1"

                def mv_name1 = prefix_str + "mtmv1"
                def mv_name2 = prefix_str + "mtmv2"
                def nested_mv_name1 = prefix_str + "nested_mtmv1"

                create_table_and_insert(tb_name)
                setSessionVariables()

                def mtmv_sql = """
                    SELECT city,sale_date,SUM(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date order by 1, 2, 3;"""
                def mtmv_sql2 = """
                    SELECT city,sale_date,count(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date order by 1, 2, 3;"""
                def nested_mtmv_sql = """
                    SELECT city,date_trunc(sale_date, 'MONTH') AS sale_date, SUM(daily_city_amount) AS monthly_city_amount FROM ${mv_name1} GROUP BY city, date_trunc(sale_date, 'MONTH') order by 1, 2, 3;"""
                // 直查表，不改写mtmv1
                def select_sql = """
                    SELECT product_id, SUM(amount) AS total_city_amount FROM ${tb_name} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY product_id order by 1, 2;"""
                // 直查表，改写mtmv1
                def mtmv_select_sql = """
                    SELECT city, SUM(amount) AS total_city_amount FROM ${tb_name} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY city order by 1, 2;"""
                // 直查nested_mtmv1，不改写
                def nested_mtmv_select_sql = """
                    select city, sum(monthly_city_amount) from ${nested_mv_name1} group by city order by 1, 2;"""
                // 直查mtmv1，改写nested_mtmv1
                def nested_mtmv_select_sql1 = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM ${mv_name1} GROUP BY date_trunc(sale_date, 'MONTH') order by 1, 2;"""
                // 直查表，改写nested_mtmv1
                def nested_mtmv_select_sql2 = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM (SELECT city, sale_date, SUM(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date) as t GROUP BY date_trunc(sale_date, 'MONTH') order by 1, 2;"""
                // 直查mtmv1，不改写nested_mtmv1
                def nested_mtmv_select_sql3 = """
                    select city, avg(daily_city_amount) from ${mv_name1} group by city order by 1, 2;"""
                def nested_mtmv_select_sql3_new = """
                    select city, avg(daily_city_amount) from ${mv_name2} group by city order by 1, 2;"""


                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                create_async_mv(dbName, mv_name1, mtmv_sql)
                create_async_mv(dbName, mv_name2, mtmv_sql2)
                create_async_mv(dbName, nested_mv_name1, nested_mtmv_sql)

                assertNoCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertNoCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                assertHasCache select_sql // 直查表，不改写mtmv1
                assertHasCache mtmv_select_sql  // 直查表，改写mtmv1
                assertHasCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                def res1 = sql select_sql // 直查表，不改写mtmv1
                def res2 = sql mtmv_select_sql  // 直查表，改写mtmv1
                def res3 = sql nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                def res4 = sql nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                def res5 = sql nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                def res6 = sql nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                sql """ALTER MATERIALIZED VIEW ${mv_name1} REPLACE WITH MATERIALIZED VIEW ${mv_name2};"""
                assertHasCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertNoCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql3_new // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                // 换回来之后无法确定是之前的缓存生效，还是执行之后重新生成的缓存生效
                // 通过查询结果来验证缓存生效情况
                sql """ALTER MATERIALIZED VIEW ${mv_name2} REPLACE WITH MATERIALIZED VIEW ${mv_name1};"""
                assertHasCache select_sql // 直查表，不改写mtmv1
                assertHasCache mtmv_select_sql  // 直查表，改写mtmv1
                assertHasCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                def new_res1 = sql select_sql // 直查表，不改写mtmv1
                def new_res2 = sql mtmv_select_sql  // 直查表，改写mtmv1
                def new_res3 = sql nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                def new_res4 = sql nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                def new_res5 = sql nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                def new_res6 = sql nested_mtmv_select_sql // 直查nested_mtmv1，不改写
                judge_res(res1, new_res1)
                judge_res(res2, new_res2)
                judge_res(res3, new_res3)
                judge_res(res4, new_res4)
                judge_res(res5, new_res5)
                judge_res(res6, new_res6)

            }),

             */


            extraThread("testPauseResumeMtmv", {
                def prefix_str = "qc_pause_resume_mtmv_"

                def tb_name = prefix_str + "table1"

                def mv_name1 = prefix_str + "mtmv1"
                def mv_name2 = prefix_str + "mtmv2"
                def mv_name3 = prefix_str + "mtmv3"
                def nested_mv_name1 = prefix_str + "nested_mtmv1"

                create_table_and_insert(tb_name)
                setSessionVariables()

                def mtmv_sql = """
                    SELECT city,sale_date,SUM(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date;"""
                def mtmv_sql2 = """
                    SELECT city,sale_date,avg(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date;"""
                def nested_mtmv_sql = """
                    SELECT city,date_trunc(sale_date, 'MONTH') AS sale_date, SUM(daily_city_amount) AS monthly_city_amount FROM ${mv_name1} GROUP BY city, date_trunc(sale_date, 'MONTH');"""
                // 直查表，不改写mtmv1
                def select_sql = """
                    SELECT product_id, SUM(amount) AS total_city_amount FROM ${tb_name} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY product_id;"""
                // 直查表，改写mtmv1
                def mtmv_select_sql = """
                    SELECT city, SUM(amount) AS total_city_amount FROM ${tb_name} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY city;"""
                // 直查nested_mtmv1，不改写
                def nested_mtmv_select_sql = """
                    select city, sum(monthly_city_amount) from ${nested_mv_name1} group by city;"""
                // 直查mtmv1，改写nested_mtmv1
                def nested_mtmv_select_sql1 = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM ${mv_name1} GROUP BY date_trunc(sale_date, 'MONTH');"""
                // 直查表，改写nested_mtmv1
                def nested_mtmv_select_sql2 = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM (SELECT city, sale_date, SUM(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date) as t GROUP BY date_trunc(sale_date, 'MONTH');"""
                // 直查mtmv1，不改写nested_mtmv1
                def nested_mtmv_select_sql3 = """
                    select city, avg(daily_city_amount) from ${mv_name1} group by city;"""
                def nested_mtmv_select_sql3_new = """
                    select city, avg(daily_city_amount) from ${mv_name2} group by city;"""


                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                create_async_mv(dbName, mv_name1, mtmv_sql)
                create_async_mv(dbName, mv_name2, mtmv_sql2)
                create_async_mv(dbName, nested_mv_name1, nested_mtmv_sql)

                assertNoCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertNoCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                assertHasCache select_sql // 直查表，不改写mtmv1
                assertHasCache mtmv_select_sql  // 直查表，改写mtmv1
                assertHasCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                sql """PAUSE MATERIALIZED VIEW JOB ON ${mv_name1};"""
                assertHasCache select_sql // 直查表，不改写mtmv1
                assertHasCache mtmv_select_sql  // 直查表，改写mtmv1
                assertHasCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                sql """RESUME MATERIALIZED VIEW JOB ON ${mv_name1};"""
                assertHasCache select_sql // 直查表，不改写mtmv1
                assertHasCache mtmv_select_sql  // 直查表，改写mtmv1
                assertHasCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                sql "REFRESH MATERIALIZED VIEW ${mv_name1} AUTO;"
                waitingMTMVTaskFinishedByMvName(mv_name1)

                assertHasCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertHasCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                sql "REFRESH MATERIALIZED VIEW ${mv_name1} complete;"
                waitingMTMVTaskFinishedByMvName(mv_name1)

                assertHasCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertHasCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                sql "INSERT OVERWRITE table ${tb_name} PARTITION(p20251001) VALUES (101, 'Beijing', '2025-10-01', 500.00);"
                assertNoCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertNoCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                sql "REFRESH MATERIALIZED VIEW ${mv_name1} AUTO;"
                assertHasCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertNoCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

            }),

            /*
            extraThread("testBaseInsertDataMtmv", {
                def prefix_str = "qc_base_insert_data_mtmv_"

                def tb_name = prefix_str + "table1"

                def mv_name1 = prefix_str + "mtmv1"
                def mv_name2 = prefix_str + "mtmv2"
                def mv_name3 = prefix_str + "mtmv3"
                def nested_mv_name1 = prefix_str + "nested_mtmv1"

                create_table_and_insert(tb_name)
                setSessionVariables()

                def mtmv_sql = """
                    SELECT city,sale_date,SUM(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date;"""
                def mtmv_sql2 = """
                    SELECT city,sale_date,avg(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date;"""
                def nested_mtmv_sql = """
                    SELECT city,date_trunc(sale_date, 'MONTH') AS sale_date, SUM(daily_city_amount) AS monthly_city_amount FROM ${mv_name1} GROUP BY city, date_trunc(sale_date, 'MONTH');"""
                // 直查表，不改写mtmv1
                def select_sql = """
                    SELECT product_id, SUM(amount) AS total_city_amount FROM ${tb_name} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY product_id;"""
                // 直查表，改写mtmv1
                def mtmv_select_sql = """
                    SELECT city, SUM(amount) AS total_city_amount FROM ${tb_name} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY city;"""
                // 直查nested_mtmv1，不改写
                def nested_mtmv_select_sql = """
                    select city, sum(monthly_city_amount) from ${nested_mv_name1} group by city;"""
                // 直查mtmv1，改写nested_mtmv1
                def nested_mtmv_select_sql1 = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM ${mv_name1} GROUP BY date_trunc(sale_date, 'MONTH');"""
                // 直查表，改写nested_mtmv1
                def nested_mtmv_select_sql2 = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM (SELECT city, sale_date, SUM(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date) as t GROUP BY date_trunc(sale_date, 'MONTH');"""
                // 直查mtmv1，不改写nested_mtmv1
                def nested_mtmv_select_sql3 = """
                    select city, avg(daily_city_amount) from ${mv_name1} group by city;"""
                def nested_mtmv_select_sql3_new = """
                    select city, avg(daily_city_amount) from ${mv_name2} group by city;"""


                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                create_async_mv(dbName, mv_name1, mtmv_sql)
                create_async_mv(dbName, mv_name2, mtmv_sql2)
                create_async_mv(dbName, nested_mv_name1, nested_mtmv_sql)

                assertNoCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertNoCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                assertHasCache select_sql // 直查表，不改写mtmv1
                assertHasCache mtmv_select_sql  // 直查表，改写mtmv1
                assertHasCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                sql "alter table ${tb_name} add partition p1 values[('2025-11-01'), ('2025-12-01'))"
                assertHasCache select_sql // 直查表，不改写mtmv1
                assertHasCache mtmv_select_sql  // 直查表，改写mtmv1
                assertHasCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                sql "insert into ${tb_name} values(111, 'Beijing', '2025-11-01', 500.00)"
                assertNoCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertNoCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

            }),
            extraThread("testRecreateMtmv", {
                def prefix_str = "qc_recreate_mtmv_"

                def tb_name = prefix_str + "table1"

                def mv_name1 = prefix_str + "mtmv1"
                def mv_name2 = prefix_str + "mtmv2"
                def mv_name3 = prefix_str + "mtmv3"
                def nested_mv_name1 = prefix_str + "nested_mtmv1"

                create_table_and_insert(tb_name)
                setSessionVariables()

                def mtmv_sql = """
                    SELECT city,sale_date,SUM(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date;"""
                def mtmv_sql2 = """
                    SELECT city,sale_date,avg(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date;"""
                def nested_mtmv_sql = """
                    SELECT city,date_trunc(sale_date, 'MONTH') AS sale_date, SUM(daily_city_amount) AS monthly_city_amount FROM ${mv_name1} GROUP BY city, date_trunc(sale_date, 'MONTH');"""
                // 直查表，不改写mtmv1
                def select_sql = """
                    SELECT product_id, SUM(amount) AS total_city_amount FROM ${tb_name} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY product_id;"""
                // 直查表，改写mtmv1
                def mtmv_select_sql = """
                    SELECT city, SUM(amount) AS total_city_amount FROM ${tb_name} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY city;"""
                // 直查nested_mtmv1，不改写
                def nested_mtmv_select_sql = """
                    select city, sum(monthly_city_amount) from ${nested_mv_name1} group by city;"""
                // 直查mtmv1，改写nested_mtmv1
                def nested_mtmv_select_sql1 = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM ${mv_name1} GROUP BY date_trunc(sale_date, 'MONTH');"""
                // 直查表，改写nested_mtmv1
                def nested_mtmv_select_sql2 = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM (SELECT city, sale_date, SUM(amount) AS daily_city_amount FROM ${tb_name} GROUP BY city, sale_date) as t GROUP BY date_trunc(sale_date, 'MONTH');"""
                // 直查mtmv1，不改写nested_mtmv1
                def nested_mtmv_select_sql3 = """
                    select city, avg(daily_city_amount) from ${mv_name1} group by city;"""
                def nested_mtmv_select_sql3_new = """
                    select city, avg(daily_city_amount) from ${mv_name2} group by city;"""


                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                create_async_mv(dbName, mv_name1, mtmv_sql)
                create_async_mv(dbName, mv_name2, mtmv_sql2)
                create_async_mv(dbName, nested_mv_name1, nested_mtmv_sql)

                assertNoCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertNoCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                assertHasCache select_sql // 直查表，不改写mtmv1
                assertHasCache mtmv_select_sql  // 直查表，改写mtmv1
                assertHasCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1}"""
                assertHasCache select_sql // 直查表，不改写mtmv1
                assertNoCache mtmv_select_sql  // 直查表，改写mtmv1
                assertNoCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                test {
                    sql nested_mtmv_select_sql1
                    exception "does not exist"
                }
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

                create_async_mv(dbName, mv_name1, mtmv_sql)
                assertHasCache select_sql // 直查表，不改写mtmv1
                assertHasCache mtmv_select_sql  // 直查表，改写mtmv1
                assertNoCache nested_mtmv_select_sql2 // 直查表，改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql1 // 直查mtmv1，改写nested_mtmv1
                assertNoCache nested_mtmv_select_sql3 // 直查mtmv1，不改写nested_mtmv1
                assertHasCache nested_mtmv_select_sql // 直查nested_mtmv1，不改写

            }),

             */

    ).get()

}
