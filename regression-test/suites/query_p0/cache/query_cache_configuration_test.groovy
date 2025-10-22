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

suite("query_cache_configuration_test") {

    def setSessionVariables = {
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set enable_sql_cache=false"
        sql "set enable_query_cache=true"
    }

    def assertHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
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
                logger.info("profileString: " + profileString)
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
                logger.info("profileString: " + profileString)
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
                logger.info("profileString: " + profileString)
                assertFalse(profileString.contains("HitCache:  0")) && assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    String dbName = context.config.getDbNameByFile(context.file)
    sql "ADMIN SET FRONTEND CONFIG ('cache_last_version_interval_second' = '0')"

    combineFutures(
            extraThread("testQueryCacheForceRefresh", {
                def tb_name = "query_cache_force_refresh_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """alter table ${tb_name} add partition p6 values[('6'),('7'))"""
                assertHasCache sql_str // mark

                sql "insert into ${tb_name} values(6, 1)"
                sql """set query_cache_force_refresh=true;"""
                assertNoCache sql_str
            }),
            extraThread("testQueryCacheEntryMaxBytes", {
                def tb_name = "query_cache_entry_max_bytes_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                sql """set query_cache_entry_max_bytes=1"""
                assertNoCache sql_str
                assertNoCache sql_str
            }),
            extraThread("testQueryCacheEntryMaxRows", {
                def tb_name = "query_cache_entry_max_rows_table"

                sql """drop table if exists ${tb_name}"""
                sql """
                    create table ${tb_name}
                    (
                    id int,
                    value int
                    )
                    partition by range(id)
                    (
                    partition p1 values[('10'), ('20')),
                    partition p2 values[('20'), ('30')),
                    partition p3 values[('30'), ('40')),
                    partition p4 values[('40'), ('50')),
                    partition p5 values[('50'), ('60'))
                    )
                    distributed by hash(id) BUCKETS 1
                    properties(
                    'replication_num'='1'
                    );"""

                sql """
                    insert into ${tb_name}
                    values 
                    (10, 1), (11, 2),(12, 1), (13, 2),(14, 1), (15, 2),
                    (20, 1), (21, 2),(22, 1), (23, 2),(24, 1), (25, 2),
                    (30, 1), (31, 2),(32, 1), (33, 2),(34, 1), (35, 2),
                    (40, 1), (41, 2),(42, 1), (43, 2),(44, 1), (45, 2),
                    (50, 1), (51, 2),(52, 1), (53, 2),(54, 1), (55, 2);
                    """

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                sql """set query_cache_entry_max_rows=1"""
                assertNoCache sql_str
                assertNoCache sql_str
            })
    ).get()

}
