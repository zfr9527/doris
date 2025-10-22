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

suite("query_cache_all_base_table_types") {

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

            extraThread("testRangeOneKeyTable", {
                def tb_name = "query_cache_range_one_key_table"
                sql """
                    create table ${tb_name}
                    (
                        id int,
                        value int
                    )
                    partition by range(id)
                    (
                        PARTITION p0 VALUES LESS THAN ('1'),
                        partition p1 values[('1'), ('2')),
                        partition p2 values[('2'), ('3')),
                        partition p3 values[('3'), ('4')),
                        partition p4 values[('4'), ('5')),
                        partition p5 values[('5'), ('6'))
                    )
                    distributed by hash(id)
                    properties(
                        'replication_num'='1'
                    );
                """
                sql """
                insert into ${tb_name}
                    values 
                    (1, 1), (1, 2),
                    (2, 1), (2, 2), 
                    (3, 1), (3, 2),
                    (4, 1), (4, 2),
                    (5, 1), (5, 2);
                """
                sql "sync"

                setSessionVariables()

                def sql_str = "select id, sum(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """alter table ${tb_name} add partition p6 values[('6'),('7'))"""
                assertHasCache sql_str

                sql "insert into ${tb_name} values(6, 1)"
                assertPartHasCache sql_str
                assertHasCache sql_str

                sql "insert into ${tb_name} values(null, null)"
                assertPartHasCache sql_str
                assertHasCache sql_str
            }),


/*
            extraThread("testRangeTwoKeyTable", {
                def tb_name = "query_cache_range_two_key_table"
                sql """
                    create table ${tb_name}
                    (
                        id int,
                        value int
                    )
                    partition by range(id, value)
                    (
                        PARTITION p0 VALUES LESS THAN ('1', '1'),
                        partition p1 values[('1', '1'), ('2', '1')),
                        partition p2 values[('2', '1'), ('3', '1')),
                        partition p3 values[('3', '1'), ('4', '1')),
                        partition p4 values[('4', '1'), ('5', '1')),
                        partition p5 values[('5', '1'), ('6', '1'))
                    )
                    distributed by hash(id)
                    properties(
                        'replication_num'='1'
                    );
                """
                sql """
                insert into ${tb_name}
                    values 
                    (1, 1), (1, 2),
                    (2, 1), (2, 2), 
                    (3, 1), (3, 2),
                    (4, 1), (4, 2),
                    (5, 1), (5, 2);
                """
                sql "sync"

                setSessionVariables()

                def sql_str = "select id, sum(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """alter table ${tb_name} add partition p6 values[('6', '1'),('7', '1'))"""
                assertHasCache sql_str // mark

                sql "insert into ${tb_name} values(6, 1)"
                assertNoCache sql_str
                assertHasCache sql_str

                sql "insert into ${tb_name} values(null, null)"
                assertNoCache sql_str
                assertHasCache sql_str
            }),

            extraThread("testListOneKeyTable", {
                def tb_name = "query_cache_list_one_key_table"
                sql """
                    create table ${tb_name}
                    (
                        id int,
                        value int
                    )
                    partition by range(id)
                    (
                        PARTITION p0 VALUES IN ((NULL)),
                        partition p1 VALUES IN ("1"),
                        partition p2 VALUES IN ("2"),
                        partition p3 VALUES IN ("3"),
                        partition p4 VALUES IN ("4"),
                        partition p5 VALUES IN ("5")
                    )
                    distributed by hash(id)
                    properties(
                        'replication_num'='1'
                    );
                """
                sql """
                insert into ${tb_name}
                    values 
                    (1, 1), (1, 2),
                    (2, 1), (2, 2), 
                    (3, 1), (3, 2),
                    (4, 1), (4, 2),
                    (5, 1), (5, 2);
                """
                sql "sync"

                setSessionVariables()

                def sql_str = "select id, sum(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """alter table ${tb_name} add partition p6 values[('6'),('7'))"""
                assertHasCache sql_str // mark

                sql "insert into ${tb_name} values(6, 1)"
                assertNoCache sql_str
                assertHasCache sql_str

                sql "insert into ${tb_name} values(null, null)"
                assertNoCache sql_str
                assertHasCache sql_str
            }),
            extraThread("testListOneKeyTable", {
                def tb_name = "query_cache_list_one_key_table"
                sql """
                    create table ${tb_name}
                    (
                        id int,
                        value int
                    )
                    distributed by hash(id)
                    properties(
                        'replication_num'='1'
                    );
                """
                sql """
                insert into ${tb_name}
                    values 
                    (1, 1), (1, 2),
                    (2, 1), (2, 2), 
                    (3, 1), (3, 2),
                    (4, 1), (4, 2),
                    (5, 1), (5, 2);
                """
                sql "sync"

                setSessionVariables()

                def sql_str = "select id, sum(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                // 非分区表应该不能增加分区
                sql """alter table ${tb_name} add partition p6 values[('6'),('7'))"""
                assertHasCache sql_str // mark

                sql "insert into ${tb_name} values(6, 1)"
                assertNoCache sql_str
                assertHasCache sql_str

                sql "insert into ${tb_name} values(null, null)"
                assertNoCache sql_str
                assertHasCache sql_str
            }),

             */
    ).get

}
