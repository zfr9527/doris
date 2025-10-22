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
            /*
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
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                sql """set query_cache_entry_max_rows=1"""
                assertNoCache sql_str
                assertNoCache sql_str
            }),
            extraThread("testQueryCacheSize", {
                def tb_name = "query_cache_size_table"
                sql """drop table if exists ${tb_name}"""
                sql """CREATE TABLE ${tb_name} (
                        id INT,
                        group_key VARCHAR(10),     
                        payload_size INT,          
                        payload VARCHAR(65533) 
                    ) DUPLICATE KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 3
                    PROPERTIES("replication_num" = "1");"""
                sql """
                    INSERT INTO ${tb_name} (id, group_key, payload_size, payload) VALUES 
                    (1, 'A', 3413, RPAD('data_1_', 32000, 'x')),
                    (2, 'A', 3413, RPAD('data_2_', 32000, 'x')),
                    (3, 'A', 3413, RPAD('data_3_', 32000, 'x')),
                    (4, 'A', 3413, RPAD('data_4_', 32000, 'x')),
                    (5, 'A', 3413, RPAD('data_5_', 32000, 'x')),
                    (6, 'A', 3413, RPAD('data_6_', 32000, 'x')),
                    (7, 'A', 3413, RPAD('data_7_', 32000, 'x')),
                    (8, 'A', 3413, RPAD('data_8_', 32000, 'x')),
                    (9, 'A', 3413, RPAD('data_9_', 32000, 'x')),
                    (10, 'A', 3413, RPAD('data_10_', 32000, 'x')),
                    (11, 'A', 3413, RPAD('data_11_', 32000, 'x')),
                    (12, 'A', 3413, RPAD('data_12_', 32000, 'x')),
                    (13, 'A', 3413, RPAD('data_13_', 32000, 'x')),
                    (14, 'A', 3413, RPAD('data_14_', 32000, 'x')),
                    (15, 'A', 3413, RPAD('data_15_', 32000, 'x')),
                    (16, 'A', 3413, RPAD('data_16_', 32000, 'x')),
                    (17, 'A', 3413, RPAD('data_17_', 32000, 'x')),
                    (18, 'A', 3413, RPAD('data_18_', 32000, 'x')),
                    (19, 'A', 3413, RPAD('data_19_', 32000, 'x')),
                    (20, 'A', 3413, RPAD('data_20_', 32000, 'x')),
                    (21, 'A', 3413, RPAD('data_21_', 32000, 'x')),
                    (22, 'A', 3413, RPAD('data_22_', 32000, 'x')),
                    (23, 'A', 3413, RPAD('data_23_', 32000, 'x')),
                    (24, 'A', 3413, RPAD('data_24_', 32000, 'x')),
                    (25, 'A', 3413, RPAD('data_25_', 32000, 'x')),
                    (26, 'A', 3413, RPAD('data_26_', 32000, 'x')),
                    (27, 'A', 3413, RPAD('data_27_', 32000, 'x')),
                    (28, 'A', 3413, RPAD('data_28_', 32000, 'x')),
                    (29, 'A', 3413, RPAD('data_29_', 32000, 'x')),
                    (30, 'A', 3413, RPAD('data_30_', 32000, 'x'));"""

                sql """INSERT INTO ${tb_name} (id, group_key, payload_size, payload) VALUES 
                    (31, 'B', 3413, RPAD('data_31_', 32000, 'y')),
                    (32, 'B', 3413, RPAD('data_32_', 32000, 'y')),
                    (33, 'B', 3413, RPAD('data_33_', 32000, 'y')),
                    (34, 'B', 3413, RPAD('data_34_', 32000, 'y')),
                    (35, 'B', 3413, RPAD('data_35_', 32000, 'y')),
                    (36, 'B', 3413, RPAD('data_36_', 32000, 'y')),
                    (37, 'B', 3413, RPAD('data_37_', 32000, 'y')),
                    (38, 'B', 3413, RPAD('data_38_', 32000, 'y')),
                    (39, 'B', 3413, RPAD('data_39_', 32000, 'y')),
                    (40, 'B', 3413, RPAD('data_40_', 32000, 'y')),
                    (41, 'B', 3413, RPAD('data_41_', 32000, 'y')),
                    (42, 'B', 3413, RPAD('data_42_', 32000, 'y')),
                    (43, 'B', 3413, RPAD('data_43_', 32000, 'y')),
                    (44, 'B', 3413, RPAD('data_44_', 32000, 'y')),
                    (45, 'B', 3413, RPAD('data_45_', 32000, 'y')),
                    (46, 'B', 3413, RPAD('data_46_', 32000, 'y')),
                    (47, 'B', 3413, RPAD('data_47_', 32000, 'y')),
                    (48, 'B', 3413, RPAD('data_48_', 32000, 'y')),
                    (49, 'B', 3413, RPAD('data_49_', 32000, 'y')),
                    (50, 'B', 3413, RPAD('data_50_', 32000, 'y')),
                    (51, 'B', 3413, RPAD('data_51_', 32000, 'y')),
                    (52, 'B', 3413, RPAD('data_52_', 32000, 'y')),
                    (53, 'B', 3413, RPAD('data_53_', 32000, 'y')),
                    (54, 'B', 3413, RPAD('data_54_', 32000, 'y')),
                    (55, 'B', 3413, RPAD('data_55_', 32000, 'y')),
                    (56, 'B', 3413, RPAD('data_56_', 32000, 'y')),
                    (57, 'B', 3413, RPAD('data_57_', 32000, 'y')),
                    (58, 'B', 3413, RPAD('data_58_', 32000, 'y')),
                    (59, 'B', 3413, RPAD('data_59_', 32000, 'y')),
                    (60, 'B', 3413, RPAD('data_60_', 32000, 'y'));"""

                sql """INSERT INTO ${tb_name} (id, group_key, payload_size, payload) VALUES 
                    (61, 'C', 3413, RPAD('data_61_', 32000, 'z')),
                    (62, 'C', 3413, RPAD('data_62_', 32000, 'z')),
                    (63, 'C', 3413, RPAD('data_63_', 32000, 'z')),
                    (64, 'C', 3413, RPAD('data_64_', 32000, 'z')),
                    (65, 'C', 3413, RPAD('data_65_', 32000, 'z')),
                    (66, 'C', 3413, RPAD('data_66_', 32000, 'z')),
                    (67, 'C', 3413, RPAD('data_67_', 32000, 'z')),
                    (68, 'C', 3413, RPAD('data_68_', 32000, 'z')),
                    (69, 'C', 3413, RPAD('data_69_', 32000, 'z')),
                    (70, 'C', 3413, RPAD('data_70_', 32000, 'z')),
                    (71, 'C', 3413, RPAD('data_71_', 32000, 'z')),
                    (72, 'C', 3413, RPAD('data_72_', 32000, 'z')),
                    (73, 'C', 3413, RPAD('data_73_', 32000, 'z')),
                    (74, 'C', 3413, RPAD('data_74_', 32000, 'z')),
                    (75, 'C', 3413, RPAD('data_75_', 32000, 'z')),
                    (76, 'C', 3413, RPAD('data_76_', 32000, 'z')),
                    (77, 'C', 3413, RPAD('data_77_', 32000, 'z')),
                    (78, 'C', 3413, RPAD('data_78_', 32000, 'z')),
                    (79, 'C', 3413, RPAD('data_79_', 32000, 'z')),
                    (80, 'C', 3413, RPAD('data_80_', 32000, 'z')),
                    (81, 'C', 3413, RPAD('data_81_', 32000, 'z')),
                    (82, 'C', 3413, RPAD('data_82_', 32000, 'z')),
                    (83, 'C', 3413, RPAD('data_83_', 32000, 'z')),
                    (84, 'C', 3413, RPAD('data_84_', 32000, 'z')),
                    (85, 'C', 3413, RPAD('data_85_', 32000, 'z')),
                    (86, 'C', 3413, RPAD('data_86_', 32000, 'z')),
                    (87, 'C', 3413, RPAD('data_87_', 32000, 'z')),
                    (88, 'C', 3413, RPAD('data_88_', 32000, 'z')),
                    (89, 'C', 3413, RPAD('data_89_', 32000, 'z')),
                    (90, 'C', 3413, RPAD('data_90_', 32000, 'z'));"""

                setSessionVariables()

                def sql_str1 = "SELECT group_key, GROUP_CONCAT(payload) FROM ${tb_name} WHERE group_key = 'A' GROUP BY group_key;"
                def sql_str2 = "SELECT group_key, GROUP_CONCAT(payload) FROM ${tb_name} WHERE group_key = 'B' GROUP BY group_key;"
                def sql_str3 = "SELECT group_key, GROUP_CONCAT(payload) FROM ${tb_name} WHERE group_key = 'C' GROUP BY group_key;"
                sql """set global query_cache_size=2"""
                assertNoCache sql_str1
                assertHasCache sql_str1

                assertNoCache sql_str2
                assertHasCache sql_str2

                assertNoCache sql_str3
                assertHasCache sql_str3

                assertNoCache sql_str1

                sql """set global query_cache_size=512"""
            }),

             */
    ).get()

}
