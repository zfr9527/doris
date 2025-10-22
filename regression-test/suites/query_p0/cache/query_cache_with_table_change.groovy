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

suite("query_cache_with_table_change") {
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
            /*
            extraThread("testDropTable", {
                def tb_name = "query_cache_drop_table_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id order by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """drop table ${tb_name}"""

                test {
                    sql sql_str
                    exception "does not exist"
                }

                sql """RECOVER TABLE ${tb_name};""" // mark 看上去recover之后仍然可以直接使用
                assertHasCache sql_str

                sql """drop table ${tb_name}"""
                sql """create table ${tb_name}
                    (
                        id int,
                        value int
                    )
                    partition by range(id)
                    (
                        partition p1 values[('1'), ('2')),
                        partition p2 values[('2'), ('3')),
                        partition p3 values[('3'), ('4')),
                        partition p4 values[('4'), ('5')),
                        partition p5 values[('5'), ('6'))
                    )
                    distributed by hash(id)
                    properties(
                        'replication_num'='1'
                    );"""
                sql """insert into ${tb_name} values (1, 1), (1, 2);"""
                assertNoCache sql_str // mark 重新建表不应该命中吧？我数据都没有了
            }),
            */


            extraThread("testRenameTable", {
                def tb_name = "query_cache_rename_table_table"
                def new_tb_name = "new_query_cache_rename_table_table"
                sql """drop table if exists ${new_tb_name}"""
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id order by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """ALTER TABLE ${tb_name} RENAME ${new_tb_name};"""

                test {
                    sql sql_str
                    exception "does not exist"
                }
                sql """ALTER TABLE ${new_tb_name} RENAME ${tb_name};""" // mark
                assertHasCache sql_str
            }),
            /*

            extraThread("testReplaceTable", {
                def tb_name = "query_cache_replace_table_table1"
                def tb_name2 = "query_cache_replace_table_table2"
                createTestTable tb_name
                createTestTable tb_name2

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id order by id;"
                def sql_str2 = "select id, count(value) from ${tb_name2} group by id order by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                assertNoCache sql_str2
                assertHasCache sql_str2

                sql """ALTER TABLE ${tb_name} REPLACE WITH TABLE ${tb_name2};"""
                assertNoCache sql_str
                assertNoCache sql_str2
            }),
            extraThread("testTruncateTable", {
                def tb_name = "query_cache_truncate_table_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id order by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """truncate table ${tb_name}"""
                noQueryCache sql_str  // mark 没有数据之后profile都是VEMPTYSET，完全不会走querycache
            }),

            extraThread("testAddColumn", {
                def tb_name = "query_cache_add_column_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id order by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """alter table ${tb_name} add column bbb int default '0'"""
                assertHasCache sql_str // mark 增加一列不会影响结果，仍然命中
            }),

            extraThread("testDropColumn", {
                def tb_name = "query_cache_drop_column_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(id) from ${tb_name} group by id order by id;"
                def sql_str2 = "select id, count(value) from ${tb_name} group by id order by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                assertNoCache sql_str2
                assertHasCache sql_str2

                sql """alter table ${tb_name} drop column value"""
                waitForSchemaChangeDone {
                    sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${tb_name}' ORDER BY createtime DESC LIMIT 1 """
                    time 600
                }

                assertNoCache sql_str  // 删除不相关的列不能命中cache
                test {
                    sql sql_str2
                    exception "Unknown column"
                }
            }),
            // rename column被禁用了
//            extraThread("testRenameColumn", {
//                def tb_name = "query_cache_rename_column_table"
//                createTestTable tb_name
//
//                setSessionVariables()
//
//                def sql_str = "select count(value) from ${tb_name} group by value;"
//                def sql_str2 = "select id, count(value) from ${tb_name} group by id, value;"
//                assertNoCache sql_str
//                assertHasCache sql_str
//
//                assertNoCache sql_str2
//                assertHasCache sql_str2
//
//                sql """alter table ${tb_name} rename column id id2"""
//                waitForSchemaChangeDone {
//                    sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${tb_name}' ORDER BY createtime DESC LIMIT 1 """
//                    time 600
//                }
//
//                assertHasCache sql_str
//                test {
//                    sql sql_str2
//                    exception "Unknown column"
//                }
//            }),

            extraThread("testModifyColumn", {
                def tb_name = "query_cache_modify_column_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """alter table ${tb_name} modify column value bigint"""
                waitForSchemaChangeDone {
                    sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${tb_name}' ORDER BY createtime DESC LIMIT 1 """
                    time 600
                }

                assertNoCache sql_str
                assertHasCache sql_str
            }),

            extraThread("testAddPartitionAndInsert", {
                def tb_name = "query_cache_add_partition_and_insert_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """alter table ${tb_name} add partition p6 values[('6'),('7'))"""
                assertHasCache sql_str // mark

                sql "insert into ${tb_name} values(6, 1)"
                assertPartHasCache sql_str
            }),
            extraThread("testAddPartitionAndInsertOverwrite", {
                def tb_name = "query_cache_add_partition_and_insert_overwrite_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql "INSERT OVERWRITE table ${tb_name} PARTITION(p5) VALUES (5, 6);"
                assertNoCache sql_str
                assertHasCache sql_str

                sql "alter table ${tb_name} add partition p6 values[('6'),('7'))"
                assertHasCache sql_str

                sql "INSERT OVERWRITE table ${tb_name} PARTITION(p6) VALUES (6, 6);"
                assertNoCache sql_str
            }),
            extraThread("testDropPartition", {
                def tb_name = "query_cache_drop_partition_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """alter table ${tb_name} add partition p6 values[('6'),('7'))"""
                assertHasCache sql_str // mark

                sql """alter table ${tb_name} drop partition p6"""
                assertHasCache sql_str // mark

                sql """alter table ${tb_name} drop partition p5"""
                assertHasCache sql_str
            }),
            extraThread("testReplacePartition", {
                def tb_name = "query_cache_replace_partition_table"
                createTestTable tb_name

                sql "alter table ${tb_name} add temporary partition tp1 values [('1'), ('2'))"
                sql """INSERT INTO ${tb_name} TEMPORARY PARTITION(tp1) values (1, 3), (1, 4)"""
//                streamLoad {
//                    table tb_name
//                    set "temporaryPartitions", "tp1"
//                    inputIterator([[1, 3], [1, 4]].iterator())
//                }
                // 这里的stream load像是没有导入成功

                sql "sync"

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """alter table ${tb_name} replace partition (p1) with temporary partition(tp1)"""
                assertNoCache sql_str
            }),
            extraThread("testRenamePartition", {
                def tb_name = "query_cache_rename_partition_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """ALTER TABLE ${tb_name} RENAME PARTITION p1 p6;"""
                assertNoCache sql_str  // mark 我以为不会使用cache，但是实际上是使用了cache。分区名称不影响查询使用cache吗？
            }),
            extraThread("testStreamLoad", {
                def tb_name = "query_cache_stream_load_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                streamLoad {
                    table tb_name
                    set "partitions", "p1"
                    inputIterator([[1, 3], [1, 4]].iterator())
                }
                sql "sync"

                assertNoCache sql_str
            }),
            extraThread("testUpdateData", {
                def tb_name = "query_cache_update_data_table"
                createTestTable(tb_name, true)

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql "update ${tb_name} set value=3 where id=1"
                assertNoCache sql_str
            }),
            extraThread("testDeleteData", {
                def tb_name = "query_cache_delete_data_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql "delete from ${tb_name} where id = 1"
                assertNoCache sql_str
            }),
            extraThread("testCreateAndAlterView", {
                def tb_name = "query_cache_create_and_alter_view_table"
                def view_name = "query_cache_create_and_alter_view_table_view"
                createTestTable tb_name
                sql """create view ${view_name} (k1, k2) as select id as k1, count(value) as k2 from ${tb_name} group by k1;"""

                setSessionVariables()

                def sql_str = "select k1, sum(k2) from ${view_name} group by k1;" // 这里好像有问题？
                assertNoCache sql_str
                assertHasCache sql_str

                sql "alter view ${view_name} as select id as k1, avg(value) as k2 from ${tb_name} group by k1;"
                assertNoCache sql_str
            }),
            extraThread("testCreateAndDropView", {
                def tb_name = "query_cache_create_and_drop_view_table"
                def view_name = "query_cache_create_and_drop_view_table_view"
                createTestTable tb_name
                sql """create view ${view_name} (k1, k2) as select id as k1, count(value) as k2 from ${tb_name} group by k1;"""

                setSessionVariables()

                def sql_str = "select k1, sum(k2) from ${view_name} group by k1;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql "drop view ${view_name}"
                test {
                    sql sql_str
                    exception "does not exist"
                }
            }),
            extraThread("testViewWithDataChange", {
                def tb_name = "query_cache_view_with_data_change_table"
                def view_name = "query_cache_view_with_data_change_table_view"
                createTestTable tb_name
                sql """drop view if exists ${view_name}"""
                sql """create view ${view_name} (k1, k2) as select id as k1, count(value) as k2 from ${tb_name} group by k1;"""

                setSessionVariables()

                def sql_str = "select k1, sum(k2) from ${view_name} group by k1;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql "insert into ${tb_name} values(1, 3)"
                assertNoCache sql_str
            }),
            extraThread("testNondeterministic", {
                def tb_name = "query_cache_nondeterministic_table"
                createTestTable tb_name

                setSessionVariables()

                def sql_str = "select id, random(), count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertNoCache sql_str  // mark 看上去支持了cache，但是不符合预期

                sql_str = "select id, year(now()), count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertNoCache sql_str
            }),
            extraThread("testUserVariable", {
                def tb_name = "query_cache_user_variable_table"
                createTestTable tb_name
                setSessionVariables()

                sql "set @custom_variable=10"
                def sql_str = "select @custom_variable, id, count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str
                def res = sql sql_str
                logger.info("res: " + res)

                sql "set @custom_variable=20"
                res = sql sql_str
                logger.info("res: " + res)
                assertNoCache sql_str  // mark 预期这里不应该命中，但是还是命中了，但是结果是对的
                assertTrue(res[0][0].toString().toInteger() == 20)

                sql "set @custom_variable=10"
                assertHasCache sql_str
                assertTrue(res[0][0].toString().toInteger() == 10)
            }),
            extraThread("testUdf", {
                def tb_name = "query_cache_udf_table"
                createTestTable tb_name
                setSessionVariables()

                def jarPath = """${context.config.suitePath}/javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
                scp_udf_file_to_all_be(jarPath)
                try_sql("DROP FUNCTION IF EXISTS ${tb_name}_test(string, int, int);")
                try_sql("DROP TABLE IF EXISTS ${tb_name}")

                sql """ DROP TABLE IF EXISTS ${tb_name}"""
                sql """
                            CREATE TABLE IF NOT EXISTS ${tb_name} (
                                `user_id`     INT         NOT NULL COMMENT "用户id",
                                `char_col`    CHAR        NOT NULL COMMENT "",
                                `varchar_col` VARCHAR(10) NOT NULL COMMENT "",
                                `string_col`  STRING      NOT NULL COMMENT ""
                                )
                                DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
                            """

                StringBuilder values = new StringBuilder()
                int i = 1
                for (; i < 9; i ++) {
                    values.append(" (${i}, '${i}','abcdefg${i}','poiuytre${i}abcdefg'),\n")
                }
                values.append("(${i}, '${i}','abcdefg${i}','poiuytre${i}abcdefg')")

                sql "INSERT INTO ${tb_name} VALUES ${values}"
                sql "sync"

                File path = new File(jarPath)
                if (!path.exists()) {
                    throw new IllegalStateException("""${jarPath} doesn't exist! """)
                }

                sql """ CREATE FUNCTION ${tb_name}_test(string, int, int) RETURNS string PROPERTIES (
                                "file"="file://${jarPath}",
                                "symbol"="org.apache.doris.udf.StringTest",
                                "type"="JAVA_UDF"
                            ); """

                assertNoCache """SELECT
                        ${tb_name}_test(varchar_col, 1, 1) AS processed_varchar,
                        COUNT(*) AS row_count
                    FROM ${tb_name}
                    GROUP BY processed_varchar
                    ORDER BY processed_varchar;
                    """
                sql """SELECT
                        ${tb_name}_test(varchar_col, 1, 1) AS processed_varchar,
                        COUNT(*) AS row_count
                    FROM ${tb_name}
                    GROUP BY processed_varchar
                    ORDER BY processed_varchar;
                    """
                assertNoCache """SELECT
                        ${tb_name}_test(varchar_col, 1, 1) AS processed_varchar,
                        COUNT(*) AS row_count
                    FROM ${tb_name}
                    GROUP BY processed_varchar
                    ORDER BY processed_varchar;
                    """
            }),

            extraThread("testDryRunQuery", {
                def tb_name = "query_cache_dry_run_query_table"
                createTestTable tb_name
                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                sql "set dry_run_query=true"
                assertNoCache sql_str
                def res = sql sql_str
                assertHasCache sql_str
                assertTrue(res.size() == 1)

                sql "set dry_run_query=false"
                assertHasCache sql_str
                res = sql sql_str
                assertTrue(res.size() > 1)

                sql "set dry_run_query=true"
                assertHasCache sql_str
                res = sql sql_str
                assertTrue(res.size() == 1)
            }),
            // 这个地方会有bdbje的问题导致fe crash
            extraThread("testMultiFrontends", {
                def tb_name = "query_cache_multi_frontends_table"

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                def aliveFrontends = sql_return_maparray("show frontends")
                        .stream()
                        .filter { it["Alive"].toString().equalsIgnoreCase("true") }
                        .collect(Collectors.toList())

                if (aliveFrontends.size() <= 1) {
                    return
                }

                def fe1 = aliveFrontends[0]["Host"] + ":" + aliveFrontends[0]["QueryPort"]
                def fe2 = fe1
                if (aliveFrontends.size() > 1) {
                    fe2 = aliveFrontends[1]["Host"] + ":" + aliveFrontends[1]["QueryPort"]
                }

                log.info("fe1: ${fe1}")
                log.info("fe2: ${fe2}")

                log.info("connect to fe: ${fe1}")
                connect( context.config.jdbcUser,  context.config.jdbcPassword,  "jdbc:mysql://${fe1}") {
                    sql "use ${dbName}"
                    createTestTable tb_name
                    sql "sync"
                    setSessionVariables()

                    assertNoCache sql_str
                    assertHasCache sql_str
                }

                log.info("connect to fe: ${fe2}")
                connect( context.config.jdbcUser,  context.config.jdbcPassword,  "jdbc:mysql://${fe2}") {
                    sql "use ${dbName}"
                    setSessionVariables()

                    assertHasCache sql_str
                }
            }),
            extraThread("testSameSqlWithDifferentDb", {
                def dbName1 = "query_cache_same_sql_with_different_db1"
                def dbName2 = "query_cache_same_sql_with_different_db2"
                def tableName = "query_cache_same_sql_with_different_db_table"
                setSessionVariables()

                sql "CREATE DATABASE IF NOT EXISTS ${dbName1}"
                sql "DROP TABLE IF EXISTS ${dbName1}.${tableName}"
                sql """
                    CREATE TABLE IF NOT EXISTS ${dbName1}.${tableName} (
                      `k1` date NOT NULL COMMENT "",
                      `k2` int(11) NOT NULL COMMENT ""
                    ) ENGINE=OLAP
                    DUPLICATE KEY(`k1`, `k2`)
                    COMMENT "OLAP"
                    PARTITION BY RANGE(`k1`)
                    (PARTITION p202411 VALUES [('2024-11-01'), ('2024-12-01')))
                    DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1
                    PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1",
                    "in_memory" = "false",
                    "storage_format" = "V2"
                    )
                    """
                sql "CREATE DATABASE IF NOT EXISTS ${dbName2}"
                sql "DROP TABLE IF EXISTS ${dbName2}.${tableName}"
                sql """
                    CREATE TABLE IF NOT EXISTS ${dbName2}.${tableName} (
                      `k1` date NOT NULL COMMENT "",
                      `k2` int(11) NOT NULL COMMENT ""
                    ) ENGINE=OLAP
                    DUPLICATE KEY(`k1`, `k2`)
                    COMMENT "OLAP"
                    PARTITION BY RANGE(`k1`)
                    (PARTITION p202411 VALUES [('2024-11-01'), ('2024-12-01')))
                    DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1
                    PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1",
                    "in_memory" = "false",
                    "storage_format" = "V2"
                    )
                    """

                sql """
                    INSERT INTO ${dbName1}.${tableName} VALUES
                                ("2024-11-29",1),
                                ("2024-11-30",2)
                    """
                sql """
                    INSERT INTO ${dbName2}.${tableName} VALUES
                                ("2024-11-29",3)
                    """

                def sql_str = "select k1, count(k2) from ${tableName} group by k1;"
                sql """use ${dbName1}"""
                assertNoCache sql_str
                assertHasCache sql_str
                def res1 = sql sql_str
                assertTrue(res1.size() == 2)

                sql """use ${dbName2}"""
                assertNoCache sql_str
                assertHasCache sql_str
                def res2 = sql sql_str
                assertTrue(res2.size() == 1)
            }),
            extraThread("testTemporaryTable", {
                def tb_name = "query_cache_temporary_table_table"
                createTestTable tb_name
                setSessionVariables()

                def sql_str = "select id, count(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str
                def rs = sql sql_str
                assertTrue(rs.size() == 5)

                connect(context.config.jdbcUser, context.config.jdbcPassword, context.jdbcUrl) {
                    setSessionVariables()
                    sql "create temporary table ${tb_name}(id int, value int) properties('replication_num'='1')"
                    sql """insert into ${tb_name} values  (1, 1), (1, 2);"""
                    assertNoCache sql_str
                    assertHasCache sql_str
                    assertEquals(1, (sql sql_str).size())
                    assertHasCache sql_str
                    assertEquals(1, (sql sql_str).size())
                    assertHasCache sql_str
                }

                assertHasCache sql_str
                rs = sql sql_str
                assertTrue(rs.size() == 5)

            }),

            extraThread("testChangeWithMv", {
                def tb_name = "query_cache_change_with_mv_table"
                def mv_name1 = "query_cache_change_with_mv_table_mv1"
                def mv_name2 = "query_cache_change_with_mv_table_mv2"
                createTestTable tb_name

                setSessionVariables()

                // rewrite mv
                def mv_sql = """select id as col1, sum(value) as col2 from ${tb_name} group by id;"""
                // directly query
                def mv_select_str = """select col1, count(col2) from ${tb_name} index ${mv_name1} group by col1"""
                def mv_select_str2 = """select col1, count(col2) from ${tb_name} index ${mv_name2} group by col1"""

                assertNoCache mv_sql
                assertHasCache mv_sql

                create_sync_mv(dbName, tb_name, mv_name1, mv_sql)
                assertNoCache mv_sql  // mark 创建mv之后mv sql不再命中cache
                assertNoCache mv_select_str
                assertHasCache mv_select_str

                // insert data
                sql """alter table ${tb_name} add partition p6 values[('6'),('7'))"""
                assertHasCache mv_sql
                assertHasCache mv_select_str

                sql "insert into ${tb_name} values(6, 1)"
                assertPartHasCache mv_sql
                assertHasCache mv_sql

                assertNoCache mv_select_str
                assertHasCache mv_select_str

                // alter rename mv
                sql """ALTER TABLE ${tb_name} RENAME ROLLUP ${mv_name1} ${mv_name2};"""
                assertNoCache mv_sql  // mark rename mv之后cache也失效蓝
                assertNoCache mv_select_str2
                assertHasCache mv_select_str2

                // drop mv
                sql """drop materialized view ${mv_name2} on ${tb_name};"""
                assertNoCache mv_sql // mark 删除mv之后cache也失效了
                test {
                    sql mv_select_str2
                    exception """doesn't have materialized view"""
                }
            })

 */


    ).get()

}
