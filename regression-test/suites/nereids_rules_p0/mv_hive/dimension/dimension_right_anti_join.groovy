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

/*
This suite is a two dimensional test case file.
It mainly tests the full join and filter positions.
 */
suite("partition_mv_rewrite_dimension_right_anti_join_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return
    }

    sql """SET materialized_view_rewrite_enable_contain_external_table = true;"""

    def create_mv = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1') 
        AS  
        ${mv_sql}
        """
    }

    def create_mv_lineitem = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        partition by(l_shipdate) 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1')  
        AS  
        ${mv_sql}
        """
    }

    def create_mv_orders = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        partition by(o_orderdate) 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1') 
        AS  
        ${mv_sql}
        """
    }

    def compare_res = { def stmt ->
        sql "SET materialized_view_rewrite_enable_contain_external_table=false"
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        sql "SET materialized_view_rewrite_enable_contain_external_table=true"
        def mv_origin_res = sql stmt
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }

    String ctl = "mv_rewrite_dimension_right_anti_join"
    String db = context.config.getDbNameByFile(context.file)
    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String catalog_name = ctl + "_" + hivePrefix
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true'
            );"""
            sql """switch ${catalog_name}"""
            sql """create database if not exists ${db}"""
            sql """use `${catalog_name}`.`${db}`"""

            def order_tb = "orders_self_conn"
            def lineitem_tb = "lineitem_self_conn"

            sql """
                drop table if exists ${order_tb}
                """

            sql """CREATE TABLE `${order_tb}` (
                  `o_orderkey` BIGINT,
                  `o_custkey` int,
                  `o_orderstatus` VARCHAR(1),
                  `o_totalprice` DECIMAL(15, 2),
                  `o_orderpriority` VARCHAR(15),
                  `o_clerk` VARCHAR(15),
                  `o_shippriority` int,
                  `o_comment` VARCHAR(79),
                  `o_orderdate` DATE
                ) ENGINE=hive
                PARTITION BY LIST (`o_orderdate`) ()
                PROPERTIES ( 
                    "replication_num" = "1",
                    'file_format'='orc'
                );"""

            sql """
                drop table if exists ${lineitem_tb}
                """
            sql """CREATE TABLE `${lineitem_tb}` (
                  `l_orderkey` BIGINT,
                  `l_linenumber` INT,
                  `l_partkey` INT,
                  `l_suppkey` INT,
                  `l_quantity` DECIMAL(15, 2),
                  `l_extendedprice` DECIMAL(15, 2),
                  `l_discount` DECIMAL(15, 2),
                  `l_tax` DECIMAL(15, 2),
                  `l_returnflag` VARCHAR(1),
                  `l_linestatus` VARCHAR(1),
                  `l_commitdate` DATE,
                  `l_receiptdate` DATE,
                  `l_shipinstruct` VARCHAR(25),
                  `l_shipmode` VARCHAR(10),
                  `l_comment` VARCHAR(44),
                  `l_shipdate` DATE
                ) ENGINE=hive
                PARTITION BY LIST (`l_shipdate`) ()
                PROPERTIES ( 
                    "replication_num" = "1",
                    'file_format'='orc'
                );"""

            sql """
                insert into ${order_tb} values 
                (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
                (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-17'),
                (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
                (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
                (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
                (3, 1, 'k', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
                (1, 3, 'o', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
                (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
                (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
                (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 
                """
            sql """
                insert into ${lineitem_tb} values 
                (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
                (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
                (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
                (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
                (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
                (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
                (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
                """

            sql """switch internal"""
            sql """create database if not exists ${db}"""
            sql """use ${db}"""



            // right semi join + filter on different position
            def mv_stmt_0 = """select o_orderkey, o_orderdate, o_custkey 
        from (select l_shipdate, l_partkey, l_suppkey, l_orderkey from `${catalog_name}`.`${db}`.${lineitem_tb} where l_shipdate = '2023-10-17') t
        right semi join `${catalog_name}`.`${db}`.${order_tb} 
        on t.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey"""

            def mv_stmt_1 = """select  t.o_orderkey, t.o_orderdate, t.o_custkey
        from `${catalog_name}`.`${db}`.${lineitem_tb}  
        right semi join (select o_orderdate, o_orderkey, o_custkey from `${catalog_name}`.`${db}`.${order_tb} where o_orderdate = '2023-10-17' ) t 
        on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t.o_orderkey"""

            def mv_stmt_2 = """select o_orderkey, o_orderdate, o_custkey 
        from `${catalog_name}`.`${db}`.${lineitem_tb}  
        right semi join `${catalog_name}`.`${db}`.${order_tb} 
        on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
        where o_orderdate = '2023-10-17'"""

            def mv_stmt_3 = """select t.l_shipdate, t.l_partkey, t.l_suppkey 
        from `${catalog_name}`.`${db}`.${order_tb} 
        right semi join  (select l_shipdate, l_orderkey, l_partkey, l_suppkey  from `${catalog_name}`.`${db}`.${lineitem_tb}  where l_shipdate = '2023-10-17') t 
        on t.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey"""

            def mv_stmt_4 = """select l_shipdate, l_partkey, l_suppkey
        from (select o_orderkey, o_orderdate, o_custkey from `${catalog_name}`.`${db}`.${order_tb} where o_orderdate = '2023-10-17' ) t 
        right semi join `${catalog_name}`.`${db}`.${lineitem_tb}   
        on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t.o_orderkey"""

            def mv_stmt_5 = """select l_shipdate, l_partkey, l_suppkey   
        from `${catalog_name}`.`${db}`.${order_tb} 
        right semi join `${catalog_name}`.`${db}`.${lineitem_tb}  
        on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
        where l_shipdate = '2023-10-17'  """

            def mv_list_1 = [mv_stmt_0, mv_stmt_1, mv_stmt_2, mv_stmt_3, mv_stmt_4, mv_stmt_5]
            def order_by_stmt = " order by 1,2,3"
            for (int i = 0; i < mv_list_1.size(); i++) {
                logger.info("i:" + i)
                def mv_name = """mv_name_2_right_anti_join_${i}"""
                if (i < 3) {
                    create_mv_orders(mv_name, mv_list_1[i])
                } else {
                    create_mv_lineitem(mv_name, mv_list_1[i])
                }
                def job_name = getJobName(db, mv_name)
                waitingMTMVTaskFinished(job_name)
                if (i == 0) {
                    for (int j = 0; j < mv_list_1.size(); j++) {
                        logger.info("j:" + j)
                        if (j in [0]) {
                            explain {
                                sql("${mv_list_1[j]}")
                                contains "${mv_name}(${mv_name})"
                            }
                            compare_res(mv_list_1[j] + order_by_stmt)
                        } else {
                            explain {
                                sql("${mv_list_1[j]}")
                                notContains "${mv_name}(${mv_name})"
                            }
                        }
                    }
                } else if (i == 1) {
                    for (int j = 0; j < mv_list_1.size(); j++) {
                        logger.info("j:" + j)
                        if (j in [1, 2]) {
                            explain {
                                sql("${mv_list_1[j]}")
                                contains "${mv_name}(${mv_name})"
                            }
                            compare_res(mv_list_1[j] + order_by_stmt)
                        } else {
                            explain {
                                sql("${mv_list_1[j]}")
                                notContains "${mv_name}(${mv_name})"
                            }
                        }
                    }
                } else if (i == 2) {
                    for (int j = 0; j < mv_list_1.size(); j++) {
                        logger.info("j:" + j)
                        if (j in [1, 2]) {
                            explain {
                                sql("${mv_list_1[j]}")
                                contains "${mv_name}(${mv_name})"
                            }
                            compare_res(mv_list_1[j] + order_by_stmt)
                        } else {
                            explain {
                                sql("${mv_list_1[j]}")
                                notContains "${mv_name}(${mv_name})"
                            }
                        }
                    }
                } else if (i == 3) {
                    for (int j = 0; j < mv_list_1.size(); j++) {
                        logger.info("j:" + j)
                        if (j in [3, 5]) {
                            explain {
                                sql("${mv_list_1[j]}")
                                contains "${mv_name}(${mv_name})"
                            }
                            compare_res(mv_list_1[j] + order_by_stmt)
                        } else {
                            explain {
                                sql("${mv_list_1[j]}")
                                notContains "${mv_name}(${mv_name})"
                            }
                        }
                    }
                } else if (i == 4) {
                    for (int j = 0; j < mv_list_1.size(); j++) {
                        logger.info("j:" + j)
                        if (j in [4]) {
                            explain {
                                sql("${mv_list_1[j]}")
                                contains "${mv_name}(${mv_name})"
                            }
                            compare_res(mv_list_1[j] + order_by_stmt)
                        } else {
                            explain {
                                sql("${mv_list_1[j]}")
                                notContains "${mv_name}(${mv_name})"
                            }
                        }
                    }
                } else if (i == 5) {
                    for (int j = 0; j < mv_list_1.size(); j++) {
                        logger.info("j:" + j)
                        if (j in [3, 5]) {
                            explain {
                                sql("${mv_list_1[j]}")
                                contains "${mv_name}(${mv_name})"
                            }
                            compare_res(mv_list_1[j] + order_by_stmt)
                        } else {
                            explain {
                                sql("${mv_list_1[j]}")
                                notContains "${mv_name}(${mv_name})"
                            }
                        }
                    }
                }
                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
            }

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
