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
suite("partition_mv_rewrite_dimension_self_conn_hive") {
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

    def checkMtmvCount = { def cur_mv_name ->
        def select_count = sql "select count(*) from ${cur_mv_name}"
        assertTrue(select_count[0][0] != 0)
    }

    String ctl = "mv_rewrite_dimension_right_join"
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

            def order_tb = "orders_2_left_anti_join"
            def lineitem_tb = "lineitem_2_left_anti_join"

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

            def mv_name = "mv_self_conn"
            def mtmv_sql = """
                select t1.l_Shipdate, t2.l_partkey, t1.l_suppkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                left join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.l_orderkey = t2.l_orderkey"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            def query_sql = """select t1.L_SHIPDATE 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                left join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.l_orderkey = t2.l_orderkey"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1")
            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


            // join filter position
            def join_filter_stmt_1 = """select t1.L_SHIPDATE, t2.l_partkey, t1.l_suppkey  
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                left join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.l_orderkey = t2.l_orderkey"""
            def join_filter_stmt_2 = """select t1.l_shipdate, t2.L_partkey, t1.l_suppkey     
                from (select * from `${catalog_name}`.`${db}`.${lineitem_tb} where l_shipdate = '2023-10-17' ) t1 
                left join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.l_orderkey = t2.l_orderkey"""
            def join_filter_stmt_3 = """select t1.l_shipdate, t2.l_Partkey, t1.l_suppkey   
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                left join (select * from `${catalog_name}`.`${db}`.${lineitem_tb} where l_shipdate = '2023-10-17' ) t2 
                on t1.l_orderkey = t2.l_orderkey"""
            def join_filter_stmt_4 = """select t1.l_shipdate, t2.l_parTkey, t1.l_suppkey  
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1
                left join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.l_orderkey = t2.l_orderkey 
                where t1.l_shipdate = '2023-10-17'"""
            def join_filter_stmt_5 = """select t1.l_shipdate, t2.l_partkey, t1.l_suppkey  
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                left join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.l_orderkey = t2.l_orderkey 
                where t1.l_suppkey=1"""

            def mv_list = [
                    join_filter_stmt_1, join_filter_stmt_2, join_filter_stmt_3, join_filter_stmt_4, join_filter_stmt_5]
            def join_self_conn_order = " order by 1, 2, 3"
            for (int i =0; i < mv_list.size(); i++) {
                logger.info("i:" + i)
                def join_self_conn_mv = """join_self_conn_mv_${i}"""
                create_mv(join_self_conn_mv, mv_list[i])
                def job_name = getJobName(db, join_self_conn_mv)
                waitingMTMVTaskFinished(job_name)
                if (i == 0) {
                    for (int j = 0; j < mv_list.size(); j++) {
                        logger.info("j:" + j)
                        if (j == 2) {
                            continue
                        }
                        explain {
                            sql("${mv_list[j]}")
                            contains "${join_self_conn_mv}(${join_self_conn_mv})"
                        }
                        compare_res(mv_list[j] + join_self_conn_order)
                    }
                } else if (i == 1) {
                    for (int j = 0; j < mv_list.size(); j++) {
                        logger.info("j:" + j)
                        if (j == 1 || j == 3) {
                            explain {
                                sql("${mv_list[j]}")
                                contains "${join_self_conn_mv}(${join_self_conn_mv})"
                            }
                            compare_res(mv_list[j] + join_self_conn_order)
                        } else {
                            explain {
                                sql("${mv_list[j]}")
                                notContains "${join_self_conn_mv}(${join_self_conn_mv})"
                            }
                        }
                    }
                } else if (i == 2) {
                    for (int j = 0; j < mv_list.size(); j++) {
                        logger.info("j:" + j)
                        if (j == 2) {
                            explain {
                                sql("${mv_list[j]}")
                                contains "${join_self_conn_mv}(${join_self_conn_mv})"
                            }
                            compare_res(mv_list[j] + join_self_conn_order)
                        } else {
                            explain {
                                sql("${mv_list[j]}")
                                notContains "${join_self_conn_mv}(${join_self_conn_mv})"
                            }
                        }

                    }
                } else if (i == 3) {
                    for (int j = 0; j < mv_list.size(); j++) {
                        logger.info("j:" + j)
                        if (j == 1 || j == 3) {
                            explain {
                                sql("${mv_list[j]}")
                                contains "${join_self_conn_mv}(${join_self_conn_mv})"
                            }
                            compare_res(mv_list[j] + join_self_conn_order)
                        } else {
                            explain {
                                sql("${mv_list[j]}")
                                notContains "${join_self_conn_mv}(${join_self_conn_mv})"
                            }
                        }
                    }
                } else if (i == 4) {
                    for (int j = 0; j < mv_list.size(); j++) {
                        logger.info("j:" + j)
                        if (j == 4) {
                            explain {
                                sql("${mv_list[j]}")
                                contains "${join_self_conn_mv}(${join_self_conn_mv})"
                            }
                            compare_res(mv_list[j] + join_self_conn_order)
                        } else {
                            explain {
                                sql("${mv_list[j]}")
                                notContains "${join_self_conn_mv}(${join_self_conn_mv})"
                            }
                        }
                    }
                }
                sql """DROP MATERIALIZED VIEW IF EXISTS ${join_self_conn_mv};"""
            }

            // join type
            def join_type_stmt_1 = """select t1.l_shipdate, t2.l_partkey, t1.l_suppkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                left join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.L_ORDERKEY = t2.L_ORDERKEY"""
            def join_type_stmt_2 = """select t1.l_shipdate, t2.l_partkey, t1.l_suppkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                inner join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.L_ORDERKEY = t2.L_ORDERKEY"""
            def join_type_stmt_3 = """select t1.l_shipdate, t2.l_partkey, t1.l_suppkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                right join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.L_ORDERKEY = t2.L_ORDERKEY"""
            def join_type_stmt_5 = """select t1.l_shipdate, t2.l_partkey, t1.l_suppkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                full join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.L_ORDERKEY = t2.L_ORDERKEY"""
            def join_type_stmt_6 = """select t1.l_shipdate, t1.l_partkey, t1.l_suppkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                left semi join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.L_ORDERKEY = t2.L_ORDERKEY"""
            def join_type_stmt_7 = """select t2.l_shipdate, t2.l_partkey, t2.l_suppkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                right semi join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.L_ORDERKEY = t2.L_ORDERKEY"""
            def join_type_stmt_8 = """select t1.l_shipdate, t1.l_partkey, t1.l_suppkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                left anti join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.L_ORDERKEY = t2.L_ORDERKEY"""
            def join_type_stmt_9 = """select t2.l_shipdate, t2.l_partkey, t2.l_suppkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                right anti join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.L_ORDERKEY = t2.L_ORDERKEY"""
            def join_type_stmt_list = [join_type_stmt_1, join_type_stmt_2, join_type_stmt_3,
                                       join_type_stmt_5, join_type_stmt_6, join_type_stmt_7, join_type_stmt_8, join_type_stmt_9]
            for (int i = 0; i < join_type_stmt_list.size(); i++) {
                logger.info("i:" + i)
                String join_type_self_conn_mv = """join_type_self_conn_mv_${i}"""
                create_mv(join_type_self_conn_mv, join_type_stmt_list[i])
                def job_name = getJobName(db, join_type_self_conn_mv)
                waitingMTMVTaskFinished(job_name)
                if (i in [4, 5]) {
                    for (int j = 0; j < join_type_stmt_list.size(); j++) {
                        logger.info("j: " + j)
                        if (j in [4, 5]) {
                            explain {
                                sql("${join_type_stmt_list[j]}")
                                contains "${join_type_self_conn_mv}(${join_type_self_conn_mv})"
                            }
                            compare_res(join_type_stmt_list[j] + " order by 1,2,3")
                        } else {
                            explain {
                                sql("${join_type_stmt_list[j]}")
                                notContains "${join_type_self_conn_mv}(${join_type_self_conn_mv})"
                            }
                        }
                    }
                } else if (i in [6, 7]) {
                    for (int j = 0; j < join_type_stmt_list.size(); j++) {
                        logger.info("j: " + j)
                        if (j in [6, 7]) {
                            explain {
                                sql("${join_type_stmt_list[j]}")
                                contains "${join_type_self_conn_mv}(${join_type_self_conn_mv})"
                            }
                            compare_res(join_type_stmt_list[j] + " order by 1,2,3")
                        } else {
                            explain {
                                sql("${join_type_stmt_list[j]}")
                                notContains "${join_type_self_conn_mv}(${join_type_self_conn_mv})"
                            }
                        }
                    }
                } else {
                    for (int j = 0; j < join_type_stmt_list.size(); j++) {
                        logger.info("j:" + j)
                        if (i == j) {
                            explain {
                                sql("${join_type_stmt_list[j]}")
                                contains "${join_type_self_conn_mv}(${join_type_self_conn_mv})"
                            }
                            compare_res(join_type_stmt_list[j] + " order by 1,2,3")
                        } else {
                            explain {
                                sql("${join_type_stmt_list[j]}")
                                notContains "${join_type_self_conn_mv}(${join_type_self_conn_mv})"
                            }
                        }
                    }
                }
                sql """DROP MATERIALIZED VIEW IF EXISTS ${join_type_self_conn_mv};"""
            }

            mtmv_sql = """
                select t2.o_orderkey, 
                sum(t1.O_TOTALPRICE) as sum_total,
                max(t1.o_totalprice) as max_total, 
                min(t1.o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when t1.o_shippriority > 1 and t1.o_orderkey IN (1, 3) then t1.o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when t1.o_shippriority > 2 and t1.o_orderkey IN (2) then t1.o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.${order_tb} as t1 
                left join `${catalog_name}`.`${db}`.${order_tb} as t2
                on t1.o_orderkey = t2.o_orderkey
                group by t2.o_orderkey"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select t2.o_orderkey,
                count(distinct case when t1.o_shippriority > 1 and t1.o_orderkey IN (1, 3) then t1.o_custkey else null end) as cnt_1, 
                count(distinct case when t1.O_SHIPPRIORITY > 2 and t1.o_orderkey IN (2) then t1.o_custkey else null end) as cnt_2, 
                sum(t1.O_totalprice), 
                max(t1.o_totalprice), 
                min(t1.o_totalprice), 
                count(*) 
                from `${catalog_name}`.`${db}`.${order_tb} as t1
                left join `${catalog_name}`.`${db}`.${order_tb} as t2
                on t1.o_orderkey = t2.o_orderkey
                group by t2.o_orderkey"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3,4,5,6,7")

            query_sql = """select t2.o_orderkey,
                count(distinct case when t1.o_shippriority > 1 and t1.o_orderkey IN (1, 3) then t1.o_custkey else null end) as cnt_1, 
                count(distinct case when t1.O_SHIPPRIORITY > 2 and t1.o_orderkey IN (2) then t1.o_custkey else null end) as cnt_2, 
                sum(t1.O_totalprice), 
                max(t1.o_totalprice), 
                min(t1.o_totalprice), 
                count(*) 
                from `${catalog_name}`.`${db}`.${order_tb} as t1
                left join `${catalog_name}`.`${db}`.${order_tb} as t2
                on t1.o_orderkey = t2.o_orderkey
                group by t2.o_orderkey"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3,4,5,6")


            mtmv_sql = """
                select t1.o_orderdatE, t2.O_SHIPPRIORITY, t1.o_comment  
                from `${catalog_name}`.`${db}`.${order_tb} as t1 
                inner join  `${catalog_name}`.`${db}`.${order_tb} as t2 
                on t1.o_orderkey = t2.o_orderkey 
                group by 
                t1.o_orderdate, 
                t2.o_shippriority, 
                t1.o_comment  """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select t2.O_SHIPPRIORITY, t1.o_comment  
                from `${catalog_name}`.`${db}`.${order_tb} as t1 
                inner join  `${catalog_name}`.`${db}`.${order_tb} as t2 
                on t1.o_orderkey = t2.o_orderkey 
                group by 
                t2.o_shippriority, 
                t1.o_comment  """
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2")


            mtmv_sql = """
                select t1.o_orderdatE, t2.o_shippriority, t1.o_comment, 
                sum(t1.o_totalprice) as sum_total, 
                max(t2.o_totalpricE) as max_total, 
                min(t1.o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when t1.o_shippriority > 1 and t1.o_orderkey IN (1, 3) then t1.o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when t2.o_shippriority > 2 and t2.o_orderkey IN (2) then t2.o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.${order_tb}  as t1 
                inner join `${catalog_name}`.`${db}`.${order_tb} as t2
                on t1.o_orderkey = t2.o_orderkey
                group by 
                t1.o_orderdatE, 
                t2.o_shippriority, 
                t1.o_comment"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select t2.o_shippriority, t1.o_comment, 
                sum(t1.o_totalprice) as sum_total, 
                max(t2.o_totalpricE) as max_total, 
                min(t1.o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when t1.o_shippriority > 1 and t1.o_orderkey IN (1, 3) then t1.o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when t2.o_shippriority > 2 and t2.o_orderkey IN (2) then t2.o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.${order_tb}  as t1 
                inner join `${catalog_name}`.`${db}`.${order_tb} as t2
                on t1.o_orderkey = t2.o_orderkey
                group by 
                t2.o_shippriority, 
                t1.o_comment"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3,4,5,6")

            mtmv_sql = """
                select t1.l_shipdatE, t2.l_partkey, t1.l_orderkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                inner join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                on t1.L_ORDERKEY = t2.L_ORDERKEY
                group by t1.l_shipdate, t2.l_partkey, t1.l_orderkeY"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select t.l_shipdate, `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey, t.l_partkey 
                from (
                    select t1.l_shipdatE as l_shipdatE, t2.l_partkey as l_partkey, t1.l_orderkey as l_orderkey 
                    from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                    inner join `${catalog_name}`.`${db}`.${lineitem_tb} as t2 
                    on t1.L_ORDERKEY = t2.L_ORDERKEY
                    group by t1.l_shipdate, t2.l_partkey, t1.l_orderkeY
                ) t 
                inner join `${catalog_name}`.`${db}`.${lineitem_tb}    
                on t.l_partkey = `${catalog_name}`.`${db}`.${lineitem_tb}.l_partkey 
                group by t.l_shipdate, `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey, t.l_partkey
                """
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")

            mtmv_sql = """
                select t1.l_shipdatE, t2.l_shipdate, t1.l_partkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                inner join `${catalog_name}`.`${db}`.${lineitem_tb} as t2  
                on t1.l_orderkey = t2.l_orderkey
                where t1.l_shipdate >= "2023-10-17"
            """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select t1.l_shipdatE, t2.l_shipdate, t1.l_partkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} as t1 
                inner join `${catalog_name}`.`${db}`.${lineitem_tb} as t2  
                on t1.l_orderkey = t2.l_orderkey
                where t1.l_shipdate >= "2023-10-17" and t1.l_partkey = 1
                """
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")


            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
