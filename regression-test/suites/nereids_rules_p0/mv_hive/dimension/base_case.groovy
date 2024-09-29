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
This suite is a one dimensional test case file.
 */
suite("partition_mv_rewrite_dimension_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return
    }

    String db = context.config.getDbNameByFile(context.file)

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
    def analyzeMtmvFunc = { def cur_mv_name ->
        sql """analyze table ${db}.${cur_mv_name}"""
    }

    def checkMtmvCount = { def cur_mv_name ->
        def select_count = sql "select count(*) from ${cur_mv_name}"
        assertTrue(select_count[0][0] != 0)
        analyzeMtmvFunc(cur_mv_name)
    }

    String ctl = "mv_rewrite_dimension_1"
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

            sql """
                drop table if exists orders_1
                """

            sql """CREATE TABLE `orders_1` (
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
                drop table if exists lineitem_1
                """
            sql """CREATE TABLE `lineitem_1` (
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
                drop table if exists partsupp_1
                """
            sql """CREATE TABLE `partsupp_1` (
                  `ps_partkey` INT NULL,
                  `ps_suppkey` INT NULL,
                  `ps_availqty` INT NULL,
                  `ps_supplycost` DECIMAL(15, 2) NULL,
                  `ps_comment` VARCHAR(199) NULL
                ) ENGINE=hive
                PROPERTIES ( 
                    "replication_num" = "1",
                    'file_format'='orc'
                );"""

            sql """
                insert into orders_1 values 
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
                insert into lineitem_1 values 
                (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
                (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
                (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
                (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
                (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
                (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
                (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
                """
            sql """
                insert into partsupp_1 values 
                (1, 1, 1, 99.5, 'yy'),
                (null, 2, 2, 109.2, 'mm'),
                (3, null, 1, 99.5, 'yy'); 
                """
            sql """analyze table ${catalog_name}.${db}.orders_1 with sync;"""
            sql """analyze table ${catalog_name}.${db}.lineitem_1 with sync;"""
            sql """analyze table ${catalog_name}.${db}.partsupp_1 with sync;"""
            sql """switch internal"""
            sql """create database if not exists ${db}"""
            sql """use ${db}"""
            def mv_name = "test_mv"
            def mtmv_sql = """
                select l_Shipdate, o_Orderdate, l_partkey, l_suppkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)


            def query_sql = """select L_SHIPDATE 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1")
            query_sql = """select L_SHIPDATE 
                from  `${catalog_name}`.`${db}`.orders_1 
                left join `${catalog_name}`.`${db}`.lineitem_1 
                on `${catalog_name}`.`${db}`.orders_1.o_orderkey = `${catalog_name}`.`${db}`.lineitem_1.L_ORDERKEY"""
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            mtmv_sql = """
                select L_SHIPDATE, O_orderdate, l_partkey, l_suppkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                inner join `${catalog_name}`.`${db}`.orders_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select l_shipdaTe 
                from `${catalog_name}`.`${db}`.lineitem_1 
                inner join `${catalog_name}`.`${db}`.orders_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1")

            query_sql = """select L_shipdate 
                from  `${catalog_name}`.`${db}`.orders_1 
                inner join `${catalog_name}`.`${db}`.lineitem_1 
                on `${catalog_name}`.`${db}`.orders_1.o_orderkey = `${catalog_name}`.`${db}`.lineitem_1.l_orderkey"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1")
            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""

            // join filter position
            def join_filter_stmt_1 = """
                select L_SHIPDATE, o_orderdate, l_partkey, l_suppkey, O_orderkey 
                from `${catalog_name}`.`${db}`.lineitem_1  
                left join `${catalog_name}`.`${db}`.orders_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
            def join_filter_stmt_2 = """
                select l_shipdate, o_orderdate, L_partkey, l_suppkey, O_ORDERKEY    
                from (select * from `${catalog_name}`.`${db}`.lineitem_1 where l_shipdate = '2023-10-17' ) t1 
                left join `${catalog_name}`.`${db}`.orders_1 
                on t1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
            def join_filter_stmt_3 = """
                select l_shipdate, o_orderdate, l_Partkey, l_suppkey, o_orderkey  
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join (select * from `${catalog_name}`.`${db}`.orders_1 where o_orderdate = '2023-10-17' ) t2 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = t2.o_orderkey"""
            def join_filter_stmt_4 = """
                select l_shipdate, o_orderdate, l_parTkey, l_suppkey, o_orderkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey 
                where l_shipdate = '2023-10-17' and o_orderdate = '2023-10-17'"""
            def join_filter_stmt_5 = """
                select l_shipdate, o_orderdate, l_partkeY, l_suppkey, o_orderkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey 
                where l_shipdate = '2023-10-17'"""
            def join_filter_stmt_6 = """
                select l_shipdatE, o_orderdate, l_partkey, l_suppkey, o_orderkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey 
                where  o_orderdate = '2023-10-17'"""
            def join_filter_stmt_7 = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkeY 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey 
                where  `${catalog_name}`.`${db}`.orders_1.O_ORDERKEY=1"""

            def mv_list = [
                    join_filter_stmt_1, join_filter_stmt_2, join_filter_stmt_3, join_filter_stmt_4,
                    join_filter_stmt_5, join_filter_stmt_6, join_filter_stmt_7]

            for (int i =0; i < mv_list.size(); i++) {
                logger.info("i:" + i)
                def join_filter_mv = """join_filter_mv_${i}"""
                create_mv(join_filter_mv, mv_list[i])
                waitingMTMVTaskFinishedByMvName(join_filter_mv)
                checkMtmvCount(join_filter_mv)

                def res_1 = sql """show partitions from ${join_filter_mv};"""
                logger.info("res_1:" + res_1)
                if (i == 0) {
                    for (int j = 0; j < mv_list.size(); j++) {
                        logger.info("j:" + j)
                        if (j == 2) {
                            continue
                        }
                        explain {
                            sql("${mv_list[j]}")
                            contains "${join_filter_mv}(${join_filter_mv})"
                        }
                        compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                    }
                } else if (i == 1) {
                    for (int j = 0; j < mv_list.size(); j++) {
                        logger.info("j:" + j)
                        if (j == 1 || j == 4 || j == 3) {
                            explain {
                                sql("${mv_list[j]}")
                                contains "${join_filter_mv}(${join_filter_mv})"
                            }
                            compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                        } else {
                            explain {
                                sql("${mv_list[j]}")
                                notContains "${join_filter_mv}(${join_filter_mv})"
                            }
                        }
                    }
                } else if (i == 2) {
                    for (int j = 0; j < mv_list.size(); j++) {
                        logger.info("j:" + j)
                        if (j == 2 || j == 3 || j == 5) {
                            explain {
                                sql("${mv_list[j]}")
                                contains "${join_filter_mv}(${join_filter_mv})"
                            }
                            compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                        } else {
                            explain {
                                sql("${mv_list[j]}")
                                notContains "${join_filter_mv}(${join_filter_mv})"
                            }
                        }

                    }
                } else if (i == 3) {
                    for (int j = 0; j < mv_list.size(); j++) {
                        logger.info("j:" + j)
                        if (j == 3) {
                            explain {
                                sql("${mv_list[j]}")
                                contains "${join_filter_mv}(${join_filter_mv})"
                            }
                            compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                        } else {
                            explain {
                                sql("${mv_list[j]}")
                                notContains "${join_filter_mv}(${join_filter_mv})"
                            }
                        }
                    }
                } else if (i == 4) {
                    for (int j = 0; j < mv_list.size(); j++) {
                        logger.info("j:" + j)
                        if (j == 4 || j == 1 || j == 3) {
                            explain {
                                sql("${mv_list[j]}")
                                contains "${join_filter_mv}(${join_filter_mv})"
                            }
                            compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                        } else {
                            explain {
                                sql("${mv_list[j]}")
                                notContains "${join_filter_mv}(${join_filter_mv})"
                            }
                        }
                    }
                } else if (i == 5) {
                    for (int j = 0; j < mv_list.size(); j++) {
                        if (j == 5 || j == 3) {
                            explain {
                                sql("${mv_list[j]}")
                                contains "${join_filter_mv}(${join_filter_mv})"
                            }
                            compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                        } else {
                            explain {
                                sql("${mv_list[j]}")
                                notContains "${join_filter_mv}(${join_filter_mv})"
                            }
                        }

                    }
                } else if (i == 6) {
                    for (int j = 0; j < mv_list.size(); j++) {
                        if (j == 6) {
                            explain {
                                sql("${mv_list[j]}")
                                contains "${join_filter_mv}(${join_filter_mv})"
                            }
                            compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                        } else {
                            explain {
                                sql("${mv_list[j]}")
                                notContains "${join_filter_mv}(${join_filter_mv})"
                            }
                        }

                    }
                }
                sql """DROP MATERIALIZED VIEW IF EXISTS ${join_filter_mv};"""
            }

            // join type
            def join_type_stmt_1 = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1 
                on `${catalog_name}`.`${db}`.lineitem_1.L_ORDERKEY = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
            def join_type_stmt_2 = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey  
                from `${catalog_name}`.`${db}`.lineitem_1 
                inner join `${catalog_name}`.`${db}`.orders_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""

            // Todo: right/cross/full/semi/anti join
            // Currently, only left join and inner join are supported.
            def join_type_stmt_3 = """
                select l_shipdate, o_orderdatE, l_partkey, l_suppkey
                from `${catalog_name}`.`${db}`.lineitem_1
                right join `${catalog_name}`.`${db}`.orders_1
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
        //    def join_type_stmt_4 = """
        //        select l_shipdate, o_orderdate, l_partkey, l_suppkey
        //        from `${catalog_name}`.`${db}`.lineitem_1
        //        cross join `${catalog_name}`.`${db}`.orders_1"""
            def join_type_stmt_5 = """
                select l_shipdate, o_orderdate, L_partkey, l_suppkey
                from `${catalog_name}`.`${db}`.lineitem_1
                full join `${catalog_name}`.`${db}`.orders_1
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
            def join_type_stmt_6 = """
                select l_shipdate, l_partkey, l_suppkey, l_Shipmode, l_orderkey 
                from `${catalog_name}`.`${db}`.lineitem_1
                left semi join `${catalog_name}`.`${db}`.orders_1
                on `${catalog_name}`.`${db}`.lineitem_1.L_ORDERKEY = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
            def join_type_stmt_7 = """
                select o_orderkey, o_custkey, o_Orderdate, o_clerk, o_totalprice 
                from `${catalog_name}`.`${db}`.lineitem_1
                right semi join `${catalog_name}`.`${db}`.orders_1
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
            def join_type_stmt_8 = """
                select l_shipdate, l_partkey, l_suppkeY, l_shipmode, l_orderkey 
                from `${catalog_name}`.`${db}`.lineitem_1
                left anti join `${catalog_name}`.`${db}`.orders_1
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkeY"""
            def join_type_stmt_9 = """
                select o_orderkey, o_custkeY, o_orderdate, o_clerk, o_totalprice 
                from `${catalog_name}`.`${db}`.lineitem_1
                right anti join `${catalog_name}`.`${db}`.orders_1
                on `${catalog_name}`.`${db}`.lineitem_1.L_ORDERKEY = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
            def join_type_stmt_list = [join_type_stmt_1, join_type_stmt_2, join_type_stmt_3,
                                       join_type_stmt_5, join_type_stmt_6, join_type_stmt_7, join_type_stmt_8, join_type_stmt_9]
            for (int i = 0; i < join_type_stmt_list.size(); i++) {
                logger.info("i:" + i)
                String join_type_mv = """join_type_mv_${i}"""
                create_mv(join_type_mv, join_type_stmt_list[i])
                waitingMTMVTaskFinishedByMvName(join_type_mv)
                checkMtmvCount(join_type_mv)
                for (int j = 0; j < join_type_stmt_list.size(); j++) {
                    logger.info("j:" + j)
                    if (i == j) {
                        explain {
                            sql("${join_type_stmt_list[j]}")
                            contains "${join_type_mv}(${join_type_mv})"
                        }
                        compare_res(join_type_stmt_list[j] + " order by 1,2,3,4")
                    } else {
                        explain {
                            sql("${join_type_stmt_list[j]}")
                            notContains "${join_type_mv}(${join_type_mv})"
                        }
                    }
                }
                sql """DROP MATERIALIZED VIEW IF EXISTS ${join_type_mv};"""
            }

            // agg
            // agg + without group by + with agg function
            mtmv_sql = """
                select
                sum(O_TOTALPRICE) as sum_total,
                max(o_totalprice) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1
                """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select 
                count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1, 
                count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2, 
                sum(O_totalprice), 
                max(o_totalprice), 
                min(o_totalprice), 
                count(*) 
                from `${catalog_name}`.`${db}`.orders_1"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3,4,5,6")

            // agg + with group by + without agg function
            mtmv_sql = """
                select o_orderdatE, O_SHIPPRIORITY, o_comment  
                from `${catalog_name}`.`${db}`.orders_1 
                group by 
                o_orderdate, 
                o_shippriority, 
                o_comment  
                """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select O_shippriority, o_commenT 
                from `${catalog_name}`.`${db}`.orders_1 
                group by 
                o_shippriority, 
                o_comment """
            def sql_explain_2 = sql """explain ${query_sql};"""
            def mv_index_1 = sql_explain_2.toString().indexOf("MaterializedViewRewriteFail:")
            assert(mv_index_1 != -1)
            assert(sql_explain_2.toString().substring(0, mv_index_1).indexOf(mv_name) != -1)
            compare_res(query_sql + " order by 1,2")

            // agg + with group by + with agg function
            mtmv_sql = """
                select o_orderdatE, o_shippriority, o_comment, 
                sum(o_totalprice) as sum_total, 
                max(o_totalpricE) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1 
                group by 
                o_orderdatE, 
                o_shippriority, 
                o_comment
                """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select o_shipprioritY, o_comment, 
                count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
                count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2, 
                sum(o_totalprice), 
                max(o_totalprice), 
                min(o_totalprice), 
                count(*) 
                from `${catalog_name}`.`${db}`.orders_1 
                group by 
                o_shippriority, 
                o_commenT """
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3,4,5,6,7,8")

            // view partital rewriting
            mtmv_sql = """
                select l_shipdatE, l_partkey, l_orderkey from `${catalog_name}`.`${db}`.lineitem_1 group by l_shipdate, l_partkey, l_orderkeY
                """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select t.l_shipdate, o_orderdate, t.l_partkey 
                from (select l_shipdate, l_partkey, l_orderkey from `${catalog_name}`.`${db}`.lineitem_1 group by l_shipdate, l_partkey, l_orderkey) t
                left join `${catalog_name}`.`${db}`.orders_1   
                on t.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey group by t.l_shipdate, o_orderdate, t.l_partkey"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")

            // predicate compensate
            mtmv_sql = """
                select l_shipdatE, o_orderdate, l_partkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1   
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                where l_shipdate >= "2023-10-17"
                """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select l_shipdate, o_orderdate, l_partkeY 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1   
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                where l_shipdate >= "2023-10-17" and l_partkey = 1"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")

            // single table
//            mtmv_sql = """
//                select l_Shipdate, l_partkey, l_suppkey
//                from `${catalog_name}`.`${db}`.lineitem_1
//                where l_commitdate like '2023-10-%'
//                """
//            create_mv(mv_name, mtmv_sql)
//            waitingMTMVTaskFinishedByMvName(mv_name)
//            checkMtmvCount(mv_name)
//
//            query_sql = """select l_Shipdate, l_partkey, l_suppkey
//                from `${catalog_name}`.`${db}`.lineitem_1
//                where l_commitdate like '2023-10-%'"""
//            explain {
//                sql("${query_sql}")
//                contains "${mv_name}(${mv_name})"
//            }
//            compare_res(query_sql + " order by 1,2,3")
//
//            query_sql = """select l_Shipdate, l_partkey, l_suppkey
//                from `${catalog_name}`.`${db}`.lineitem_1
//                where l_commitdate like '2023-10-%' and l_partkey > 0 + 1"""
//            explain {
//                sql("${query_sql}")
//                contains "${mv_name}(${mv_name})"
//            }
//            compare_res(query_sql + " order by 1,2,3")

            mtmv_sql = """
                select l_Shipdate, l_partkey, l_suppkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                where l_commitdate in (select l_commitdate from `${catalog_name}`.`${db}`.lineitem_1) 
                """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select l_Shipdate, l_partkey, l_suppkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                where l_commitdate in (select l_commitdate from `${catalog_name}`.`${db}`.lineitem_1) """
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")

            mtmv_sql = """
                select o_orderdate, o_shippriority, o_comment, 
                sum(o_totalprice) as sum_total, 
                max(o_totalprice) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1 
                left join `${catalog_name}`.`${db}`.lineitem_1 on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                group by 
                o_orderdate, 
                o_shippriority, 
                o_comment"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select o_shippriority, o_comment, 
                count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
                count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2, 
                sum(o_totalprice), 
                max(o_totalprice), 
                min(o_totalprice), 
                count(*) 
                from `${catalog_name}`.`${db}`.orders_1 
                left join `${catalog_name}`.`${db}`.lineitem_1 on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                group by 
                o_shippriority, 
                o_comment"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3,4,5,6,7,8")

            mtmv_sql = """
                select l_shipdate, l_partkey, l_orderkey, o_orderdate 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select l_shipdate, o_orderdate, l_partkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1  on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")

            mtmv_sql = """
                select l_shipdate, o_orderdate, l_partkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1   
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                where l_shipdate >= '2023-10-17'"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select l_shipdate, o_orderdate, l_partkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                left join `${catalog_name}`.`${db}`.orders_1   
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                where l_shipdate >= "2023-10-17" and l_partkey = 3"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")

            mtmv_sql = """
                select o_orderdate, o_shippriority, o_comment, l_suppkey, o_shippriority + o_custkey, 
                case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end cnt_1, 
                case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1  left join `${catalog_name}`.`${db}`.lineitem_1   on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                where  o_orderkey > 1 + 1 """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select o_shippriority, o_comment, o_shippriority + o_custkey  + l_suppkey, 
                case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end as cnt_1,
                case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1  left join `${catalog_name}`.`${db}`.lineitem_1   on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                where  o_orderkey > (-3) + 5 """
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3,4,5")

            mtmv_sql = """
                select 
                o_totalprice, 
                o_shippriority,
                o_orderkey,
                l_orderkey,
                o_custkey 
                from `${catalog_name}`.`${db}`.orders_1 
                left join `${catalog_name}`.`${db}`.lineitem_1
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select 
                o_totalprice, 
                o_shippriority,
                o_orderkey,
                l_orderkey,
                o_custkey 
                from `${catalog_name}`.`${db}`.orders_1 
                left join `${catalog_name}`.`${db}`.lineitem_1
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                left join `${catalog_name}`.`${db}`.partsupp_1 on `${catalog_name}`.`${db}`.partsupp_1.ps_partkey = `${catalog_name}`.`${db}`.lineitem_1.l_orderkey"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3,4,5")

            mtmv_sql = """
                select o_orderdate, o_shippriority, o_comment 
                from `${catalog_name}`.`${db}`.orders_1 
                left join `${catalog_name}`.`${db}`.lineitem_1 on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                group by 
                o_orderdate, 
                o_shippriority, 
                o_comment"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select o_orderdate, o_shippriority, o_comment
                from `${catalog_name}`.`${db}`.orders_1
                left join `${catalog_name}`.`${db}`.lineitem_1 on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                left join `${catalog_name}`.`${db}`.partsupp_1 on `${catalog_name}`.`${db}`.partsupp_1.ps_partkey = `${catalog_name}`.`${db}`.lineitem_1.l_orderkey
                group by
                o_orderdate,
                o_shippriority,
                o_comment """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")

            mtmv_sql = """
                select  o_orderdate, o_shippriority, o_comment,
                sum(o_totalprice) as sum_total, 
                max(o_totalprice) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1 
                left join `${catalog_name}`.`${db}`.lineitem_1 on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                group by 
                o_orderdate, 
                o_shippriority, 
                o_comment"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select  o_orderdate, o_shippriority, o_comment,
                sum(o_totalprice) as sum_total, 
                max(o_totalprice) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1 
                left join `${catalog_name}`.`${db}`.lineitem_1 on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                left join `${catalog_name}`.`${db}`.partsupp_1 on `${catalog_name}`.`${db}`.partsupp_1.ps_partkey = `${catalog_name}`.`${db}`.lineitem_1.l_orderkey 
                group by 
                o_orderdate, 
                o_shippriority, 
                o_comment"""
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3,4,5,6,7")

            mtmv_sql = """
                select 
                sum(o_totalprice) as sum_total, 
                max(o_totalprice) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1 
                where o_orderdate >= '2023-10-17'"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select t.sum_total from (select 
                sum(o_totalprice) as sum_total, 
                max(o_totalprice) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1 where o_orderdate >= "2023-10-17" )  as t
                where t.count_all = 3"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1")

            mtmv_sql = """
                select o_orderdate, o_shippriority, o_comment 
                from `${catalog_name}`.`${db}`.orders_1 
                where o_orderdate >= "2023-10-17"
                group by 
                o_orderdate, 
                o_shippriority, 
                o_comment"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select o_orderdate, o_shippriority, o_comment
                from `${catalog_name}`.`${db}`.orders_1
                where o_orderdate >= "2023-10-17" and o_totalprice = 1
                group by
                o_orderdate,
                o_shippriority,
                o_comment """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")

            mtmv_sql = """
                select o_orderdate, o_shippriority, o_comment, o_totalprice 
                from `${catalog_name}`.`${db}`.orders_1 
                where o_orderdate >= "2023-10-17"
                group by 
                o_orderdate, 
                o_shippriority, 
                o_comment,
                o_totalprice"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select o_orderdate, o_shippriority, o_comment
                from `${catalog_name}`.`${db}`.orders_1
                where o_orderdate >= "2023-10-17" and o_totalprice = 1
                group by
                o_orderdate,
                o_shippriority,
                o_comment """
            sql_explain_2 = sql """explain ${query_sql};"""
            mv_index_1 = sql_explain_2.toString().indexOf("MaterializedViewRewriteFail:")
            assert(mv_index_1 != -1)
            assert(sql_explain_2.toString().substring(0, mv_index_1).indexOf(mv_name) != -1)
            compare_res(query_sql + " order by 1,2,3")

            mtmv_sql = """
                select o_orderdate, o_shippriority, o_comment , o_totalprice, 
                sum(o_totalprice) as sum_total, 
                max(o_totalprice) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1 
                where o_orderdate >= "2023-10-17" 
                group by 
                o_orderdate, 
                o_shippriority, 
                o_comment,
                o_totalprice"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select t.o_orderdate, t.o_shippriority, t.o_comment, 
                t.sum_total, t.max_total, t.min_total, t.count_all 
                from  (
                select o_orderdate, o_shippriority, o_comment , o_totalprice, 
                sum(o_totalprice) as sum_total, 
                max(o_totalprice) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1 where o_orderdate >= "2023-10-17" 
                group by 
                o_orderdate, 
                o_shippriority, 
                o_comment,
                o_totalprice 
                ) as t 
                where t.o_totalprice = 1 """
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3,4,5,6,7")

            mtmv_sql = """
                select sum(o_totalprice) as sum_total, 
                max(o_totalprice) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1  
                where  o_orderkey > 1 + 1"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select sum(o_totalprice) + count(*) , 
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2 
            from `${catalog_name}`.`${db}`.orders_1  
            where o_orderkey > (-3) + 5"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1")

            mtmv_sql = """
                select sum(o_totalprice) as sum_total, 
                max(o_totalprice) as max_total, 
                min(o_totalprice) as min_total, 
                count(*) as count_all, 
                bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
                bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1  
                where  o_orderkey > 1 + 1"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select sum(o_totalprice) + count(*) , 
                count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
                count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2 
                from `${catalog_name}`.`${db}`.orders_1  
                where o_orderkey > (-3) + 5"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3,4,5")

            mtmv_sql = """
                select o_orderdate, o_shippriority, o_comment, l_orderkey, o_orderkey 
                from `${catalog_name}`.`${db}`.orders_1  
                left join `${catalog_name}`.`${db}`.lineitem_1 on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                group by 
                o_orderdate, 
                o_shippriority, 
                o_comment,
                l_orderkey,
                o_orderkey"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select o_orderdate, o_shippriority, o_comment
                from `${catalog_name}`.`${db}`.orders_1
                left join `${catalog_name}`.`${db}`.lineitem_1 on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                left join `${catalog_name}`.`${db}`.partsupp_1 on `${catalog_name}`.`${db}`.partsupp_1.ps_partkey = `${catalog_name}`.`${db}`.lineitem_1.l_orderkey
                group by
                o_orderdate,
                o_shippriority,
                o_comment"""
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")

            query_sql = """select o_orderdate, o_shippriority, o_comment
                from `${catalog_name}`.`${db}`.orders_1
                left join `${catalog_name}`.`${db}`.lineitem_1 on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                left join `${catalog_name}`.`${db}`.partsupp_1 on `${catalog_name}`.`${db}`.partsupp_1.ps_partkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                group by
                o_orderdate,
                o_shippriority,
                o_comment"""
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")

            mtmv_sql = """
                select l_shipdate, l_partkey, l_orderkey 
                from `${catalog_name}`.`${db}`.lineitem_1 
                where l_shipdate >= "2023-10-17"
                group by l_shipdate, l_partkey, l_orderkey"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select t.l_shipdate, o_orderdate, t.l_partkey 
                from (select l_shipdate, l_partkey, l_orderkey from `${catalog_name}`.`${db}`.lineitem_1 group by l_shipdate, l_partkey, l_orderkey) t 
                left join `${catalog_name}`.`${db}`.orders_1   
                on t.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey 
                where l_shipdate >= "2023-10-17" and l_partkey  > 1 + 1 
                group by t.l_shipdate, o_orderdate, t.l_partkey"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")

            mtmv_sql = """
                select l_shipdate, l_partkey, l_orderkey
                from `${catalog_name}`.`${db}`.lineitem_1
                where l_partkey  > 1 + 1
                group by l_shipdate, l_partkey, l_orderkey"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select t.l_shipdate, o_orderdate, t.l_partkey * 2
                from (select l_shipdate, l_partkey, l_orderkey from `${catalog_name}`.`${db}`.lineitem_1 group by l_shipdate, l_partkey, l_orderkey) t
                left join `${catalog_name}`.`${db}`.orders_1
                on t.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey
                where  l_partkey  > (-3) + 5
                group by t.l_shipdate, o_orderdate, t.l_partkey"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3")

            mtmv_sql = """
                select o_orderdate, o_shippriority, o_comment, o_custkey,
                case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end cnt_1,
                case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2
                from `${catalog_name}`.`${db}`.orders_1 
                left join `${catalog_name}`.`${db}`.lineitem_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey 
                where  o_custkey > 1 + 1"""
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """select o_orderdate, o_shippriority, o_comment, o_shippriority + o_custkey,
                case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end cnt_1,
                case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2
                from `${catalog_name}`.`${db}`.orders_1 
                left join `${catalog_name}`.`${db}`.lineitem_1 
                on `${catalog_name}`.`${db}`.lineitem_1.l_orderkey = `${catalog_name}`.`${db}`.orders_1.o_orderkey 
                where  o_custkey > (-3) + 5 and o_orderdate >= '2023-10-17'"""
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }
            compare_res(query_sql + " order by 1,2,3,4,5,6")

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }

}
