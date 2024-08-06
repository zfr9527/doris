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
suite("hive_join_agg_replenish") {
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

    String ctl = "mv_rewrite_hive_join_agg"
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

            def order_tb = "orders_hive_join_agg"
            def lineitem_tb = "lineitem_hive_join_agg"
            def partsupp_tb = "partsupp_hive_join_agg"

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
                drop table if exists ${partsupp_tb}
                """
            sql """CREATE TABLE `${partsupp_tb}` (
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
            sql"""
                insert into ${partsupp_tb} values 
                (1, 1, 1, 99.5, 'yy'),
                (null, 2, 2, 109.2, 'mm'),
                (3, null, 1, 99.5, 'yy'); 
                """
            sql """switch internal"""
            sql """create database if not exists ${db}"""
            sql """use ${db}"""
            // left join + agg (function + group by + +-*/ + filter)
            def left_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            left join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

            def left_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def left_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def left_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
            def left_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            left join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey 
            group by t1.sum_total, col3, count_all       
            """

            def left_mv_stmt_6 = """select t1.l_shipdate, t1.l_quantity, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            left join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.l_quantity, t1.l_orderkey """

            def left_mv_stmt_7 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            left join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey 
            """
            def left_mv_stmt_8 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            left join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey
            """
            def left_mv_stmt_9 = """select t1.l_shipdate, t1.col1, t1.l_orderkey + t1.col2 as col3
            from `${catalog_name}`.`${db}`.${order_tb} 
            left join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, col3
            """
            def left_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from `${catalog_name}`.`${db}`.${order_tb} 
            left join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.sum_total, col3, count_all
            """

            // inner join + agg (function + group by + +-*/ + filter)
            def inner_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            inner join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

            def inner_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            inner join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def inner_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            inner join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def inner_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            inner join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
            def inner_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            inner join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey 
            group by t1.sum_total, col3, count_all       
            """

            def inner_mv_stmt_6 = """select t1.l_shipdate, t1.l_quantity, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            inner join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.l_quantity, t1.l_orderkey """

            def inner_mv_stmt_7 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            inner join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey 
            """
            def inner_mv_stmt_8 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            inner join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey
            """
            def inner_mv_stmt_9 = """select t1.l_shipdate, t1.col1, t1.l_orderkey + t1.col2 as col3
            from `${catalog_name}`.`${db}`.${order_tb} 
            inner join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, col3
            """
            def inner_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from `${catalog_name}`.`${db}`.${order_tb} 
            inner join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.sum_total, col3, count_all
            """

            // right join + agg (function + group by + +-*/ + filter)
            def right_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            right join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

            def right_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def right_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def right_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
            def right_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            right join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey 
            group by t1.sum_total, col3, count_all       
            """

            def right_mv_stmt_6 = """select t1.l_shipdate, t1.l_quantity, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            right join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.l_quantity, t1.l_orderkey """

            def right_mv_stmt_7 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            right join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey 
            """
            def right_mv_stmt_8 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            right join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey
            """
            def right_mv_stmt_9 = """select t1.l_shipdate, t1.col1, t1.l_orderkey + t1.col2 as col3
            from `${catalog_name}`.`${db}`.${order_tb} 
            right join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, col3
            """
            def right_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from `${catalog_name}`.`${db}`.${order_tb} 
            right join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.sum_total, col3, count_all
            """

            // Todo: full/cross/semi/anti join + agg (function + group by + +-*/ + filter)
            def full_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            full join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

            def full_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            full join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def full_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            full join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def full_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            full join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
            def full_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            full join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey 
            group by t1.sum_total, col3, count_all       
            """

            def full_mv_stmt_6 = """select t1.l_shipdate, t1.l_quantity, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            full join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.l_quantity, t1.l_orderkey """

            def full_mv_stmt_7 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            full join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey 
            """
            def full_mv_stmt_8 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            full join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey
            """
            def full_mv_stmt_9 = """select t1.l_shipdate, t1.col1, t1.l_orderkey + t1.col2 as col3
            from `${catalog_name}`.`${db}`.${order_tb} 
            full join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, col3
            """
            def full_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from `${catalog_name}`.`${db}`.${order_tb} 
            full join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.sum_total, col3, count_all
            """

            def cross_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            cross join `${catalog_name}`.`${db}`.${lineitem_tb} 
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

            def cross_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            cross join `${catalog_name}`.`${db}`.${lineitem_tb} 
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def cross_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            cross join `${catalog_name}`.`${db}`.${lineitem_tb} 
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def cross_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            cross join `${catalog_name}`.`${db}`.${lineitem_tb} 
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
            def cross_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            cross join `${catalog_name}`.`${db}`.${lineitem_tb} 
            group by t1.sum_total, col3, count_all       
            """

            def cross_mv_stmt_6 = """select t1.l_shipdate, t1.l_quantity, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            cross join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            group by
            t1.l_shipdate, t1.l_quantity, t1.l_orderkey """

            def cross_mv_stmt_7 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            cross join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey 
            """
            def cross_mv_stmt_8 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            cross join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey
            """
            def cross_mv_stmt_9 = """select t1.l_shipdate, t1.col1, t1.l_orderkey + t1.col2 as col3
            from `${catalog_name}`.`${db}`.${order_tb} 
            cross join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            group by
            t1.l_shipdate, t1.col1, col3
            """
            def cross_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from `${catalog_name}`.`${db}`.${order_tb} 
            cross join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            group by
            t1.sum_total, col3, count_all
            """

            def left_semi_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            left semi join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

            def left_semi_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left semi join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def left_semi_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left semi join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def left_semi_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left semi join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
            def left_semi_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            left semi join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey 
            group by t1.sum_total, col3, count_all       
            """

            def left_semi_mv_stmt_6 = """select  `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            left semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey  """

            def left_semi_mv_stmt_7 = """select `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            left semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
            """
            def left_semi_mv_stmt_8 = """select `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            left semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
            """
            def left_semi_mv_stmt_9 = """select `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            left semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
            """
            def left_semi_mv_stmt_10 = """select `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            left semi join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
            """

            def right_semi_mv_stmt_1 = """select l_orderkey, l_shipdate, l_partkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            right semi join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by
            l_orderkey, l_shipdate, l_partkey """

            def right_semi_mv_stmt_2 = """select l_orderkey, l_shipdate, l_partkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right semi join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            l_orderkey, l_shipdate, l_partkey 
            """
            def right_semi_mv_stmt_3 = """select l_orderkey, l_shipdate, l_partkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right semi join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            l_orderkey, l_shipdate, l_partkey 
            """
            def right_semi_mv_stmt_4 = """select l_orderkey, l_shipdate, l_partkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right semi join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            l_orderkey, l_shipdate, l_partkey 
            """
            def right_semi_mv_stmt_5 = """select l_orderkey, l_shipdate, l_partkey 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            right semi join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey 
            group by l_orderkey, l_shipdate, l_partkey 
            """

            def right_semi_mv_stmt_6 = """select t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            right semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_orderkey, t1.l_shipdate, t1.l_partkey  """

            def right_semi_mv_stmt_7 = """select t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            right semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            """
            def right_semi_mv_stmt_8 = """select t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            right semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            """
            def right_semi_mv_stmt_9 = """select t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            right semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            """
            def right_semi_mv_stmt_10 = """select t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            right semi join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_orderkey
            """

            def left_anti_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            left anti join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

            def left_anti_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left anti join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def left_anti_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left anti join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
            def left_anti_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left anti join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
            def left_anti_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            left anti join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey 
            group by t1.sum_total, col3, count_all       
            """

            def left_anti_mv_stmt_6 = """select `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            left anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey"""

            def left_anti_mv_stmt_7 = """select `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey  
            from `${catalog_name}`.`${db}`.${order_tb} 
            left anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
            """
            def left_anti_mv_stmt_8 = """select `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey  
            from `${catalog_name}`.`${db}`.${order_tb} 
            left anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            """
            def left_anti_mv_stmt_9 = """select `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            from `${catalog_name}`.`${db}`.${order_tb} 
            left anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            """
            def left_anti_mv_stmt_10 = """select `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey   
            from `${catalog_name}`.`${db}`.${order_tb} 
            left anti join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from `${catalog_name}`.`${db}`.${lineitem_tb} where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            `${catalog_name}`.`${db}`.${order_tb}.o_orderdate, `${catalog_name}`.`${db}`.${order_tb}.o_shippriority, `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            """

            def right_anti_mv_stmt_1 = """select l_orderkey, l_shipdate, l_partkey
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            right anti join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by
            l_orderkey, l_shipdate, l_partkey"""

            def right_anti_mv_stmt_2 = """select l_orderkey, l_shipdate, l_partkey
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right anti join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            l_orderkey, l_shipdate, l_partkey
            """
            def right_anti_mv_stmt_3 = """select l_orderkey, l_shipdate, l_partkey
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right anti join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            l_orderkey, l_shipdate, l_partkey
            """
            def right_anti_mv_stmt_4 = """select l_orderkey, l_shipdate, l_partkey
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right anti join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
            group by 
            l_orderkey, l_shipdate, l_partkey
            """
            def right_anti_mv_stmt_5 = """select l_orderkey, l_shipdate, l_partkey
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from `${catalog_name}`.`${db}`.${order_tb} where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            right anti join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey 
            group by l_orderkey, l_shipdate, l_partkey
            """

            def right_anti_mv_stmt_6 = """select t1.l_shipdate, t1.l_quantity, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            right anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.l_quantity, t1.l_orderkey """

            def right_anti_mv_stmt_7 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            right anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey 
            """
            def right_anti_mv_stmt_8 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from `${catalog_name}`.`${db}`.${order_tb} 
            right anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_shipdate >= "2023-10-17" group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey
            """
            def right_anti_mv_stmt_9 = """select t1.l_shipdate, t1.col1, t1.l_orderkey + t1.col2 as col3
            from `${catalog_name}`.`${db}`.${order_tb} 
            right anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from `${catalog_name}`.`${db}`.${lineitem_tb} where l_shipdate >= "2023-10-17" group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.l_shipdate, t1.col1, col3
            """
            def right_anti_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from `${catalog_name}`.`${db}`.${order_tb} 
            right anti join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from `${catalog_name}`.`${db}`.${lineitem_tb} where l_shipdate >= "2023-10-17" group by l_orderkey) as t1 
            on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
            group by
            t1.sum_total, col3, count_all
            """

            def sql_list = [
                    left_mv_stmt_1,left_mv_stmt_2,left_mv_stmt_3,left_mv_stmt_4,left_mv_stmt_5,left_mv_stmt_6,left_mv_stmt_7,left_mv_stmt_8,left_mv_stmt_9,left_mv_stmt_10,
                    right_mv_stmt_1,right_mv_stmt_2,right_mv_stmt_3,right_mv_stmt_4,right_mv_stmt_5,right_mv_stmt_6,right_mv_stmt_7,right_mv_stmt_8,right_mv_stmt_9,right_mv_stmt_10,
                    inner_mv_stmt_1,inner_mv_stmt_2,inner_mv_stmt_3,inner_mv_stmt_4,inner_mv_stmt_5,inner_mv_stmt_6,inner_mv_stmt_7,inner_mv_stmt_8,inner_mv_stmt_9,inner_mv_stmt_10,
                    full_mv_stmt_1,full_mv_stmt_2,full_mv_stmt_3,full_mv_stmt_4,full_mv_stmt_5,full_mv_stmt_6,full_mv_stmt_7,full_mv_stmt_8,full_mv_stmt_9,full_mv_stmt_10,

                    // agg pulled up, and the struct info is invalid. need to support aggregate merge then support following query rewriting
                    // left_semi_mv_stmt_1,left_semi_mv_stmt_2,left_semi_mv_stmt_3,left_semi_mv_stmt_4,left_semi_mv_stmt_5,left_semi_mv_stmt_6,left_semi_mv_stmt_7,left_semi_mv_stmt_8,left_semi_mv_stmt_9,left_semi_mv_stmt_10,
                    // left_anti_mv_stmt_1,left_anti_mv_stmt_2,left_anti_mv_stmt_3,left_anti_mv_stmt_4,left_anti_mv_stmt_5,left_anti_mv_stmt_6,left_anti_mv_stmt_7,left_anti_mv_stmt_8,left_anti_mv_stmt_9,left_anti_mv_stmt_10,
                    // right_semi_mv_stmt_1,right_semi_mv_stmt_2,right_semi_mv_stmt_3,right_semi_mv_stmt_4,right_semi_mv_stmt_5,right_semi_mv_stmt_6,right_semi_mv_stmt_7,right_semi_mv_stmt_8,right_semi_mv_stmt_9,right_semi_mv_stmt_10,
                    // right_anti_mv_stmt_1,right_anti_mv_stmt_2,right_anti_mv_stmt_3,right_anti_mv_stmt_4,right_anti_mv_stmt_5,right_anti_mv_stmt_6,right_anti_mv_stmt_7,right_anti_mv_stmt_8,right_anti_mv_stmt_9,right_anti_mv_stmt_10

                    // query rewrite by materialized view doesn't support cross join currently
                    // cross_mv_stmt_1,cross_mv_stmt_2,cross_mv_stmt_3,cross_mv_stmt_4,cross_mv_stmt_5,cross_mv_stmt_6,cross_mv_stmt_7,cross_mv_stmt_8,cross_mv_stmt_9,cross_mv_stmt_10,
            ]
            for (int i = 0; i < sql_list.size(); i++) {
                def origin_res = sql sql_list[i]
                assert (origin_res.size() > 0)
            }

            for (int i = 0; i < sql_list.size(); i++) {
                logger.info("sql_list current index: " + (i + 1))

                def mv_name = ctl + "_mv_" + (i + 1)

                create_mv(mv_name, sql_list[i])
                waitingMTMVTaskFinishedByMvName(mv_name)
                checkMtmvCount(mv_name)

                explain {
                    sql("${sql_list[i]}")
                    contains "${mv_name}(${mv_name})"
                }

                compare_res(sql_list[i] + " order by 1,2,3")
                sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
            }

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }

}
