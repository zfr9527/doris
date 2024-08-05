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
suite("negative_partition_mv_rewrite_hive") {
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

    def checkMtmvCount = { def cur_mv_name ->
        def select_count = sql "select count(*) from ${cur_mv_name}"
        assertTrue(select_count[0][0] != 0)
    }

    String ctl = "negative_hive"
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

            def order_tb = "orders_negative"
            def lineitem_tb = "lineitem_negative"
            def partsupp_tb = "partsupp_negative"

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

            def mv_name = ctl + "_mv"
            def mtmv_sql = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} 
                left join `${catalog_name}`.`${db}`.${order_tb} 
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
                """

            create_mv_lineitem(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            // mtmv not exists query col
            def query_sql = """
                select o_orderkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} 
                left join `${catalog_name}`.`${db}`.${order_tb} 
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // Swap tables on either side of the left join
            query_sql = """
                select l_shipdate 
                from  `${catalog_name}`.`${db}`.${order_tb} 
                left join `${catalog_name}`.`${db}`.${lineitem_tb} 
                on `${catalog_name}`.`${db}`.${order_tb}.o_orderkey = `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // The filter condition of the query is not in the filter range of mtmv
            mtmv_sql = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey   
                from (select * from `${catalog_name}`.`${db}`.${lineitem_tb} where l_shipdate = '2023-10-17' ) t1 
                left join `${catalog_name}`.`${db}`.${order_tb} 
                on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
                """
            create_mv_lineitem(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)
            query_sql = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey   
                from (select * from `${catalog_name}`.`${db}`.${lineitem_tb} where l_shipdate = '2023-10-18' ) t1 
                left join `${catalog_name}`.`${db}`.${order_tb} 
                on t1.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // The filter range of the query is larger than that of the mtmv
            mtmv_sql = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} 
                left join `${catalog_name}`.`${db}`.${order_tb} 
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
                where  `${catalog_name}`.`${db}`.${order_tb}.o_orderkey > 2
                """
            create_mv_lineitem(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)
            query_sql = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} 
                left join `${catalog_name}`.`${db}`.${order_tb} 
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
                where  `${catalog_name}`.`${db}`.${order_tb}.o_orderkey > 1
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            query_sql = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} 
                left join `${catalog_name}`.`${db}`.${order_tb} 
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
                where  `${catalog_name}`.`${db}`.${order_tb}.o_orderkey > 2 or `${catalog_name}`.`${db}`.${order_tb}.o_orderkey < 0
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // filter in
            mtmv_sql = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} 
                left join `${catalog_name}`.`${db}`.${order_tb} 
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
                where  `${catalog_name}`.`${db}`.${order_tb}.o_orderkey in (1, 2, 3)
                """
            create_mv_lineitem(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)
            query_sql = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} 
                left join `${catalog_name}`.`${db}`.${order_tb} 
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
                where  `${catalog_name}`.`${db}`.${order_tb}.o_orderkey in (1, 2, 3, 4)
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // agg not roll up
            mtmv_sql = """
                select o_orderdate, o_shippriority, o_comment 
                from `${catalog_name}`.`${db}`.${order_tb} 
                group by 
                o_orderdate, 
                o_shippriority, 
                o_comment 
                """
            create_mv_orders(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """
                select o_orderdate 
                    from `${catalog_name}`.`${db}`.${order_tb}
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            mtmv_sql = """
                select o_orderdate, o_shippriority, o_comment,
                    sum(o_totalprice) as sum_total,
                    max(o_totalprice) as max_total,
                    min(o_totalprice) as min_total,
                    count(*) as count_all,
                    bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
                    bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
                    from `${catalog_name}`.`${db}`.${order_tb}
                    group by
                    o_orderdate,
                    o_shippriority,
                    o_comment
                """
            create_mv_orders(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """
                select 
                    sum(o_totalprice) as sum_total,
                    max(o_totalprice) as max_total,
                    min(o_totalprice) as min_total,
                    count(*) as count_all,
                    bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
                    bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
                    from `${catalog_name}`.`${db}`.${order_tb}
                """
            explain {
                sql("${query_sql}")
                contains "${mv_name}(${mv_name})"
            }

            // query partial rewriting
            mtmv_sql = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey, count(*)
                from `${catalog_name}`.`${db}`.${lineitem_tb}
                left join `${catalog_name}`.`${db}`.${order_tb}
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
                group by l_shipdate, o_orderdate, l_partkey, l_suppkey
                """
            create_mv_lineitem(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """
                select l_shipdate, l_partkey, count(*) from `${catalog_name}`.`${db}`.${lineitem_tb} 
                group by l_shipdate, l_partkey
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            mtmv_sql = """
                select
                sum(o_totalprice) as sum_total,
                max(o_totalprice) as max_total,
                min(o_totalprice) as min_total,
                count(*) as count_all,
                bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
                bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
                from `${catalog_name}`.`${db}`.${order_tb}
                left join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
                """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """
                select
                count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
                count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2,
                sum(o_totalprice),
                max(o_totalprice),
                min(o_totalprice),
                count(*)
                from `${catalog_name}`.`${db}`.${order_tb}
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // view partial rewriting
            mtmv_sql = """
                select l_shipdate, l_partkey, l_orderkey, count(*) from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_shipdate, l_partkey, l_orderkey
                """
            create_mv_lineitem(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """
                select l_shipdate, l_partkey, l_orderkey, count(*) 
                from `${catalog_name}`.`${db}`.${lineitem_tb} left join `${catalog_name}`.`${db}`.${order_tb} 
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_shipdate=`${catalog_name}`.`${db}`.${order_tb}.o_orderdate 
                group by l_shipdate, l_partkey, l_orderkey
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            mtmv_sql = """
                select o_orderdate, o_shippriority, o_comment, l_orderkey, o_orderkey, sum(o_orderkey)   
                    from `${catalog_name}`.`${db}`.${order_tb} 
                    left join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
                    group by 
                    o_orderdate, 
                    o_shippriority, 
                    o_comment,
                    l_orderkey,
                    o_orderkey
                """
            create_mv_orders(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """
                select o_orderdate, o_shippriority, o_comment, l_orderkey, ps_partkey, sum(o_orderkey)
                    from `${catalog_name}`.`${db}`.${order_tb}
                    left join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
                    left join `${catalog_name}`.`${db}`.`${partsupp_tb}` on `${catalog_name}`.`${db}`.`${partsupp_tb}`.ps_partkey = `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey
                    group by
                    o_orderdate,
                    o_shippriority,
                    o_comment,
                    l_orderkey, 
                    ps_partkey
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // union rewrite
            mtmv_sql = """
                select l_shipdate, o_orderdate, l_partkey, count(*)
                from `${catalog_name}`.`${db}`.${lineitem_tb}
                left join `${catalog_name}`.`${db}`.${order_tb}
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
                where l_shipdate >= "2023-10-17"
                group by l_shipdate, o_orderdate, l_partkey
                """
            create_mv_lineitem(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """
                select l_shipdate, o_orderdate, l_partkey, count(*)
                from `${catalog_name}`.`${db}`.${lineitem_tb}
                left join `${catalog_name}`.`${db}`.${order_tb}
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
                where l_shipdate >= "2023-10-15"
                group by l_shipdate, o_orderdate, l_partkey
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            mtmv_sql = """
                select l_shipdate, l_partkey, l_orderkey
                from `${catalog_name}`.`${db}`.${lineitem_tb}
                where l_shipdate >= "2023-10-10"
                group by l_shipdate, l_partkey, l_orderkey
                """
            create_mv_lineitem(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """
                select t.l_shipdate, o_orderdate, t.l_partkey
                from (select l_shipdate, l_partkey, l_orderkey from `${catalog_name}`.`${db}`.${lineitem_tb}) t
                left join `${catalog_name}`.`${db}`.${order_tb}
                on t.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey
                where l_shipdate >= "2023-10-10"
                group by t.l_shipdate, o_orderdate, t.l_partkey
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // project rewriting
            mtmv_sql = """
                select o_orderdate, o_shippriority, o_comment, o_orderkey, o_shippriority + o_custkey,
                case when o_shippriority > 0 and o_orderkey IN (1, 3) then o_custkey else null end cnt_1,
                case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2
                from `${catalog_name}`.`${db}`.${order_tb};
                """
            create_mv_orders(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """
                select o_shippriority, o_comment, o_shippriority + o_custkey  + o_orderkey,
                case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end cnt_1,
                case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2
                from `${catalog_name}`.`${db}`.${order_tb};
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // agg under join
            mtmv_sql = """
                select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
                from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from `${catalog_name}`.`${db}`.${order_tb} group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
                left join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
                group by
                t1.o_orderdate, t1.o_shippriority, t1.o_orderkey
                """
            create_mv_orders(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)

            query_sql = """
                select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
                from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from `${catalog_name}`.`${db}`.${order_tb}  where o_custkey > 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
                left join `${catalog_name}`.`${db}`.${lineitem_tb} on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = t1.o_orderkey
                group by
                t1.o_orderdate, t1.o_shippriority, t1.o_orderkey
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // filter include and or
            mtmv_sql = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} 
                left join `${catalog_name}`.`${db}`.${order_tb} 
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
                where  `${catalog_name}`.`${db}`.${order_tb}.o_orderkey > 2 and `${catalog_name}`.`${db}`.${order_tb}.o_orderdate >= "2023-10-17" or l_partkey > 1
                """
            create_mv_lineitem(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)
            query_sql = """
                select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
                from `${catalog_name}`.`${db}`.${lineitem_tb} 
                left join `${catalog_name}`.`${db}`.${order_tb} 
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
                where  `${catalog_name}`.`${db}`.${order_tb}.o_orderkey > 2 and (`${catalog_name}`.`${db}`.${order_tb}.o_orderdate >= "2023-10-17" or l_partkey > 1)
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // group by under group by
            mtmv_sql = """
                SELECT c_count, count(*) AS custdist 
                FROM (SELECT l_orderkey, count(o_orderkey) AS c_count FROM `${catalog_name}`.`${db}`.${lineitem_tb} 
                      LEFT OUTER JOIN `${catalog_name}`.`${db}`.${order_tb} ON l_orderkey = o_custkey AND o_comment NOT LIKE '%special%requests%' 
                      GROUP BY l_orderkey) AS c_orders 
                GROUP BY c_count ORDER BY custdist DESC, c_count DESC;
                """
            create_mv(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)
            query_sql = """
                SELECT c_count, count(*) AS custdist 
                FROM (SELECT l_orderkey, count(o_orderkey) AS c_count FROM `${catalog_name}`.`${db}`.${lineitem_tb} 
                      LEFT OUTER JOIN `${catalog_name}`.`${db}`.${order_tb} ON l_orderkey = o_custkey AND o_comment NOT LIKE '%special%requests%' 
                      GROUP BY l_orderkey) AS c_orders 
                GROUP BY c_count ORDER BY custdist DESC, c_count DESC;"""
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // condition on not equal
            mtmv_sql = """
                select l_shipdate, o_orderdate, l_partkey, count(*)
                from `${catalog_name}`.`${db}`.${lineitem_tb}
                left join `${catalog_name}`.`${db}`.${order_tb}
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey > `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
                group by l_shipdate, o_orderdate, l_partkey;
                """
            create_mv_lineitem(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)
            query_sql = """
                select l_shipdate, o_orderdate, l_partkey, count(*)
                from `${catalog_name}`.`${db}`.${lineitem_tb}
                left join `${catalog_name}`.`${db}`.${order_tb}
                on `${catalog_name}`.`${db}`.${lineitem_tb}.l_orderkey > `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
                group by l_shipdate, o_orderdate, l_partkey
            """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            // mtmv exists join but not exists agg, query exists agg
            mtmv_sql = """
                select l_shipdate, o_orderdate, l_partkey 
                from (select l_shipdate, l_partkey, l_orderkey from `${catalog_name}`.`${db}`.${lineitem_tb}) as t
                left join `${catalog_name}`.`${db}`.${order_tb}
                on t.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey;
                """
            create_mv_lineitem(mv_name, mtmv_sql)
            waitingMTMVTaskFinishedByMvName(mv_name)
            checkMtmvCount(mv_name)
            query_sql = """    
                select l_shipdate, o_orderdate, l_partkey 
                from (select l_shipdate, l_partkey, l_orderkey from `${catalog_name}`.`${db}`.${lineitem_tb} group by l_shipdate, l_partkey, l_orderkey) as t
                left join `${catalog_name}`.`${db}`.${order_tb}
                on t.l_orderkey = `${catalog_name}`.`${db}`.${order_tb}.o_orderkey 
                group by l_shipdate, o_orderdate, l_partkey ;
                """
            explain {
                sql("${query_sql}")
                notContains "${mv_name}(${mv_name})"
            }

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }

}
