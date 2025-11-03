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

suite("mtmv_range_date_datetrunc_date_part_up_union_multi_pct_tables") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"
    String mv_prefix = "range_datetrunc_date_up_multi_pct_tables"
    String tb_name1 = mv_prefix + "_tb1"
    String tb_name2 = mv_prefix + "_tb2"
    String mv_name = mv_prefix + "_mv"

    def initTable = {
        sql """
            drop table if exists ${tb_name1}
            """

        sql """CREATE TABLE `${tb_name1}` (
              `l_orderkey` BIGINT NULL,
              `l_linenumber` INT NULL,
              `l_partkey` INT NULL,
              `l_suppkey` INT NULL,
              `l_quantity` DECIMAL(15, 2) NULL,
              `l_extendedprice` DECIMAL(15, 2) NULL,
              `l_discount` DECIMAL(15, 2) NULL,
              `l_tax` DECIMAL(15, 2) NULL,
              `l_returnflag` VARCHAR(1) NULL,
              `l_linestatus` VARCHAR(1) NULL,
              `l_commitdate` DATE NULL,
              `l_receiptdate` DATE NULL,
              `l_shipinstruct` VARCHAR(25) NULL,
              `l_shipmode` VARCHAR(10) NULL,
              `l_comment` VARCHAR(44) NULL,
              `l_shipdate` DATEtime not NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey)
            COMMENT 'OLAP'
            partition by range (`l_shipdate`) (
                partition p1 values [("2023-10-29 00:00:00"), ("2023-10-30 00:00:00")),
                partition p2 values [("2023-10-30 00:00:00"), ("2023-10-31 00:00:00")),
                partition p3 values [("2023-10-31 00:00:00"), ("2023-11-01 00:00:00")),
                partition p4 values [("2023-11-01 00:00:00"), ("2023-11-02 00:00:00")),
                partition p5 values [("2023-11-02 00:00:00"), ("2023-11-03 00:00:00"))
            )
            DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 2
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""

        sql """
            insert into ${tb_name1} values 
            (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29 00:00:00'),
            (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-30 01:00:00'),
            (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-31 02:00:00'),
            (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-11-01 03:00:00'),
            (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-11-02 04:00:00');
            """
        sql """alter table ${tb_name1} modify column l_comment set stats ('row_count'='5');"""


        sql """
            drop table if exists ${tb_name2}
            """

        sql """
            CREATE TABLE IF NOT EXISTS ${tb_name2}  (
              o_orderkey       INTEGER NOT NULL,
              o_custkey        INTEGER NOT NULL,
              o_orderstatus    CHAR(1) NOT NULL,
              o_totalprice     DECIMALV3(15,2) NOT NULL,
              o_orderdate      DATEtime NOT NULL,
              o_orderpriority  CHAR(15) NOT NULL,  
              o_clerk          CHAR(15) NOT NULL, 
              o_shippriority   INTEGER NOT NULL,
              o_comment        VARCHAR(79) NOT NULL
            )
            DUPLICATE KEY(o_orderkey, o_custkey)
            partition by range (`o_orderdate`) (
                partition p1 values [("2023-10-29 00:00:00"), ("2023-10-30 00:00:00")),
                partition p2 values [("2023-10-30 00:00:00"), ("2023-10-31 00:00:00")),
                partition p3 values [("2023-10-31 00:00:00"), ("2023-11-01 00:00:00")),
                partition p4 values [("2023-11-01 00:00:00"), ("2023-11-02 00:00:00")),
                partition p5 values [("2023-11-02 00:00:00"), ("2023-11-03 00:00:00"))
            )
            DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
            PROPERTIES (
              "replication_num" = "1"
            )
            """

        sql """
            insert into ${tb_name2} values
            (1, 1, 'o', 9.5, '2023-10-29 00:00:00', 'a', 'b', 1, 'yy'),
            (1, 1, 'o', 10.5, '2023-10-29 00:00:00', 'a', 'b', 1, 'yy'),
            (2, 1, 'o', 11.5, '2023-10-30 01:00:00', 'a', 'b', 1, 'yy'),
            (3, 1, 'o', 12.5, '2023-10-30 01:00:00', 'a', 'b', 1, 'yy'),
            (3, 1, 'o', 33.5, '2023-10-31 02:00:00', 'a', 'b', 1, 'yy'),
            (4, 2, 'o', 43.2, '2023-10-31 02:00:00', 'c','d',2, 'mm'),
            (5, 2, 'o', 56.2, '2023-11-01 03:00:00', 'c','d',2, 'mi'),
            (5, 2, 'o', 1.2, '2023-11-02 04:00:00', 'c','d',2, 'mi');  
            """
    }


    def compare_res = { def stmt ->
        sql "SET enable_materialized_view_rewrite=false"
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        sql "SET enable_materialized_view_rewrite=true"
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

    def create_mv = { cur_mv_name, mv_sql, col_name, date_trunc_range ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${cur_mv_name};"""
        sql """DROP TABLE IF EXISTS ${cur_mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${cur_mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        partition by(date_trunc(`${col_name}`, '${date_trunc_range}')) 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1')  
        AS  
        ${mv_sql}
        """
    }

    def select_list1_1 = "l_shipdate"
    def select_list1_2 = "date_trunc(`l_shipdate`, 'day') as col1"
    def select_list1_3 = "DATE_FORMAT(`l_shipdate`, '%Y-%m-%d')"
    def select_list2_1 = "date_trunc(`l_shipdate`, 'day') as col1, l_shipdate"
    def select_list2_2 = "date_trunc(`l_shipdate`, 'day') as col1, DATE_FORMAT(`l_shipdate`, '%Y-%m-%d')"
    def select_list2_3 = "l_shipdate, DATE_FORMAT(`l_shipdate`, '%Y-%m-%d')"
    def select_list3_1 = "date_trunc(`l_shipdate`, 'day') as col1, l_shipdate, DATE_FORMAT(`l_shipdate`, '%Y-%m-%d')"
    def select_list3_2 = "date_trunc(`l_shipdate`, 'day') as col1, DATE_FORMAT(`l_shipdate`, '%Y-%m-%d'), l_shipdate"
    def select_list3_3 = "l_shipdate, DATE_FORMAT(`l_shipdate`, '%Y-%m-%d'), date_trunc(`l_shipdate`, 'day') as col1"

    def select_lists = [select_list1_1, select_list1_2, select_list1_3, select_list2_1, select_list2_2,
                        select_list2_3, select_list3_1, select_list3_2, select_list3_3]
    def select_list1_1_2 = "o_orderdate"
    def select_list1_2_2 = "date_trunc(`o_orderdate`, 'day') as col1"
    def select_list1_3_2 = "DATE_FORMAT(`o_orderdate`, '%Y-%m-%d')"
    def select_list2_1_2 = "date_trunc(`o_orderdate`, 'day') as col1, o_orderdate"
    def select_list2_2_2 = "date_trunc(`o_orderdate`, 'day') as col1, DATE_FORMAT(`o_orderdate`, '%Y-%m-%d')"
    def select_list2_3_2 = "o_orderdate, DATE_FORMAT(`o_orderdate`, '%Y-%m-%d')"
    def select_list3_1_2 = "date_trunc(`o_orderdate`, 'day') as col1, o_orderdate, DATE_FORMAT(`o_orderdate`, '%Y-%m-%d')"
    def select_list3_2_2 = "date_trunc(`o_orderdate`, 'day') as col1, DATE_FORMAT(`o_orderdate`, '%Y-%m-%d'), o_orderdate"
    def select_list3_3_2 = "o_orderdate, DATE_FORMAT(`o_orderdate`, '%Y-%m-%d'), date_trunc(`o_orderdate`, 'day') as col1"

    def select_lists2 = [select_list1_1_2, select_list1_2_2, select_list1_3_2, select_list2_1_2, select_list2_2_2,
                       select_list2_3_2, select_list3_1_2, select_list3_2_2, select_list3_3_2]

    for (int i = 0; i < select_lists.size(); i++) {
        initTable()

        def str = "select " + select_lists[i] + ", L_LINENUMBER from ${tb_name1} union all select ${select_lists2[i]}, O_CUSTKEY from ${tb_name2}"
        sql str

        if (select_lists[i].replaceAll("`l_shipdate`", "").indexOf("l_shipdate") != -1) {
            create_mv(mv_name, str, "l_shipdate", "day")
            waitingMTMVTaskFinishedByMvName(mv_name)

            sql """
                insert into ${tb_name1} values 
                (3, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-29 04:00:00');
                """
            sql """
                REFRESH MATERIALIZED VIEW ${mv_name} AUTO
                """
            waitingMTMVTaskFinishedByMvName(mv_name)
            def mv_infos = sql "select RefreshMode, NeedRefreshPartitions, Progress from tasks('type'='mv') where MvName='${mv_name}' order by CreateTime desc limit 1"
            logger.info("mv_infos1:" + mv_infos)
            assertTrue(mv_infos[0][0] == "PARTIAL")
            assertTrue(mv_infos[0][1] == "[\"p_20231029000000_20231030000000\"]")
            assertTrue(mv_infos[0][2] == "100.00% (1/1)")


            sql """
                insert into ${tb_name2} values
                (3, 2, 'o', 1.2, '2023-10-29 04:00:00', 'c','d',2, 'mi');  
                """
            sql """
                REFRESH MATERIALIZED VIEW ${mv_name} AUTO
                """
            waitingMTMVTaskFinishedByMvName(mv_name)
            mv_infos = sql "select RefreshMode, NeedRefreshPartitions, Progress from tasks('type'='mv') where MvName='${mv_name}' order by CreateTime desc limit 1"
            logger.info("mv_infos1:" + mv_infos)
            assertTrue(mv_infos[0][0] == "PARTIAL")
            assertTrue(mv_infos[0][1] == "[\"p_20231029000000_20231030000000\"]")
            assertTrue(mv_infos[0][2] == "100.00% (1/1)")

        }

        if (select_lists[i].indexOf("col1") != -1) {
            create_mv(mv_name, str, "col1", "day")
            waitingMTMVTaskFinishedByMvName(mv_name)

            sql """
                insert into ${tb_name1} values 
                (3, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-29 04:00:00');
                """
            sql """
                REFRESH MATERIALIZED VIEW ${mv_name} AUTO
                """
            waitingMTMVTaskFinishedByMvName(mv_name)
            def mv_infos = sql "select RefreshMode, NeedRefreshPartitions, Progress from tasks('type'='mv') where MvName='${mv_name}' order by CreateTime desc limit 1"
            logger.info("mv_infos1:" + mv_infos)
            assertTrue(mv_infos[0][0] == "PARTIAL")
            assertTrue(mv_infos[0][1] == "[\"p_20231029000000_20231030000000\"]")
            assertTrue(mv_infos[0][2] == "100.00% (1/1)")


            sql """
                insert into ${tb_name2} values
                (3, 2, 'o', 1.2, '2023-10-29 04:00:00', 'c','d',2, 'mi');  
                """
            sql """
                REFRESH MATERIALIZED VIEW ${mv_name} AUTO
                """
            waitingMTMVTaskFinishedByMvName(mv_name)
            mv_infos = sql "select RefreshMode, NeedRefreshPartitions, Progress from tasks('type'='mv') where MvName='${mv_name}' order by CreateTime desc limit 1"
            logger.info("mv_infos1:" + mv_infos)
            assertTrue(mv_infos[0][0] == "PARTIAL")
            assertTrue(mv_infos[0][1] == "[\"p_20231029000000_20231030000000\"]")
            assertTrue(mv_infos[0][2] == "100.00% (1/1)")

        }

    }

}
