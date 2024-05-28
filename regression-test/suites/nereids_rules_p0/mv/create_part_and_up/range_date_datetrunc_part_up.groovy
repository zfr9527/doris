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

suite("mtmv_range_date_datetrunc_date_part_up", "zfr_mtmv_test") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"
    String mv_prefix = "range_datetrunc_date_up"
    String tb_name = mv_prefix + "_tb"
    String mv_name = mv_prefix + "_mv"

    sql """
    drop table if exists ${tb_name}
    """

    sql """CREATE TABLE `${tb_name}` (
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
    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
    COMMENT 'OLAP'
    partition by range (`l_shipdate`) (
        partition p1 values [("2023-10-29 01:00:00"), ("2023-10-29 02:00:00")), 
        partition p2 values [("2023-10-30 01:00:00"), ("2023-10-30 02:00:00")), 
        partition p3 values [("2023-10-31 01:00:00"), ("2023-10-31 02:00:00")))
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into ${tb_name} values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29 01:00:00'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-29 01:00:00'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-31 01:00:00'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29 01:00:00'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-30 01:00:00'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-31 01:00:00'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29 01:00:00');
    """

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

    def sql1 = """select date_trunc(`l_shipdate`, 'day') as col1, l_shipdate 
                        from ${tb_name} 
                        group by col1, l_shipdate"""

    def sql2 = """select date_trunc(`l_shipdate`, 'hour') as col1, l_shipdate 
                        from ${tb_name} 
                        group by col1, l_shipdate"""

    def sql3 = """select date_trunc(`l_shipdate`, 'day') as col1, DATE_FORMAT(`l_shipdate`, '%Y-%m-%d') as col2, l_shipdate 
                        from ${tb_name} 
                        group by col1, col2, l_shipdate"""

    def sql4 = """select DATE_FORMAT(`l_shipdate`, '%Y-%m-%d'), date_trunc(`l_shipdate`, 'day') as col1,  l_shipdate 
                        from ${tb_name} 
                        group by DATE_FORMAT(`l_shipdate`, '%Y-%m-%d'), col1, l_shipdate"""

    def sql5 = """select DATE_FORMAT(`l_shipdate`, '%Y-%m-%d %H:'), MINUTE(`l_shipdate`) div 15, date_trunc(`l_shipdate`, 'day') as col1,  l_shipdate 
                        from ${tb_name}  
                        group by DATE_FORMAT(`l_shipdate`, '%Y-%m-%d %H:'), MINUTE(`l_shipdate`) div 15, col1, l_shipdate"""

    def sql6 = """select DATE_FORMAT(`l_shipdate`, '%Y-%m-%d %H:'), date_trunc(`l_shipdate`, 'day') as col1,  l_shipdate, MINUTE(`l_shipdate`) div 15 
                        from ${tb_name}  
                        group by DATE_FORMAT(`l_shipdate`, '%Y-%m-%d %H:'), MINUTE(`l_shipdate`) div 15, col1, l_shipdate"""

    // l_shipdate
    create_mv(mv_name, sql1, "l_shipdate", "day")
    waitingMTMVTaskFinishedByMvName(mv_name)
    explain {
        sql("${sql1}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(sql1 + " order by 1,2")

    create_mv(mv_name, sql2, "l_shipdate", "hour")
    waitingMTMVTaskFinishedByMvName(mv_name)
    explain {
        sql("${sql2}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(sql2 + " order by 1,2")

    create_mv(mv_name, sql3, "l_shipdate", "day")
    waitingMTMVTaskFinishedByMvName(mv_name)
    explain {
        sql("${sql3}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(sql3 + " order by 1,2,3")

    create_mv(mv_name, sql4, "l_shipdate", "day")
    waitingMTMVTaskFinishedByMvName(mv_name)
    explain {
        sql("${sql4}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(sql4 + " order by 1,2,3")

    create_mv(mv_name, sql5, "l_shipdate", "day")
    waitingMTMVTaskFinishedByMvName(mv_name)
    explain {
        sql("${sql5}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(sql5 + " order by 1,2,3,4")

    create_mv(mv_name, sql6, "l_shipdate", "day")
    waitingMTMVTaskFinishedByMvName(mv_name)
    explain {
        sql("${sql6}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(sql6 + " order by 1,2,3,4")

    // date_trunc(l_shipdate)
    create_mv(mv_name, sql1, "col1", "day")
    waitingMTMVTaskFinishedByMvName(mv_name)
    explain {
        sql("${sql1}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(sql1 + " order by 1,2")

    create_mv(mv_name, sql2, "col1", "day")
    waitingMTMVTaskFinishedByMvName(mv_name)
    explain {
        sql("${sql2}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(sql2 + " order by 1,2")

    create_mv(mv_name, sql3, "col1", "day")
    waitingMTMVTaskFinishedByMvName(mv_name)
    explain {
        sql("${sql3}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(sql3 + " order by 1,2,3")

    create_mv(mv_name, sql4, "col1", "day")
    waitingMTMVTaskFinishedByMvName(mv_name)
    explain {
        sql("${sql4}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(sql4 + " order by 1,2,3")

    create_mv(mv_name, sql5, "col1", "day")
    waitingMTMVTaskFinishedByMvName(mv_name)
    explain {
        sql("${sql5}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(sql5 + " order by 1,2,3,4")

    create_mv(mv_name, sql6, "col1", "day")
    waitingMTMVTaskFinishedByMvName(mv_name)
    explain {
        sql("${sql6}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(sql6 + " order by 1,2,3,4")

}
