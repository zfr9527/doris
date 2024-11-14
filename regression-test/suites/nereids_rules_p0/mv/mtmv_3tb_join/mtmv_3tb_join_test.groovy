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

suite("mtmv_3tb_join_test") {
    String db = context.config.getDbNameByFile(context.file)
    def orders_tb = "orders"
    def lineitem_tb = "lineitem"
    def partsupp_tb = "partsupp"

    sql """
    drop table if exists ${orders_tb}
    """

    sql """CREATE TABLE `${orders_tb}` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL,
      `o_orderdate` DATE not NULL,
      `public_col` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`, `o_custkey`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists ${lineitem_tb}
    """

    sql """CREATE TABLE `${lineitem_tb}` (
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
      `l_shipdate` DATE not NULL,
      `public_col` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists ${partsupp_tb}
    """

    sql """CREATE TABLE `${partsupp_tb}` (
      `ps_partkey` INT NULL,
      `ps_suppkey` INT NULL,
      `ps_availqty` INT NULL,
      `ps_supplycost` DECIMAL(15, 2) NULL,
      `ps_comment` VARCHAR(199) NULL,
      `public_col` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`ps_partkey`, `ps_suppkey`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into ${orders_tb} values 
    (null, 1, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-17', null),
    (1, null, 'k', 109.2, 'c','d',2, 'mm', '2023-10-17', null),
    (3, null, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19', null),
    (1, null, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20', 1),
    (2, null, 'k', 109.2, null,'d',2, 'mm', '2023-10-21', null),
    (3, 1, 'o', 99.5, 'a', null, 1, 'yy', '2023-10-22', 3),
    (3, 3, 'o', 99.5, 'a', null, 1, 'yy', '2023-10-22', 3),
    (1, 3, 'k', 99.5, 'a', 'b', null, 'yy', '2023-10-19', 1),
    (1, 1, 'k', 99.5, 'a', 'b', null, 'yy', '2023-10-19', 1),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18', 2),
    (2, 2, 'o', 109.2, 'c','d',2, null, '2023-10-18', 2),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17', 3),
    (null, null, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17', 3),
    (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19', 4); 
    """

    sql """
    insert into ${lineitem_tb} values 
    (null, 1, 1, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17', null),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17', null),
    (3, 3, null, null, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19', null),
    (1, 2, 3, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17', 1),
    (2, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18', 2),
    (2, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18', null),
    (3, 1, 1, 1, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19', 3),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17', 1);
    """

    sql"""
    insert into ${partsupp_tb} values 
    (1, 1, 1, 99.5, 'yy', 1),
    (1, 1, 1, 99.5, 'yy', null),
    (null, null, null, 99.5, 'yy', 1),
    (null, null, null, 99.5, 'yy', null),
    (null, 2, 2, 109.2, 'mm', null),
    (null, 2, 2, 109.2, 'mm', 2),
    (2, 2, 2, 109.2, 'mm', 2),
    (3, null, 1, 99.5, 'yy', 3),
    (3, null, 1, 99.5, 'yy', null); 
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

    def sql_template = """select o_orderdate, o_shippriority, o_comment, o_orderkey, ${orders_tb}.public_col as col1,
        l_orderkey, l_partkey, l_suppkey, ${lineitem_tb}.public_col as col2,
        ps_partkey, ps_suppkey, ${partsupp_tb}.public_col as col3, ${partsupp_tb}.public_col * 2 as col4,
        o_orderkey + l_orderkey + ps_partkey * 2, 
        sum(o_orderkey + l_orderkey + ps_partkey * 2),
        count() as count_all
        from (select o_orderdate, o_shippriority, o_comment, o_orderkey, ${orders_tb}.public_col as public_col from ${orders_tb} filter1) ${orders_tb} 
        jointype1 (select l_orderkey, l_partkey, l_suppkey, ${lineitem_tb}.public_col as public_col from ${lineitem_tb} filter2) ${lineitem_tb} on condition1 
        jointype2 (select ps_partkey, ps_suppkey, ${partsupp_tb}.public_col as public_col from ${partsupp_tb} filter3) ${partsupp_tb} on condition2 
        filter4
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14"""


    def generateCombinations = { int length ->
        def combinations = []
        int totalCombinations = (1 << length)
        (0..<totalCombinations).each { i ->
            def binaryString = String.format("%${length}d", Integer.parseInt(Integer.toBinaryString(i))).replace(' ', '0')
            def cur_sql_template = sql_template
            for (int j = 0; j < binaryString.size(); j++) {
                if (binaryString[j] == '0') {
                    cur_sql_template = cur_sql_template.replaceAll("filter${j+1}", "")
                }
            }
            combinations << cur_sql_template
        }
        return combinations
    }

    def template_results = generateCombinations(4)
    def join_list = ["inner join", "left join", "right join", "full join"]
    def join_special_list = ["left semi join", "right semi join", "left anti join", "right anti join"]
    def join_no_on_list = ["join", "cross join"]
    def condition1_list = ["l_orderkey = o_orderkey", "${lineitem_tb}.public_col = ${orders_tb}.public_col"]
    def condition2_list = [
            "ps_partkey = l_partkey and ps_suppkey = l_suppkey",
            "ps_partkey = o_orderkey and ps_suppkey = o_custkey",
            "${partsupp_tb}.public_col = ${lineitem_tb}.public_col",
            "${partsupp_tb}.public_col = ${orders_tb}.public_col"]
    def filter1_list = [
            "o_orderkey <> 1", "o_orderkey is null or o_orderkey <> 1", "${orders_tb}.public_col is null or ${orders_tb}.public_col <> 1"]
    def filter2_list = [
            "l_orderkey <> 1", "l_orderkey is null or l_orderkey <> 1", "${lineitem_tb}.public_col is null or ${lineitem_tb}.public_col <> 1"]
    def filter3_list = [
            "ps_partkey <> 1", "ps_partkey is null or ps_partkey <> 1", "${partsupp_tb}.public_col is null or ${partsupp_tb}.public_col <> 1"]
    def filter4_list = [
            "o_orderkey <> 1", "o_orderkey is null or o_orderkey <> 1", "${orders_tb}.public_col is null or ${orders_tb}.public_col <> 1",
            "l_orderkey <> 1", "l_orderkey is null or l_orderkey <> 1", "${lineitem_tb}.public_col is null or ${lineitem_tb}.public_col <> 1",
            "ps_partkey <> 1", "ps_partkey is null or ps_partkey <> 1", "${partsupp_tb}.public_col is null or ${partsupp_tb}.public_col <> 1"]

    def count = 0
    for (int template_results_it = 0; template_results_it < template_results.size(); template_results_it++) {
        logger.info("template_results_it:" + template_results_it)
        def sql_pt = template_results[template_results_it]

        // join type
        for (int join_list_1 = 0; join_list_1 < join_list.size(); join_list_1++) {
            def sql_pt1 = sql_pt.replaceAll("jointype1", join_list[join_list_1])
            for (int join_list_2 = 0; join_list_2 < join_list.size(); join_list_2++) {
                def sql_pt2 = sql_pt1.replaceAll("jointype2", join_list[join_list_2])

                // on condition
                for (int condition1_list_it = 0; condition1_list_it < condition1_list.size(); condition1_list_it ++) {
                    def sql_pt3 = sql_pt2.replaceAll("condition1", condition1_list[condition1_list_it])
                    for (int condition2_list_it = 0; condition2_list_it < condition2_list.size(); condition2_list_it ++) {
                        def sql_pt4 = sql_pt3.replaceAll("condition2", condition2_list[condition2_list_it])

                        // where filter condition
                        def sql_queue = new LinkedList()
                        sql_queue.add(sql_pt4)
                        if (sql_pt.toString().indexOf("filter1") != -1) {
                            def sz = sql_queue.size()
                            for (int z = 0; z < sz; z++) {
                                def cur_sql = sql_queue.poll()
                                for (int filter1_list_it = 0; filter1_list_it < filter1_list.size(); filter1_list_it++) {
                                    sql_queue.add(cur_sql.replaceAll("filter1", "where " + filter1_list[filter1_list_it]))
                                }
                            }
                        }
                        if (sql_pt.toString().indexOf("filter2") != -1) {
                            def sz = sql_queue.size()
                            for (int z = 0; z < sz; z++) {
                                def cur_sql = sql_queue.poll()
                                for (int filter2_list_it = 0; filter2_list_it < filter2_list.size(); filter2_list_it++) {
                                    sql_queue.add(cur_sql.replaceAll("filter2", "where " + filter2_list[filter2_list_it]))
                                }
                            }
                        }
                        if (sql_pt.toString().indexOf("filter3") != -1) {
                            def sz = sql_queue.size()
                            for (int z = 0; z < sz; z++) {
                                def cur_sql = sql_queue.poll()
                                for (int filter3_list_it = 0; filter3_list_it < filter3_list.size(); filter3_list_it++) {
                                    sql_queue.add(cur_sql.replaceAll("filter3", "where " + filter3_list[filter3_list_it]))
                                }
                            }
                        }
                        if (sql_pt.toString().indexOf("filter4") != -1) {
                            def sz = sql_queue.size()
                            for (int z = 0; z < sz; z++) {
                                def cur_sql = sql_queue.poll()
                                for (int filter4_list_it = 0; filter4_list_it < filter4_list.size(); filter4_list_it++) {
                                    sql_queue.add(cur_sql.replaceAll("filter4", "where " + filter4_list[filter4_list_it]))
                                }
                            }
                        }

                        sql_queue.each { item ->
                            def res = sql item
                            assertTrue(res.size() > 0)
                            count ++
                            logger.info("count: " + count)
                            mv_rewrite_success(item, "three_tb_join_mtmv")
                            compare_res(item + " order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14")
                            sql """DROP MATERIALIZED VIEW IF EXISTS three_tb_join_mtmv;"""
                        }

                    }
                }

            }
        }

    }

/*
    """
    select o_orderdate, o_shippriority, o_comment, o_orderkey, ${orders_tb}.public_col as col1,
        l_orderkey, l_partkey, l_suppkey, ${lineitem_tb}.public_col as col2,
        ps_partkey, ps_suppkey, ${partsupp_tb}.public_col as col3, ${partsupp_tb}.public_col * 2 as col4,
        sum(o_orderkey + l_orderkey + ps_partkey * 2),
        count() as count_all
    from
    (select o_orderdate, o_shippriority, o_comment, o_orderkey, ${orders_tb}.public_col as col1,
        l_orderkey, l_partkey, l_suppkey, ${lineitem_tb}.public_col as col2,
        ps_partkey, ps_suppkey, ${partsupp_tb}.public_col as col3, ${partsupp_tb}.public_col * 2 as col4,
        sum(o_orderkey + l_orderkey + ps_partkey * 2),
        count() as count_all
        from ${orders_tb}
        inner join ${lineitem_tb} on ${lineitem_tb}.l_orderkey = ${orders_tb}.o_orderkey
        inner join ${partsupp_tb} on ${partsupp_tb}.ps_partkey = ${lineitem_tb}.l_partkey and ${partsupp_tb}.ps_suppkey = ${lineitem_tb}.l_suppkey
        where
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13) as public_col

    """

 */

    /*
join:
    inner join
    left join
    right join
    full join
    left semi join
    left anti join
    right semi join
    right anti join
    self join
    cross join

where:
    where o_orderkey is null
    where o_orderkey <> 1
    where o_orderkey is null or o_orderkey <> 1

    where ${orders_tb}.public_col is null
    where ${orders_tb}.public_col <> 1
    where ${orders_tb}.public_col is null or ${orders_tb}.public_col <> 1

    where l_orderkey is null
    where l_orderkey <> 1
    where l_orderkey is null or l_orderkey <> 1

    where ${lineitem_tb}.public_col is null
    where ${lineitem_tb}.public_col <> 1
    where ${lineitem_tb}.public_col is null or ${lineitem_tb}.public_col <> 1


    where ps_partkey is null
    where ps_partkey <> 1
    where ps_partkey is null or ps_partkey <> 1

    where ${partsupp_tb}.public_col is null
    where ${partsupp_tb}.public_col <> 1
    where ${partsupp_tb}.public_col is null or ${partsupp_tb}.public_col <> 1


on:

    ${lineitem_tb}.l_orderkey = ${orders_tb}.o_orderkey
    ${partsupp_tb}.ps_partkey = ${lineitem_tb}.l_partkey and ${partsupp_tb}.ps_suppkey = ${lineitem_tb}.l_suppkey
    ${partsupp_tb}.ps_partkey = ${orders_tb}.o_orderkey and ${partsupp_tb}.ps_suppkey = ${orders_tb}.o_custkey

    l_orderkey = o_orderkey
    ps_partkey = l_partkey and ps_suppkey = l_suppkey
    ps_partkey = o_orderkey and ps_suppkey = o_custkey

    ${lineitem_tb}.public_col = ${orders_tb}.public_col
    ${partsupp_tb}.public_col = ${lineitem_tb}.public_col
    ${partsupp_tb}.public_col = ${orders_tb}.public_col


     */


}
