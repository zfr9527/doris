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
    (1, null, 'k', 109.2, 'c','d',2, 'mm', '2023-10-17', 1),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19', 3),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20', 1),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21', 2),
    (3, 1, 'o', 99.5, 'a', null, 1, 'yy', '2023-10-22', 3),
    (1, 3, 'k', 99.5, 'a', 'b', null, 'yy', '2023-10-19', 1),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18', 2),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17', 3),
    (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19', 4); 
    """

    sql """
    insert into ${lineitem_tb} values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17', null),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17', 1),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19', 3),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17', 1),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18', 2),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19', 3),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17', 1);
    """

    sql"""
    insert into ${partsupp_tb} values 
    (1, 1, 1, 99.5, 'yy', 1),
    (null, 2, 2, 109.2, 'mm', null),
    (3, null, 1, 99.5, 'yy', 3); 
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



    sql """select o_orderdate, o_shippriority, o_comment, o_orderkey, ${orders_tb}.public_col as col1,
        l_orderkey, l_partkey, l_suppkey, ${lineitem_tb}.public_col as col2,
        ps_partkey, ps_suppkey, ${partsupp_tb}.public_col as col3, ${partsupp_tb}.public_col * 2 as col4,
        o_orderkey + l_orderkey + ps_partkey * 2, 
        count() as count_all
        from ${orders_tb}
        join_type1 ${lineitem_tb} on ${lineitem_tb}.l_orderkey = ${orders_tb}.o_orderkey 
        join_type2 ${partsupp_tb} on ${partsupp_tb}.ps_partkey = ${lineitem_tb}.l_partkey and ${partsupp_tb}.ps_suppkey = ${lineitem_tb}.l_suppkey
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 """

/*
    """select * from
        (select o_orderdate, o_shippriority, o_comment, o_orderkey, ${orders_tb}.public_col as col1,
        l_orderkey, l_partkey, l_suppkey, ${lineitem_tb}.public_col as col2,
        ps_partkey, ps_suppkey, ${partsupp_tb}.public_col as col3, ${partsupp_tb}.public_col * 2 as col4,
        o_orderkey + l_orderkey + ps_partkey * 2,
        count() as count_all
        from ${orders_tb}
        join_type1 ${lineitem_tb} on ${lineitem_tb}.l_orderkey = ${orders_tb}.o_orderkey
        join_type2 ${partsupp_tb} on ${partsupp_tb}.ps_partkey = ${lineitem_tb}.l_partkey and ${partsupp_tb}.ps_suppkey = ${lineitem_tb}.l_suppkey
        ) as public_col where  public_col.

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
    where o_orderkey is null and o_orderkey <> 1

    where ${orders_tb}.public_col is null
    where ${orders_tb}.public_col <> 1
    where ${orders_tb}.public_col is null or ${orders_tb}.public_col <> 1
    where ${orders_tb}.public_col is null and ${orders_tb}.public_col <> 1

    where l_orderkey is null
    where l_orderkey <> 1
    where l_orderkey is null or l_orderkey <> 1
    where l_orderkey is null and l_orderkey <> 1

    where ${lineitem_tb}.public_col is null
    where ${lineitem_tb}.public_col <> 1
    where ${lineitem_tb}.public_col is null or ${lineitem_tb}.public_col <> 1
    where ${lineitem_tb}.public_col is null and ${lineitem_tb}.public_col <> 1


    where ps_partkey is null
    where ps_partkey <> 1
    where ps_partkey is null or ps_partkey <> 1
    where ps_partkey is null and ps_partkey <> 1

    where ${partsupp_tb}.public_col is null
    where ${partsupp_tb}.public_col <> 1
    where ${partsupp_tb}.public_col is null or ${partsupp_tb}.public_col <> 1
    where ${partsupp_tb}.public_col is null and ${partsupp_tb}.public_col <> 1

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
