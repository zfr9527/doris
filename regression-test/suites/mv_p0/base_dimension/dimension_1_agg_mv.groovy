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
suite("partition_mv_rewrite_dimension_1_agg_mv", "partition_mv_rewrite_dimension") {
    String db = context.config.getDbNameByFile(context.file)
    String order_tb = "orders_agg"
    String lineitem_tb = "lineitem_agg"
    sql "use ${db}"

    sql """
    drop table if exists orders_agg
    """

    sql """CREATE TABLE `orders_agg` (
      `o_orderkey` BIGINT not NULL,
      `o_custkey` INT not NULL,
      `o_orderdate` DATE not null,
      `o_orderstatus` VARCHAR(1) replace,
      `o_totalprice` DECIMAL(15, 2) sum,
      `o_orderpriority` VARCHAR(15) replace,
      `o_clerk` VARCHAR(15) replace,
      `o_shippriority` INT sum,
      `o_comment` VARCHAR(79) replace
    ) ENGINE=OLAP
    aggregate KEY(`o_orderkey`, `o_custkey`, `o_orderdate`)
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_agg
    """

    sql """CREATE TABLE `lineitem_agg` (
      `l_orderkey` BIGINT not NULL,
      `l_linenumber` INT not NULL,
      `l_partkey` INT not NULL,
      `l_suppkey` INT not NULL,
      `l_shipdate` DATE not null,
      `l_quantity` DECIMAL(15, 2) sum,
      `l_extendedprice` DECIMAL(15, 2) sum,
      `l_discount` DECIMAL(15, 2) sum,
      `l_tax` DECIMAL(15, 2) sum,
      `l_returnflag` VARCHAR(1) replace,
      `l_linestatus` VARCHAR(1) replace,
      `l_commitdate` DATE replace,
      `l_receiptdate` DATE replace,
      `l_shipinstruct` VARCHAR(25) replace,
      `l_shipmode` VARCHAR(10) replace,
      `l_comment` VARCHAR(44) replace
    ) ENGINE=OLAP
    aggregate KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate)
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_agg values 
    (2, 1, '2023-10-17', 'k', 99.5, 'a', 'b', 1, 'yy'),
    (1, 2, '2023-10-17', 'o', 109.2, 'c','d',2, 'mm'),
    (3, 3, '2023-10-19', null, 99.5, 'a', 'b', 1, 'yy'),
    (1, 2, '2023-10-20', 'o', null, 'a', 'b', 1, 'yy'),
    (2, 3, '2023-10-21', 'k', 109.2, null,'d',2, 'mm'),
    (3, 1, '2023-10-22', 'k', 99.5, 'a', null, 1, 'yy'),
    (1, 3, '2023-10-19', 'o', 99.5, 'a', 'b', null, 'yy'),
    (2, 1, '2023-10-18', 'o', 109.2, 'c','d',2, null),
    (3, 2, '2023-10-17', 'k', 99.5, 'a', 'b', 1, 'yy'),
    (4, 5, '2023-10-19', 'k', 99.5, 'a', 'b', 1, 'yy'); 
    """

    sql """
    insert into lineitem_agg values 
    (2, 1, 2, 3, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 1, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 3, 1, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (1, 2, 3, 3, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 3, 2, 1, '2023-10-18', 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 1, 1, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx'),
    (1, 3, 2, 2, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """

    sql """analyze table orders_agg with sync;"""
    sql """analyze table lineitem_agg with sync;"""

    def create_mv_lineitem = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name} on lineitem_agg;"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        AS  
        ${mv_sql}
        """
    }

    def create_mv_orders = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name} on orders_agg;"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        AS  
        ${mv_sql}
        """
    }

    def create_mv = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        AS  
        ${mv_sql}
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

    // agg
    // agg + without group by + with agg function
    def agg_mv_name_1 = "agg_mv_name_1"
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_1} ON orders_agg;"""
    sql """DROP TABLE IF EXISTS ${agg_mv_name_1}"""
    sql """
        CREATE MATERIALIZED VIEW ${agg_mv_name_1}
        AS
        select
            o_orderkey,
            sum(O_TOTALPRICE) as sum_total  
            from orders_agg
            group by o_orderkey
        """
    waitingMVTaskFinished(order_tb, agg_mv_name_1)

    def agg_sql_1 = """select 
        sum(O_totalprice) 
        from orders_agg
        """
    explain {
        sql("${agg_sql_1}")
        contains "(${agg_mv_name_1})"
    }
    compare_res(agg_sql_1 + " order by 1")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_1} ON orders_agg;"""

    // agg + with group by + without agg function
    def agg_mv_name_2 = "agg_mv_name_2"
    def agg_mv_stmt_2 = """
        select o_orderdatE, o_custkey, o_orderkey  
            from orders_agg 
            group by 
            o_orderdate, 
            o_custkey, 
            o_orderkey
        """
    create_mv_orders(agg_mv_name_2, agg_mv_stmt_2)
    waitingMVTaskFinished(order_tb, agg_mv_name_2)

    def agg_sql_2 = """select o_custkey, o_orderkey  
            from orders_agg 
            group by 
            o_custkey, 
            o_orderkey
        """
    def agg_sql_explain_2 = sql """explain ${agg_sql_2};"""
    def mv_index_1 = agg_sql_explain_2.toString().indexOf("MaterializedViewRewriteFail:")
    assert(mv_index_1 != -1)
    assert(agg_sql_explain_2.toString().substring(0, mv_index_1).indexOf(agg_mv_name_2) != -1)
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_2} ON orders_agg;"""

    /*
    // agg + with group by + with agg function
    def agg_mv_name_3 = "agg_mv_name_3"
    def agg_mv_stmt_3 = """
        select o_orderdatE, o_custkey, o_orderkey, 
            sum(o_totalprice) as sum_total 
            from orders_agg 
            group by 
            o_orderdatE, 
            o_custkey, 
            o_orderkey 
        """
    create_mv_orders(agg_mv_name_3, agg_mv_stmt_3)
    waitingMVTaskFinished(order_tb, agg_mv_name_3)

    def agg_sql_3 = """select o_custkey, o_orderkey, 
            sum(o_totalprice) as sum_total 
            from orders_agg 
            group by 
            o_custkey, 
            o_orderkey 
        """
    def agg_sql_explain_3 = sql """explain ${agg_sql_3};"""
    def mv_index_2 = agg_sql_explain_3.toString().indexOf("MaterializedViewRewriteFail:")
    assert(mv_index_2 != -1)
    assert(agg_sql_explain_3.toString().substring(0, mv_index_2).indexOf(agg_mv_name_3) != -1)
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_3} ON orders_agg;"""
     */

    def agg_mv_name_3 = "agg_mv_name_3"
    def agg_mv_stmt_3 = """
        select o_custkey, o_orderkey, 
            sum(o_totalprice) as sum_total 
            from orders_agg 
            group by 
            o_custkey, 
            o_orderkey 
        """
    create_mv_orders(agg_mv_name_3, agg_mv_stmt_3)
    waitingMVTaskFinished(order_tb, agg_mv_name_3)

    def agg_sql_3 = """select o_custkey,  
            sum(o_totalprice) as sum_total 
            from orders_agg 
            group by 
            o_custkey
        """
    def agg_sql_explain_3 = sql """explain ${agg_sql_3};"""
    def mv_index_2 = agg_sql_explain_3.toString().indexOf("MaterializedViewRewriteFail:")
    assert(mv_index_2 != -1)
    assert(agg_sql_explain_3.toString().substring(0, mv_index_2).indexOf(agg_mv_name_3) != -1)
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_3} ON orders_agg;"""

    // view partital rewriting
    def view_partition_mv_name_1 = "view_partition_mv_name_1"
    def view_partition_mv_stmt_1 = """
        select l_shipdatE, l_partkey, l_orderkey from lineitem_agg group by l_shipdate, l_partkey, l_orderkeY"""
    create_mv_lineitem(view_partition_mv_name_1, view_partition_mv_stmt_1)
    waitingMVTaskFinished(lineitem_tb, view_partition_mv_name_1)

    def view_partition_sql_1 = """select t.l_shipdate, t.l_partkey 
        from (select l_shipdate, l_partkey, l_orderkey from lineitem_agg group by l_shipdate, l_partkey, l_orderkey) t 
        group by t.l_shipdate, t.l_partkey
        """
    explain {
        sql("${view_partition_sql_1}")
        contains "(${view_partition_mv_name_1})"
    }
    compare_res(view_partition_sql_1 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${view_partition_mv_name_1} ON lineitem_agg;"""

    // predicate compensate
    def predicate_mv_name_1 = "predicate_mv_name_1"
    def predicate_mv_stmt_1 = """
        select l_shipdatE, l_partkey 
        from lineitem_agg 
        where l_shipdate >= "2023-10-17"
        group by l_shipdatE, l_partkey
        """
    create_mv_lineitem(predicate_mv_name_1, predicate_mv_stmt_1)
    waitingMVTaskFinished(lineitem_tb, predicate_mv_name_1)

    def predicate_sql_1 = """
        select l_shipdate, l_partkeY 
        from lineitem_agg 
        where l_shipdate >= "2023-10-17" and l_partkey = 1
        group by l_shipdate, l_partkeY
        """
    explain {
        sql("${predicate_sql_1}")
        contains "(${predicate_mv_name_1})"
    }
    compare_res(predicate_sql_1 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${predicate_mv_name_1} on lineitem_agg;"""

    def rewriting_mv_name_1 = "rewriting_mv_name_1"
    def rewriting_mv_stmt_1 = """
            select o_custkey, o_orderkey, 
            sum(o_totalprice) as sum_total 
            from orders_agg 
            where  o_orderkey > 1 + 1
            group by 
            o_custkey, 
            o_orderkey 
        """
    create_mv_orders(rewriting_mv_name_1, rewriting_mv_stmt_1)
    waitingMVTaskFinished(order_tb, rewriting_mv_name_1)

    def rewriting_sql_1 = """select o_custkey, o_orderkey, 
                sum(o_totalprice) as sum_total 
                from orders_agg 
                where  o_orderkey > (-3) + 5
                group by 
                o_custkey, 
                o_orderkey 
                """
    explain {
        sql("${rewriting_sql_1}")
        contains "(${rewriting_mv_name_1})"
    }
    compare_res(rewriting_sql_1 + " order by 1,2,3,4,5")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${rewriting_mv_name_1} on orders_agg;"""

    // single table
    def mv_name_1 = "single_tb_mv_1"
    def single_table_mv_stmt_1 = """
        select o_orderdate, o_orderkey, 
            sum(o_totalprice) as sum_total 
            from orders_agg 
            where o_orderdate like '2023-10-%'
            group by 
            o_orderdate, 
            o_orderkey 
        """

    create_mv_orders(mv_name_1, single_table_mv_stmt_1)
    waitingMVTaskFinished(order_tb, mv_name_1)

    def single_table_query_stmt_1 = """        
        select o_orderdate, 
            sum(o_totalprice) as sum_total 
            from orders_agg 
            where o_orderdate like '2023-10-%'
            group by 
            o_orderdate, 
            o_orderkey 
        """
    def single_table_query_stmt_2 = """        
        select o_orderdate, 
            sum(o_totalprice) as sum_total 
            from orders_agg 
            where o_orderdate like '2023-10-%'
            group by 
            o_orderdate
        """
    def single_table_query_stmt_3 = """
        select o_orderdate, o_orderkey, 
            sum(o_totalprice) as sum_total 
            from orders_agg 
            where o_orderdate like '2023-10-%' and o_orderkey > 0 + 1
            group by 
            o_orderdate, 
            o_orderkey 
        """
    def single_table_query_stmt_4 = """
        select o_orderdate, o_orderkey, 
            sum(o_totalprice) as sum_total 
            from orders_agg 
            where o_orderdate like '2023-10-%' and o_orderkey >= o_orderkey
            group by 
            o_orderdate, 
            o_orderkey 
        """

    explain {
        sql("${single_table_query_stmt_1}")
        contains "(${mv_name_1})"
    }
    compare_res(single_table_query_stmt_1 + " order by 1,2,3")

    explain {
        sql("${single_table_query_stmt_2}")
        contains "(${mv_name_1})"
    }
    compare_res(single_table_query_stmt_2 + " order by 1,2,3")

    explain {
        sql("${single_table_query_stmt_3}")
        contains "(${mv_name_1})"
    }
    compare_res(single_table_query_stmt_3 + " order by 1,2,3")

    explain {
        sql("${single_table_query_stmt_4}")
        contains "(${mv_name_1})"
    }
    compare_res(single_table_query_stmt_4 + " order by 1,2,3")

}
