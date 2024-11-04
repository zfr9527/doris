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


suite("create_table_nested_mtmv") {
    sql """set global enable_materialized_view_rewrite=true;"""
    sql """set global enable_materialized_view_nest_rewrite=true;"""
    sql """set global enable_materialized_view_union_rewrite=true;"""

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

    String db = context.config.getDbNameByFile(context.file)
    String table_order = "orders"
    String table_lineitem = "lineitem"

    sql """
    drop table if exists ${table_order}
    """

    sql """CREATE TABLE `${table_order}` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL,
      `o_orderdate` DATE not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`, `o_custkey`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists ${table_lineitem}
    """

    sql """CREATE TABLE `${table_lineitem}` (
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
      `l_shipdate` DATE not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into ${table_order} values 
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
    insert into ${table_lineitem} values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    // table -> view
    def t_v_view = "t_v_view"
    def t_v_view_stmt = """select o_orderdate, o_shippriority, o_comment, l_orderkey, 
            sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from ${table_order} 
            left join ${table_lineitem} on ${table_lineitem}.l_orderkey = ${table_order}.o_orderkey
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            l_orderkey"""
    sql """drop view if exists ${t_v_view}"""
    sql """CREATE VIEW ${t_v_view} (k1, k2, k3, k4, v1, v2, v3, v4, v5, v6) as ${t_v_view_stmt}"""
    def t_v_view_sql = """select * from ${t_v_view}"""
    order_qt_nested_mtmv_query_t_v_view "${t_v_view_sql}"


    // table -> mtmv
    def t_mt_mtmv = "t_mt_mtmv"
    sql """DROP MATERIALIZED VIEW IF EXISTS ${t_mt_mtmv};"""
    create_async_mv(db, t_mt_mtmv, t_v_view_stmt)
    sql """analyze table ${t_mt_mtmv} with sync;"""
    def t_mt_mtmv_sql = "select * from ${t_mt_mtmv}"
    order_qt_nested_mtmv_query_t_mt_mtmv "${t_mt_mtmv_sql}"

    mv_rewrite_success(t_v_view_stmt, t_mt_mtmv)
    compare_res(t_v_view_stmt + " order by 1,2,3,4,5,6,7,8")

    // table -> mv
    def t_mv_mv = "t_mv_mv"
    def t_mv_mv_stmt = """select
            o_orderkey,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
            from ${table_order} group by o_orderkey
            """
    sql """drop materialized view if exists ${t_mv_mv} on ${table_order};"""
    createMV(getMVStmt(t_mv_mv, t_mv_mv_stmt))
    sql """analyze table ${table_order} with sync;"""
    def t_mv_mv_sql = """select * from ${table_order} index ${t_mv_mv}"""
    order_qt_nested_mtmv_query_t_mv_mv "${t_mv_mv_sql}"

    mv_rewrite_success(t_mv_mv_stmt, t_mv_mv)
    compare_res(t_mv_mv_stmt + " order by 1,2,3,4,5")

    // table -> view -> table
    def t_v_t_tb = "t_v_t_tb"
    sql """drop table if exists ${t_v_t_tb}"""
    sql """create table ${t_v_t_tb} PROPERTIES("replication_num" = "1") as select k1, k2, k3, k4, v1, v2, v3, v4 from ${t_v_view}"""
    def t_v_t_tb_sql = """select * from ${t_v_t_tb}"""
    order_qt_nested_mtmv_query_t_v_t_tb "${t_v_t_tb_sql}"

    // table -> view -> table -> mv
    def t_v_t_mv_mv = "t_v_t_mv_mv"
    def t_v_t_mv_mv_stmt = """select k1, k2, k3, k4, 
            sum(v1) as sum_total, 
            max(v2) as max_total, 
            min(v3) as min_total, 
            count(v4) as count_all, 
            bitmap_union(to_bitmap(case when k1 > 1 and k2 IN (1, 3) then k3 else null end)) v5, 
            bitmap_union(to_bitmap(case when k1 > 2 and k2 IN (2) then k3 else null end)) as v6 
            from ${t_v_t_tb} 
            group by 
            k1, 
            k2, 
            k3, 
            k4 """
    sql """drop materialized view if exists ${t_v_t_mv_mv} on ${t_v_t_tb};"""
    createMV(getMVStmt(t_v_t_mv_mv, t_v_t_mv_mv_stmt))
    sql """analyze table ${t_v_t_tb} with sync;"""
    def t_v_t_mv_mv_sql = """select * from ${t_v_t_tb} index ${t_v_t_mv_mv}"""
    order_qt_nested_mtmv_query_t_v_t_mv_mv "${t_v_t_mv_mv_sql}"

    mv_rewrite_success(t_v_t_mv_mv_stmt, t_v_t_mv_mv)
    compare_res(t_v_t_mv_mv_stmt + " order by 1,2,3,4,5,6,7,8")

    // table -> view -> table -> mtmv
    def t_v_t_mt_mtmv = "t_v_t_mt_mtmv"
    def t_v_t_mt_mtmv_stmt = """select k1, k2, k3, l_shipdate, 
            sum(v1) as sum_total, 
            max(v2) as max_total, 
            min(v3) as min_total, 
            count(v4) as count_all, 
            bitmap_union(to_bitmap(case when k1 > 1 then k2 else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when k1 > 2 then k2 else null end)) as cnt_2 
            from ${t_v_t_tb} 
            left join ${table_lineitem} on ${table_lineitem}.l_shipdate = ${t_v_t_tb}.k1
            group by 
            k1, 
            k2, 
            k3, 
            l_shipdate"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ${t_v_t_mt_mtmv};"""
    create_async_mv(db, t_v_t_mt_mtmv, t_v_t_mt_mtmv_stmt)
    sql """analyze table ${t_v_t_mt_mtmv} with sync;"""
    def t_v_t_mt_mtmv_sql = """select * from ${t_v_t_mt_mtmv}"""
    order_qt_nested_mtmv_query_t_v_t_mt_mtmv "${t_v_t_mt_mtmv_sql}"

    mv_rewrite_success(t_v_t_mt_mtmv_stmt, t_v_t_mt_mtmv)
    compare_res(t_v_t_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8")

    // table -> view -> table -> view
    def t_v_t_v_view = "t_v_t_v_view"
    def t_v_t_v_view_stmt = """select k1, k2, k3, l_shipdate, 
            sum(v1) as sum_total, 
            max(v2) as max_total, 
            min(v3) as min_total, 
            count(v4) as count_all, 
            bitmap_union(to_bitmap(case when k2 > 1 then k3 else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when k2 > 2 then k3 else null end)) as cnt_2 
            from ${t_v_t_tb} 
            left join ${table_lineitem} on ${table_lineitem}.l_shipdate = ${t_v_t_tb}.k1
            group by 
            k1, 
            k2, 
            k3, 
            l_shipdate"""
    sql """drop view if exists ${t_v_t_v_view}"""
    sql """CREATE VIEW ${t_v_t_v_view} (k1, k2, k3, k4, v1, v2, v3, v4, v5, v6) as ${t_v_t_v_view_stmt}"""
    def t_v_t_v_view_sql = """select * from ${t_v_t_v_view}"""
    order_qt_nested_mtmv_query_t_v_t_v_view "${t_v_t_v_view_sql}"

    // table -> view -> view
    def t_v_v_view = "t_v_v_view"
    def t_v_v_view_stmt = """select k1, k2, k3, k4, 
            sum(v1) as sum_total, 
            max(v2) as max_total, 
            min(v3) as min_total, 
            count(v4) as count_all, 
            bitmap_union(to_bitmap(case when k1 > 1 and k2 IN (1, 3) then k3 else null end)) v5, 
            bitmap_union(to_bitmap(case when k1 > 2 and k2 IN (2) then k3 else null end)) as v6 
            from ${t_v_view} 
            left join ${table_lineitem} on ${table_lineitem}.l_orderkey = ${t_v_view}.k1
            group by 
            k1, 
            k2, 
            k3, 
            k4 """
    sql """drop view if exists ${t_v_v_view}"""
    sql """CREATE VIEW ${t_v_v_view} (k1, k2, k3, k4, v1, v2, v3, v4, v5, v6) as ${t_v_v_view_stmt}"""
    def t_v_v_view_sql = """select * from ${t_v_v_view}"""
    order_qt_nested_mtmv_query_t_v_v_view "${t_v_v_view_sql}"


    // TODO: create mtmv based on view
//    // table -> view -> mtmv
//    def t_v_mt_mtmv = "t_v_mt_mtmv"
//    sql """DROP MATERIALIZED VIEW IF EXISTS ${t_v_mt_mtmv};"""
//    create_async_mv(db, t_v_mt_mtmv, t_v_v_view_stmt)
//    sql """analyze table ${t_v_mt_mtmv} with sync;"""

    // table -> view -> mv
    def t_v_mv_mv = "t_v_mv_mv"
    def t_v_mv_mv_stmt = """select k1, k2, k3, k4, v1, v2 from ${t_v_view}"""
    try {
        createMV(getMVStmt(t_v_mv_mv, t_v_mv_mv_stmt))
    } catch (Exception e) {
        logger.info(e.getMessage())
        assertTrue(e.getMessage().contains("The materialized view only support olap table"), e.getMessage())
    }

    // table -> mtmv -> table
    def t_mt_t_tb = "t_mt_t_tb"
    def t_mt_t_tb_stmt = """select * from ${t_mt_mtmv}"""
    sql """drop table if exists ${t_mt_t_tb}"""
    sql """create table ${t_mt_t_tb} PROPERTIES("replication_num" = "1") as ${t_mt_t_tb_stmt}"""
    def t_mt_t_tb_sql = """select * from ${t_mt_t_tb}"""
    order_qt_nested_mtmv_query_t_mt_t_tb "${t_mt_t_tb_sql}"


    // table -> mtmv -> mtmv
    def t_mt_mt_mtmv = "t_mt_mt_mtmv"
    def t_mt_mt_mtmv_stmt = """select o_orderdate, o_shippriority, o_comment, l_shipdate, 
            sum(sum_total) as sum_total, 
            max(max_total) as max_total, 
            min(min_total) as min_total, 
            count(count_all) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 then o_comment else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 then o_comment else null end)) as cnt_2 
            from ${t_mt_mtmv} 
            left join ${table_lineitem} on ${table_lineitem}.l_shipdate = ${t_mt_mtmv}.o_orderdate
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment, 
            l_shipdate"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ${t_mt_mt_mtmv};"""
    create_async_mv(db, t_mt_mt_mtmv, t_mt_mt_mtmv_stmt)
    sql """analyze table ${t_mt_mt_mtmv} with sync;"""
    def t_mt_mt_mtmv_sql = """select * from ${t_mt_mt_mtmv}"""
    order_qt_nested_mtmv_query_t_mt_mt_mtmv "${t_mt_mt_mtmv_sql}"

    mv_rewrite_success(t_mt_mt_mtmv_stmt, t_mt_mt_mtmv)
    compare_res(t_mt_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8")

    def t_mt_mt_mtmv_hit = """select t.o_orderdate, t.o_shippriority, t.o_comment, l_shipdate, 
            sum(t.sum_total) as sum_total, 
            max(t.max_total) as max_total, 
            min(t.min_total) as min_total, 
            count(t.count_all) as count_all, 
            bitmap_union(to_bitmap(case when t.o_shippriority > 1 then t.o_comment else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when t.o_shippriority > 2 then t.o_comment else null end)) as cnt_2 
            from (select o_orderdate, o_shippriority, o_comment, l_orderkey, 
            sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from ${table_order} 
            left join ${table_lineitem} on ${table_lineitem}.l_orderkey = ${table_order}.o_orderkey
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            l_orderkey) as t 
            left join ${table_lineitem} on ${table_lineitem}.l_shipdate = t.o_orderdate
            group by 
            t.o_orderdate, 
            t.o_shippriority, 
            t.o_comment, 
            l_shipdate"""
    mv_rewrite_success(t_mt_mt_mtmv_hit, t_mt_mt_mtmv)
    compare_res(t_mt_mt_mtmv_hit + " order by 1,2,3,4,5,6,7,8")

    // table -> mtmv -> mtmv -> table
    def t_mt_mt_t_tb = "t_mt_mt_t_tb"
    def t_mt_mt_t_tb_stmt = """select * from ${t_mt_mt_mtmv}"""
    sql """drop table if exists ${t_mt_mt_t_tb}"""
    sql """create table ${t_mt_mt_t_tb} PROPERTIES("replication_num" = "1") as ${t_mt_mt_t_tb_stmt}"""
    def t_mt_mt_t_tb_sql = """select * from ${t_mt_mt_t_tb}"""
    order_qt_nested_mtmv_query_t_mt_mt_t_tb "${t_mt_mt_t_tb_sql}"

    // table -> mtmv -> mtmv -> mtmv
    def t_mt_mt_mt_mtmv = "t_mt_mt_mt_mtmv"
    def t_mt_mt_mt_mtmv_stmt = """select o_orderdate, o_shippriority, o_comment, t2.l_shipdate, 
            sum(sum_total) as sum_total, 
            max(max_total) as max_total, 
            min(min_total) as min_total, 
            count(count_all) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 then o_comment else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 then o_comment else null end)) as cnt_2 
            from ${t_mt_mt_mtmv} 
            left join ${table_lineitem} as t2 on t2.l_shipdate = ${t_mt_mt_mtmv}.o_orderdate
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment, 
            t2.l_shipdate"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ${t_mt_mt_mt_mtmv};"""
    create_async_mv(db, t_mt_mt_mt_mtmv, t_mt_mt_mt_mtmv_stmt)
    sql """analyze table ${t_mt_mt_mt_mtmv} with sync;"""
    def t_mt_mt_mt_mtmv_sql = """select * from ${t_mt_mt_mt_mtmv}"""
    order_qt_nested_mtmv_query_t_mt_mt_mt_mtmv "${t_mt_mt_mt_mtmv_sql}"

    mv_rewrite_success(t_mt_mt_mt_mtmv_stmt, t_mt_mt_mt_mtmv)
    compare_res(t_mt_mt_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8")

    def t_mt_mt_mt_mtmv_hit = """select tt.o_orderdate, tt.o_shippriority, tt.o_comment, t2.l_shipdate, 
            sum(tt.sum_total) as sum_total, 
            max(tt.max_total) as max_total, 
            min(tt.min_total) as min_total, 
            count(tt.count_all) as count_all, 
            bitmap_union(to_bitmap(case when tt.o_shippriority > 1 then tt.o_comment else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when tt.o_shippriority > 2 then tt.o_comment else null end)) as cnt_2 
            from 
            (
            select t.o_orderdate as o_orderdate, t.o_shippriority as o_shippriority, t.o_comment as o_comment, l_shipdate, 
            sum(t.sum_total) as sum_total, 
            max(t.max_total) as max_total, 
            min(t.min_total) as min_total, 
            count(t.count_all) as count_all, 
            bitmap_union(to_bitmap(case when t.o_shippriority > 1 then t.o_comment else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when t.o_shippriority > 2 then t.o_comment else null end)) as cnt_2 
            from (select o_orderdate, o_shippriority, o_comment, l_orderkey, 
            sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from ${table_order} 
            left join ${table_lineitem} on ${table_lineitem}.l_orderkey = ${table_order}.o_orderkey
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            l_orderkey) as t 
            left join ${table_lineitem} on ${table_lineitem}.l_shipdate = t.o_orderdate
            group by 
            t.o_orderdate, 
            t.o_shippriority, 
            t.o_comment, 
            l_shipdate
            ) as tt 
            left join ${table_lineitem} as t2 on t2.l_shipdate = tt.o_orderdate
            group by 
            tt.o_orderdate, 
            tt.o_shippriority, 
            tt.o_comment, 
            t2.l_shipdate"""
    mv_rewrite_success(t_mt_mt_mt_mtmv_hit, t_mt_mt_mt_mtmv)
    compare_res(t_mt_mt_mt_mtmv_hit + " order by 1,2,3,4,5,6,7,8")

    // table -> mtmv -> mtmv -> view
    def t_mt_mt_v_view = "t_mt_mt_v_view"
    def t_mt_mt_v_view_stmt = """select o_orderdate, o_shippriority, o_comment, t2.l_shipdate, 
            sum(sum_total) as sum_total, 
            max(max_total) as max_total, 
            min(min_total) as min_total, 
            count(count_all) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 then o_comment else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 then o_comment else null end)) as cnt_2 
            from ${t_mt_mt_mtmv} 
            left join ${table_lineitem} as t2 on t2.l_shipdate = ${t_mt_mt_mtmv}.o_orderdate
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment, 
            t2.l_shipdate"""
    sql """drop view if exists ${t_mt_mt_v_view}"""
    sql """CREATE VIEW ${t_mt_mt_v_view} (k1, k2, k3, k4, v1, v2, v3, v4, v5, v6) as ${t_mt_mt_v_view_stmt}"""
    def t_mt_mt_v_view_sql = """select * from ${t_mt_mt_v_view}"""
    order_qt_nested_mtmv_query_t_mt_mt_v_view "${t_mt_mt_v_view_sql}"

    // table -> mtmv -> mtmv -> mv
    def t_mt_mt_mv_mv = "t_mt_mt_mv_mv"
    def t_mt_mt_mv_mv_stmt = """select o_orderdate, o_shippriority, o_comment, 
            sum(sum_total) as sum_total, 
            max(max_total) as max_total, 
            min(min_total) as min_total, 
            count(count_all) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 then o_orderdate else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 then o_orderdate else null end)) as cnt_2 
            from ${t_mt_mt_mtmv} 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment"""
    sql """drop materialized view if exists ${t_mt_mt_mv_mv} on ${t_mt_mt_mtmv};"""
    createMV(getMVStmt(t_mt_mt_mv_mv, t_mt_mt_mv_mv_stmt))
    sql """analyze table ${t_mt_mt_mtmv} with sync;"""
    def t_mt_mt_mv_mv_sql = """select * from ${t_mt_mt_mtmv} index ${t_mt_mt_mv_mv}"""
    order_qt_nested_mtmv_query_t_mt_mt_mv_mv "${t_mt_mt_mv_mv_sql}"

    mv_rewrite_success(t_mt_mt_mv_mv_stmt, t_mt_mt_mv_mv)
    compare_res(t_mt_mt_mv_mv_stmt + " order by 1,2,3,4,5,6,7")

    def t_mt_mt_mv_mv_hit1 = """select tt.o_orderdate, tt.o_shippriority, tt.o_comment, 
            sum(tt.sum_total) as sum_total, 
            max(tt.max_total) as max_total, 
            min(tt.min_total) as min_total, 
            count(tt.count_all) as count_all, 
            bitmap_union(to_bitmap(case when tt.o_shippriority > 1 then tt.o_orderdate else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when tt.o_shippriority > 2 then tt.o_orderdate else null end)) as cnt_2 
            from 
            
            (
            select t.o_orderdate, t.o_shippriority, t.o_comment, l_shipdate, 
            sum(t.sum_total) as sum_total, 
            max(t.max_total) as max_total, 
            min(t.min_total) as min_total, 
            count(t.count_all) as count_all, 
            bitmap_union(to_bitmap(case when t.o_shippriority > 1 then t.o_comment else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when t.o_shippriority > 2 then t.o_comment else null end)) as cnt_2 
            from (select o_orderdate, o_shippriority, o_comment, l_orderkey, 
            sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from ${table_order} 
            left join ${table_lineitem} on ${table_lineitem}.l_orderkey = ${table_order}.o_orderkey
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            l_orderkey) as t 
            left join ${table_lineitem} on ${table_lineitem}.l_shipdate = t.o_orderdate
            group by 
            t.o_orderdate, 
            t.o_shippriority, 
            t.o_comment, 
            l_shipdate
            ) as tt 
            
            group by 
            tt.o_orderdate, 
            tt.o_shippriority, 
            tt.o_comment"""
    mv_rewrite_success(t_mt_mt_mv_mv_hit1, t_mt_mt_mtmv)
    compare_res(t_mt_mt_mv_mv_hit1 + " order by 1,2,3,4,5,6,7")

    def t_mt_mt_mv_mv_hit2 = """select o_orderdate, o_shippriority, o_comment, 
            sum(sum_total) as sum_total, 
            max(max_total) as max_total, 
            min(min_total) as min_total, 
            count(count_all) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 then o_orderdate else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 then o_orderdate else null end)) as cnt_2 
            from ${t_mt_mt_mtmv} 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment"""
    mv_rewrite_success(t_mt_mt_mv_mv_hit2, t_mt_mt_mv_mv)
    compare_res(t_mt_mt_mv_mv_hit2 + " order by 1,2,3,4,5,6,7")


    // table -> mtmv -> view
    def t_mt_v_view = "t_mt_v_view"
    def t_mt_v_view_stmt = """select o_orderdate, o_shippriority, o_comment, l_shipdate, 
            sum(sum_total) as sum_total, 
            max(max_total) as max_total, 
            min(min_total) as min_total, 
            count(count_all) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 then o_comment else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 then o_comment else null end)) as cnt_2 
            from ${t_mt_mtmv} 
            left join ${table_lineitem} on ${table_lineitem}.l_shipdate = ${t_mt_mtmv}.o_orderdate
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            l_shipdate """
    sql """drop view if exists ${t_mt_v_view}"""
    sql """CREATE VIEW ${t_mt_v_view} (k1, k2, k3, k4, v1, v2, v3, v4, v5, v6) as ${t_mt_v_view_stmt}"""
    def t_mt_v_view_sql = """select * from ${t_mt_v_view}"""
    order_qt_nested_mtmv_query_t_mt_v_view "${t_mt_v_view_sql}"

    // table -> mtmv -> mv
    def t_mt_mv_mv = "t_mt_mv_mv"
    def t_mt_mv_mv_stmt = """select o_orderdate, o_shippriority, o_comment, l_orderkey, 
            sum(sum_total) as sum_total, 
            max(max_total) as max_total, 
            min(min_total) as min_total, 
            count(count_all) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 then l_orderkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 then l_orderkey else null end)) as cnt_2 
            from ${t_mt_mtmv} 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            l_orderkey """
    sql """drop materialized view if exists ${t_mt_mv_mv} on ${t_mt_mtmv};"""
    createMV(getMVStmt(t_mt_mv_mv, t_mt_mv_mv_stmt))
    sql """analyze table ${t_mt_mtmv} with sync;"""
    def t_mt_mv_mv_sql = """select * from ${t_mt_mtmv} index ${t_mt_mv_mv}"""
    order_qt_nested_mtmv_query_t_mt_mv_mv "${t_mt_mv_mv_sql}"

    mv_rewrite_success(t_mt_mv_mv_stmt, t_mt_mv_mv)
    compare_res(t_mt_mv_mv_stmt + " order by 1,2,3,4,5,6,7,8")

    def t_mt_mv_mv_hit1 = """select t.o_orderdate, t.o_shippriority, t.o_comment, t.l_orderkey, 
            sum(t.sum_total) as sum_total, 
            max(t.max_total) as max_total, 
            min(t.min_total) as min_total, 
            count(t.count_all) as count_all, 
            bitmap_union(to_bitmap(case when t.o_shippriority > 1 then t.l_orderkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when t.o_shippriority > 2 then t.l_orderkey else null end)) as cnt_2 
            from (
            
            select o_orderdate, o_shippriority, o_comment, l_orderkey, 
            sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from ${table_order} 
            left join ${table_lineitem} on ${table_lineitem}.l_orderkey = ${table_order}.o_orderkey
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            l_orderkey
            
            ) as t 
            group by 
            t.o_orderdate, 
            t.o_shippriority, 
            t.o_comment,
            t.l_orderkey """
    mv_rewrite_success(t_mt_mv_mv_hit1, t_mt_mtmv)
    compare_res(t_mt_mv_mv_hit1 + " order by 1,2,3,4,5,6,7,8")

    def t_mt_mv_mv_hit2 = """select o_orderdate, o_shippriority, o_comment, l_orderkey, 
            sum(sum_total) as sum_total, 
            max(max_total) as max_total, 
            min(min_total) as min_total, 
            count(count_all) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 then l_orderkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 then l_orderkey else null end)) as cnt_2 
            from ${t_mt_mtmv} 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            l_orderkey"""
    mv_rewrite_success(t_mt_mv_mv_hit2, t_mt_mv_mv)
    compare_res(t_mt_mv_mv_hit2 + " order by 1,2,3,4,5,6,7,8")

    // table -> mtmv -> mv -> table
    def t_mt_mv_t_tb = "t_mt_mv_t_tb"
    def t_mt_mv_t_tb_stmt = """select 
            `mv_o_orderdate` as k1, 
            `mv_o_shippriority` as k6,
            `mv_o_comment` as k7, 
            `mv_l_orderkey` as k8, 
            `mva_SUM__``sum_total``` as k2, 
            `mva_MAX__``max_total``` as k3, 
            `mva_MIN__``min_total``` as k4, 
            `mva_SUM__CASE WHEN ``count_all`` IS NULL THEN 0 ELSE 1 END` as k5, 
            `mva_BITMAP_UNION__to_bitmap_with_check(if((``o_shippriority`` > 1), ``l_orderkey``, NULL))` as k9, 
            `mva_BITMAP_UNION__to_bitmap_with_check(if((``o_shippriority`` > 2), ``l_orderkey``, NULL))` as k10
            from ${t_mt_mtmv} index ${t_mt_mv_mv}"""
    sql """drop table if exists ${t_mt_mv_t_tb}"""
    sql """create table ${t_mt_mv_t_tb} PROPERTIES("replication_num" = "1") as ${t_mt_mv_t_tb_stmt}"""
    def t_mt_mv_t_tb_sql = """select * from ${t_mt_mv_t_tb}"""
    order_qt_nested_mtmv_query_t_mt_mv_t_tb "${t_mt_mv_t_tb_sql}"


    // table -> mtmv -> mv -> mtmv
    def t_mt_mv_mt_mtmv = "t_mt_mv_mt_mtmv"
    def t_mt_mv_mt_mtmv_stmt = """
            select 
            `mv_o_orderdate` as k1, 
            l_shipdate, 
            `mv_o_shippriority` as k6,
            `mv_o_comment` as k7, 
            `mv_l_orderkey` as k8, 
            `mva_SUM__``sum_total``` as k2, 
            `mva_MAX__``max_total``` as k3, 
            `mva_MIN__``min_total``` as k4, 
            `mva_SUM__CASE WHEN ``count_all`` IS NULL THEN 0 ELSE 1 END` as k5, 
            bitmap_union(`mva_BITMAP_UNION__to_bitmap_with_check(if((``o_shippriority`` > 1), ``l_orderkey``, NULL))`) as k9, 
            bitmap_union(`mva_BITMAP_UNION__to_bitmap_with_check(if((``o_shippriority`` > 2), ``l_orderkey``, NULL))`) as k10 
            from ${t_mt_mtmv} index ${t_mt_mv_mv} 
            left join ${table_lineitem} on ${table_lineitem}.l_shipdate = ${t_mt_mtmv}.mv_o_orderdate
            group by l_shipdate, k1, k2, k3, k4, k5, k6, k7, k8
            """
    sql """DROP MATERIALIZED VIEW IF EXISTS ${t_mt_mv_mt_mtmv};"""
    create_async_mv(db, t_mt_mv_mt_mtmv, t_mt_mv_mt_mtmv_stmt)
    sql """analyze table ${t_mt_mv_mt_mtmv} with sync;"""
    def t_mt_mv_mt_mtmv_sql = """select * from ${t_mt_mv_mt_mtmv}"""
    order_qt_nested_mtmv_query_t_mt_mv_mt_mtmv "${t_mt_mv_mt_mtmv_sql}"

    mv_rewrite_success(t_mt_mv_mt_mtmv_stmt, t_mt_mv_mt_mtmv)
    compare_res(t_mt_mv_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8,9")

    // table -> mtmv -> mv -> view
    def t_mt_mv_v_view = "t_mt_mv_v_view"
    def t_mt_mv_v_view_stmt = """select 
            `mv_o_orderdate` as k1, 
            l_shipdate, 
            `mv_o_shippriority` as k6,
            `mv_o_comment` as k7, 
            `mv_l_orderkey` as k8, 
            `mva_SUM__``sum_total``` as k2, 
            `mva_MAX__``max_total``` as k3, 
            `mva_MIN__``min_total``` as k4, 
            `mva_SUM__CASE WHEN ``count_all`` IS NULL THEN 0 ELSE 1 END` as k5, 
            bitmap_union(`mva_BITMAP_UNION__to_bitmap_with_check(if((``o_shippriority`` > 1), ``l_orderkey``, NULL))`) as k9, 
            bitmap_union(`mva_BITMAP_UNION__to_bitmap_with_check(if((``o_shippriority`` > 2), ``l_orderkey``, NULL))`) as k10 
            from ${t_mt_mtmv} index ${t_mt_mv_mv} 
            left join ${table_lineitem} on ${table_lineitem}.l_shipdate = ${t_mt_mtmv}.mv_o_orderdate
            group by l_shipdate, k1, k2, k3, k4, k5, k6, k7, k8"""
    sql """drop view if exists ${t_mt_mv_v_view}"""
    sql """CREATE VIEW ${t_mt_mv_v_view} (k1, l_shipdate, k2, k3, k4, k5, k6, k7, k8, k9, k10) as ${t_mt_mv_v_view_stmt}"""
    def t_mt_mv_v_view_sql = """select * from ${t_mt_mv_v_view}"""
    order_qt_nested_mtmv_query_t_mt_mv_v_view "${t_mt_mv_v_view_sql}"

//    // table -> mtmv -> mv -> view -> mtmv
//    def t_mt_mv_v_mt_mtmv = "t_mt_mv_v_mt_mtmv"
//    sql """DROP MATERIALIZED VIEW IF EXISTS ${t_mt_mv_v_mt_mtmv};"""
//    def t_mt_mv_v_mt_mtmv_stmt = """
//            select k1, l_shipdate, k2, k3, k4, k5, k6, k7, k8, k9, k10 from ${t_mt_mv_v_view}
//            group by k1, l_shipdate, k2, k3, k4, k5, k6, k7, k8, k9, k10"""
//    create_async_mv(db, t_mt_mv_v_mt_mtmv, t_mt_mv_v_mt_mtmv_stmt)
//    sql """analyze table ${t_mt_mv_v_mt_mtmv} with sync;"""
//    def t_mt_mv_v_mt_mtmv_sql = "select * from ${t_mt_mv_v_mt_mtmv}"
//    order_qt_nested_mtmv_query_t_mt_mv_v_mt_mtmv "${t_mt_mv_v_mt_mtmv_sql}"
//
//    mv_rewrite_success(t_mt_mv_v_mt_mtmv_stmt, t_mt_mv_v_mt_mtmv)
//    compare_res(t_mt_mv_v_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8,9")

    // table -> mv -> table
    def t_mv_t_tb = "t_mv_t_tb"
    sql """drop table if exists ${t_mv_t_tb}"""
    sql """create table ${t_mv_t_tb} PROPERTIES("replication_num" = "1") as 
            select `mv_o_orderkey` as k1, 
            `mva_SUM__``o_totalprice``` as k2, 
            `mva_MAX__``o_totalprice``` as k3, 
            `mva_MIN__``o_totalprice``` as k4, 
            `mva_SUM__CASE WHEN 1 IS NULL THEN 0 ELSE 1 END` as k5, 
            `mva_BITMAP_UNION__to_bitmap_with_check(CAST(if(((``o_shippriority`` > 1) AND ``o_orderkey`` IN (1, 3)), ``o_custkey``, NULL) AS bigint))` as k6, 
            `mva_BITMAP_UNION__to_bitmap_with_check(CAST(if(((``o_shippriority`` > 2) AND ``o_orderkey`` IN (2)), ``o_custkey``, NULL) AS bigint))` as k7 
            from ${table_order} 
            index ${t_mv_mv} """
    def t_mv_t_tb_sql = """select * from ${t_mv_t_tb}"""
    order_qt_nested_mtmv_query_t_mv_t_tb "${t_mv_t_tb_sql}"


    // table -> mv -> mtmv
    def t_mv_mt_mtmv = "t_mv_mt_mtmv"
    def t_mv_mt_mtmv_stmt = """select 
            `mv_o_orderkey` as k1, 
            `mva_SUM__``o_totalprice``` as k2, 
            `mva_MAX__``o_totalprice``` as k3, 
            `mva_MIN__``o_totalprice``` as k4, 
            `mva_SUM__CASE WHEN 1 IS NULL THEN 0 ELSE 1 END` as k5, 
            l_orderkey, 
            sum(`mv_o_orderkey`) as sum_total, 
            max(`mv_o_orderkey`) as max_total, 
            min(`mv_o_orderkey`) as min_total, 
            count(`mva_SUM__``o_totalprice```) as count_all, 
            bitmap_union(to_bitmap(case when mv_o_orderkey > 1 then `mva_SUM__``o_totalprice``` else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when mv_o_orderkey > 2 then `mva_MAX__``o_totalprice``` else null end)) as cnt_2 
            from ${table_order} index ${t_mv_mv}
            left join ${table_lineitem} on ${table_lineitem}.l_orderkey = ${table_order}.mv_o_orderkey
            group by 
            k1, k2, k3, k4, k5, l_orderkey, mv_o_orderkey"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ${t_mv_mt_mtmv};"""
    create_async_mv(db, t_mv_mt_mtmv, t_mv_mt_mtmv_stmt)
    sql """analyze table ${t_mv_mt_mtmv} with sync;"""
    def t_mv_mt_mtmv_sql = """select * from ${t_mv_mt_mtmv}"""
    order_qt_nested_mtmv_query_t_mv_mt_mtmv "${t_mv_mt_mtmv_sql}"

    mv_rewrite_success(t_mv_mt_mtmv_stmt, t_mv_mt_mtmv)
    compare_res(t_mv_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8,9,10")

    // table -> mv -> mtmv -> table
    def t_mv_mt_t_tb = "t_mv_mt_t_tb"
    def t_mv_mt_t_tb_stmt = """select * from ${t_mv_mt_mtmv}"""
    sql """drop table if exists ${t_mv_mt_t_tb}"""
    sql """create table ${t_mv_mt_t_tb} PROPERTIES("replication_num" = "1") as ${t_mv_mt_t_tb_stmt}"""
    def t_mv_mt_t_tb_sql = """select * from ${t_mv_mt_t_tb}"""
    order_qt_nested_mtmv_query_t_mv_mt_t_tb "${t_mv_mt_t_tb_sql}"

    // table -> mv -> mtmv -> mtmv
    def t_mv_mt_mt_mtmv = "t_mv_mt_mt_mtmv"
    def t_mv_mt_mt_mtmv_stmt = """select k1, k2, k3, k4, k5, t1.l_orderkey, l_shipdate, 
            sum(sum_total) as sum_total, 
            max(max_total) as max_total, 
            min(min_total) as min_total, 
            count(count_all) as count_all, 
            bitmap_union(to_bitmap(case when k1 > 1 then k2 else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when k1 > 2 then k2 else null end)) as cnt_2 
            from ${t_mv_mt_mtmv} as t1 
            left join ${table_lineitem} as t2 on t2.l_orderkey = t1.l_orderkey
            group by 
            k1, k2, k3, k4, k5, t1.l_orderkey, l_shipdate
            """
    sql """DROP MATERIALIZED VIEW IF EXISTS ${t_mv_mt_mt_mtmv};"""
    create_async_mv(db, t_mv_mt_mt_mtmv, t_mv_mt_mt_mtmv_stmt)
    sql """analyze table ${t_mv_mt_mt_mtmv} with sync;"""
    def t_mv_mt_mt_mtmv_sql = """select * from ${t_mv_mt_mt_mtmv}"""
    order_qt_nested_mtmv_query_t_mv_mt_mt_mtmv "${t_mv_mt_mt_mtmv_sql}"

    mv_rewrite_success(t_mv_mt_mt_mtmv_stmt, t_mv_mt_mt_mtmv)
    compare_res(t_mv_mt_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8,9,10,11")

    // table -> mv -> mtmv -> view
    def t_mv_mt_v_view = "t_mv_mt_v_view"
    def t_mv_mt_v_view_stmt = """select k1, k2, k3, k4, k5, t1.l_orderkey, l_shipdate, 
            sum(sum_total) as sum_total, 
            max(max_total) as max_total, 
            min(min_total) as min_total, 
            count(count_all) as count_all, 
            bitmap_union(to_bitmap(case when k1 > 1 then k2 else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when k1 > 2 then k2 else null end)) as cnt_2 
            from ${t_mv_mt_mtmv} as t1 
            left join ${table_lineitem} as t2 on t2.l_orderkey = t1.l_orderkey
            group by 
            k1, k2, k3, k4, k5, t1.l_orderkey, l_shipdate"""
    sql """drop view if exists ${t_mv_mt_v_view}"""
    sql """CREATE VIEW ${t_mv_mt_v_view} (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13) as ${t_mv_mt_v_view_stmt}"""
    def t_mv_mt_v_view_sql = """select * from ${t_mv_mt_v_view}"""
    order_qt_nested_mtmv_query_t_mv_mt_v_view "${t_mv_mt_v_view_sql}"

    // table -> mv -> mtmv -> mv
    def t_mv_mt_mv_mv = "t_mv_mt_mv_mv"
    def t_mv_mt_mv_mv_stmt = """select k1, k2, k3, k4, k5, l_orderkey, 
            sum(sum_total) as sum_total, 
            max(max_total) as max_total, 
            min(min_total) as min_total, 
            count(count_all) as count_all, 
            bitmap_union(to_bitmap(case when k1 > 1 then k2 else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when k1 > 2 then k2 else null end)) as cnt_2 
            from ${t_mv_mt_mtmv} 
            group by 
            k1, k2, k3, k4, k5, l_orderkey"""
    sql """drop materialized view if exists ${t_mv_mt_mv_mv} on ${t_mv_mt_mtmv};"""
    createMV(getMVStmt(t_mv_mt_mv_mv, t_mv_mt_mv_mv_stmt))
    sql """analyze table ${t_mv_mt_mtmv} with sync;"""
    def t_mv_mt_mv_mv_sql = """select * from ${t_mv_mt_mtmv} index ${t_mv_mt_mv_mv}"""
    order_qt_nested_mtmv_query_t_mv_mt_mv_mv "${t_mv_mt_mv_mv_sql}"

    mv_rewrite_success(t_mv_mt_mv_mv_stmt, t_mv_mt_mv_mv)
    compare_res(t_mv_mt_mv_mv_stmt + " order by 1,2,3,4,5,6,7,8,9,10")

    // table -> mv -> view
    def t_mv_v_view = "t_mv_v_view"
    def t_mv_v_view_stmt = """select 
            `mv_o_orderkey` as k1, 
            `mva_SUM__``o_totalprice``` as k2, 
            `mva_MAX__``o_totalprice``` as k3, 
            `mva_MIN__``o_totalprice``` as k4, 
            `mva_SUM__CASE WHEN 1 IS NULL THEN 0 ELSE 1 END` as k5, 
            l_orderkey, 
            sum(`mv_o_orderkey`) as sum_total, 
            max(`mv_o_orderkey`) as max_total, 
            min(`mv_o_orderkey`) as min_total, 
            count(`mva_SUM__``o_totalprice```) as count_all, 
            bitmap_union(to_bitmap(case when mv_o_orderkey > 1 then `mva_SUM__``o_totalprice``` else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when mv_o_orderkey > 2 then `mva_MAX__``o_totalprice``` else null end)) as cnt_2 
            from ${table_order} index ${t_mv_mv}
            left join ${table_lineitem} on ${table_lineitem}.l_orderkey = ${table_order}.mv_o_orderkey
            group by 
            k1, k2, k3, k4, k5, l_orderkey, mv_o_orderkey"""
    sql """drop view if exists ${t_mv_v_view}"""
    sql """CREATE VIEW ${t_mv_v_view} (k1, k2, k3, k4, k5, k6, v1, v2, v3, v4, v5, v6) as ${t_mv_v_view_stmt}"""
    def t_mv_v_view_sql = """select * from ${t_mv_v_view}"""
    order_qt_nested_mtmv_query_t_mv_v_view "${t_mv_v_view_sql}"



//    // table -> mv -> view -> mtmv
//    def t_mv_v_mt_mtmv = "t_mv_v_mt_mtmv"
//    sql """DROP MATERIALIZED VIEW IF EXISTS ${t_mv_v_mt_mtmv};"""
//    def t_mv_v_mt_mtmv_stmt = """select
//            `mv_o_orderkey` as k1,
//            `mva_SUM__``o_totalprice``` as k2,
//            `mva_MAX__``o_totalprice``` as k3,
//            `mva_MIN__``o_totalprice``` as k4,
//            `mva_SUM__CASE WHEN 1 IS NULL THEN 0 ELSE 1 END` as k5,
//            l_orderkey,
//            sum(`mv_o_orderkey`) as sum_total,
//            max(`mv_o_orderkey`) as max_total,
//            min(`mv_o_orderkey`) as min_total,
//            count(`mva_SUM__``o_totalprice```) as count_all,
//            bitmap_union(to_bitmap(case when mv_o_orderkey > 1 then `mva_SUM__``o_totalprice``` else null end)) cnt_1,
//            bitmap_union(to_bitmap(case when mv_o_orderkey > 2 then `mva_MAX__``o_totalprice``` else null end)) as cnt_2
//            from ${table_order} index ${t_mv_mv}
//            left join ${table_lineitem} on ${table_lineitem}.l_orderkey = ${table_order}.mv_o_orderkey
//            group by
//            k1, k2, k3, k4, k5, l_orderkey, mv_o_orderkey"""
//    create_async_mv(db, t_mv_v_mt_mtmv, t_mv_v_mt_mtmv_stmt)
//    sql """analyze table ${t_mv_v_mt_mtmv} with sync;"""
//    def t_mv_v_mt_mtmv_sql = "select * from ${t_mv_v_mt_mtmv}"
//    order_qt_nested_mtmv_query_t_mv_v_mt_mtmv "${t_mv_v_mt_mtmv_sql}"
//
//    mv_rewrite_success(t_mv_v_mt_mtmv_stmt, t_mv_v_mt_mtmv)
//    compare_res(t_mv_v_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8,9,10")

    // table -> mv -> mv
    def t_mv_mv_mv = "t_mv_mv_mv"
    def t_mv_mv_mv_stmt = """select
            `mv_o_orderkey` as k1,
            `mva_SUM__``o_totalprice``` as k2,
            `mva_MAX__``o_totalprice``` as k3,
            `mva_MIN__``o_totalprice``` as k4,
            `mva_SUM__CASE WHEN 1 IS NULL THEN 0 ELSE 1 END` as k5,
            sum(`mv_o_orderkey`) as sum_total,
            max(`mv_o_orderkey`) as max_total,
            min(`mv_o_orderkey`) as min_total,
            count(`mva_SUM__``o_totalprice```) as count_all,
            bitmap_union(to_bitmap(case when mv_o_orderkey > 1 then `mva_SUM__``o_totalprice``` else null end)) cnt_1,
            bitmap_union(to_bitmap(case when mv_o_orderkey > 2 then `mva_MAX__``o_totalprice``` else null end)) as cnt_2
            from ${table_order} index ${t_mv_mv}
            group by k1, k2, k3, k4, k5, mv_o_orderkey"""
    sql """drop materialized view if exists ${t_mv_mv_mv} on ${table_order};"""
    try {
        createMV(getMVStmt(t_mv_mv_mv, t_mv_mv_mv_stmt))
    } catch (Exception e) {
        logger.info(e.getMessage())
    }

    def run_sql_hit_mtmv = {
        mv_rewrite_success(t_v_view_stmt, t_mt_mtmv)
        compare_res(t_v_view_stmt + " order by 1,2,3,4,5,6,7,8")
        mv_rewrite_success(t_v_t_mt_mtmv_stmt, t_v_t_mt_mtmv)
        compare_res(t_v_t_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8")
        mv_rewrite_success(t_mt_mt_mtmv_hit, t_mt_mt_mtmv)
        compare_res(t_mt_mt_mtmv_hit + " order by 1,2,3,4,5,6,7,8")
        mv_rewrite_success(t_mt_mt_mt_mtmv_hit, t_mt_mt_mt_mtmv)
        compare_res(t_mt_mt_mt_mtmv_hit + " order by 1,2,3,4,5,6,7,8")
        mv_rewrite_success(t_mt_mt_mv_mv_hit1, t_mt_mt_mtmv)
        compare_res(t_mt_mt_mv_mv_hit1 + " order by 1,2,3,4,5,6,7")
        mv_rewrite_success(t_mt_mv_mv_hit1, t_mt_mtmv)
        compare_res(t_mt_mv_mv_hit1 + " order by 1,2,3,4,5,6,7,8")

        mv_rewrite_success(t_mt_mt_mt_mtmv_stmt, t_mt_mt_mt_mtmv)
        compare_res(t_mt_mt_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8")
        mv_rewrite_success(t_mt_mt_mv_mv_stmt, t_mt_mt_mv_mv)
        compare_res(t_mt_mt_mv_mv_stmt + " order by 1,2,3,4,5,6,7")
        mv_rewrite_success(t_mt_mv_mv_stmt, t_mt_mv_mv)
        compare_res(t_mt_mv_mv_stmt + " order by 1,2,3,4,5,6,7,8")
        mv_rewrite_success(t_mt_mv_mt_mtmv_stmt, t_mt_mv_mt_mtmv)
        compare_res(t_mt_mv_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8,9")
        mv_rewrite_success(t_mv_mt_mtmv_stmt, t_mv_mt_mtmv)
        compare_res(t_mv_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8,9,10")
        mv_rewrite_success(t_mv_mt_mt_mtmv_stmt, t_mv_mt_mt_mtmv)
        compare_res(t_mv_mt_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8,9,10,11")
        mv_rewrite_success(t_mv_mt_mv_mv_stmt, t_mv_mt_mv_mv)
        compare_res(t_mv_mt_mv_mv_stmt + " order by 1,2,3,4,5,6,7,8,9,10")
    }

    def run_sql_hit_mv = {
        mv_rewrite_success(t_mv_mv_stmt, t_mv_mv)
        compare_res(t_mv_mv_stmt + " order by 1,2,3,4,5")
        mv_rewrite_success(t_v_t_mv_mv_stmt, t_v_t_mv_mv)
        compare_res(t_v_t_mv_mv_stmt + " order by 1,2,3,4,5,6,7,8")
        mv_rewrite_success(t_mt_mt_mv_mv_hit2, t_mt_mt_mv_mv)
        compare_res(t_mt_mt_mv_mv_hit2 + " order by 1,2,3,4,5,6,7")
        mv_rewrite_success(t_mt_mv_mv_hit2, t_mt_mv_mv)
        compare_res(t_mt_mv_mv_hit2 + " order by 1,2,3,4,5,6,7,8")
    }

    // insert data into base table
    sql """
    insert into ${table_order} values
    (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-27'),
    (11, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-27'),
    (13, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-29'),
    (11, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
    (12, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-28'),
    (13, 1, 'k', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
    (11, 3, 'o', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
    (12, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
    (13, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (14, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19');
    """

    // All materialized views failed to be rewritten before refreshing
    mv_not_part_in(t_v_view_stmt, t_mt_mtmv)
    mv_rewrite_success(t_v_t_mt_mtmv_stmt, t_v_t_mt_mtmv)
    compare_res(t_v_t_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8")
    mv_rewrite_fail(t_mt_mt_mtmv_hit, t_mt_mt_mtmv)
    mv_rewrite_fail(t_mt_mt_mt_mtmv_hit, t_mt_mt_mt_mtmv)
    mv_rewrite_fail(t_mt_mt_mv_mv_hit1, t_mt_mt_mtmv)
    mv_not_part_in(t_mt_mv_mv_hit1, t_mt_mtmv)

    mv_rewrite_success(t_mv_mv_stmt, t_mv_mv)
    compare_res(t_mv_mv_stmt + " order by 1,2,3,4,5")
    mv_rewrite_success(t_v_t_mv_mv_stmt, t_v_t_mv_mv)
    compare_res(t_v_t_mv_mv_stmt + " order by 1,2,3,4,5,6,7,8")

    mv_rewrite_success(t_mt_mt_mv_mv_hit2, t_mt_mt_mv_mv)
    compare_res(t_mt_mt_mv_mv_hit2 + " order by 1,2,3,4,5,6,7")
    mv_not_part_in(t_mt_mt_mv_mv_hit1, t_mt_mt_mv_mv)
    mv_not_part_in(t_mt_mt_mv_mv_hit1, t_mt_mtmv)

    mv_rewrite_success(t_mt_mv_mv_hit2, t_mt_mv_mv)
    compare_res(t_mt_mv_mv_hit2 + " order by 1,2,3,4,5,6,7,8")
    mv_not_part_in(t_mt_mv_mv_hit1, t_mt_mv_mv)


    // refresh
    order_qt_nested_mtmv_query_t_v_view_insert1 "${t_v_view_sql}"
    refresh_async_mv(db, t_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mtmv_insert1 "${t_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mv_mv_insert1 "${t_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_v_t_tb_insert1 "${t_v_t_tb_sql}"
    order_qt_nested_mtmv_query_t_v_t_mv_mv_insert1 "${t_v_t_mv_mv_sql}"
    refresh_async_mv(db, t_v_t_mt_mtmv)
    order_qt_nested_mtmv_query_t_v_t_mt_mtmv_insert1 "${t_v_t_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_v_t_v_view_insert1 "${t_v_t_v_view_sql}"
    order_qt_nested_mtmv_query_t_v_v_view_insert1 "${t_v_v_view_sql}"
    order_qt_nested_mtmv_query_t_mt_t_tb_insert1 "${t_mt_t_tb_sql}"
    refresh_async_mv(db, t_mt_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mt_mtmv_insert1 "${t_mt_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mt_mt_t_tb_insert1 "${t_mt_mt_t_tb_sql}"
    refresh_async_mv(db, t_mt_mt_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mt_mt_mtmv_insert1 "${t_mt_mt_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mt_mt_v_view_insert1 "${t_mt_mt_v_view_sql}"
    order_qt_nested_mtmv_query_t_mt_mt_mv_mv_insert1 "${t_mt_mt_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_mt_v_view_insert1 "${t_mt_v_view_sql}"
    order_qt_nested_mtmv_query_t_mt_mv_mv_insert1 "${t_mt_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_mt_mv_t_tb_insert1 "${t_mt_mv_t_tb_sql}"
    refresh_async_mv(db, t_mt_mv_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mv_mt_mtmv_insert1 "${t_mt_mv_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mt_mv_v_view_insert1 "${t_mt_mv_v_view_sql}"
    order_qt_nested_mtmv_query_t_mv_t_tb_insert1 "${t_mv_t_tb_sql}"
    refresh_async_mv(db, t_mv_mt_mtmv)
    order_qt_nested_mtmv_query_t_mv_mt_mtmv_insert1 "${t_mv_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mv_mt_t_tb_insert1 "${t_mv_mt_t_tb_sql}"
    refresh_async_mv(db, t_mv_mt_mt_mtmv)
    order_qt_nested_mtmv_query_t_mv_mt_mt_mtmv_insert1 "${t_mv_mt_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mv_mt_v_view_insert1 "${t_mv_mt_v_view_sql}"
    order_qt_nested_mtmv_query_t_mv_mt_mv_mv_insert1 "${t_mv_mt_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_mv_v_view_insert1 "${t_mv_v_view_sql}"


    // After refreshing, all queries can successfully hit the materialized views
    run_sql_hit_mtmv()
    run_sql_hit_mv()


    sql """
    insert into ${table_lineitem} values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-27'),
    (11, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-27'),
    (13, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-29'),
    (11, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (12, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-28'),
    (13, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (11, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    // All materialized views failed to be rewritten before refreshing
    mv_not_part_in(t_v_view_stmt, t_mt_mtmv)
    mv_not_part_in(t_v_t_mt_mtmv_stmt, t_v_t_mt_mtmv)
    mv_not_part_in(t_mt_mt_mtmv_hit, t_mt_mt_mtmv)
    mv_not_part_in(t_mt_mt_mt_mtmv_hit, t_mt_mt_mt_mtmv)
    mv_not_part_in(t_mt_mt_mv_mv_hit1, t_mt_mt_mtmv)
    mv_not_part_in(t_mt_mv_mv_hit1, t_mt_mtmv)

    mv_rewrite_success(t_mv_mv_stmt, t_mv_mv)
    compare_res(t_mv_mv_stmt + " order by 1,2,3,4,5")
    mv_rewrite_success(t_v_t_mv_mv_stmt, t_v_t_mv_mv)
    compare_res(t_v_t_mv_mv_stmt + " order by 1,2,3,4,5,6,7,8")

    mv_rewrite_success(t_mt_mt_mv_mv_hit2, t_mt_mt_mv_mv)
    compare_res(t_mt_mt_mv_mv_hit2 + " order by 1,2,3,4,5,6,7")
    mv_not_part_in(t_mt_mt_mv_mv_hit1, t_mt_mt_mv_mv)
    mv_not_part_in(t_mt_mt_mv_mv_hit1, t_mt_mtmv)

    mv_rewrite_success(t_mt_mv_mv_hit2, t_mt_mv_mv)
    compare_res(t_mt_mv_mv_hit2 + " order by 1,2,3,4,5,6,7,8")
    mv_not_part_in(t_mt_mv_mv_hit1, t_mt_mv_mv)


    // refresh
    order_qt_nested_mtmv_query_t_v_view_insert2 "${t_v_view_sql}"
    refresh_async_mv(db, t_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mtmv_insert2 "${t_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mv_mv_insert2 "${t_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_v_t_tb_insert2 "${t_v_t_tb_sql}"
    order_qt_nested_mtmv_query_t_v_t_mv_mv_insert2 "${t_v_t_mv_mv_sql}"
    refresh_async_mv(db, t_v_t_mt_mtmv)
    order_qt_nested_mtmv_query_t_v_t_mt_mtmv_insert2 "${t_v_t_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_v_t_v_view_insert2 "${t_v_t_v_view_sql}"
    order_qt_nested_mtmv_query_t_v_v_view_insert2 "${t_v_v_view_sql}"
    order_qt_nested_mtmv_query_t_mt_t_tb_insert2 "${t_mt_t_tb_sql}"
    refresh_async_mv(db, t_mt_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mt_mtmv_insert2 "${t_mt_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mt_mt_t_tb_insert2 "${t_mt_mt_t_tb_sql}"
    refresh_async_mv(db, t_mt_mt_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mt_mt_mtmv_insert2 "${t_mt_mt_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mt_mt_v_view_insert2 "${t_mt_mt_v_view_sql}"
    order_qt_nested_mtmv_query_t_mt_mt_mv_mv_insert2 "${t_mt_mt_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_mt_v_view_insert2 "${t_mt_v_view_sql}"
    order_qt_nested_mtmv_query_t_mt_mv_mv_insert2 "${t_mt_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_mt_mv_t_tb_insert2 "${t_mt_mv_t_tb_sql}"
    refresh_async_mv(db, t_mt_mv_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mv_mt_mtmv_insert2 "${t_mt_mv_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mt_mv_v_view_insert2 "${t_mt_mv_v_view_sql}"
    order_qt_nested_mtmv_query_t_mv_t_tb_insert2 "${t_mv_t_tb_sql}"
    refresh_async_mv(db, t_mv_mt_mtmv)
    order_qt_nested_mtmv_query_t_mv_mt_mtmv_insert2 "${t_mv_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mv_mt_t_tb_insert2 "${t_mv_mt_t_tb_sql}"
    refresh_async_mv(db, t_mv_mt_mt_mtmv)
    order_qt_nested_mtmv_query_t_mv_mt_mt_mtmv_insert2 "${t_mv_mt_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mv_mt_v_view_insert2 "${t_mv_mt_v_view_sql}"
    order_qt_nested_mtmv_query_t_mv_mt_mv_mv_insert2 "${t_mv_mt_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_mv_v_view_insert2 "${t_mv_v_view_sql}"

    // After refreshing, all queries can successfully hit the materialized views
    run_sql_hit_mtmv()
    run_sql_hit_mv()

    // schema change on base table
    // drop column
    sql """ALTER TABLE ${db}.${table_order} DROP COLUMN o_clerk;"""
    mv_not_part_in(t_v_view_stmt, t_mt_mtmv)
    mv_rewrite_success(t_v_t_mt_mtmv_stmt, t_v_t_mt_mtmv)
    compare_res(t_v_t_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8")
    mv_not_part_in(t_mt_mt_mtmv_hit, t_mt_mt_mtmv)
    mv_not_part_in(t_mt_mt_mt_mtmv_hit, t_mt_mt_mt_mtmv)
    mv_not_part_in(t_mt_mt_mv_mv_hit1, t_mt_mt_mtmv)
    mv_not_part_in(t_mt_mv_mv_hit1, t_mt_mtmv)

    mv_rewrite_success(t_mv_mv_stmt, t_mv_mv)
    compare_res(t_mv_mv_stmt + " order by 1,2,3,4,5")
    mv_rewrite_success(t_v_t_mv_mv_stmt, t_v_t_mv_mv)
    compare_res(t_v_t_mv_mv_stmt + " order by 1,2,3,4,5,6,7,8")

    mv_rewrite_success(t_mt_mt_mv_mv_hit2, t_mt_mt_mv_mv)
    compare_res(t_mt_mt_mv_mv_hit2 + " order by 1,2,3,4,5,6,7")
    mv_not_part_in(t_mt_mt_mv_mv_hit1, t_mt_mt_mv_mv)
    mv_not_part_in(t_mt_mt_mv_mv_hit1, t_mt_mtmv)

    mv_rewrite_success(t_mt_mv_mv_hit2, t_mt_mv_mv)
    compare_res(t_mt_mv_mv_hit2 + " order by 1,2,3,4,5,6,7,8")
    mv_not_part_in(t_mt_mv_mv_hit1, t_mt_mv_mv)


    // refresh
    order_qt_nested_mtmv_query_t_v_view_sc1 "${t_v_view_sql}"
    refresh_async_mv(db, t_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mtmv_sc1 "${t_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mv_mv_sc1 "${t_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_v_t_tb_sc1 "${t_v_t_tb_sql}"
    order_qt_nested_mtmv_query_t_v_t_mv_mv_sc1 "${t_v_t_mv_mv_sql}"
    refresh_async_mv(db, t_v_t_mt_mtmv)
    order_qt_nested_mtmv_query_t_v_t_mt_mtmv_sc1 "${t_v_t_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_v_t_v_view_sc1 "${t_v_t_v_view_sql}"
    order_qt_nested_mtmv_query_t_v_v_view_sc1 "${t_v_v_view_sql}"
    order_qt_nested_mtmv_query_t_mt_t_tb_sc1 "${t_mt_t_tb_sql}"
    refresh_async_mv(db, t_mt_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mt_mtmv_sc1 "${t_mt_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mt_mt_t_tb_sc1 "${t_mt_mt_t_tb_sql}"
    refresh_async_mv(db, t_mt_mt_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mt_mt_mtmv_sc1 "${t_mt_mt_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mt_mt_v_view_sc1 "${t_mt_mt_v_view_sql}"
    order_qt_nested_mtmv_query_t_mt_mt_mv_mv_sc1 "${t_mt_mt_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_mt_v_view_sc1 "${t_mt_v_view_sql}"
    order_qt_nested_mtmv_query_t_mt_mv_mv_sc1 "${t_mt_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_mt_mv_t_tb_sc1 "${t_mt_mv_t_tb_sql}"
    refresh_async_mv(db, t_mt_mv_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mv_mt_mtmv_sc1 "${t_mt_mv_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mt_mv_v_view_sc1 "${t_mt_mv_v_view_sql}"
    order_qt_nested_mtmv_query_t_mv_t_tb_sc1 "${t_mv_t_tb_sql}"
    refresh_async_mv(db, t_mv_mt_mtmv)
    order_qt_nested_mtmv_query_t_mv_mt_mtmv_sc1 "${t_mv_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mv_mt_t_tb_sc1 "${t_mv_mt_t_tb_sql}"
    refresh_async_mv(db, t_mv_mt_mt_mtmv)
    order_qt_nested_mtmv_query_t_mv_mt_mt_mtmv_sc1 "${t_mv_mt_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mv_mt_v_view_sc1 "${t_mv_mt_v_view_sql}"
    order_qt_nested_mtmv_query_t_mv_mt_mv_mv_sc1 "${t_mv_mt_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_mv_v_view_sc1 "${t_mv_v_view_sql}"

    // After refreshing, all queries can successfully hit the materialized views
    run_sql_hit_mtmv()
    run_sql_hit_mv()


    /*
    // schema change on base table
    sql """ALTER TABLE ${db}.${table_order} ADD COLUMN new_col INT KEY DEFAULT "0";"""
    waitingColumnTaskFinished(db, table_order)
    sql """ALTER TABLE ${db}.${table_lineitem} ADD COLUMN new_col INT KEY DEFAULT "0";"""
    waitingColumnTaskFinished(db, table_lineitem)

    // All materialized views failed to be rewritten before refreshing
    run_sql_hit_mtmv()
    run_sql_hit_mv()



    // refresh
    order_qt_nested_mtmv_query_t_v_view_sc "${t_v_view_sql}"
    refresh_async_mv(db, t_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mtmv_sc "${t_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mv_mv_sc "${t_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_v_t_tb_sc "${t_v_t_tb_sql}"
    order_qt_nested_mtmv_query_t_v_t_mv_mv_sc "${t_v_t_mv_mv_sql}"
    refresh_async_mv(db, t_v_t_mt_mtmv)
    order_qt_nested_mtmv_query_t_v_t_mt_mtmv_sc "${t_v_t_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_v_t_v_view_sc "${t_v_t_v_view_sql}"
    order_qt_nested_mtmv_query_t_v_v_view_sc "${t_v_v_view_sql}"
    order_qt_nested_mtmv_query_t_mt_t_tb_sc "${t_mt_t_tb_sql}"
    refresh_async_mv(db, t_mt_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mt_mtmv_sc "${t_mt_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mt_mt_t_tb_sc "${t_mt_mt_t_tb_sql}"
    refresh_async_mv(db, t_mt_mt_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mt_mt_mtmv_sc "${t_mt_mt_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mt_mt_v_view_sc "${t_mt_mt_v_view_sql}"
    order_qt_nested_mtmv_query_t_mt_mt_mv_mv_sc "${t_mt_mt_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_mt_v_view_sc "${t_mt_v_view_sql}"
    order_qt_nested_mtmv_query_t_mt_mv_mv_sc "${t_mt_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_mt_mv_t_tb_sc "${t_mt_mv_t_tb_sql}"
    refresh_async_mv(db, t_mt_mv_mt_mtmv)
    order_qt_nested_mtmv_query_t_mt_mv_mt_mtmv_sc "${t_mt_mv_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mt_mv_v_view_sc "${t_mt_mv_v_view_sql}"
    order_qt_nested_mtmv_query_t_mv_t_tb_sc "${t_mv_t_tb_sql}"
    refresh_async_mv(db, t_mv_mt_mtmv)
    order_qt_nested_mtmv_query_t_mv_mt_mtmv_sc "${t_mv_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mv_mt_t_tb_sc "${t_mv_mt_t_tb_sql}"
    refresh_async_mv(db, t_mv_mt_mt_mtmv)
    order_qt_nested_mtmv_query_t_mv_mt_mt_mtmv_sc "${t_mv_mt_mt_mtmv_sql}"
    order_qt_nested_mtmv_query_t_mv_mt_v_view_sc "${t_mv_mt_v_view_sql}"
    order_qt_nested_mtmv_query_t_mv_mt_mv_mv_sc "${t_mv_mt_mv_mv_sql}"
    order_qt_nested_mtmv_query_t_mv_v_view_sc "${t_mv_v_view_sql}"

    // After refreshing, all queries can successfully hit the materialized views
    run_sql_hit_mtmv()
    run_sql_hit_mv()
     */



    // table -> view -> table -> mtmv
    // Drop a table at a certain level in the middle
    sql """drop table ${t_v_t_tb}"""
    // Direct query on the MTMV on the table is fine
    order_qt_nested_mtmv_query_t_v_t_mt_mtmv_sc "${t_v_t_mt_mtmv_sql}"
    // But the rewrite fails
    try {
        mv_rewrite_fail(t_v_t_mt_mtmv_stmt, t_v_t_mt_mtmv)
    } catch (Exception e) {
        logger.info(e.getMessage())
    }
    // refresh failed due to table is not exist
    refresh_async_mv_fail(db, t_v_t_mt_mtmv)
    order_qt_nested_mtmv_query_t_v_t_mt_mtmv_sc "${t_v_t_mt_mtmv_sql}"
    // Reconstruct the intermediate table that was just deleted
    sql """create table ${t_v_t_tb} PROPERTIES("replication_num" = "1") as select k1, k2, k3, k4, v1, v2, v3, v4 from ${t_v_view}"""
    order_qt_nested_mtmv_query_t_v_t_mt_mtmv_sc "${t_v_t_mt_mtmv_sql}"
    // refresh success
    refresh_async_mv(db, t_v_t_mt_mtmv)
    // rewriter success
    mv_rewrite_success(t_v_t_mt_mtmv_stmt, t_v_t_mt_mtmv)
    // Check whether the results before and after the hit are consistent
    compare_res(t_v_t_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8")


    // table -> mtmv -> mtmv -> mtmv
    // Delete the mtmv of a certain intermediate layer
    sql """drop materialized view ${t_mt_mt_mtmv}"""
    // The mtmv at its bottom layer is fine
    order_qt_nested_mtmv_query_t_mt_mtmv_sc "${t_mt_mtmv_sql}"
    mv_rewrite_success(t_v_view_stmt, t_mt_mtmv)
    compare_res(t_v_view_stmt + " order by 1,2,3,4,5,6,7,8")

    // The mtmv at its upper layer cannot be rewritten normally
    order_qt_nested_mtmv_query_t_mt_mt_mt_mtmv_sc "${t_mt_mt_mt_mtmv_sql}"
    mv_not_part_in(t_mt_mt_mt_mtmv_hit, t_mt_mt_mt_mtmv)

    // Reconstruct the deleted mtmv
    create_async_mv(db, t_mt_mt_mtmv, t_mt_mt_mtmv_stmt)
    order_qt_nested_mtmv_query_t_mt_mtmv_sc "${t_mt_mtmv_sql}"
    mv_rewrite_success(t_v_view_stmt, t_mt_mtmv)
    compare_res(t_v_view_stmt + " order by 1,2,3,4,5,6,7,8")

    // The top-level mtmv refresh and hit are normal
    refresh_async_mv(db, t_mt_mt_mt_mtmv)
    mv_rewrite_success(t_mt_mt_mt_mtmv_hit, t_mt_mt_mt_mtmv)
    compare_res(t_mt_mt_mt_mtmv_hit + " order by 1,2,3,4,5,6,7,8")

    // delete base table, the top-level mtmv will not be affected
    sql """drop table ${table_order}"""
    order_qt_nested_mtmv_query_t_v_t_mt_mtmv_sc "${t_v_t_mt_mtmv_sql}"
    mv_rewrite_success(t_v_t_mt_mtmv_stmt, t_v_t_mt_mtmv)
    compare_res(t_v_t_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8")
    refresh_async_mv(db, t_v_t_mt_mtmv)
    order_qt_nested_mtmv_query_t_v_t_mt_mtmv_sc "${t_v_t_mt_mtmv_sql}"
    mv_rewrite_success(t_v_t_mt_mtmv_stmt, t_v_t_mt_mtmv)
    compare_res(t_v_t_mt_mtmv_stmt + " order by 1,2,3,4,5,6,7,8")


}
