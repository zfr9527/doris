suite ("many_union_mtmv_rewrite") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"
//    sql "SET enable_materialized_view_nest_rewrite = true"

    sql """
    drop table if exists orders_1
    """

    sql """CREATE TABLE `orders_1` (
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
    auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_1
    """

    sql """CREATE TABLE `lineitem_1` (
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
    auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
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
    ) ENGINE=OLAP
    DUPLICATE KEY(`ps_partkey`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
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
    (1, 1, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, 3, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 1, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 3, 1, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 1, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 2, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 3, 3, 3, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 1, 1, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql"""
    insert into partsupp_1 values 
    (1, 1, 1, 99.5, 'yy'),
    (2, 2, 2, 109.2, 'mm'),
    (3, 3, 1, 99.5, 'yy'),
    (3, null, 1, 99.5, 'yy'); 
    """

    sql """analyze table orders_1 with sync;"""
    sql """analyze table lineitem_1 with sync;"""
    sql """analyze table partsupp_1 with sync;"""

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


    def mv_stmt_1 = """
        select l_orderkey, l_linenumber, l_partkey, o_orderkey, o_custkey, ps_partkey, cast(sum(IFNULL(ps_suppkey, 0) * IFNULL(ps_partkey, 0)) as decimal(28, 8)) as col1 
        from lineitem_1 
        inner join orders_1 on lineitem_1.l_orderkey = orders_1.o_orderkey
        inner join partsupp_1 on lineitem_1.l_partkey = partsupp_1.ps_partkey and lineitem_1.l_suppkey = partsupp_1.ps_suppkey
        where lineitem_1.l_shipdate >= "2023-10-17" 
        group by l_orderkey, l_linenumber, l_partkey, o_orderkey, o_custkey, ps_partkey
        """
    def mv_level1_name = "mv_level1_name"
    def mv_stmt_2 = """
        select l_orderkey, l_linenumber, l_partkey, o_orderkey, o_custkey, ps_partkey, col1
        from ${mv_level1_name}
        """
    def mv_level2_name = "mv_level2_name"
    def mv_stmt_3 = """
        select t1.l_orderkey, t2.l_linenumber, t1.l_partkey, t2.o_orderkey, t1.o_custkey, t2.ps_partkey, t1.col1
        from ${mv_level1_name} as t1
        left join ${mv_level1_name} as t2 
        on t1.l_orderkey = t2.l_orderkey
        """
    def mv_level3_name = "mv_level3_name"
    def mv_stmt_4 = """
        select t1.l_orderkey, t2.l_linenumber, t1.l_partkey, t2.o_orderkey, t1.o_custkey, t2.ps_partkey, t1.col1
        from ${mv_level2_name} as t1
        left join ${mv_level2_name} as t2 
        on t1.l_orderkey = t2.l_orderkey
        """
    def mv_level4_name = "mv_level4_name"
    def mv_stmt_5 = """
        select l_orderkey, l_linenumber, l_partkey, o_orderkey, o_custkey, ps_partkey, col1
        from ${mv_level4_name}
        union 
        select l_orderkey, l_linenumber, l_partkey, o_orderkey, o_custkey, ps_partkey, col1
        from ${mv_level4_name} 
        """
    def mv_level5_name = "mv_level5_name"

    create_mv(mv_level1_name, mv_stmt_1)
    def job_name_1 = getJobName(db, mv_level1_name)
    waitingMTMVTaskFinished(job_name_1)

    create_mv(mv_level2_name, mv_stmt_2)
    job_name_1 = getJobName(db, mv_level2_name)
    waitingMTMVTaskFinished(job_name_1)

    create_mv(mv_level3_name, mv_stmt_3)
    job_name_1 = getJobName(db, mv_level3_name)
    waitingMTMVTaskFinished(job_name_1)

    create_mv(mv_level4_name, mv_stmt_4)
    job_name_1 = getJobName(db, mv_level4_name)
    waitingMTMVTaskFinished(job_name_1)

    create_mv(mv_level5_name, mv_stmt_5)
    job_name_1 = getJobName(db, mv_level5_name)
    waitingMTMVTaskFinished(job_name_1)

}
