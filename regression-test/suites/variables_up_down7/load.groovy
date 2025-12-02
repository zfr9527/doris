suite("variables_up_down_load7") {

    // =============case1: history data to mv===================
    multi_sql """
    drop table if exists test_decimal_mul_overflow_for_sync_mv;
    CREATE TABLE `test_decimal_mul_overflow_for_sync_mv` (
        `f1` decimal(20,5) NULL,
        `f2` decimal(21,6) NULL
    )DISTRIBUTED BY HASH(f1)
    PROPERTIES("replication_num" = "1");
    insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"""

    sql """
    set enable_decimal256=true;
    drop materialized view if exists mv_var_sync_1 on test_decimal_mul_overflow_for_sync_mv;
    """
    createMV("""create materialized view mv_var_sync_1
            as select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;""")

    // test refresh
    sql "set enable_decimal256=true;"
    // expect scale is 11
    order_qt_history_data_mv_master_sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
    // test rewrite
    sql "set enable_decimal256=true;"
//    explain {
//        sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
//        contains "mv_var_sync_1 chose"
//    }
    mv_rewrite_success_without_check_chosen("select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;", "mv_var_sync_1")
    sql "set enable_decimal256=false;"
//    explain {
//        sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
//        contains "mv_var_sync_1 not chose"
//    }

    // new insert data refresh
    sql "insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"
    sql "set enable_decimal256=true;"
    // expect scale is 11
    order_qt_insert_refresh_mv_master_sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"

    // =============case2: new insert data add to mv and mv where condition also need guard===================
    multi_sql """
    drop table if exists test_decimal_mul_overflow_for_sync_mv;
    CREATE TABLE `test_decimal_mul_overflow_for_sync_mv` (
    `f1` decimal(20,5) NULL,
    `f2` decimal(21,6) NULL
    )DISTRIBUTED BY HASH(f1)
    PROPERTIES("replication_num" = "1");
    insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);
    """

    sql """set enable_decimal256=true;
    drop materialized view if exists mv_var_sync_1 on test_decimal_mul_overflow_for_sync_mv;"""
    createMV("""create materialized view mv_var_sync_1
            as select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv
            where f1*f2==999999999999998246906000000000.76833464320;""")
    // turn off 256 and insert，expect can insert this row
    // because if not guard where, this row will not be inserted into mv
    // because in 128 mode f1*f2==999999999999998246906000000000.76833464320 return false
    // in 256 mode f1*f2==999999999999998246906000000000.76833464320 return true
    sql "set enable_decimal256=false;"
    sql "insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"

    // expect 2 rows
    sql "set enable_decimal256=true;"
    order_qt_where_mv_master_sql "select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv where f1*f2==999999999999998246906000000000.76833464320;"
//    explain {
//        sql "select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv where f1*f2==999999999999998246906000000000.76833464320;"
//        contains "mv_var_sync_1 fail"
//    }
    mv_rewrite_fail("select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv where f1*f2==999999999999998246906000000000.76833464320;", "mv_var_sync_1")

    // ===================case3: create mv with 128 mode,test history refresh mv and insert into refresh mv=====================
    sql """drop table if exists test_decimal_mul_overflow_for_sync_mv;
    CREATE TABLE `test_decimal_mul_overflow_for_sync_mv` (
    `f1` decimal(20,5) NULL,
    `f2` decimal(21,6) NULL
    )DISTRIBUTED BY HASH(f1)
    PROPERTIES("replication_num" = "1");
    insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"""

    sql "set enable_decimal256=false;"
    sql "drop materialized view if exists mv_var_sync_1 on test_decimal_mul_overflow_for_sync_mv;"

    createMV("""create materialized view mv_var_sync_1
            as select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;""")
    sql "set enable_decimal256=true;"
    sql "insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"

    sql "set enable_decimal256=false;"
    order_qt_expect_8_scale_master_sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
//    explain {
//        sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
//        contains "mv_var_sync_1 chose"
//    }
    mv_rewrite_success_without_check_chosen("select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;", "mv_var_sync_1")
    sql "set enable_decimal256=true;"
//    explain {
//        sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
//        contains "mv_var_sync_1 chose"
//    }
    mv_rewrite_success_without_check_chosen("select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;", "mv_var_sync_1")
}
