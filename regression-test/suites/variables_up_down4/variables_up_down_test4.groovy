suite("variables_up_down_test4") {

    // 旧版本需要执行一遍
    // 新版本再执行一遍
//    multi_sql """
//    drop table if exists test_decimal_mul_overflow_for_mv;
//    CREATE TABLE `test_decimal_mul_overflow_for_mv` (
//        `f1` decimal(20,5) NULL,
//        `f2` decimal(21,6) NULL
//    )DISTRIBUTED BY HASH(f1)
//    PROPERTIES("replication_num" = "1");
//    insert into test_decimal_mul_overflow_for_mv values(999999999999999.12345,999999999999999.123456);"""

    def query_sql = """select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_mv;"""

    //打开256创建物化视图
//    multi_sql """
//    set enable_decimal256=true;
//    drop materialized view  if exists mv_var_1;
//    create materialized view mv_var_1
//    BUILD IMMEDIATE
//    REFRESH COMPLETE
//    ON MANUAL
//    PROPERTIES ('replication_num' = '1')
//    as select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_mv;"""
//    // 关闭256进行刷新
//    sql """set enable_decimal256=false;
//    insert into test_decimal_mul_overflow_for_mv values(1.12345,1.234567);"""

    def db = context.config.getDbNameByFile(context.file)
    def job_name = getJobName(db, "mv_var_1");
    waitingMTMVTaskFinished(job_name)
    sql """sync;"""

    // 预期multi_col的scale是11
    order_qt_refresh_master_sql "select f1,f2,multi_col from mv_var_1 order by 1,2,3;"

    // 测试改写
    sql "set enable_decimal256=true;"
    explain {
        sql query_sql
        contains "mv_var_1 chose"
    }
    order_qt_rewite_open256_master_sql "$query_sql"

    sql "set enable_decimal256=false;"
    explain {
        sql query_sql
        contains "mv_var_1 chose"
    }
    order_qt_rewite_open128_master_sql "$query_sql"

    // 测试直查
    sql "set enable_decimal256=true;"
    order_qt_directe_sql256_master_sql """select * from mv_var_1"""
    sql "set enable_decimal256=false;"
    order_qt_directe_sql128_master_sql """select * from mv_var_1"""

    // 测试刷新
    sql "set enable_decimal256=true;"
    sql """refresh materialized view mv_var_1 complete"""
    waitingMTMVTaskFinishedByMvName("mv_var_1")
    sql "set enable_decimal256=false;"
    sql """refresh materialized view mv_var_1 complete"""
    waitingMTMVTaskFinishedByMvName("mv_var_1")

}
