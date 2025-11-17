suite("variables_up_down_load") {

    multi_sql """
    drop table if exists test_decimal_mul_overflow1;
    CREATE TABLE `test_decimal_mul_overflow1` (
        `f1` decimal(20,5) NULL,
        `f2` decimal(21,6) NULL
    )DISTRIBUTED BY HASH(f1)
    PROPERTIES("replication_num" = "1");
    insert into test_decimal_mul_overflow1 values(999999999999999.12345,999999999999999.123456);"""

    // 打开enable_decimal256建表
    multi_sql """
        set enable_decimal256=true;
        drop function if exists multiply_plus_1(decimalv3(20,5), decimalv3(20,6));
        CREATE ALIAS FUNCTION multiply_plus_1(decimalv3(20,5), decimalv3(20,6)) WITH PARAMETER(a,b) AS add(multiply(a,b),1);
        set enable_decimal256=false;
    """

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_add(decimalv3(38,9), decimalv3(38,10));
        CREATE ALIAS FUNCTION func_add(decimalv3(38,9), decimalv3(38,10)) WITH PARAMETER(a,b) AS add(a,b);
        set enable_decimal256=false;
    """

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_subtract(decimalv3(38,9), decimalv3(38,10));
        CREATE ALIAS FUNCTION func_subtract(decimalv3(38,9), decimalv3(38,10)) WITH PARAMETER(a,b) AS subtract(a,b);
        set enable_decimal256=false;
    """

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_divide(decimalv3(38,18), decimalv3(38,18));
        CREATE ALIAS FUNCTION func_divide(decimalv3(38,18), decimalv3(38,18)) WITH PARAMETER(a,b) AS divide(a,b);
        set enable_decimal256=false;
    """

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_mod(decimalv3(38,9), decimalv3(38,10));
        CREATE ALIAS FUNCTION func_mod(decimalv3(38,9), decimalv3(38,10)) WITH PARAMETER(a,b) AS mod(a,b);
        set enable_decimal256=false;
    """

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_nested(decimalv3(20,5), decimalv3(21,6));
        CREATE ALIAS FUNCTION func_nested(decimalv3(20,5), decimalv3(21,6)) WITH PARAMETER(a,b) AS add(multiply(a,b),multiply(a,2));
        set enable_decimal256=false;
    """
}
