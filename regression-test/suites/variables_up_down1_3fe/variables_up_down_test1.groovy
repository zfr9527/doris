suite("variables_up_down_test1_3fe") {

    // 旧版本需要执行一遍
    // 新版本再执行一遍
//    multi_sql """
//        set enable_decimal256=true;
//        drop function if exists multiply_plus_1(decimalv3(20,5), decimalv3(20,6));
//        CREATE ALIAS FUNCTION multiply_plus_1(decimalv3(20,5), decimalv3(20,6)) WITH PARAMETER(a,b) AS add(multiply(a,b),1);
//        set enable_decimal256=false;
//    """
    // 预期为256精度计算的结果：999999999999998246906000000001.76833464320
    // 打开/关闭256结果应该一样才对,下同
    sql "set enable_decimal256=false;"
    order_qt_multiply_add "select multiply_plus_1(f1,f2) from test_decimal_mul_overflow1;"
    sql "set enable_decimal256=true;"
    order_qt_multiply_add "select multiply_plus_1(f1,f2) from test_decimal_mul_overflow1;"

//    multi_sql """
//        set enable_decimal256=true;
//        drop function if exists func_add(decimalv3(38,9), decimalv3(38,10));
//        CREATE ALIAS FUNCTION func_add(decimalv3(38,9), decimalv3(38,10)) WITH PARAMETER(a,b) AS add(a,b);
//        set enable_decimal256=false;
//    """
    sql "set enable_decimal256=false;"
    order_qt_add "select func_add(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_add "select func_add(a,b) from t_decimalv3;"

//    multi_sql """
//        set enable_decimal256=true;
//        drop function if exists func_subtract(decimalv3(38,9), decimalv3(38,10));
//        CREATE ALIAS FUNCTION func_subtract(decimalv3(38,9), decimalv3(38,10)) WITH PARAMETER(a,b) AS subtract(a,b);
//        set enable_decimal256=false;
//    """
    sql "set enable_decimal256=false;"
    order_qt_subtract "select func_subtract(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_subtract "select func_subtract(a,b) from t_decimalv3;"

//    multi_sql """
//        set enable_decimal256=true;
//        drop function if exists func_divide(decimalv3(38,18), decimalv3(38,18));
//        CREATE ALIAS FUNCTION func_divide(decimalv3(38,18), decimalv3(38,18)) WITH PARAMETER(a,b) AS divide(a,b);
//        set enable_decimal256=false;
//    """
    sql "set enable_decimal256=false;"
    order_qt_divide "select func_divide(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_divide "select func_divide(a,b) from t_decimalv3;"

//    multi_sql """
//        set enable_decimal256=true;
//        drop function if exists func_mod(decimalv3(38,9), decimalv3(38,10));
//        CREATE ALIAS FUNCTION func_mod(decimalv3(38,9), decimalv3(38,10)) WITH PARAMETER(a,b) AS mod(a,b);
//        set enable_decimal256=false;
//    """
    sql "set enable_decimal256=false;"
    order_qt_mod "select func_mod(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_mod "select func_mod(a,b) from t_decimalv3;"

//    multi_sql """
//        set enable_decimal256=true;
//        drop function if exists func_nested(decimalv3(20,5), decimalv3(21,6));
//        CREATE ALIAS FUNCTION func_nested(decimalv3(20,5), decimalv3(21,6)) WITH PARAMETER(a,b) AS add(multiply(a,b),multiply(a,2));
//        set enable_decimal256=false;
//    """
    sql "set enable_decimal256=false;"
    order_qt_nested "select func_nested(f1,f2) from test_decimal_mul_overflow1;"
    sql "set enable_decimal256=true;"
    order_qt_nested "select func_nested(f1,f2) from test_decimal_mul_overflow1;"

}
