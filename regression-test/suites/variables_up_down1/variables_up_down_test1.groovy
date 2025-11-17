suite("variables_up_down_test1") {

    // 旧版本需要执行一遍
    // 新版本再执行一遍
    // 预期为256精度计算的结果为：999999999999998246906000000001.76833464320
    sql "set enable_decimal256=false;"
    qt_multiply_add "select multiply_plus_1(f1,f2) from test_decimal_mul_overflow1;"
    qt_add "select func_add(a,b) from t_decimalv3;"
    qt_subtract "select func_subtract(a,b) from t_decimalv3;"
    qt_divide "select func_divide(a,b) from t_decimalv3;"
    qt_mod "select func_mod(a,b) from t_decimalv3;"
    qt_nested "select func_nested(f1,f2) from test_decimal_mul_overflow1;"

    sql "set enable_decimal256=true;"
    qt_multiply_add "select multiply_plus_1(f1,f2) from test_decimal_mul_overflow1;"
    qt_add "select func_add(a,b) from t_decimalv3;"
    qt_subtract "select func_subtract(a,b) from t_decimalv3;"
    qt_divide "select func_divide(a,b) from t_decimalv3;"
    qt_mod "select func_mod(a,b) from t_decimalv3;"
    qt_nested "select func_nested(f1,f2) from test_decimal_mul_overflow1;"

}
