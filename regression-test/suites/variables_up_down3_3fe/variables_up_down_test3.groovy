suite("variables_up_down_test3_3fe") {

    // 旧版本需要执行一遍
    // 新版本再执行一遍
    // ========== 测试1: 乘法运算 ==========
    // 打开enable_decimal256建表
//    multi_sql """
//        set enable_decimal256=true;
//        drop table if exists t_gen_col_multi_decimalv3;
//        create table t_gen_col_multi_decimalv3(a decimal(20,5),b decimal(21,6),c decimal(38,11) generated always as (a*b) not null)
//        DISTRIBUTED BY HASH(a)
//        PROPERTIES("replication_num" = "1");
//    """
    // 关闭enable_decimal256，插入数据
    sql "set enable_decimal256=false;"
//    sql "insert into t_gen_col_multi_decimalv3 values(1.12343,1.123457,default);"

    // 查询数据,预期column c的scale为11
    order_qt_c_scale_is_128 "select * from t_gen_col_multi_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_c_scale_is_256 "select * from t_gen_col_multi_decimalv3;"

    // ========== 测试4: 除法运算 ==========
//    multi_sql """
//        set enable_decimal256=true;
//        drop table if exists t_gen_col_divide_decimalv3;
//        create table t_gen_col_divide_decimalv3(a decimal(38,18),b decimal(38,18),c decimal(38,18) generated always as (a/b) not null)
//        DISTRIBUTED BY HASH(a)
//        PROPERTIES("replication_num" = "1");
//    """
//    sql "set enable_decimal256=false;"
//    sql "insert into t_gen_col_divide_decimalv3 values(100.123456789012345678,2.123456789012345678,default);"
    sql "set enable_decimal256=false;"
    order_qt_divide_scale "select * from t_gen_col_divide_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_divide_scale_256 "select * from t_gen_col_divide_decimalv3;"

    // ========== 测试2: 加法运算 ==========
//    multi_sql """
//        set enable_decimal256=true;
//        drop table if exists t_gen_col_add_sub_mod_decimalv3;
//        create table t_gen_col_add_sub_mod_decimalv3(a decimal(38,9),b decimal(38,10),c decimal(38,10) generated always as (a+b) not null, d decimal(38,10) generated always as (a-b) not null,
//        f decimal(38,10) generated always as (mod(b,a)) not null)
//        DISTRIBUTED BY HASH(a)
//        PROPERTIES("replication_num" = "1");
//    """
//    sql "set enable_decimal256=false;"
//    sql "insert into t_gen_col_add_sub_mod_decimalv3 values(1.012345678,1.0123456789,default,default,default);"
    sql "set enable_decimal256=false;"
    order_qt_add_sub_mod1 "select * from t_gen_col_add_sub_mod_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_add_sub_mod2 "select * from t_gen_col_add_sub_mod_decimalv3;"


    // ========== 测试7: 嵌套生成列（生成列引用其他生成列） ==========
//    multi_sql """
//        set enable_decimal256=true;
//        drop table if exists t_gen_col_nested;
//        create table t_gen_col_nested(
//            a decimal(20,5),
//            b decimal(21,6),
//            c decimal(38,11) generated always as (a*b) not null,
//            d decimal(38,11) generated always as (c+1) not null
//        )
//        DISTRIBUTED BY HASH(a)
//        PROPERTIES("replication_num" = "1");
//    """
//    sql "set enable_decimal256=false;"
//    sql "insert into t_gen_col_nested values(1.12343,1.123457,default,default);"
    sql "set enable_decimal256=false;"
    order_qt_nested_cols1 "select * from t_gen_col_nested;"
    sql "set enable_decimal256=true;"
    order_qt_nested_cols2 "select * from t_gen_col_nested;"

    // ========== 测试8: 复杂表达式组合 ==========
//    multi_sql """
//        set enable_decimal256=true;
//        drop table if exists t_gen_col_complex;
//        create table t_gen_col_complex(
//            a decimal(20,5),
//            b decimal(21,6),
//            c decimal(38,11) generated always as (a*b) not null,
//            d decimal(38,11) generated always as (a*b+a) not null,
//            e decimal(38,11) generated always as (a*b-b) not null
//        )
//        DISTRIBUTED BY HASH(a)
//        PROPERTIES("replication_num" = "1");
//    """
//    sql "set enable_decimal256=false;"
//    sql "insert into t_gen_col_complex values(1.12343,1.123457,default,default,default);"
    sql "set enable_decimal256=false;"
    order_qt_complex_expr1 "select * from t_gen_col_complex;"
    sql "set enable_decimal256=true;"
    order_qt_complex_expr2 "select * from t_gen_col_complex;"

    // ========== 测试18: 生成列在CASE WHEN中使用 ==========
//    multi_sql """
//        set enable_decimal256=true;
//        drop table if exists t_gen_col_case;
//        create table t_gen_col_case(
//            a decimal(20,5),
//            b decimal(21,6),
//            c decimal(38,11) generated always as (a*b) not null,
//            d decimal(38,11) generated always as (case when a > 1 then a*b else a end) not null
//        )
//        DISTRIBUTED BY HASH(a)
//        PROPERTIES("replication_num" = "1");
//    """
//    sql "set enable_decimal256=false;"
//    sql "insert into t_gen_col_case values(1.12343,1.123457,default,default);"
    sql "set enable_decimal256=false;"
    order_qt_gen_col_case1 "select * from t_gen_col_case;"
    sql "set enable_decimal256=true;"
    order_qt_gen_col_case2 "select * from t_gen_col_case;"

    // ========== 测试19: 生成列在IF函数中使用 ==========
//    multi_sql """
//        set enable_decimal256=true;
//        drop table if exists t_gen_col_if;
//        create table t_gen_col_if(
//            a decimal(20,5),
//            b decimal(21,6),
//            c decimal(38,11) generated always as (if(a > 1, a*b, a)) not null
//        )
//        DISTRIBUTED BY HASH(a)
//        PROPERTIES("replication_num" = "1");
//    """
//    sql "set enable_decimal256=false;"
//    sql "insert into t_gen_col_if values(1.12343,1.123457,default);"
    sql "set enable_decimal256=false;"
    order_qt_gen_col_if1 "select * from t_gen_col_if;"
    sql "set enable_decimal256=true;"
    order_qt_gen_col_if2 "select * from t_gen_col_if;"

    // ========== 测试20: 生成列在COALESCE/GREATEST/LEAST等函数中使用 ==========
//    multi_sql """
//        set enable_decimal256=true;
//        drop table if exists t_gen_col_funcs;
//        create table t_gen_col_funcs(
//            a decimal(20,5),
//            b decimal(21,6),
//            c decimal(38,11) generated always as (a*b) not null,
//            d decimal(38,11) generated always as (greatest(a*b, a)) not null,
//            e decimal(38,11) generated always as (least(a*b, b)) not null
//        )
//        DISTRIBUTED BY HASH(a)
//        PROPERTIES("replication_num" = "1");
//    """
//    sql "set enable_decimal256=false;"
//    sql "insert into t_gen_col_funcs values(1.12343,1.123457,default,default,default);"
    sql "set enable_decimal256=false;"
    order_qt_gen_col_funcs1 "select * from t_gen_col_funcs;"
    sql "set enable_decimal256=true;"
    order_qt_gen_col_funcs2 "select * from t_gen_col_funcs;"

}
