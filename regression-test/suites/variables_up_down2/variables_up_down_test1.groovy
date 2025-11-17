suite("variables_up_down_test1") {

    sql "set enable_decimal256=false;"
    qt_sum1 "select * from v_test_array_sum order by 1,2,3,4,5,6, 7;"
    qt_avg1 "select * from v_test_array_avg order by 1,2,3,4,5,6, 7;"
    qt_product1 "select * from v_test_array_product order by 1,2,3,4,5,6, 7;"
    qt_cum_sum1 "select *, array_cum_sum(a_int), array_cum_sum(a_float), array_cum_sum(a_double), array_cum_sum(a_dec_v3_64), array_cum_sum(a_dec_v3_128), array_cum_sum(a_dec_v3_256) from test_array_agg_view order by 1,2,3,4,5,6, 7;"
    qt_cum_sum_view1 "select * from v_test_array_cum_sum order by 1,2,3,4,5,6, 7;"

    sql "set enable_decimal256=true;"
    qt_sum2 "select * from v_test_array_sum order by 1,2,3,4,5,6, 7;"
    qt_avg2 "select * from v_test_array_avg order by 1,2,3,4,5,6, 7;"
    qt_product2 "select * from v_test_array_product order by 1,2,3,4,5,6, 7;"
    qt_cum_sum2 "select *, array_cum_sum(a_int), array_cum_sum(a_float), array_cum_sum(a_double), array_cum_sum(a_dec_v3_64), array_cum_sum(a_dec_v3_128), array_cum_sum(a_dec_v3_256) from test_array_agg_view order by 1,2,3,4,5,6, 7;"
    qt_cum_sum_view2 "select * from v_test_array_cum_sum order by 1,2,3,4,5,6, 7;"


}
