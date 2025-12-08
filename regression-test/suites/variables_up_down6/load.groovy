suite("variables_up_down_load6") {

    sql "set enable_agg_state=true"
    sql "set enable_decimal256=false;"
    sql """ DROP TABLE IF EXISTS t01; """
    sql """
        create table t01(id int, col_sum agg_state<sum(decimalv3(20,6))> generic, col_avg agg_state<avg(decimalv3(20,6))> generic)  properties ("replication_num" = "1");
        """

    sql """insert into t01 values (1, sum_state(10.1), avg_state(10.1)), (1, sum_state(20.1), avg_state(20.1)), (2, sum_state(10.2), avg_state(10.2)), (2, sum_state(11.0), avg_state(11.0));
"""

    order_qt_sum0_master_sql """ select sum_merge(col_sum) from t01 group by id order by id;
             """
    order_qt_avg0_master_sql """ select avg_merge(col_avg) from t01 group by id order by id;
             """
    /*
    // TODO: need to fix
    sql "set enable_decimal256=true;"
    qt_sum1 """ select sum_merge(col_sum) from t01 group by id order by id;
             """
    qt_avg1 """ select avg_merge(col_avg) from t01 group by id order by id;
             """
    */

//    sql "set enable_decimal256=true;"
//    sql """ DROP TABLE IF EXISTS t01; """
//    sql """
//        create table t01(id int, col_sum agg_state<sum(decimalv3(76,6))> generic, col_avg agg_state<avg(decimalv3(76,6))> generic)  properties ("replication_num" = "1");
//        """
//    sql """
//    insert into t01 values (1, sum_state(10.1), avg_state(10.1)), (1, sum_state(20.1), avg_state(20.1)), (2, sum_state(10.2), avg_state(10.2)), (2, sum_state(11.0), avg_state(11.0));
//    """
//    sql "set enable_decimal256=false;"
//    qt_sum_256_0 """
//    select sum_merge(col_sum) from t01 group by id order by id;
//    """
//    qt_avg_256_0 """ select avg_merge(col_avg) from t01 group by id order by id;
//             """
//    sql "set enable_decimal256=true;"
//    qt_sum_256_256 """
//    select sum_merge(col_sum) from t01 group by id order by id;
//    """
//    qt_avg_256_256 """ select avg_merge(col_avg) from t01 group by id order by id;
//             """

    /*
    // TODO: need to fix
    sql "set enable_decimal256=false;"
    qt_sum_256_1 """
    select sum_merge(col_sum) from t01 group by id order by id;
    """
    qt_avg_256_1 """ select avg_merge(col_avg) from t01 group by id order by id;
             """
    */
}
