suite("agg_negative_mv_test") {

    String db = context.config.getDbNameByFile(context.file)
    def prefix_str = "mv_agg_negative"
    def tb_name = prefix_str + "_tb"

    sql """set enable_agg_state=true;"""
    sql """drop table if exists ${tb_name};"""
    sql """
        CREATE TABLE `${tb_name}` (
        `col1` datetime NULL,
        `col2` varchar(60) NULL,
        `col3` int(11) NOT NULL,
        `col4` boolean NULL,
        `col15` ipv4 NULL,
        `col5` string REPLACE NULL,
        `col6` ARRAY<int(11)> REPLACE  NULL COMMENT "",
        `col7` int(11) SUM NULL DEFAULT "0",
        `col8` int(11) min NULL DEFAULT "0",
        `col9` int(11) max NULL DEFAULT "0",
        `col10` int(11) REPLACE NULL,
        `col11` bitmap BITMAP_UNION,
        `col12` agg_state<max_by(int not null,int)> generic,
        `col13` hll hll_union NOT NULL COMMENT "hll",
        `col14` ipv4 REPLACE NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`col1`, `col2`, `col3`, `col4`, col15)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`col2`, `col3`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """insert into ${tb_name} values 
            ("2023-08-16 22:28:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), max_by_state(3,1), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax1",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), max_by_state(3,1), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",2,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), max_by_state(3,1), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,0,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), max_by_state(3,1), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd2",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), max_by_state(3,1), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[5,4,3,2,1], 1, 1, 1, 1, to_bitmap(243), max_by_state(3,1), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 3, 1, 1, 1, to_bitmap(243), max_by_state(3,1), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 4, 1, 1, to_bitmap(243), max_by_state(3,1), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 5, 6, to_bitmap(243), max_by_state(3,1), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(2), max_by_state(3,1), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), max_by_state(30,100), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), max_by_state(3,1), HLL_HASH(100), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), max_by_state(3,1), HLL_HASH(1), "'255.255.255.255'"),
            ("2023-08-16 24:27:00","ax1",2,0,"'0.0.0.0'","asd",[5,4,3,2,1], 3, 4, 5, 6, to_bitmap(2), max_by_state(30,100), HLL_HASH(100), "'255.255.255.255'"),
            ("2024-08-17 22:27:00","ax2",3,1,"'0.0.0.0'","asd3",[1,2,3,4,6], 7, 8, 9, 10, to_bitmap(3), max_by_state(6,2), HLL_HASH(1000), "'0.0.1.0'"),
            ("2023-09-16 22:27:00","ax4",4,0,"'0.0.0.0'","asd2",[1,2,9,4,5], 11, 11, 11, 11, to_bitmap(4), max_by_state(3,1), HLL_HASH(1), "'0.10.0.0'");"""

    def mv_name = """${prefix_str}_mv"""
    def no_mv_name = """no_${prefix_str}_mv"""
    sql """create materialized view ${mv_name} as select col1, col2, col3, sum(col7) from ${tb_name} group by col3, col1, col2, order by col1, col2, col3"""
    // 验证col1,col2,col3是key列，sum col7不是key列
    // 这里可以搞一个复杂类型，看看能不能成为key列



    test {
        sql """create materialized view ${no_mv_name} as select col1, col2, col3, sum(col7) from ${tb_name} group by col3 having col3 > 1"""
        exception "LogicalHaving is not supported"
    }

    test {
        sql """create materialized view ${no_mv_name} as select col3, sum(col7) from ${tb_name} group by col3 limit 1"""
        exception "LogicalTopN is not supported"
    }

    test {
        sql """create materialized view ${no_mv_name} as select col3, 1, sum(col7) from ${tb_name} group by col3"""
        exception "The materialized view contain constant expr is disallowed"
    }

    test {
        sql """create materialized view ${no_mv_name} as select col3, col3, sum(col7) from ${tb_name} group by col3"""
        exception "The select expr is duplicated"
    }

    test {
        sql """create materialized view ${no_mv_name} as select col3, sum(col7) / 1 from ${tb_name} group by col3"""
        exception "materialized view's expr calculations cannot be included outside aggregate functions"
    }

    test {
        sql """create materialized view ${no_mv_name} as select  sum(col7), col3 from ${tb_name} group by col3"""
        exception "The aggregate column should be after none agg column"
    }

    test {
        sql """create materialized view ${no_mv_name} as select col1, col2, col3 from ${tb_name} order by col1, col2, col3;"""
        exception """agg mv must has group by clause"""
    }

    test {
        sql """create materialized view ${no_mv_name} as select col1, col2, col3, sum(col7) from ${tb_name} group by col3, col1, col2 order by col3, col1, col2"""
        exception "The order of columns in order by clause must be same as the order of columnsin select list"
    }

    test {
        sql """create materialized view ${no_mv_name} as select sum(col3) from ${tb_name}"""
        exception """The materialized view must contain at least one key column"""
    }

    test {
        sql """create materialized view ${no_mv_name} as select col3, min(col7) from ${tb_name} group by col3"""
        exception """Aggregate function require same with slot aggregate type"""
    }









//    create materialized view no_mv_agg_negative_mv as select col6, sum(col7) from mv_agg_negative_tb group by col6
//    create materialized view no_mv_agg_negative_mv as select col14, sum(col7) from mv_agg_negative_tb group by col14



/*
    create materialized view no_mv_agg_negative_mv as select col3, sum(col7) from mv_agg_negative_tb group by col3 order by col3

    create materialized view no_mv_agg_negative_mv1 as select col3, sum(col7) from mv_agg_negative_tb group by col3 order by col3

    create materialized view no_mv_agg_negative_mv1 as select col2, sum(col7) from mv_agg_negative_tb group by col2 order by col2

    create materialized view no_mv_agg_negative_mv1 as select  sum(col7), col3 from mv_agg_negative_tb group by col3
    The aggregate column should be after none agg column

 */

}
