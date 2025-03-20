suite("agg_negative_mv_test") {

    String db = context.config.getDbNameByFile(context.file)
    def prefix_str = "mv_mow_negative"
    def tb_name = prefix_str + "_tb"

    sql """drop table if exists ${tb_name};"""
    sql """
        CREATE TABLE `${tb_name}` (
        `col1` datetime NULL,
        `col2` varchar(60) NULL,
        `col3` int(11) NOT NULL,
        `col4` boolean NULL,
        `col15` ipv4 NULL,
        `col5` string NULL,
        `col6` ARRAY<int(11)> NULL COMMENT "",
        `col7` int(11) NULL DEFAULT "0",
        `col8` int(11) NULL DEFAULT "0",
        `col9` int(11) NULL DEFAULT "0",
        `col10` int(11) NULL,
        `col11` bitmap NOT NULL,
        `col13` hll not NULL COMMENT "hll",
        `col14` ipv4 NULL
        ) ENGINE=OLAP
        unique KEY(`col1`, `col2`, `col3`, `col4`, `col15`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`col2`, `col3`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true"
        );
        """
    sql """insert into ${tb_name} values 
            ("2023-08-16 22:28:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax1",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",2,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,0,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd2",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[5,4,3,2,1], 1, 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 3, 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 4, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 5, 6, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(2), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), HLL_HASH(100), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'","asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'255.255.255.255'"),
            ("2023-08-16 24:27:00","ax1",2,0,"'0.0.0.0'","asd",[5,4,3,2,1], 3, 4, 5, 6, to_bitmap(2), HLL_HASH(100), "'255.255.255.255'"),
            ("2024-08-17 22:27:00","ax2",3,1,"'0.0.0.0'","asd3",[1,2,3,4,6], 7, 8, 9, 10, to_bitmap(3), HLL_HASH(1000), "'0.0.1.0'"),
            ("2023-09-16 22:27:00","ax4",4,0,"'0.0.0.0'","asd2",[1,2,9,4,5], 11, 11, 11, 11, to_bitmap(4), HLL_HASH(1), "'0.10.0.0'");"""

    def mv_name = """${prefix_str}_mv"""
    def no_mv_name = """no_${prefix_str}_mv"""
    sql """create materialized view ${mv_name} as select col4, col1, col2, col3, col15, sum(col7) from ${tb_name} where col1 = "2023-08-16 22:27:00" group by col4, col1, col2, col3, col15 order by col4, col1, col2, col3, col15"""
    // 验证col1,col2,col3是key列，sum col7不是key列
    def desc_res = sql """desc ${tb_name} all;"""
    
    // 这里可以搞一个复杂类型，看看能不能成为key列

//    create materialized view mv_agg_negative_mv as select  col1, col2, col3, col4, col15, sum(col7) from mv_agg_negative_tb where col1 = "2023-08-16 22:27:00" group by col1, col2, col3, col4, col15 order by  col1, col2, col3, col4, col15
//    create materialized view mv_agg_negative_mv as select  col1, col2, col3, sum(col7) from mv_agg_negative_tb where col1 = "2023-08-16 22:27:00" group by col1, col2, col3  order by  col1, col2, col3

    explain {
        sql("""select col1, col2, col3, sum(col7) from ${tb_name} where col1 = "2023-08-16 22:27:00" group by col3, col1, col2 order by col1, col2, col3""")
        contains "${mv_name}(${mv_name})"
    }

    test {
        sql """create materialized view ${no_mv_name} as select col3, sum(col7) from ${tb_name} group by col3 having col3 > 1"""
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


//    create materialized view mv_agg_negative_mv11 as select col3, col1, col2, col15, sum(col7) from mv_agg_negative_tb group by 1,2,3,4  order by  1,2,3,4
//
//
//    create materialized view mv_agg_negative_mv11 as select col3, col1, col2, col15, case when col2 > 1 then 1 else 2 end, sum(col7) from mv_agg_negative_tb group by 1,2,3,4,5  order by  1,2,3,4,5
//
//    create materialized view mv_agg_negative_mv12 as select col3, col1, col2, col15, sum(case when col2 > 1 then 1 else 2 end), sum(col7) from mv_agg_negative_tb group by 1,2,3,4  order by  1,2,3,4
//
//
//    create materialized view mv_agg_negative_mv11 as select col3, col1, col2, col15, sum(col7), count(*) from mv_agg_negative_tb group by 1,2,3,4  order by  1,2,3,4
//
//    create materialized view mv_agg_negative_mv14 as select col3, col1, col2, col15, sum(col7), bitmap_union(to_bitmap(case when col2 > 1 then 1 else 2 end)) from mv_agg_negative_tb group by 1,2,3,4  order by  1,2,3,4
//
//    create materialized view mv_agg_negative_mv14 as select col3, col1, col2, col15, sum(col7), bitmap_union(to_bitmap(case when col10 > 1 then 1 else 2 end)) from mv_agg_negative_tb group by 1,2,3,4  order by  1,2,3,4
//
//    create materialized view mv_agg_negative_mv14 as select case when col2 > 1 then 1 else 2 end, sum(col7), bitmap_union(to_bitmap(case when col10 > 1 then 1 else 2 end)) from mv_agg_negative_tb group by 1  order by  1


}
