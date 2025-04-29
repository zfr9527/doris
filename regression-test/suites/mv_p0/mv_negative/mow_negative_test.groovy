suite("mow_negative_mv_test") {

    String db = context.config.getDbNameByFile(context.file)
    def prefix_str = "mv_mow_negative"
    def tb_name = prefix_str + "_tb"

    sql """drop table if exists ${tb_name};"""
    sql """
        CREATE TABLE `${tb_name}` (
        `col1` datetime NULL,
        `col2` varchar(60) NULL,
        `col3` bigint(11) NOT NULL,
        `col4` boolean NULL,
        `col15` ipv4 NULL,
        `col8` int(11) NULL DEFAULT "0",
        `col5` string NULL,
        `col6` ARRAY<int(11)> NULL COMMENT "",
        `col7` bigint(11) NOT NULL AUTO_INCREMENT,
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
            ("2023-08-16 22:28:00","ax",1,1,"'0.0.0.0'",1,"asd",[1,2,3,4,5], 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax1",1,1,"'0.0.0.0'",1,"asd",[1,2,3,4,5], 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",2,1,"'0.0.0.0'",1,"asd",[1,2,3,4,5], 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,0,"'0.0.0.0'",1,"asd",[1,2,3,4,5], 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'",1,"asd2",[1,2,3,4,5], 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'",1,"asd",[5,4,3,2,1], 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'",1,"asd",[1,2,3,4,5], 3, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'",4,"asd",[1,2,3,4,5], 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'",1,"asd",[1,2,3,4,5], 1, 5, 6, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'",1,"asd",[1,2,3,4,5], 1, 1, 1, to_bitmap(2), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'",1,"asd",[1,2,3,4,5], 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'",1,"asd",[1,2,3,4,5], 1, 1, 1, to_bitmap(243), HLL_HASH(100), "'0.0.0.0'"),
            ("2023-08-16 22:27:00","ax",1,1,"'0.0.0.0'",1,"asd",[1,2,3,4,5], 1, 1, 1, to_bitmap(243), HLL_HASH(1), "'255.255.255.255'"),
            ("2023-08-16 24:27:00","ax1",2,0,"'0.0.0.0'",4,"asd",[5,4,3,2,1], 3, 5, 6, to_bitmap(2), HLL_HASH(100), "'255.255.255.255'"),
            ("2024-08-17 22:27:00","ax2",3,1,"'0.0.0.0'",8,"asd3",[1,2,3,4,6], 7, 9, 10, to_bitmap(3), HLL_HASH(1000), "'0.0.1.0'"),
            ("2023-09-16 22:27:00","ax4",4,0,"'0.0.0.0'",11,"asd2",[1,2,9,4,5], 11, 11, 11, to_bitmap(4), HLL_HASH(1), "'0.10.0.0'");"""

    def mv_name = """${prefix_str}_mv"""
    def no_mv_name = """no_${prefix_str}_mv"""
    sql """create materialized view ${mv_name} as select col4, col1, col2, col3, col15 from ${tb_name} where col1 = '2023-08-16 22:27:00' order by col4, col1, col2, col3, col15"""
    waitingMVTaskFinishedByMvName(db, tb_name, mv_name)
    def desc_res = sql """desc ${tb_name} all;"""
    for (int i = 0; i < desc_res.size(); i++) {
        if (desc_res[i][0] == mv_name) {
            for (int j = i; j < i+5; j++) {
                assertTrue(desc_res[j][6] == "true")
            }
            break
        }
    }

    explain {
        sql("""select col1, col2, col3, sum(col3) from ${tb_name} where col1 = "2023-08-16 22:27:00" group by col3, col1, col2 order by col1, col2, col3""")
        contains "(${mv_name})"
    }

    test {
        sql """create materialized view mv_mor_negative_mv_1 as select col4, col1, col2, col3, col15, col7 from mv_mor_negative_tb where col1 = '2023-08-16 22:27:00' order by col4, col1, col2, col3, col15, col7"""
        exception "The materialized view can not involved auto increment column"
    }

    test {
        sql """create materialized view mv_mor_negative_mv_1 as select col4, col1, col2, col3, col15, col8 from mv_mor_negative_tb where col1 = '2023-08-16 22:27:00' group by col4, col1, col2, col3, col15, col8 order by col4, col1, col2, col3, col15, col8"""
        exception "The materialized view of unique table must not has grouping columns"
    }

    test {
        sql """create materialized view mv_mor_negative_mv_2 as select col4, col1, col2, col3, col15, col8, sum(col3) from mv_mor_negative_tb where col1 = '2023-08-16 22:27:00' group by col4, col1, col2, col3, col15, col8 order by col4, col1, col2, col3, col15, col8"""
        exception "The materialized view of unique table must not has grouping columns"
    }

    // 这个可以创建?
    test {
        sql """create materialized view mv_mor_negative_mv_4 as select col4, col1, col2, col3, col15 from mv_mor_negative_tb having col3 > 1"""
        exception "LogicalHaving is not supported"
    }

    test {
        sql """create materialized view mv_mor_negative_mv_1 as select col4, col1, col2, col3, col15 from mv_mor_negative_tb limit 1"""
        exception "LogicalLimit is not supported"
    }

    test {
        sql """create materialized view mv_mor_negative_mv_1 as select col4, col1, col2, col3, col15, 1 from mv_mor_negative_tb"""
        exception "The materialized view contain constant expr is disallowed"
    }

    test {
        sql """create materialized view mv_mor_negative_mv_1 as select col4, col1, col2, col3, col15, col3 from mv_mor_negative_tb"""
        exception "The select expr is duplicated"
    }


    test {
        sql """create materialized view mv_mor_negative_mv_6 as select col3 from mv_mor_negative_tb"""
        exception "The materialized view of uniq table must contain all key columns"
    }

    test {
        sql """create materialized view mv_mor_negative_mv_6 as select col4, col1, col2, col3, col15 from mv_mor_negative_tb order by col1, col2, col3, col4, col15"""
        exception "The order of columns in order by clause must be same as the order of columnsin select list"
    }

    test {
        sql """create materialized view mv_mor_negative_mv_1 as select sum(col3) from mv_mor_negative_tb"""
        exception """The materialized view must contain at least one key column"""
    }

    test {
        sql """create materialized view mv_mor_negative_mv_1 as select col3, min(col7) from mv_mor_negative_tb group by col3"""
        exception """Aggregate function require same with slot aggregate type"""
    }

    test {
        sql """create materialized view mv_mor_negative_mv_1 as select col3, col1, col2, col15, case when col2 > 1 then 1 else 2 end from mv_mor_negative_tb order by 1,2,3,4,5"""
        exception """only support the single column or function expr. Error column: CASE WHEN"""
    }

    test {
        sql """create materialized view mv_mor_negative_mv_1 as select col1, bitmap_union(to_bitmap(col3)) from mv_mor_negative_tb group by col1;"""
        exception "The materialized view of unique table must not has grouping columns"
    }

    test {
        sql """create materialized view mv_mor_negative_mv_7 as select bitmap_union(col11) from mv_mor_negative_tb"""
        exception "Aggregate function require same with slot aggregate type"
    }

    sql """create materialized view mv_mor_negative_mv_3 as select col4, col1, col2, col3, col15, col8 from mv_mor_negative_tb where col1 = '2023-08-16 22:27:00' order by col4, col1, col2, col3, col15, col8"""

    sql """create materialized view mv_mor_negative_mv_5 as select col4, col1, col2, col3, col15, col3 / (cast(1 as decimal)) from mv_mor_negative_tb;"""


}
