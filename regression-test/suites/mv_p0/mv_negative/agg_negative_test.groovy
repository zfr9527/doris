suite("agg_negative_mv_test") {

    String db = context.config.getDbNameByFile(context.file)
    def prefix_str = "agg_negative_mv"
    def tb_name = prefix_str + "tb"

    sql """set enable_agg_state=true;"""

    sql """
        CREATE TABLE `${tb_name}` (
        `col1` datetime NULL,
        `col2` varchar(20) NULL,
        `col3` int(11) NULL,
        `col4` boolean NULL,
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
        AGGREGATE KEY(`col1`, `col2`, `col3`, `col4`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`col2`, `col3`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """insert into ${tb_name} values 
            ("2023-08-16 22:27:00","ax",1,1,"asd",[1,2,3,4,5], 1, 1, 1, 1, to_bitmap(243), HLL_UNION_AGG(1), "'0.0.0.0'");"""




}
