CREATE TABLE `stream_load_list_test_table_string_key`(
  `col1` bigint not null,
  `col2` varchar(16384) not null
) duplicate KEY(`col1`)
AUTO PARTITION BY list(`col2`)
(
)
DISTRIBUTED BY HASH(`col1`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);