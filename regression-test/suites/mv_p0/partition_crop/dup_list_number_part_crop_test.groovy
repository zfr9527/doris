// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("dup_list_number_part_crop", "mv_part_crop") {
    String db = context.config.getDbNameByFile(context.file)
    String orders_tb = "dup_orders_list_date_crop_part"

    sql """
    drop table if exists ${orders_tb}
    """

    sql """CREATE TABLE `${orders_tb}` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL,
      `o_orderdate` DATE not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`, `o_custkey`)
    COMMENT 'OLAP'
    partition by list (o_orderkey) (
        PARTITION `p_1` VALUES IN ("1"),
        PARTITION `p_2` VALUES IN ("2"),
        PARTITION `p_3` VALUES IN ("3"),
        PARTITION `p_4` VALUES IN ("4"),
        PARTITION `pX` values in ((NULL))
    )
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into ${orders_tb} values 
    (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-18'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (4, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (2, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20');
    """
    sql """analyze table ${orders_tb} with sync;"""

    def create_mv_orders = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name} on ${orders_tb};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        AS  
        ${mv_sql}
        """
    }

    def mv_name = "dup_list_number_crop_part_mtmv"
    def mv_sql = """
        select o_orderkey, o_custkey, o_orderdate
        from ${orders_tb} 
        """
    create_mv_orders(mv_name, mv_sql)
    waitingMVTaskFinished(orders_tb, mv_name)

    def query_sql = """
        select o_orderkey, o_custkey, o_orderdate
        from ${orders_tb} 
        where o_orderkey = 1
        """
    explain {
        sql("${query_sql}")
        contains "partitions=1/5 (p_1)"
    }
    explain {
        sql("${query_sql}")
        contains "(${mv_name})"
    }

    query_sql = """
        select o_orderkey, o_custkey, o_orderdate
        from ${orders_tb} 
        where o_orderkey >= 1 and o_orderkey < 2
        """
    explain {
        sql("${query_sql}")
        contains "partitions=1/5 (p_1)"
    }
    explain {
        sql("${query_sql}")
        contains "(${mv_name})"
    }

    query_sql = """
        select o_orderkey, o_custkey, o_orderdate
        from ${orders_tb} 
        where o_orderkey <> 1
        """
    explain {
        sql("${query_sql}")
        contains "partitions=3/5"
    }
    explain {
        sql("${query_sql}")
        contains "(${mv_name})"
    }

}
