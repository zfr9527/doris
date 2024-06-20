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

suite("dup_range_date_part_crop", "mv_part_crop") {
    String db = context.config.getDbNameByFile(context.file)
    String orders_tb = "dup_orders_range_date_crop_part"

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
    auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into ${orders_tb} values 
    (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-18'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20');
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

    def mv_name = "dup_rang_date_crop_part_mtmv"
    def mv_sql = """
        select o_orderkey, o_custkey, o_orderdate
        from ${orders_tb} 
        """
    create_mv_orders(mv_name, mv_sql)
    waitingMVTaskFinished(orders_tb, mv_name)

    def query_sql = """
        select o_orderkey, o_custkey, o_orderdate
        from ${orders_tb} 
        where o_orderdate = '2023-10-17'
        """
    explain {
        sql("${query_sql}")
        contains "partitions=1/4 (p20231017000000)"
    }
    explain {
        sql("${query_sql}")
        contains "(${mv_name})"
    }

    query_sql = """
        select o_orderkey, o_custkey, o_orderdate
        from ${orders_tb} 
        where o_orderdate >= '2023-10-17' and o_orderdate < '2023-10-18'
        """
    explain {
        sql("${query_sql}")
        contains "partitions=1/4 (p20231017000000)"
    }
    explain {
        sql("${query_sql}")
        contains "(${mv_name})"
    }

    query_sql = """
        select o_orderkey, o_custkey, o_orderdate
        from ${orders_tb} 
        where o_orderdate <> '2023-10-17'
        """
    explain {
        sql("${query_sql}")
        contains "partitions=3/4"
    }
    explain {
        sql("${query_sql}")
        contains "(${mv_name})"
    }




}
