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

suite("test_view_with_tb_change") {

    String dbName = context.config.getDbNameByFile(context.file)
    String prefix = "mtmv_view_with_tb_change_"
    String tbName = prefix + "table"
    String viewName = prefix + "view"
    String mtmvName = prefix + "mtmv"

    def compare_res = { def stmt ->
        sql "SET enable_materialized_view_rewrite=false"
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        sql "SET enable_materialized_view_rewrite=true"
        def mv_origin_res = sql stmt
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }

    sql """drop table if exists ${tbName}"""
    sql """drop view if exists ${viewName}"""
    sql """drop materialized view if exists ${mtmvName}"""

    sql """CREATE TABLE ${tbName}
        (
            `event_time` datetime NOT NULL,
            `user_id` bigint(20) NOT NULL,
            `item_id` int(11) NOT NULL,
            `amount` decimal(10, 2) NOT NULL,
            `city` varchar(64) NOT NULL
        )
        DUPLICATE KEY(`event_time`, `user_id`)
        partition by range(`event_time`) (
            partition p1 values [('2025-10-01 00:00:00'), ('2025-10-02 00:00:00')),
            partition p2 values [('2025-10-02 00:00:00'), ('2025-10-03 00:00:00'))
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );"""
    sql """INSERT INTO ${tbName} VALUES
        ('2025-10-01 10:00:00', 1001, 1, 10.50, 'Beijing'),
        ('2025-10-01 11:00:00', 1002, 2, 20.00, 'Shanghai'),
        ('2025-10-02 12:00:00', 1001, 3, 5.50, 'Beijing'),
        ('2025-10-02 13:00:00', 1003, 1, 30.00, 'Guangzhou');"""
//    sql """create view ${viewName} as
//        SELECT DATE_TRUNC('day', event_time) AS trading_date,user_id,item_id,amount,city,amount * 0.1 AS tax
//        FROM ${tbName}
//        WHERE amount > 10.00
//        group by trading_date,user_id,item_id,amount,city,tax;"""
    sql """create view ${viewName} as 
        SELECT event_time AS trading_date,user_id,item_id,amount,city,amount * 0.1 AS tax
        FROM ${tbName}
        WHERE amount > 10.00 
        group by trading_date,user_id,item_id,amount,city,tax;"""
    def sql_view_str = """SELECT
            trading_date,
            city,
            COUNT(DISTINCT user_id) AS distinct_users,
            SUM(amount) AS total_amount,
            SUM(tax) AS total_tax
        FROM ${viewName}
        GROUP BY trading_date, city
        ORDER BY 1,2,3,4,5"""
    def sql_table_str = """SELECT
            trading_date,
            city,
            COUNT(DISTINCT user_id) AS distinct_users,
            SUM(amount) AS total_amount,
            SUM(tax) AS total_tax
        FROM (SELECT DATE_TRUNC('day', event_time) AS trading_date,user_id,item_id,amount,city,amount * 0.1 AS tax
        FROM ${tbName}
        WHERE amount > 10.00 
        group by trading_date,user_id,item_id,amount,city,tax) as vw
        GROUP BY trading_date, city
        ORDER BY 1,2,3,4,5"""
    sql """
        CREATE MATERIALIZED VIEW ${mtmvName}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        partition by (trading_date)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        ${sql_view_str}
        """
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)


// 检查物化视图状态，和查询是否可以命中
    // 刷新物化视图，检查物化视图状态，和查询是否可以命中
    // 基表数据变动，insert into 或者 insert overwrite
    sql """INSERT INTO ${tbName} VALUES
        ('2025-10-01 12:00:00', 1001, 1, 10.50, 'Beijing');"""
    def zz = "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"

    // 基表分区变动
    // 基表结构变动，增加一列，或者删除一列
    // 基表列数据类型的变化
    // 基表删除重建

    // view结构的变化
    // 表结构的变动可能造成view结构的变动，也可能不会造成view结构的变动
    // 删除重建一个相同结构的view
    // 删除重建一个结构不同的view，增加一列，或者某列数据类型变化
    // 构建view的语句的变化




}
