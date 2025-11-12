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
    def sql_view_str = """SELECT * FROM ${viewName} ORDER BY 1,2,3,4,5"""
    def sql_table_str = """SELECT
            trading_date,
            city,
            COUNT(DISTINCT user_id) AS distinct_users,
            SUM(amount) AS total_amount,
            SUM(tax) AS total_tax
        FROM (SELECT * FROM ${viewName} ORDER BY 1,2,3,4,5) as vw
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
    // insert into
    sql """INSERT INTO ${tbName} VALUES
        ('2025-10-01 12:00:00', 1001, 1, 10.50, 'Beijing');"""
    def mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == false)
    def part_info = sql "show partitions from ${mtmvName}"
    logger.info("part_info:" + part_info)
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        if (part_info[i][1] == "p_20251001000000_20251002000000") {
            assertTrue(part_info[i][18] == false)
        } else {
            assertTrue(part_info[i][18] == true)
        }
    }
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    def mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "PARTIAL")
    assertTrue(mv_tasks[0][1] == "100.00% (1/1)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)


    // insert overwrite
    sql """INSERT overwrite ${tbName} VALUES
        ('2025-10-02 12:00:00', 1001, 1, 10.50, 'Beijing');"""
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == false)
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        if (part_info[i][1] == "p_20251001000000_20251002000000") {
            assertTrue(part_info[i][18] == false)
        } else {
            assertTrue(part_info[i][18] == true)
        }
    }
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "PARTIAL")
    assertTrue(mv_tasks[0][1] == "100.00% (1/1)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)



    // 基表分区变动
    // add partition
    sql """ALTER TABLE ${tbName} ADD PARTITION p3 VALUES [('2025-10-03 00:00:00'), ('2025-10-04 00:00:00'))"""
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "PARTIAL")
    assertTrue(mv_tasks[0][1] == "100.00% (1/1)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // drop partition
    sql """ALTER TABLE ${tbName} drop PARTITION p3"""
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == false)
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 3)
    for (int i = 0; i < part_info.size(); i++) {
        if (part_info[0][1] == "p_20251003000000_20251004000000") {
            assertTrue(part_info[i][18] == false)
        } else {
            assertTrue(part_info[i][18] == true)
        }
    }
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "PARTIAL")
    assertTrue(mv_tasks[0][1] == "100.00% (1/1)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)


    // 基表结构变动，增加一列，或者删除一列
    // add column
    sql """alter table ${tbName} add column dt varchar(100)"""
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "NOT_REFRESH")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // drop column
    sql """alter table ${tbName} drop column dt"""
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == true)
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "NOT_REFRESH")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // drop column that exists view
    sql """alter table ${tbName} drop column city"""
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == true)
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == true)
    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "\\N")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "FAILED")
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // add column
    sql """alter table ${tbName} add column city varchar(64)"""
    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    assertTrue(mv_infos[0][1] == "SUCCESS")
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "NOT_REFRESH")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)


    // 基表列数据类型的变化
    sql """ALTER TABLE ${mtmvName} MODIFY COLUMN amount decimal(20, 2);"""
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == true)
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == true)
    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "\\N")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "FAILED")
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // 基表删除重建
    sql """drop table ${tbName}"""
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == false)
    }
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "\\N")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "FAILED")
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

// recreate table
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
    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    assertTrue(mv_infos[0][2] == "SUCCESS")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "COMPLETE")
    assertTrue(mv_tasks[0][1] == "100.00% (2/2)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // view结构的变化
    // 表结构的变动可能造成view结构的变动，也可能不会造成view结构的变动
    // 删除重建一个相同结构的view
    sql """drop view ${viewName}"""
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
//    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == false)
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "\\N")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "FAILED")
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // recreate same view
    sql """create view ${viewName} as 
        SELECT event_time AS trading_date,user_id,item_id,amount,city,amount * 0.1 AS tax
        FROM ${tbName}
        WHERE amount > 10.00 
        group by trading_date,user_id,item_id,amount,city,tax;"""
    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    assertTrue(mv_infos[0][2] == "SUCCESS")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "COMPLETE")
    assertTrue(mv_tasks[0][1] == "100.00% (2/2)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // modify view
    sql """ALTER VIEW ${viewName}
        (
            trading_date COMMENT "column 1",
            user_id COMMENT "column 2",
            item_id COMMENT "column 3",
            amount COMMENT "column 4",
            city COMMENT "column 5",
            tax COMMENT "column 6"
        )
        AS SELECT event_time AS trading_date,user_id,item_id,amount,city,amount * 0.1 AS tax
        FROM ${tbName}
        WHERE amount > 10.00 
        group by trading_date,user_id,item_id,amount,city,tax;"""
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
//    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == false)
    }
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    assertTrue(mv_infos[0][2] == "SUCCESS")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "COMPLETE")
    assertTrue(mv_tasks[0][1] == "100.00% (2/2)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)


    // 删除重建一个结构不同的view，增加一列，或者某列数据类型变化
    sql """drop view ${viewName}"""
    sql """create view ${viewName} as 
        SELECT event_time AS trading_date,user_id,item_id,amount,city,amount * 0.1 AS tax, 1 
        FROM ${tbName}
        WHERE amount > 10.00 
        group by trading_date,user_id,item_id,amount,city,tax;"""
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
//    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == false)
    }
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)


    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    assertTrue(mv_infos[0][2] == "SUCCESS")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == true)
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "COMPLETE")
    assertTrue(mv_tasks[0][1] == "100.00% (2/2)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // 构建view的语句的变化
    sql """ALTER VIEW ${viewName}
        AS SELECT event_time AS trading_date,user_id+1,item_id,amount,city,amount * 0.1 AS tax
        FROM ${tbName}
        WHERE amount > 10.00 
        group by trading_date,user_id,item_id,amount,city,tax;"""
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
//    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == false)
    }
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18] == false)
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "\\N")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "FAILED")
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)


}
