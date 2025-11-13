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
    String tbName2 = prefix + "table2"
    String viewName = prefix + "view"
    String viewName2 = prefix + "view2"
    String mtmvName = prefix + "mtmv"
    String mtmvName2 = prefix + "mtmv2"

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

    def waitingColumnTaskFinished = { def cur_db_name, def cur_table_name ->
        Thread.sleep(2000)
        String showTasks = "SHOW ALTER TABLE COLUMN from ${cur_db_name} where TableName='${cur_table_name}' ORDER BY CreateTime ASC"
        String status = "NULL"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
        do {
            result = sql(showTasks)
            logger.info("result: " + result.toString())
            if (!result.isEmpty()) {
                status = result.last().get(9)
            }
            logger.info("The state of ${showTasks} is ${status}")
            Thread.sleep(1000)
        } while (timeoutTimestamp > System.currentTimeMillis() && (status != 'FINISHED'))
        if (status != "FINISHED") {
            logger.info("status is not success")
            return false
        }
        assertTrue(status == "FINISHED")
        return true
    }

    sql """drop table if exists ${tbName}"""
    sql """drop table if exists ${tbName2}"""
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
    sql """CREATE TABLE ${tbName2}
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
    sql """INSERT INTO ${tbName2} VALUES
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
    def sql_table_str = """
        SELECT * FROM (SELECT event_time AS trading_date,user_id,item_id,amount,city,amount * 0.1 AS tax
        FROM ${tbName}
        WHERE amount > 10.00 
        group by trading_date,user_id,item_id,amount,city,tax) as t ORDER BY 1,2,3,4,5
        """
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
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
    compare_res(sql_table_str)

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
        if (part_info[i][1].toString() == "p_20251001000000_20251002000000") {
            assertTrue(part_info[i][18].toString() == "false")
        } else {
            assertTrue(part_info[i][18].toString() == "true")
        }
    }
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
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
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
    compare_res(sql_table_str)


    // insert overwrite
    sql """INSERT overwrite table ${tbName} partition(p2) VALUES
        ('2025-10-02 12:00:00', 1001, 1, 10.50, 'Beijing');"""
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == false)
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        if (part_info[i][1].toString() == "p_20251002000000_20251003000000") {
            assertTrue(part_info[i][18].toString() == "false")
        } else {
            assertTrue(part_info[i][18].toString() == "true")
        }
    }
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
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
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
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
        assertTrue(part_info[i][18].toString() == "true")
    }
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
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
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
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
        if (part_info[i][1].toString() == "p_20251003000000_20251004000000") {
            assertTrue(part_info[i][18].toString() == "false")
        } else {
            assertTrue(part_info[i][18].toString() == "true")
        }
    }
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
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
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
    compare_res(sql_table_str)


    // 基表结构变动，增加一列，或者删除一列
    // add column
    sql """alter table ${tbName} add column dt varchar(100)"""
    waitingColumnTaskFinished(dbName, tbName)
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18].toString() == "true")
    }
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
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
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // drop column
    sql """alter table ${tbName} drop column dt"""
    waitingColumnTaskFinished(dbName, tbName)
    mv_infos = sql "select State,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == true)
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18].toString() == "true")
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
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // drop column that exists view
    sql """alter table ${tbName} drop column city"""
    waitingColumnTaskFinished(dbName, tbName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == true)
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18].toString() == "true")
    }
    test {
        sql sql_view_str
        exception "Unknown column"
    }
    test {
        sql sql_table_str
        exception "Unknown column"
    }

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    def jobName = getJobName(dbName, mtmvName)
    waitingMTMVTaskFinishedNotNeedSuccess(jobName)
//    waitingMTMVTaskFinishedNotNeedSuccess(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == true)
    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18].toString() == "true")
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "\\N")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "FAILED")
    test {
        sql sql_view_str
        exception "Unknown column"
    }
    test {
        sql sql_table_str
        exception "Unknown column"
    }

    // add column
    // mark
    sql """alter table ${tbName} add column city varchar(64)"""
    waitingColumnTaskFinished(dbName, tbName)
    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    assertTrue(mv_infos[0][2] == "SUCCESS")
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "NOT_REFRESH") // complete
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
    compare_res(sql_table_str)


    // 基表列数据类型的变化
    sql """ALTER TABLE ${tbName} MODIFY COLUMN amount decimal(20, 2);"""
    waitingColumnTaskFinished(dbName, tbName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == true)
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18].toString() == "true")
    }
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
//    waitingMTMVTaskFinishedByMvName(mtmvName)
    jobName = getJobName(dbName, mtmvName)
    waitingMTMVTaskFinishedNotNeedSuccess(jobName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == true)
    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18].toString() == "true")
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "\\N")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "FAILED")
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // mark 需要恢复mtmv的健康再继续删除
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
        assertTrue(part_info[i][18].toString() == "false")
    }
    test {
        sql sql_view_str
        exception "does not exist"
    }
    test {
        sql sql_table_str
        exception "does not exist"
    }

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
//    waitingMTMVTaskFinishedByMvName(mtmvName)
    jobName = getJobName(dbName, mtmvName)
    waitingMTMVTaskFinishedNotNeedSuccess(jobName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18].toString() == "false")
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "\\N")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "FAILED")
    test {
        sql sql_view_str
        exception "does not exist"
    }
    test {
        sql sql_table_str
        exception "does not exist"
    }

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
        assertTrue(part_info[i][18].toString() == "true")
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "COMPLETE")
    assertTrue(mv_tasks[0][1] == "100.00% (2/2)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
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
        assertTrue(part_info[i][18].toString() == "false")
    }
    test {
        sql sql_view_str
        exception "does not exist"
    }
    test {
        sql sql_table_str
        exception "does not exist"
    }

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
//    waitingMTMVTaskFinishedByMvName(mtmvName)
    jobName = getJobName(dbName, mtmvName)
    waitingMTMVTaskFinishedNotNeedSuccess(jobName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18].toString() == "false")
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "\\N")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "FAILED")
    test {
        sql sql_view_str
        exception "does not exist"
    }
    test {
        sql sql_table_str
        exception "does not exist"
    }

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
        assertTrue(part_info[i][18].toString() == "true")
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "COMPLETE")
    assertTrue(mv_tasks[0][1] == "100.00% (2/2)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
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
        assertTrue(part_info[i][18].toString() == "false")
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
        assertTrue(part_info[i][18].toString() == "true")
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "COMPLETE")
    assertTrue(mv_tasks[0][1] == "100.00% (2/2)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
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
        assertTrue(part_info[i][18].toString() == "false")
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
        assertTrue(part_info[i][18].toString() == "true")
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "COMPLETE")
    assertTrue(mv_tasks[0][1] == "100.00% (2/2)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // 构建view的语句的变化
    sql """ALTER VIEW ${viewName}
        AS SELECT event_time AS trading_date,user_id+1 as user_id,item_id,amount,city,amount * 0.1 AS tax
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
        assertTrue(part_info[i][18].toString() == "false")
    }
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
//    waitingMTMVTaskFinishedByMvName(mtmvName)
    jobName = getJobName(dbName, mtmvName)
    waitingMTMVTaskFinishedNotNeedSuccess(jobName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "NORMAL")
    assertTrue(mv_infos[0][1] == true)
    assertTrue(mv_infos[0][2] == "SUCCESS")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18].toString() == "true")
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "COMPLETE")
    assertTrue(mv_tasks[0][1] == "100.00% (2/2)")
    assertTrue(mv_tasks[0][2] == "SUCCESS")
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // 构建view的语句的变化  int+1会变成bigint
    sql """ALTER VIEW ${viewName}
        AS SELECT event_time AS trading_date,user_id,item_id+1 as item_id,amount,city,amount * 0.1 AS tax
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
        assertTrue(part_info[i][18].toString() == "false")
    }
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
//    waitingMTMVTaskFinishedByMvName(mtmvName)
    jobName = getJobName(dbName, mtmvName)
    waitingMTMVTaskFinishedNotNeedSuccess(jobName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18].toString() == "false")
    }
    mv_tasks = sql """select RefreshMode,Progress,Status from tasks("type"="mv") where MvName = '${mtmvName}' order by CreateTime desc limit 1"""
    assertTrue(mv_tasks[0][0] == "\\N")
    assertTrue(mv_tasks[0][1] == "\\N")
    assertTrue(mv_tasks[0][2] == "FAILED")
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    // pct table column change
    sql """create view ${viewName2} as
        SELECT t1.event_time AS trading_date, t2.user_id as user_id, t1.item_id as item_id, 
        t2.amount as amount, t1.city as city, t2.amount * 0.1 AS tax
        FROM ${tbName} as t1 inner join ${tbName2} as t2 
        on t1.event_time = t2.event_time
        WHERE t1.amount > 10.00 
        group by trading_date,user_id,item_id,amount,city,tax;"""
    sql_view_str = """SELECT * FROM ${viewName2} ORDER BY 1,2,3,4,5"""
    sql_table_str = """
        SELECT * FROM (SELECT t1.event_time AS trading_date, t2.user_id as user_id, t1.item_id as item_id, 
        t2.amount as amount, t1.city as city, t2.amount * 0.1 AS tax
        FROM ${tbName} as t1 inner join ${tbName2} as t2 
        on t1.event_time = t2.event_time
        WHERE t1.amount > 10.00 
        group by trading_date,user_id,item_id,amount,city,tax) as t ORDER BY 1,2,3,4,5
        """
    sql """
        CREATE MATERIALIZED VIEW ${mtmvName2}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        partition by (trading_date)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        ${sql_view_str}
        """
    waitingMTMVTaskFinishedByMvName(mtmvName2)
    mv_rewrite_success_without_check_chosen(sql_view_str, mtmvName2)
    compare_res(sql_view_str)
    mv_rewrite_success_without_check_chosen(sql_table_str, mtmvName2)
    compare_res(sql_table_str)

    //
    sql """ALTER VIEW ${viewName2} AS 
        SELECT t2.event_time AS trading_date, t2.user_id as user_id, t1.item_id as item_id, 
        t2.amount as amount, t1.city as city, t2.amount * 0.1 AS tax
        FROM ${tbName} as t1 inner join ${tbName2} as t2 
        on t1.event_time = t2.event_time
        WHERE t1.amount > 10.00 
        group by trading_date,user_id,item_id,amount,city,tax;"""
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
//    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18].toString() == "false")
    }
    mv_not_part_in(sql_view_str, mtmvName)
    compare_res(sql_view_str)
    mv_not_part_in(sql_table_str, mtmvName)
    compare_res(sql_table_str)

    sql """refresh MATERIALIZED VIEW ${mtmvName} auto"""
//    waitingMTMVTaskFinishedByMvName(mtmvName)
    jobName = getJobName(dbName, mtmvName)
    waitingMTMVTaskFinishedNotNeedSuccess(jobName)
    mv_infos = sql "select State,SyncWithBaseTables,RefreshState from mv_infos('database'='${dbName}') where Name='${mtmvName}'"
    assertTrue(mv_infos.size() == 1)
    assertTrue(mv_infos[0][0] == "SCHEMA_CHANGE")
    assertTrue(mv_infos[0][1] == false)
    assertTrue(mv_infos[0][2] == "FAIL")
    part_info = sql "show partitions from ${mtmvName}"
    assertTrue(part_info.size() == 2)
    for (int i = 0; i < part_info.size(); i++) {
        assertTrue(part_info[i][18].toString() == "false")
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
