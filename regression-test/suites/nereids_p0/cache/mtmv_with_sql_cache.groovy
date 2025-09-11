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

// 什么时候走直查，什么时候走改写？
// 基表变动会让查询无法走mtmv，所以无法命中物化视图，所以就是走原始查询，不能被改写
// 能命中物化视图，其实我们走改写，因为物化视图没有刷新，sql cache仍然生效
// 不能命中物化视图，就是直查原表，原表是改变的，那么就不能命中cache，反之就可以用cache
suite("mtmv_with_sql_cache") {

    def assertHasCache = { String sqlStr ->
        explain {
            sql ("physical plan ${sqlStr}")
            contains("PhysicalSqlCache")
        }
    }

    def assertNoCache = { String sqlStr ->
        explain {
            sql ("physical plan ${sqlStr}")
            notContains("PhysicalSqlCache")
        }
    }

    def cur_create_async_partition_mv = { db, mv_name, mv_sql, partition_col ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${db}.${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${db}.${mv_name} 
        BUILD IMMEDIATE REFRESH auto ON MANUAL 
        PARTITION BY ${partition_col} 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1')  
        AS ${mv_sql}
        """
        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        sql "analyze table ${db}.${mv_name} with sync;"
        // force meta sync to avoid stale meta data on follower fe
        sql """sync;"""
    }

    String dbName = context.config.getDbNameByFile(context.file)
    def prefix_str = "mtmv_with_sql_cache_"

    sql "ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '10')"

    def tb_name1 = prefix_str + "table1"
    def tb_name2 = prefix_str + "table2"
    createTestTable tb_name1
    createTestTable tb_name2

    def mv_name1 = prefix_str + "mtmv1"
    def mv_name2 = prefix_str + "mtmv2"
    def mv_name3 = prefix_str + "mtmv3"
    def mv_name4 = prefix_str + "mtmv4"
    def nested_mv_name1 = prefix_str + "nested_mtmv1"
    def mtmv_sql1 = """
        select t1.id as id, t2.value as value
        from ${tb_name1} as t1
        left join ${tb_name2} as t2
        on t1.id = t2.id
    """
    def mtmv_sql2 = """
        select t2.id as id, t2.value as value
        from ${tb_name1} as t1
        right join ${tb_name2} as t2
        on t1.id = t2.id
    """
    def mtmv_sql4 = """
        select t1.id as id, t1.value as value1 
        from ${tb_name1} as t1
        left join ${tb_name2} as t2
        on t1.id = t2.id
    """
    def nested_mtmv_sql1 = """
        select t1.id as id, t2.value as value
        from ${mv_name1} as t1
        left join ${mv_name2} as t2
        on t1.id = t2.id
    """
    def nested_mtmv_sql3 = """
        select t1.id as id, t2.value as value
        from ${mv_name3} as t1
        left join ${mv_name2} as t2
        on t1.id = t2.id
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name3};"""
    cur_create_async_partition_mv(dbName, mv_name1, mtmv_sql1, "(id)")
    cur_create_async_partition_mv(dbName, mv_name2, mtmv_sql2, "(id)")
    cur_create_async_partition_mv(dbName, mv_name4, mtmv_sql4, "(id)")
    cur_create_async_partition_mv(dbName, nested_mv_name1, nested_mtmv_sql1, "(id)")

    sleep(10000)
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_sql_cache=true"

    // Direct Query
    assertNoCache "select * from ${mv_name1}"
    assertNoCache "select * from ${mv_name2}"
    assertNoCache "select * from ${mv_name4}"
    assertNoCache "select * from ${nested_mv_name1}"
    // mtmv rewrite
    assertNoCache mtmv_sql1
    assertNoCache mtmv_sql2
    assertNoCache mtmv_sql4
    assertNoCache nested_mtmv_sql1

    sql "select * from ${mv_name1}"
    sql "select * from ${mv_name2}"
    sql "select * from ${mv_name4}"
    sql "select * from ${nested_mv_name1}"
    sql mtmv_sql1
    sql mtmv_sql2
    sql mtmv_sql4
    sql nested_mtmv_sql1

    assertHasCache "select * from ${mv_name1}"
    assertHasCache "select * from ${mv_name2}"
    assertHasCache "select * from ${mv_name4}"
    assertHasCache "select * from ${nested_mv_name1}"
    assertHasCache mtmv_sql1
    assertHasCache mtmv_sql2
    assertHasCache mtmv_sql4
    assertHasCache nested_mtmv_sql1

    // rename mtmv, 直查和改写是否可以命中sql cache
    sql """ALTER MATERIALIZED VIEW ${mv_name1} rename ${mv_name3};"""
    assertNoCache "select * from ${mv_name3}"
    assertNoCache mtmv_sql1   // -->   "select * from mv1"   --> rename don't affect table version --> hit
    assertHasCache "select * from ${nested_mv_name1}"
    assertNoCache nested_mtmv_sql3

    sql """ALTER MATERIALIZED VIEW ${mv_name3} rename ${mv_name1};"""
    assertHasCache "select * from ${mv_name1}"  // 之前没有查询过这个sql，所以之前的cache没有被删，现在再查询仍然生效
    assertNoCache mtmv_sql1 // 这个之前查询检测到失效，所以cache被删除了，所以这个地方无法用cache
    assertHasCache "select * from ${nested_mv_name1}" // nested mtmv未变化
    assertHasCache nested_mtmv_sql1 // 这个之前没查询过，所以cache仍然可以生效

    sql mtmv_sql1
    assertHasCache mtmv_sql1

    // replace mtmv, 直查和改写是否可以命中sql cache
    sql """ALTER MATERIALIZED VIEW ${mv_name1} REPLACE WITH MATERIALIZED VIEW ${mv_name2};"""
    assertNoCache "select * from ${mv_name1}"
    assertNoCache "select * from ${mv_name2}"
    assertNoCache mtmv_sql1   // -->   "select * from mv1/mv2" --> version change  --> nocache
    assertHasCache "select * from ${nested_mv_name1}"
    assertNoCache nested_mtmv_sql1

    sql "select * from ${mv_name1}"
    sql "select * from ${mv_name2}"
    sql mtmv_sql1
    sql "select * from ${nested_mv_name1}"
    sql nested_mtmv_sql1

    assertHasCache "select * from ${mv_name1}"
    assertHasCache "select * from ${mv_name2}"
    assertHasCache mtmv_sql1
    assertHasCache "select * from ${nested_mv_name1}"
    assertHasCache nested_mtmv_sql1

    // pause/resume mtmv
    // 暂停和恢复不影响sql cache工作
    sql """PAUSE MATERIALIZED VIEW JOB ON ${mv_name1};"""
    assertHasCache "select * from ${mv_name1}"
    assertHasCache mtmv_sql1
    assertHasCache "select * from ${nested_mv_name1}"
    assertHasCache nested_mtmv_sql1

    sql """PAUSE MATERIALIZED VIEW JOB ON ${mv_name4};"""
    assertHasCache "select * from ${mv_name4}"
    assertHasCache mtmv_sql4

    sql """RESUME MATERIALIZED VIEW JOB ON ${mv_name1};"""
    assertHasCache "select * from ${mv_name1}"
    assertHasCache mtmv_sql1

    sql """RESUME MATERIALIZED VIEW JOB ON ${mv_name4};"""
    assertHasCache "select * from ${mv_name4}"
    assertHasCache mtmv_sql4

    // 刷新mtmv，确保mtmv初始表现正常
    sql "REFRESH MATERIALIZED VIEW ${mv_name1} AUTO;"
    waitingMTMVTaskFinishedByMvName(mv_name1)

    sleep(15 * 1000)
    assertHasCache "select * from ${mv_name1}"
    assertHasCache mtmv_sql1
    assertHasCache "select * from ${nested_mv_name1}"
    assertHasCache nested_mtmv_sql1

    // refresh mtmv complete
    sql "REFRESH MATERIALIZED VIEW ${mv_name1} complete;"
    sleep(15 * 1000)
    assertNoCache "select * from ${mv_name1}"
    assertNoCache mtmv_sql1 // 可以改写成mtmv1，但是mtmv1的元数据改变，没办法继续用cache，所以执行原查询
    assertHasCache "select * from ${nested_mv_name1}"
    assertNoCache nested_mtmv_sql1

    sql mtmv_sql1
    sql "select * from ${mv_name1}"
    sql nested_mtmv_sql1

    assertHasCache mtmv_sql1
    assertHasCache "select * from ${mv_name1}"
    assertHasCache nested_mtmv_sql1

    // base table insert overwrite
    sql "INSERT OVERWRITE table ${tb_name1} PARTITION(p5) VALUES (5, 6);"
    sleep(10 * 1000)
    assertHasCache "select * from ${mv_name1}"
    assertNoCache mtmv_sql1
    assertHasCache "select * from ${nested_mv_name1}"
    assertHasCache nested_mtmv_sql1

    sql mtmv_sql1
    assertHasCache mtmv_sql1

    sql "REFRESH MATERIALIZED VIEW ${mv_name1} AUTO;"
    waitingMTMVTaskFinishedByMvName(mv_name1)
    sleep(15 * 1000)
    assertNoCache "select * from ${mv_name1}"
    assertHasCache mtmv_sql1
    assertHasCache "select * from ${nested_mv_name1}"
    assertNoCache nested_mtmv_sql1

    sql "select * from ${mv_name1}"
    sql nested_mtmv_sql1

    assertHasCache "select * from ${mv_name1}"
    assertHasCache mtmv_sql1
    assertHasCache "select * from ${nested_mv_name1}"
    assertHasCache nested_mtmv_sql1


    // add partition
    // 给基表插入分区之后是否仍然能
    sql "alter table ${tb_name1} add partition p6 values[('6'),('7'))"
    assertHasCache "select * from ${mv_name1}"
    assertHasCache mtmv_sql1
    assertHasCache "select * from ${nested_mv_name1}"
    assertHasCache nested_mtmv_sql1

    // base table insert data
    sql "insert into ${tb_name1} values(6, 1)"
    sleep(10 * 1000)
    assertHasCache "select * from ${mv_name1}"
    assertNoCache mtmv_sql1
    assertHasCache "select * from ${nested_mv_name1}"
    assertHasCache nested_mtmv_sql1

    sql mtmv_sql1
    assertHasCache mtmv_sql1

    // recreate mtmv to add column
    cur_create_async_partition_mv(dbName, mv_name1, mtmv_sql4, "(id)")
    sleep(15 * 1000)
    assertNoCache "select * from ${mv_name1}"
    assertHasCache "select * from ${mv_name2}"
    assertHasCache mtmv_sql1 // ???

    assertNoCache mtmv_sql4  // base table change, not hit mtmv1/mtmv4
    assertHasCache "select * from ${nested_mv_name1}"
    assertNoCache nested_mtmv_sql1

    sql "select * from ${mv_name1}"
    assertHasCache "select * from ${mv_name1}"

    sql "REFRESH MATERIALIZED VIEW ${mv_name2} AUTO;"
    waitingMTMVTaskFinishedByMvName(mv_name2)
    sleep(15 * 1000)
    assertNoCache "select * from ${mv_name2}"
    sql "select * from ${mv_name2}"
    assertHasCache "select * from ${mv_name2}"

    assertHasCache mtmv_sql1

    sql mtmv_sql4
    sql nested_mtmv_sql1
    assertHasCache mtmv_sql4
    assertHasCache nested_mtmv_sql1

    // insert overwrite
    sql "INSERT OVERWRITE table ${tb_name1} PARTITION(p4) VALUES (4, 6);"
    sleep(15 * 1000)
    assertHasCache "select * from ${mv_name1}"
    assertNoCache mtmv_sql4
    assertHasCache "select * from ${nested_mv_name1}"
    assertHasCache nested_mtmv_sql1

    sql mtmv_sql4
    assertHasCache mtmv_sql4

    sql "REFRESH MATERIALIZED VIEW ${mv_name1} AUTO;"
    waitingMTMVTaskFinishedByMvName(mv_name1)
    sleep(15 * 1000)
    assertNoCache "select * from ${mv_name1}"
    assertHasCache "select * from ${nested_mv_name1}"
    assertNoCache nested_mtmv_sql1

    sql "select * from ${mv_name1}"
    sql nested_mtmv_sql1

    assertHasCache "select * from ${mv_name1}"
    assertHasCache "select * from ${nested_mv_name1}"
    assertHasCache nested_mtmv_sql1

    // add partition
    // 给基表插入分区之后是否仍然能
    sql "alter table ${tb_name1} add partition p7 values[('7'),('8'))"
    assertHasCache "select * from ${mv_name1}"
    assertHasCache mtmv_sql4
    assertHasCache "select * from ${nested_mv_name1}"
    assertHasCache nested_mtmv_sql1

    // insert data
    sql "insert into ${tb_name1} values(7, 1)"
    sleep(15 * 1000)
    assertHasCache "select * from ${mv_name1}"
    assertNoCache mtmv_sql4
    assertHasCache "select * from ${nested_mv_name1}"
    assertHasCache nested_mtmv_sql1

    sql "REFRESH MATERIALIZED VIEW ${mv_name1} AUTO;"
    waitingMTMVTaskFinishedByMvName(mv_name1)
    sleep(15 * 1000)
    assertNoCache "select * from ${mv_name1}"
    assertNoCache mtmv_sql4
    assertHasCache "select * from ${nested_mv_name1}"
    assertNoCache nested_mtmv_sql1



}
