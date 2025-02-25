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

suite("test_upgrade_downgrade_olap_mtmv","p0,mtmv,restart_fe") {
    String suiteName = "mtmv_up_down_olap"
    String dbName = context.config.getDbNameByFile(context.file)
    String mvName = "${suiteName}_mtmv"
    String tableName = "${suiteName}_table"
    // test data is normal
    order_qt_refresh_init "SELECT * FROM ${mvName}"
    // test is sync
    order_qt_mtmv_sync "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} complete
        """
    // test can refresh success
    waitingMTMVTaskFinishedByMvName(mvName)


    String dropTableName1 = """${suiteName}_DropTableName1"""
    String dropTableName2 = """${suiteName}_DropTableName2"""
    String dropTableName4 = """${suiteName}_DropTableName4"""
    String dropMtmvName1 = """${suiteName}_dropMtmvName1"""
    String dropMtmvName2 = """${suiteName}_dropMtmvName2"""
    String dropMtmvName3 = """${suiteName}_dropMtmvName3"""

    def state_mtmv1 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${dropMtmvName1}';"""
    assertTrue(state_mtmv1[0][0] == "NORMAL")
    assertTrue(state_mtmv1[0][1] == "SUCCESS")
    assertTrue(state_mtmv1[0][2] == true)
    def state_mtmv2 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${dropMtmvName2}';"""
    assertTrue(state_mtmv2[0][0] == "NORMAL")
    assertTrue(state_mtmv2[0][1] == "SUCCESS")
    assertTrue(state_mtmv2[0][2] == true)


    // 删除原表
    sql """drop table ${dropTableName1}"""
    state_mtmv1 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${dropMtmvName1}';"""
    assertTrue(state_mtmv1[0][0] == "SCHEMA_CHANGE")
    assertTrue(state_mtmv1[0][1] == "SUCCESS")
    assertTrue(state_mtmv1[0][2] == false)

    def sql1 = "SELECT a.* FROM ${dropTableName1} a inner join ${dropTableName4} b on a.user_id=b.user_id;"
    mv_not_part_in(sql1, dropMtmvName1)


    // 删除表分区
    def parts_res = sql """show partitions from ${dropTableName2}"""
    sql """ALTER TABLE ${dropTableName2} DROP PARTITION ${parts_res[0][1]};"""
    state_mtmv2 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${dropMtmvName2}';"""
    assertTrue(state_mtmv2[0][0] == "NORMAL")
    assertTrue(state_mtmv2[0][1] == "SUCCESS")
    assertTrue(state_mtmv2[0][2] == false)
    def mtmv_part_res = sql """show partitions from ${dropMtmvName2}"""
    assertTrue(mtmv_part_res.size() == 3)
    assertTrue(mtmv_part_res[0][18] == false)
    assertTrue(mtmv_part_res[0][19] == dropTableName2)

    def sql2 = "SELECT a.* FROM ${dropTableName2} a inner join ${dropTableName4} b on a.user_id=b.user_id;"
    mv_rewrite_success(sql2, dropMtmvName2)

    // 单独刷新分区执行报错，且刷新之后分区没有被删除
    test {
        sql """refresh MATERIALIZED VIEW ${dropMtmvName2} partition(${mtmv_part_res[0][1]})"""
        exception "partition not exist"
    }

    // 刷新整个mtmv，分区会被删除
    sql """refresh MATERIALIZED VIEW ${dropMtmvName2} auto"""
    mtmv_part_res = sql """show partitions from ${dropMtmvName2}"""
    assertTrue(mtmv_part_res.size() == 2)

    state_mtmv2 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${dropMtmvName2}';"""
    assertTrue(state_mtmv2[0][0] == "NORMAL")
    assertTrue(state_mtmv2[0][1] == "SUCCESS")
    assertTrue(state_mtmv2[0][2] == true)
    mv_rewrite_success(sql2, dropMtmvName2)

    // 删除表之后新建mtmv
    sql """
        CREATE MATERIALIZED VIEW ${dropMtmvName3}
            REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT user_id, age FROM ${dropTableName4};
        """

}
