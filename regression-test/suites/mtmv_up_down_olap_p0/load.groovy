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

suite("test_upgrade_downgrade_prepare_olap_mtmv","p0,mtmv,restart_fe") {
    String suiteName = "mtmv_up_down_olap"
    String mvName = "${suiteName}_mtmv"
    String tableName = "${suiteName}_table"
    String tableName2 = "${suiteName}_table2"

    sql """drop materialized view if exists ${mvName};"""
    sql """drop table if exists `${tableName}`"""
    sql """drop table if exists `${tableName2}`"""

    sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_all VALUES [('2017-03-01'), ('2017-04-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3);
        """

    sql """
        CREATE TABLE `${tableName2}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `age` SMALLINT NOT NULL COMMENT '\"年龄\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `age`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName2} values(1,1),(2,2),(3,3);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${tableName} a inner join ${tableName2} b on a.user_id=b.user_id;
    """
    waitingMTMVTaskFinishedByMvName(mvName)


    String dropTableName1 = """${suiteName}_DropTableName1"""
    String dropTableName2 = """${suiteName}_DropTableName2"""
    String dropTableName4 = """${suiteName}_DropTableName4"""
    String dropMtmvName1 = """${suiteName}_dropMtmvName1"""
    String dropMtmvName2 = """${suiteName}_dropMtmvName2"""
    String dropMtmvName3 = """${suiteName}_dropMtmvName3"""

    sql """drop materialized view if exists ${dropMtmvName1};"""
    sql """drop materialized view if exists ${dropMtmvName2};"""
    sql """drop materialized view if exists ${dropMtmvName3};"""
    sql """drop table if exists `${dropTableName1}`"""
    sql """drop table if exists `${dropTableName2}`"""
    sql """drop table if exists `${dropTableName4}`"""


    sql """
        CREATE TABLE `${dropTableName1}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_all VALUES [('2017-03-01'), ('2017-04-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${dropTableName1} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3);
        """

    sql """
        CREATE TABLE `${dropTableName2}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_all VALUES [('2017-03-01'), ('2017-04-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${dropTableName2} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3);
        """

    sql """
        CREATE TABLE `${dropTableName4}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `age` SMALLINT NOT NULL COMMENT '\"年龄\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `age`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${dropTableName4} values(1,1),(2,2),(3,3);
        """


    sql """
        CREATE MATERIALIZED VIEW ${dropMtmvName1}
            REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${dropTableName1} a inner join ${dropTableName4} b on a.user_id=b.user_id;
    """
    waitingMTMVTaskFinishedByMvName(dropMtmvName1)

    sql """
        CREATE MATERIALIZED VIEW ${dropMtmvName2}
            REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${dropTableName2} a inner join ${dropTableName4} b on a.user_id=b.user_id;
    """
    waitingMTMVTaskFinishedByMvName(dropMtmvName2)

    def state_mtmv1 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${dropMtmvName1}';"""
    assertTrue(state_mtmv1[0][0] == "NORMAL")
    assertTrue(state_mtmv1[0][1] == "SUCCESS")
    assertTrue(state_mtmv1[0][2] == true)
    def state_mtmv2 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${dropMtmvName2}';"""
    assertTrue(state_mtmv2[0][0] == "NORMAL")
    assertTrue(state_mtmv2[0][1] == "SUCCESS")
    assertTrue(state_mtmv2[0][2] == true)

}
