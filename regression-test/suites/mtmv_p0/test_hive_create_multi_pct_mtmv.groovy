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

suite("test_hive_create_multi_pct_mtmv","mtmv") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return
    }
    sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
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
    String suiteName = "test_hive_multi_pct_mtmv_"
    String db_name = suiteName + "db"
    sql """create database if not exists ${db_name}"""
    sql """use ${db_name}"""
    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        String catalog_name = "${suiteName}${hivePrefix}_catalog"
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
                );"""

        def hive_database = suiteName + context.config.getDbNameByFile(context.file)
        def hive_table1 = suiteName + "table1"
        def hive_table2 = suiteName + "table2"

        def sql_str1 = """SELECT t2.user_id, t1.num, t1.year, t1.month as month 
            from ${catalog_name}.${hive_database}.${hive_table1} t1 
            inner join ${catalog_name}.${hive_database}.${hive_table2} t2 
            on t1.month = t2.month"""
        def sql_str2 = """SELECT t1.user_id, t1.num, t1.year, t1.month  
            from ${catalog_name}.${hive_database}.${hive_table1} t1 
            union all
            SELECT t2.user_id, t2.num, t2.year, t2.month  
            from ${catalog_name}.${hive_database}.${hive_table2} t2"""
        def sql_list = [sql_str1, sql_str2]

        for (int i = 0; i < sql_list.size(); i++) {
            sql """ create database if not exists ${catalog_name}.${hive_database} """
            sql """ drop table if exists ${catalog_name}.${hive_database}.${hive_table1} """
            sql """ drop table if exists ${catalog_name}.${hive_database}.${hive_table2} """
            sql """ CREATE TABLE ${catalog_name}.${hive_database}.${hive_table1} (
                user_id INT,
                num INT,
                year int,
                month int
            )                            
            PARTITION BY LIST (year, month) ()
            PROPERTIES (
              'file_format'='orc',
              'compression'='zlib'
            );                      
            """
            sql """ CREATE TABLE ${catalog_name}.${hive_database}.${hive_table2} (
                user_id INT,
                num INT,
                year int,
                month int
            )                            
            PARTITION BY LIST (year, month) ()
            PROPERTIES (
              'file_format'='orc',
              'compression'='zlib'
            );
            """

            sql """INSERT INTO ${catalog_name}.${hive_database}.${hive_table1}(user_id, num, year, month) values (1,1,2020, 1);"""
            sql """INSERT INTO ${catalog_name}.${hive_database}.${hive_table1}(user_id, num, year, month) values (2,2,2020, null);"""
            sql """INSERT INTO ${catalog_name}.${hive_database}.${hive_table2}(user_id, num, year, month) values (1,1,2020, 1);"""
            sql """INSERT INTO ${catalog_name}.${hive_database}.${hive_table2}(user_id, num, year, month) values (2,2,2020, null);"""

            sql """use ${db_name}"""
            def mvName = suiteName + "hive_mtmv"
            def sql_str = sql_list[i]
            sql """drop materialized view if exists ${mvName};"""
            sql """
                CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by(`month`)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                ${sql_str}
                """
            sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
            waitingMTMVTaskFinishedByMvName(mvName, db_name)
            if (i == 0) {
                mv_rewrite_success_without_check_chosen(sql_str, mvName)
            }
            compare_res(sql_str + " order by 1,2,3,4,5")

            sql """INSERT INTO ${catalog_name}.${hive_database}.${hive_table1}(user_id, num, year, month) values (3,3,2020, 3);"""
            waitingMTMVTaskFinishedByMvName(mvName, db_name)
            if (i == 0) {
                mv_rewrite_success_without_check_chosen(sql_str, mvName)
            }
            compare_res(sql_str + " order by 1,2,3,4,5")

            sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
            waitingMTMVTaskFinishedByMvName(mvName, db_name)
            if (i == 0) {
                mv_rewrite_success_without_check_chosen(sql_str, mvName)
            }
            compare_res(sql_str + " order by 1,2,3,4,5")

            sql """INSERT INTO ${catalog_name}.${hive_database}.${hive_table2}(user_id, num, year, month) values (3,3,2020, 3);"""
            waitingMTMVTaskFinishedByMvName(mvName, db_name)
            if (i == 0) {
                mv_rewrite_success_without_check_chosen(sql_str, mvName)
            }
            compare_res(sql_str + " order by 1,2,3,4,5")

            sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
            waitingMTMVTaskFinishedByMvName(mvName, db_name)
            if (i == 0) {
                mv_rewrite_success_without_check_chosen(sql_str, mvName)
            }
            compare_res(sql_str + " order by 1,2,3,4,5")

        }
    }

}
