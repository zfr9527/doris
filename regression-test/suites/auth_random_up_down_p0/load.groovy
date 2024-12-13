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

suite("test_random_upgrade_downgrade_prepare_auth","p0,auth,restart_fe") {

    def version_res = sql """show backends;"""
    def version_no_1 = true
    if (version_res[0][21].startsWith("doris-1.")) {
        version_no_1 = false
    }

    String user1 = 'test_random_up_down_auth_user1'
    String role1 = 'test_random_up_down_auth_role1'
    String pwd = 'C123_567p'

    String dbName = 'test_auth_up_down_db'
    String tableName1 = 'test_auth_up_down_table1'

    try_sql("DROP USER ${user1}")
    try_sql("DROP role ${role1}")
    sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user1}"""
    sql """CREATE ROLE ${role1}"""

    try_sql """drop table if exists ${dbName}.${tableName1}"""
    sql """drop database if exists ${dbName}"""
    sql """create database ${dbName}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName1}` (
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

    def priv_level_list = ["${dbName}.${tableName1}.id", "${dbName}.${tableName1}", "${dbName}"]
    def priv_type_list = ["SELECT_PRIV", "LOAD_PRIV", "ALTER_PRIV", "CREATE_PRIV", "DROP_PRIV", "SHOW_VIEW_PRIV"]

    // grant all priv to user
    for (int i = 0; i < priv_level_list.size(); i++) {
        for (int j = 0; j < priv_type_list.size()-1; j++) {
            sql """grant ${priv_type_list[j]} on ${priv_level_list[i]} to ${user1}"""
        }
    }
    if (version_no_1) {
        for (int i = 0; i < priv_level_list.size(); i++) {
            for (int j = priv_type_list.size()-1; j < priv_type_list.size(); j++) {
                sql """grant ${priv_type_list[j]} on ${priv_level_list[i]} to ${user1}"""
            }
        }
    }

    // grant all priv to role
    if (version_no_1) {
        for (int i = 0; i < priv_level_list.size(); i++) {
            for (int j = 0; j < priv_type_list.size(); j++) {
                sql """grant ${priv_type_list[j]} on ${priv_level_list[i]} to ROLE '${role1}'"""
            }
        }
    }
}
