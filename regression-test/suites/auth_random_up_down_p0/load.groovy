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

    String user1 = 'test_random_up_down_auth_user1'
    String role1 = 'test_random_up_down_auth_role1'
    String pwd = 'C123_567p'

    String dbName = 'test_auth_up_down_db'
    String tableName1 = 'test_auth_up_down_table1'
    String tableName2 = 'test_auth_up_down_table2'

    // grant/revoke
    // catalog/database/table/column
    // SELECT_PRIV/LOAD_PRIV/ALTER_PRIV/CREATE_PRIV/DROP_PRIV/SHOW_VIEW_PRIV

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

    sql """grant select_priv on ${dbName}.${tableName1}.id to ${user1}"""
    sql """grant select_priv on ${dbName}.${tableName1} to ${user1}"""
    sql """grant select_priv on ${dbName} to ${user1}"""
    sql """grant LOAD_PRIV on ${dbName}.${tableName1}.id to ${user1}"""
    sql """grant LOAD_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant LOAD_PRIV on ${dbName} to ${user1}"""
    sql """grant ALTER_PRIV on ${dbName}.${tableName1}.id to ${user1}"""
    sql """grant ALTER_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant ALTER_PRIV on ${dbName} to ${user1}"""
    sql """grant CREATE_PRIV on ${dbName}.${tableName1}.id to ${user1}"""
    sql """grant CREATE_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant CREATE_PRIV on ${dbName} to ${user1}"""
    sql """grant DROP_PRIV on ${dbName}.${tableName1}.id to ${user1}"""
    sql """grant DROP_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant DROP_PRIV on ${dbName} to ${user1}"""

    connect(user1, "${pwd}", context.config.jdbcUrl) {
        def grants_res = sql """show grants;"""
        assertTrue(grants_res[0][4] == """internal.default_cluster:information_schema: Select_priv  (false); internal.default_cluster:regression_test: Select_priv  (false); internal.default_cluster:test_auth_up_down_db: Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false)""")
        assertTrue(grants_res[0][5] == """internal.default_cluster:test_auth_up_down_db.test_auth_up_down_table1: Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false); test_auth_up_down_db.default_cluster:test_auth_up_down_table1.id: Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false)""")
    }


    sql """revoke select_priv on ${dbName}.${tableName1}.id from ${user1}"""
    sql """revoke select_priv on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke select_priv on ${dbName} from ${user1}"""
    sql """revoke LOAD_PRIV on ${dbName}.${tableName1}.id from ${user1}"""
    sql """revoke LOAD_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke LOAD_PRIV on ${dbName} from ${user1}"""
    sql """revoke ALTER_PRIV on ${dbName}.${tableName1}.id from ${user1}"""
    sql """revoke ALTER_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke ALTER_PRIV on ${dbName} from ${user1}"""
    sql """revoke CREATE_PRIV on ${dbName}.${tableName1}.id from ${user1}"""
    sql """revoke CREATE_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke CREATE_PRIV on ${dbName} from ${user1}"""
    sql """revoke DROP_PRIV on ${dbName}.${tableName1}.id from ${user1}"""
    sql """revoke DROP_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke DROP_PRIV on ${dbName} from ${user1}"""
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        def grants_res = sql """show grants;"""
        assertTrue(grants_res[0][4] == """internal.default_cluster:information_schema: Select_priv  (false); internal.default_cluster:regression_test: Select_priv  (false)""")
        logger.info("grants_res[0][5]: |" + grants_res[0][5] + "|")
        assertTrue(grants_res[0][5] == """NULL""")
    }


}
