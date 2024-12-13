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

suite("test_random_upgrade_downgrade_compatibility_auth","p0,auth,restart_fe") {

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
    String tableName2 = 'test_auth_up_down_table2'

    // grant/revoke
    // catalog/database/table/column
    // SELECT_PRIV/LOAD_PRIV/ALTER_PRIV/CREATE_PRIV/DROP_PRIV/SHOW_VIEW_PRIV

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
    if (version_no_1) {
        sql """grant SHOW_VIEW_PRIV on ${dbName}.${tableName1}.id to ${user1}"""
        sql """grant SHOW_VIEW_PRIV on ${dbName}.${tableName1} to ${user1}"""
        sql """grant SHOW_VIEW_PRIV on ${dbName} to ${user1}"""
    }



    connect(user1, "${pwd}", context.config.jdbcUrl) {
        def grants_res = sql """show grants;"""
        logger.info("grants_res1: " + grants_res)
        if (!version_no_1) {
            assertTrue(grants_res[0][4] == """internal.default_cluster:information_schema: Select_priv  (false); internal.default_cluster:regression_test: Select_priv  (false); internal.default_cluster:test_auth_up_down_db: Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false)""")
            assertTrue(grants_res[0][5] == """internal.default_cluster:test_auth_up_down_db.test_auth_up_down_table1: Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false); test_auth_up_down_db.default_cluster:test_auth_up_down_table1.id: Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false)""")
        } else {
            logger.info("grants_res[0][2]: |" + grants_res[0][2] + "|")
            logger.info("grants_res[0][5]: |" + grants_res[0][5] + "|")
            logger.info("grants_res[0][6]: |" + grants_res[0][6] + "|")
            assertTrue(grants_res[0][2] == "")
            assertTrue(grants_res[0][5] == "internal.default_cluster:information_schema: Select_priv ; internal.default_cluster:mysql: Select_priv ; internal.default_cluster:regression_test: Select_priv ; internal.default_cluster:test_auth_up_down_db: Select_priv Load_priv Alter_priv Create_priv Drop_priv Show_view_priv ")
            assertTrue(grants_res[0][6] == "internal.default_cluster:test_auth_up_down_db.test_auth_up_down_table1: Select_priv Load_priv Alter_priv Create_priv Drop_priv Show_view_priv ; test_auth_up_down_db.default_cluster:test_auth_up_down_table1.id: Select_priv Load_priv Alter_priv Create_priv Drop_priv Show_view_priv ")
        }

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
    if (version_no_1) {
        sql """revoke SHOW_VIEW_PRIV on ${dbName}.${tableName1}.id from ${user1}"""
        sql """revoke SHOW_VIEW_PRIV on ${dbName}.${tableName1} from ${user1}"""
        sql """revoke SHOW_VIEW_PRIV on ${dbName} from ${user1}"""
    }


    connect(user1, "${pwd}", context.config.jdbcUrl) {
        def grants_res = sql """show grants;"""
        logger.info("grants_res2: " + grants_res)
        if (!version_no_1) {
            assertTrue(grants_res[0][4] == """internal.default_cluster:information_schema: Select_priv  (false); internal.default_cluster:regression_test: Select_priv  (false)""")
            assertTrue(grants_res[0][5] == null)
        } else {
            logger.info("grants_res[0][5]: |" + grants_res[0][5] + "|")
            logger.info("grants_res[0][6]: |" + grants_res[0][6] + "|")
            assertTrue(grants_res[0][2] == "")
            assertTrue(grants_res[0][5] == "internal.default_cluster:information_schema: Select_priv ; internal.default_cluster:mysql: Select_priv ; internal.default_cluster:regression_test: Select_priv ")
            assertTrue(grants_res[0][6] == null)
        }

    }

    if (version_no_1) {
        sql """grant '${role1}' to '${user1}'"""

        sql """grant select_priv on ${dbName}.${tableName1}.id to ROLE '${role1}'"""
        sql """grant select_priv on ${dbName}.${tableName1} to ROLE '${role1}'"""
        sql """grant select_priv on ${dbName} to ROLE '${role1}'"""
        sql """grant LOAD_PRIV on ${dbName}.${tableName1}.id to ROLE '${role1}'"""
        sql """grant LOAD_PRIV on ${dbName}.${tableName1} to ROLE '${role1}'"""
        sql """grant LOAD_PRIV on ${dbName} to ROLE '${role1}'"""
        sql """grant ALTER_PRIV on ${dbName}.${tableName1}.id to ROLE '${role1}'"""
        sql """grant ALTER_PRIV on ${dbName}.${tableName1} to ROLE '${role1}'"""
        sql """grant ALTER_PRIV on ${dbName} to ROLE '${role1}'"""
        sql """grant CREATE_PRIV on ${dbName}.${tableName1}.id to ROLE '${role1}'"""
        sql """grant CREATE_PRIV on ${dbName}.${tableName1} to ROLE '${role1}'"""
        sql """grant CREATE_PRIV on ${dbName} to ROLE '${role1}'"""
        sql """grant DROP_PRIV on ${dbName}.${tableName1}.id to ROLE '${role1}'"""
        sql """grant DROP_PRIV on ${dbName}.${tableName1} to ROLE '${role1}'"""
        sql """grant DROP_PRIV on ${dbName} to ROLE '${role1}'"""
        sql """grant SHOW_VIEW_PRIV on ${dbName}.${tableName1}.id to ROLE '${role1}'"""
        sql """grant SHOW_VIEW_PRIV on ${dbName}.${tableName1} to ROLE '${role1}'"""
        sql """grant SHOW_VIEW_PRIV on ${dbName} to ROLE '${role1}'"""


        connect(user1, "${pwd}", context.config.jdbcUrl) {
            def grants_res = sql """show grants;"""
            logger.info("grants_res[0][2]: |" + grants_res[0][2] + "|")
            logger.info("grants_res[0][5]: |" + grants_res[0][5] + "|")
            logger.info("grants_res[0][6]: |" + grants_res[0][6] + "|")
            assertTrue(grants_res[0][2] == """default_cluster:test_random_up_down_auth_role1""")
            assertTrue(grants_res[0][5] == "internal.default_cluster:information_schema: Select_priv ; internal.default_cluster:mysql: Select_priv ; internal.default_cluster:regression_test: Select_priv ; internal.default_cluster:test_auth_up_down_db: Select_priv Load_priv Alter_priv Create_priv Drop_priv Show_view_priv ")
            assertTrue(grants_res[0][6] == "internal.default_cluster:test_auth_up_down_db.test_auth_up_down_table1: Select_priv Load_priv Alter_priv Create_priv Drop_priv Show_view_priv ; test_auth_up_down_db.default_cluster:test_auth_up_down_table1.id: Select_priv Load_priv Alter_priv Create_priv Drop_priv Show_view_priv ")
        }

        sql """revoke '${role1}' from '${user1}'"""
        connect(user1, "${pwd}", context.config.jdbcUrl) {
            def grants_res = sql """show grants;"""
            assertTrue(grants_res[0][2] == "")
            assertTrue(grants_res[0][5] == "internal.default_cluster:information_schema: Select_priv ; internal.default_cluster:mysql: Select_priv ; internal.default_cluster:regression_test: Select_priv ; internal.default_cluster:test_auth_up_down_db: Select_priv Load_priv Alter_priv Create_priv Drop_priv Show_view_priv ")
            assertTrue(grants_res[0][6] == "internal.default_cluster:test_auth_up_down_db.test_auth_up_down_table1: Select_priv Load_priv Alter_priv Create_priv Drop_priv Show_view_priv ; test_auth_up_down_db.default_cluster:test_auth_up_down_table1.id: Select_priv Load_priv Alter_priv Create_priv Drop_priv Show_view_priv ")
        }

        sql """grant '${role1}' to '${user1}'"""
        sql """revoke select_priv on ${dbName}.${tableName1}.id from ROLE '${role1}'"""
        sql """revoke select_priv on ${dbName}.${tableName1} from ROLE '${role1}'"""
        sql """revoke select_priv on ${dbName} from ROLE '${role1}'"""
        sql """revoke LOAD_PRIV on ${dbName}.${tableName1}.id from ROLE '${role1}'"""
        sql """revoke LOAD_PRIV on ${dbName}.${tableName1} from ROLE '${role1}'"""
        sql """revoke LOAD_PRIV on ${dbName} from ROLE '${role1}'"""
        sql """revoke ALTER_PRIV on ${dbName}.${tableName1}.id from ROLE '${role1}'"""
        sql """revoke ALTER_PRIV on ${dbName}.${tableName1} from ROLE '${role1}'"""
        sql """revoke ALTER_PRIV on ${dbName} from ROLE '${role1}'"""
        sql """revoke CREATE_PRIV on ${dbName}.${tableName1}.id from ROLE '${role1}'"""
        sql """revoke CREATE_PRIV on ${dbName}.${tableName1} from ROLE '${role1}'"""
        sql """revoke CREATE_PRIV on ${dbName} from ROLE '${role1}'"""
        sql """revoke DROP_PRIV on ${dbName}.${tableName1}.id from ROLE '${role1}'"""
        sql """revoke DROP_PRIV on ${dbName}.${tableName1} from ROLE '${role1}'"""
        sql """revoke DROP_PRIV on ${dbName} from ROLE '${role1}'"""
        sql """revoke SHOW_VIEW_PRIV on ${dbName}.${tableName1}.id from ROLE '${role1}'"""
        sql """revoke SHOW_VIEW_PRIV on ${dbName}.${tableName1} from ROLE '${role1}'"""
        sql """revoke SHOW_VIEW_PRIV on ${dbName} from ROLE '${role1}'"""

        connect(user1, "${pwd}", context.config.jdbcUrl) {
            def grants_res = sql """show grants;"""
            assertTrue(grants_res[0][2] == """default_cluster:test_random_up_down_auth_role1 """)
            assertTrue(grants_res[0][5] == "internal.default_cluster:information_schema: Select_priv ; internal.default_cluster:mysql: Select_priv ; internal.default_cluster:regression_test: Select_priv ; internal.default_cluster:test_auth_up_down_db: Select_priv Load_priv Alter_priv Create_priv Drop_priv Show_view_priv ")
            assertTrue(grants_res[0][6] == "internal.default_cluster:test_auth_up_down_db.test_auth_up_down_table1: Select_priv Load_priv Alter_priv Create_priv Drop_priv Show_view_priv ; test_auth_up_down_db.default_cluster:test_auth_up_down_table1.id: Select_priv Load_priv Alter_priv Create_priv Drop_priv Show_view_priv ")
        }
    }


//    如何检测远程机器上文件是否存在
}
