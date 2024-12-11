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

suite("test_random_upgrade_downgrade_compatibility_auth_1","p0,auth,restart_fe") {

    def judge_version = { def version_1, def version_2_0, def version_2_1, def version_3, def url ->
        logger.info("url: " + url)
        def res = sql """show grants"""
        logger.info("res: " + res)

        def version_res = sql """show frontends;"""
        logger.info("version_res:" + version_res)
        version_1 = false
        version_2_0 = false
        version_2_1 = false
        version_3 = false
        if (version_res[0][15].startsWith("doris-1.")) {
            version_1 = true
        } else if (version_res[0][15].startsWith("doris-2.0.")) {
            version_1 = true
            version_2_0 = true
        } else if (version_res[0][17].startsWith("doris-2.1.")) {
            version_1 = true
            version_2_0 = true
            version_2_1 = true
        } else {
            version_1 = true
            version_2_0 = true
            version_2_1 = true
            version_3 = true
        }
    }

    String user1 = 'test_random_up_down_auth_user1'
    String role1 = 'test_random_up_down_auth_role1'
    String pwd = 'C123_567p'

    String dbName = 'test_auth_up_down_db'
    String tableName1 = 'test_auth_up_down_table1'
    String tableName2 = 'test_auth_up_down_table2'

    connect(user1, "${pwd}", context.config.jdbcUrl) {
        def version_1 = false
        def version_2_0 = false
        def version_2_1 = false
        def version_3 = false
        judge_version(version_1, version_2_0, version_2_1, version_3, context.config.jdbcUrl)

        def grants_res = sql """show grants;"""
        logger.info("grants_res:" + grants_res)
        if (version_2_0) {
            assertTrue(grants_res[0][2] == "")
            assertTrue(grants_res[0][5] == "hive2_test_random_up_down_auth_ctl.default_cluster:test_auth_up_down_db: Select_priv Load_priv Alter_priv Create_priv Drop_priv ; hive3_test_random_up_down_auth_ctl.default_cluster:test_auth_up_down_db: Select_priv Load_priv Alter_priv Create_priv Drop_priv ; internal.default_cluster:information_schema: Select_priv ; internal.default_cluster:mysql: Select_priv ; internal.default_cluster:regression_test: Select_priv ; internal.default_cluster:test_auth_up_down_db: Select_priv Load_priv Alter_priv Create_priv Drop_priv ")
            assertTrue(grants_res[0][6] == "hive2_test_random_up_down_auth_ctl.default_cluster:test_auth_up_down_db.test_auth_up_down_table1: Select_priv Load_priv Alter_priv Create_priv Drop_priv ; hive3_test_random_up_down_auth_ctl.default_cluster:test_auth_up_down_db.test_auth_up_down_table1: Select_priv Load_priv Alter_priv Create_priv Drop_priv ; internal.default_cluster:test_auth_up_down_db.test_auth_up_down_table1: Select_priv Load_priv Alter_priv Create_priv Drop_priv ")
        } else if (version_1) {
            assertTrue(grants_res[0][4] == """hive2_test_random_up_down_auth_ctl.default_cluster:test_auth_up_down_db: Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false); hive3_test_random_up_down_auth_ctl.default_cluster:test_auth_up_down_db: Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false); internal.default_cluster:information_schema: Select_priv  (false); internal.default_cluster:regression_test: Select_priv  (false); internal.default_cluster:test_auth_up_down_db: Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false)""")
            assertTrue(grants_res[0][5] == """hive2_test_random_up_down_auth_ctl.default_cluster:test_auth_up_down_db.test_auth_up_down_table1: Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false); hive3_test_random_up_down_auth_ctl.default_cluster:test_auth_up_down_db.test_auth_up_down_table1: Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false); internal.default_cluster:test_auth_up_down_db.test_auth_up_down_table1: Select_priv Load_priv Alter_priv Create_priv Drop_priv  (false)""")
        }
    }


    sql """revoke select_priv on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke select_priv on ${dbName} from ${user1}"""

    sql """revoke LOAD_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke LOAD_PRIV on ${dbName} from ${user1}"""

    sql """revoke ALTER_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke ALTER_PRIV on ${dbName} from ${user1}"""

    sql """revoke CREATE_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke CREATE_PRIV on ${dbName} from ${user1}"""

    sql """revoke DROP_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke DROP_PRIV on ${dbName} from ${user1}"""


    for (String hivePrefix : ["hive2", "hive3"]) {
        String catalog_name = "${hivePrefix}_test_random_up_down_auth_ctl"

        sql """revoke select_priv on ${catalog_name}.${dbName}.${tableName1} from ${user1}"""
        sql """revoke select_priv on ${catalog_name}.${dbName}.* from ${user1}"""

        sql """revoke LOAD_PRIV on ${catalog_name}.${dbName}.${tableName1} from ${user1}"""
        sql """revoke LOAD_PRIV on ${catalog_name}.${dbName}.* from ${user1}"""

        sql """revoke ALTER_PRIV on ${catalog_name}.${dbName}.${tableName1} from ${user1}"""
        sql """revoke ALTER_PRIV on ${catalog_name}.${dbName}.* from ${user1}"""

        sql """revoke CREATE_PRIV on ${catalog_name}.${dbName}.${tableName1} from ${user1}"""
        sql """revoke CREATE_PRIV on ${catalog_name}.${dbName}.* from ${user1}"""

        sql """revoke DROP_PRIV on ${catalog_name}.${dbName}.${tableName1} from ${user1}"""
        sql """revoke DROP_PRIV on ${catalog_name}.${dbName}.* from ${user1}"""

    }

    connect(user1, "${pwd}", context.config.jdbcUrl) {
        def version_1 = false
        def version_2_0 = false
        def version_2_1 = false
        def version_3 = false
        judge_version(version_1, version_2_0, version_2_1, version_3, context.config.jdbcUrl)

        def grants_res = sql """show grants;"""
        logger.info("grants_res:" + grants_res)
        if (version_2_0) {
            assertTrue(grants_res[0][2] == "")
            assertTrue(grants_res[0][5] == "internal.default_cluster:information_schema: Select_priv ; internal.default_cluster:mysql: Select_priv ; internal.default_cluster:regression_test: Select_priv ")
            assertTrue(grants_res[0][6] == null)
        } else if (version_1) {
            assertTrue(grants_res[0][4] == """internal.default_cluster:information_schema: Select_priv  (false); internal.default_cluster:regression_test: Select_priv  (false)""")
            assertTrue(grants_res[0][5] == null)
        }
    }

    sql """grant select_priv on ${dbName}.${tableName1} to ${user1}"""
    sql """grant select_priv on ${dbName} to ${user1}"""

    sql """grant LOAD_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant LOAD_PRIV on ${dbName} to ${user1}"""

    sql """grant ALTER_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant ALTER_PRIV on ${dbName} to ${user1}"""

    sql """grant CREATE_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant CREATE_PRIV on ${dbName} to ${user1}"""

    sql """grant DROP_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant DROP_PRIV on ${dbName} to ${user1}"""


    for (String hivePrefix : ["hive2", "hive3"]) {
        String catalog_name = "${hivePrefix}_test_random_up_down_auth_ctl"

        sql """grant select_priv on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
        sql """grant select_priv on ${catalog_name}.${dbName}.* to ${user1}"""

        sql """grant LOAD_PRIV on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
        sql """grant LOAD_PRIV on ${catalog_name}.${dbName}.* to ${user1}"""

        sql """grant ALTER_PRIV on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
        sql """grant ALTER_PRIV on ${catalog_name}.${dbName}.* to ${user1}"""

        sql """grant CREATE_PRIV on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
        sql """grant CREATE_PRIV on ${catalog_name}.${dbName}.* to ${user1}"""

        sql """grant DROP_PRIV on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
        sql """grant DROP_PRIV on ${catalog_name}.${dbName}.* to ${user1}"""

    }
}
