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

// 不让做checkpoint操作，需要在fe.conf文件设置enable_checkpoint = false
// 测试checkpoint操作生成的image，需要配置edit_log_roll_num = 10，配置一个比较小的值，加快触发checkpoint操作

// checkpoint 开关只在2.1和3.0，2.0和1.2没有这个开关
// 预期：1.2 -> 2.0 -> 2.1 -> 3.0
// 实际：
// 1.2 -> 2.0     default_cluster     最好是可以直接升三个版本，评估一下，用户是否使用的多   滚动升级性价比不高
// 2.0 -> 2.1
// 2.1 -> 3.0
// 1.2 -> 2.1
//

//
//

// 流程上doris低版本不能直接操作hive的数据，需要首先在hive上创建好环境
// 如果不想手动创建，可以先搭建高版本的环境，执行此load文件，可以把环境创建好，之后切换到其他环境正常做升降级流程测试

suite("test_random_upgrade_downgrade_prepare_auth","p0,auth,restart_fe") {

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

    def version_res = sql """show frontends;"""
    def version_1 = false
    def version_2_0 = false
    def version_2_1 = false
    def version_3 = false
    if (version_res[0][21].startsWith("doris-1.")) {
        version_1 = true
    } else if (version_res[0][15].startsWith("doris-2.0.")) {
        version_1 = true
        version_2_0 = true
    } else if (version_res[0][21].startsWith("doris-2.1.")) {
        version_1 = true
        version_2_0 = true
        version_2_1 = true
    } else {
        version_1 = true
        version_2_0 = true
        version_2_1 = true
        version_3 = true
    }

    if (version_1) {
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
    }
    if (version_2_0) {
        sql """grant SHOW_VIEW_PRIV on ${dbName}.${tableName1} to ${user1}"""
        sql """grant SHOW_VIEW_PRIV on ${dbName} to ${user1}"""

        sql """grant select_priv on ${dbName}.${tableName1} to ROLE '${role1}'"""
        sql """grant select_priv on ${dbName} to ROLE '${role1}'"""

        sql """grant LOAD_PRIV on ${dbName}.${tableName1} to ROLE '${role1}'"""
        sql """grant LOAD_PRIV on ${dbName} to ROLE '${role1}'"""

        sql """grant ALTER_PRIV on ${dbName}.${tableName1} to ROLE '${role1}'"""
        sql """grant ALTER_PRIV on ${dbName} to ROLE '${role1}'"""

        sql """grant CREATE_PRIV on ${dbName}.${tableName1} to ROLE '${role1}'"""
        sql """grant CREATE_PRIV on ${dbName} to ROLE '${role1}'"""

        sql """grant DROP_PRIV on ${dbName}.${tableName1} to ROLE '${role1}'"""
        sql """grant DROP_PRIV on ${dbName} to ROLE '${role1}'"""

        sql """grant SHOW_VIEW_PRIV on ${dbName}.${tableName1} to ROLE '${role1}'"""
        sql """grant SHOW_VIEW_PRIV on ${dbName} to ROLE '${role1}'"""
    }
    if (version_2_1) {
        sql """grant select_priv(id) on ${dbName}.${tableName1} to ${user1}"""
        sql """grant select_priv(id) on ${dbName}.${tableName1} to ROLE '${role1}'"""
    }
    if (version_3) {

    }


    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_random_up_down_auth_ctl"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""

        sql """switch ${catalog_name}"""
        if (version_2_1 || version_3) {
            sql """create database if not exists ${dbName}"""
            sql """
                CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName1}` (
                    id INT,
                    username VARCHAR(20)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES (
                    "replication_num" = "1"
                );
                """
            sql """INSERT INTO ${dbName}.${tableName1} VALUES(1,"clz"),(2,"zhangsang");"""
        }

        if (version_1) {
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
        if (version_2_0) {
            sql """grant SHOW_VIEW_PRIV on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
            sql """grant SHOW_VIEW_PRIV on ${catalog_name}.${dbName}.* to ${user1}"""

            sql """grant select_priv on ${catalog_name}.${dbName}.${tableName1} to ROLE '${role1}'"""
            sql """grant select_priv on ${catalog_name}.${dbName}.* to ROLE '${role1}'"""

            sql """grant LOAD_PRIV on ${catalog_name}.${dbName}.${tableName1} to ROLE '${role1}'"""
            sql """grant LOAD_PRIV on ${catalog_name}.${dbName}.* to ROLE '${role1}'"""

            sql """grant ALTER_PRIV on ${catalog_name}.${dbName}.${tableName1} to ROLE '${role1}'"""
            sql """grant ALTER_PRIV on ${catalog_name}.${dbName}.* to ROLE '${role1}'"""

            sql """grant CREATE_PRIV on ${catalog_name}.${dbName}.${tableName1} to ROLE '${role1}'"""
            sql """grant CREATE_PRIV on ${catalog_name}.${dbName}.* to ROLE '${role1}'"""

            sql """grant DROP_PRIV on ${catalog_name}.${dbName}.${tableName1} to ROLE '${role1}'"""
            sql """grant DROP_PRIV on ${catalog_name}.${dbName}.* to ROLE '${role1}'"""

            sql """grant SHOW_VIEW_PRIV on ${catalog_name}.${dbName}.${tableName1} to ROLE '${role1}'"""
            sql """grant SHOW_VIEW_PRIV on ${catalog_name}.${dbName}.* to ROLE '${role1}'"""
        }
        if (version_2_1) {
            sql """grant select_priv(id) on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
            sql """grant select_priv(id) on ${catalog_name}.${dbName}.${tableName1} to ROLE '${role1}'"""
        }
        if (version_3) {

        }
    }





}
