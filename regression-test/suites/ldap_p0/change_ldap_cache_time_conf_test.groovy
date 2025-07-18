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

suite("change_ldap_cache_time_conf_test", "external_docker, nonConcurrent, ldap_p0") {
    String enabled = context.config.otherConfigs.get("enableLdapTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("LDAP test is disabled in regression-conf.groovy, skipping.")
        return
    }

    String prefix_str = "z10102_"
    String dbName = prefix_str + "db"
    String tbName = prefix_str + "tb"
    sql """create database if not exists ${dbName}"""
    sql """drop table if exists ${dbName}.${tbName}"""
    sql """create table ${dbName}.${tbName} (
                id BIGINT,
                username VARCHAR(20)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );"""
    sql """
        insert into ${dbName}.`${tbName}` values 
        (1, "111"),
        (2, "222"),
        (3, "333");
        """

    // Get LDAP connection details from config
    String ldapHost = context.config.otherConfigs.get("ldapHost")
    String ldapPort = context.config.otherConfigs.get("ldapPort")
    String ldapAdminUser = context.config.otherConfigs.get("ldapUser")
    String ldapAdminPassword = context.config.otherConfigs.get("ldapPassword")
    String ldapBaseDn = context.config.otherConfigs.get("ldapBaseDn")

    sql """set ldap_admin_password = password('${ldapAdminPassword}');"""

    // Define the new test entities
    String testGroup = prefix_str + "group"
    String testUser = prefix_str + "user"
    String testUserPassword = "{SSHA}4fqyv30HZK25GEzQ8J7R+3Wa7gvnfzSu"
    String testUserPlaintextPassword = "654321"
    
    String testUserDn = "uid=${testUser},cn=${testGroup},${ldapBaseDn}"
    String testGroupDn = "cn=${testGroup},${ldapBaseDn}"

    for (String dn in [testUserDn, testGroupDn]) {
        deleteLdapEntry("""ldap://${ldapHost}:${ldapPort}""", ldapAdminUser, ldapAdminPassword, dn)
    }
    
    // Prepare the multi-entry LDIF file content
    String ldifContent = """dn: cn=${testGroup},${ldapBaseDn}
        objectClass: groupOfNames
        cn: ${testGroup}
        member: cn=${testGroup},${ldapBaseDn}
        member: uid=${testUser},cn=${testGroup},${ldapBaseDn}
        
        dn: uid=${testUser},cn=${testGroup},${ldapBaseDn}
        objectClass: person
        objectClass: inetOrgPerson
        cn: ${testUser}
        sn: ${testUser}
        uid: ${testUser}
        userPassword: ${testUserPassword}"""

    // Add OU, group, and user to LDAP server in one go
    addLdapEntry("""ldap://${ldapHost}:${ldapPort}""", ldapAdminUser, ldapAdminPassword, ldifContent)
    sql """REFRESH LDAP FOR ${testUser};"""

    // Create a role in Doris and a mapping for the LDAP group
    sql """drop role if exists ${testGroup}"""
    sql "CREATE ROLE '${testGroup}';"
    sql "GRANT SELECT_PRIV ON ${dbName}.${tbName} TO ROLE '${testGroup}';"

    logger.info("Successfully created role '${testGroup}' in Doris.")

    sql """ADMIN SET FRONTEND CONFIG ('ldap_user_cache_timeout_s' = '10')"""

    // Verify that the new user can log in and has the correct role's permissions
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/information_schema?authenticationPlugins=org.apache.doris.regression.util.MysqlClearPasswordPluginWithoutSSL&defaultAuthenticationPlugin=org.apache.doris.regression.util.MysqlClearPasswordPluginWithoutSSL&disabledAuthenticationPlugins=org.apache.doris.regression.util.MysqlClearPasswordPlugin"
    log.info("url: " + url)
    connect(testUser, testUserPlaintextPassword, url) {
        def grants = sql """show grants;"""
        logger.info("grants: " + grants)
        assertTrue(grants.toString().contains("${testGroup}"))
        def res = sql """select * from ${dbName}.${tbName}"""
        assertTrue(res.size() == 3)
        logger.info("SUCCESS: LDAP user '${testUser}' successfully logged in to Doris.")
    }

    deleteLdapEntry("""ldap://${ldapHost}:${ldapPort}""", ldapAdminUser, ldapAdminPassword, testUserDn)

    connect(testUser, testUserPlaintextPassword, url) {
        def grants = sql """show grants;"""
        logger.info("grants: " + grants)
        assertTrue(grants.toString().contains("${testGroup}"))
        def res = sql """select * from ${dbName}.${tbName}"""
        assertTrue(res.size() == 3)
        logger.info("SUCCESS: LDAP user '${testUser}' successfully logged in to Doris.")
    }

    sleep(10 * 1000)

    try {
        connect(testUser, testUserPlaintextPassword, url) {
            assert  false
        }
    } catch (Exception e) {
        log.info("e.getMessage(): " + e.getMessage())
        assertTrue(e.getMessage().contains('Access denied'))
    }

    // Clean up: always try to remove all created entities
    logger.info("Starting cleanup process...")
    sql "DROP ROLE '${testGroup}';"

    for (String dn in [testUserDn, testGroupDn]) {
        deleteLdapEntry("""ldap://${ldapHost}:${ldapPort}""", ldapAdminUser, ldapAdminPassword, dn)
    }

    sql """ADMIN SET FRONTEND CONFIG ('ldap_user_cache_timeout_s' = '300')"""

}
