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

suite("ldap_special_password_char", "external_docker") {
    String enabled = context.config.otherConfigs.get("enableLdapTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("LDAP test is disabled in regression-conf.groovy, skipping.")
        return
    }

    String prefix_str = "z10091_"

    // Get LDAP connection details from config
    String ldapHost = context.config.otherConfigs.get("ldapHost")
    String ldapPort = context.config.otherConfigs.get("ldapPort")
    String ldapAdminUser = context.config.otherConfigs.get("ldapUser")
    String ldapAdminPassword = context.config.otherConfigs.get("ldapPassword")
    String ldapBaseDn = context.config.otherConfigs.get("ldapBaseDn")

    sql """set ldap_admin_password = password('${ldapAdminPassword}');"""

    // "test#comment",
    def special_character = ["test,group", "test+admin", "test\\user", "user;rm -rf /", "user' OR 1=1--", "user\$(id)", "\"test\"", "test user", "user\\00", "一"]

    for (def each_it : special_character) {
        logger.info("each_it: " + each_it)
        // Define the new test entities
        String testGroup = each_it
        String testUser = prefix_str + "user"
        String testUserPassword = "C123_567p"
        String testUserPlaintextPassword = "C123_567p"

        String testUserDn = "uid=${testUser},cn=${testGroup},${ldapBaseDn}"
        String testGroupDn = "cn=${testGroup},${ldapBaseDn}"

        for (String dn in [testUserDn, testGroupDn]) {
            deleteLdapEntry("""ldap://${ldapHost}:${ldapPort}""", ldapAdminUser, ldapAdminPassword, dn)
        }

        // Prepare the multi-entry LDIF file content
        String ldifContent = """dn: cn=${testGroup},${ldapBaseDn}
        objectClass: groupOfNames
        cn: ${testGroup}
        member: uid=${testUser},cn=${testGroup},${ldapBaseDn}
        
        dn: uid=${testUser},cn=${testGroup},${ldapBaseDn}
        objectClass: person
        objectClass: inetOrgPerson
        cn: ${testUser}
        sn: ${testUser}
        uid: ${testUser}
        userPassword: ${testUserPassword}"""

//        sql """drop role if exists '${testGroup}'"""
        try {
            addLdapEntry("""ldap://${ldapHost}:${ldapPort}""", ldapAdminUser, ldapAdminPassword, ldifContent)
            assert false
        } catch (Exception e) {
            log.info("e.getMessage(): " + e.getMessage())
            assertTrue(e.getMessage().contains('invalid DN'))
        }

//        sql """REFRESH LDAP FOR '${testUser}';"""
//
//        def tokens = context.config.jdbcUrl.split('/')
//        def url = tokens[0] + "//" + tokens[2] + "/" + "information_schema?authenticationPlugins=org.apache.doris.regression.util.MysqlClearPasswordPluginWithoutSSL&defaultAuthenticationPlugin=org.apache.doris.regression.util.MysqlClearPasswordPluginWithoutSSL&disabledAuthenticationPlugins=org.apache.doris.regression.util.MysqlClearPasswordPlugin"
//        log.info("url: " + url)
//        connect(testUser, testUserPlaintextPassword, url) {
//            def grants = sql """show grants;"""
//            logger.info("grants:" + grants)
//            logger.info("SUCCESS: LDAP user '${testUser}' successfully logged in to Doris.")
//        }
//
//        // Clean up: always try to remove all created entities
//        logger.info("Starting cleanup process...")
//
//        for (String dn in [testUserDn, testGroupDn]) {
//            deleteLdapEntry("""ldap://${ldapHost}:${ldapPort}""", ldapAdminUser, ldapAdminPassword, dn)
//        }
    }



}
