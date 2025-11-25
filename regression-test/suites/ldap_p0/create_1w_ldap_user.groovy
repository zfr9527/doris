suite("create_1w_ldap_user", "p2") {

    String enabled = context.config.otherConfigs.get("enableLdapTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("LDAP test is disabled in regression-conf.groovy, skipping.")
        return
    }

    String prefix_str = "create_1w_ldap_user_"
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

    for (int i = 1; i <= 10000; i++) {
        prefix_str = "z_${i}_"

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

        // Step 1: Add OU, group, and user to LDAP server in one go
        addLdapEntry("""ldap://${ldapHost}:${ldapPort}""", ldapAdminUser, ldapAdminPassword, ldifContent)
        sql """REFRESH LDAP FOR ${testUser};"""

        // Step 2: Create a role in Doris and a mapping for the LDAP group
        sql """drop role if exists ${testGroup}"""
        sql "CREATE ROLE '${testGroup}';"
        sql "GRANT SELECT_PRIV ON ${dbName}.${tbName} TO ROLE '${testGroup}';" // Grant some privilege to the role

        logger.info("Successfully created role '${testGroup}' in Doris.")

        // Step 3: Verify that the new user can log in and has the correct role's permissions
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

//    // Clean up: always try to remove all created entities
//    logger.info("Starting cleanup process...")
//    sql "DROP ROLE '${testGroup}';"
//
//    for (String dn in [testUserDn, testUserDn2, testGroupDn, testGroupDn2]) {
//        deleteLdapEntry("""ldap://${ldapHost}:${ldapPort}""", ldapAdminUser, ldapAdminPassword, dn)
//    }
    }

}
