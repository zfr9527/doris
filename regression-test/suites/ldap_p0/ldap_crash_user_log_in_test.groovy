// 这个case是模拟ldap restart的case，
suite("ldap_crash_user_log_in_test", "p2") {

    String enabled = context.config.otherConfigs.get("enableLdapTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("LDAP test is disabled in regression-conf.groovy, skipping.")
        return
    }

    String prefix_str = "z10103_"
    String dbName = prefix_str + "db"
    String tbName = prefix_str + "tb"
    String tbName2 = prefix_str + "tb2"
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

    sql """drop table if exists ${dbName}.${tbName2}"""
    sql """create table ${dbName}.${tbName2} (
                id BIGINT,
                username VARCHAR(20)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );"""
    sql """
        insert into ${dbName}.`${tbName2}` values 
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
    String testUser2 = prefix_str + "user2"

    String testUserDn = "uid=${testUser},cn=${testGroup},${ldapBaseDn}"
    String testUserDn2 = "uid=${testUser2},cn=${testGroup},${ldapBaseDn}"
    String testGroupDn = "cn=${testGroup},${ldapBaseDn}"

    for (String dn in [testUserDn, testUserDn2, testGroupDn]) {
        deleteLdapEntry("""ldap://${ldapHost}:${ldapPort}""", ldapAdminUser, ldapAdminPassword, dn)
    }

    // Prepare the multi-entry LDIF file content
    String ldifContent = """dn: cn=${testGroup},${ldapBaseDn}
        objectClass: groupOfNames
        cn: ${testGroup}
        member: cn=${testGroup},${ldapBaseDn}
        member: uid=${testUser},cn=${testGroup},${ldapBaseDn}
        member: uid=${testUser2},cn=${testGroup},${ldapBaseDn}
        
        dn: uid=${testUser},cn=${testGroup},${ldapBaseDn}
        objectClass: person
        objectClass: inetOrgPerson
        cn: ${testUser}
        sn: ${testUser}
        uid: ${testUser}
        userPassword: ${testUserPassword}

        dn: uid=${testUser2},cn=${testGroup},${ldapBaseDn}
        objectClass: person
        objectClass: inetOrgPerson
        cn: ${testUser2}
        sn: ${testUser2}
        uid: ${testUser2}
        userPassword: ${testUserPassword}"""

    // Step 1: Add OU, group, and user to LDAP server in one go
    addLdapEntry("""ldap://${ldapHost}:${ldapPort}""", ldapAdminUser, ldapAdminPassword, ldifContent)
    sql """REFRESH LDAP FOR ${testUser};"""
    sql """REFRESH LDAP FOR ${testUser2};"""

    // Step 2: Create a role in Doris and a mapping for the LDAP group
    sql """drop role if exists ${testGroup}"""
    sql "CREATE ROLE '${testGroup}';"
    sql "GRANT SELECT_PRIV ON ${dbName}.${tbName} TO ROLE '${testGroup}';" // Grant some privilege to the role

    sql """drop user if exists ${testUser2}"""
    sql """create user '${testUser2}'"""
    sql "GRANT SELECT_PRIV ON ${dbName}.${tbName2} TO '${testUser2}';" // Grant some privilege to the role

    logger.info("Successfully created role '${testGroup}' and user ${testUser2} in Doris.")

    // Step 3: Verify that the new user can logger in and has the correct role's permissions
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/information_schema?authenticationPlugins=org.apache.doris.regression.util.MysqlClearPasswordPluginWithoutSSL&defaultAuthenticationPlugin=org.apache.doris.regression.util.MysqlClearPasswordPluginWithoutSSL&disabledAuthenticationPlugins=org.apache.doris.regression.util.MysqlClearPasswordPlugin"
    logger.info("url: " + url)
    connect(testUser, testUserPlaintextPassword, url) {
        def grants = sql """show grants;"""
        logger.info("grants: " + grants)
        assertTrue(grants.toString().contains("${testGroup}"))
        def res = sql """select * from ${dbName}.${tbName}"""
        assertTrue(res.size() == 3)
        logger.info("SUCCESS: LDAP user '${testUser}' successfully logged in to Doris.")
    }

    connect(testUser2, testUserPlaintextPassword, url) {
        def grants = sql """show grants;"""
        logger.info("grants: " + grants)
        assertTrue(grants.toString().contains("${testGroup}"))
        def res = sql """select * from ${dbName}.${tbName}"""
        assertTrue(res.size() == 3)
        res = sql """select * from ${dbName}.${tbName2}"""
        assertTrue(res.size() == 3)
        logger.info("SUCCESS: LDAP user '${testUser}' successfully logged in to Doris.")
    }

    def verifyLdapRestart = { def remoteIP, def remoteUser, def containerName -> 
        // 检查容器状态是否是 'Up'
        def verifyCmd = "docker ps --filter name=${containerName} --format '{{.Status}}'"
        def sshVerifyCommand = "ssh -o StrictHostKeyChecking=no ${remoteUser}@${remoteIP} '${verifyCmd}'"

        logger.info("Verifying LDAP status on ${remoteIP}: ${sshVerifyCommand}")

        def verifyProc = new ProcessBuilder("/bin/bash", "-c", sshVerifyCommand)
                .redirectErrorStream(true)
                .start()

        verifyProc.waitFor()

        def status = verifyProc.getInputStream().getText().trim()

        if (status.startsWith("Up")) {
            logger.info("✅ Verification success: Container ${containerName} status is ${status}")
        } else {
            logger.error("❌ Verification failed: Container ${containerName} status is NOT UP. Status: ${status}")
        }
    }


    for (int i = 0; i < 100; i++) {
        
        // 远程机器的 IP 地址，假设它来自你的 curIpAndPort 变量
        def remoteIP = ldapHost
        def remoteUser = "root" // 或其他有权限执行 docker 命令的用户
        def ldapContainerName = "doris--zfr-openldap" // 你的 LDAP 容器名称

        // 1. 构造 Docker 重启命令
        // 注意：这是直接重启整个容器，这是Docker管理服务的最佳实践。
        def restartCmd = "docker restart ${ldapContainerName}"
        def startCmd = "docker start ${ldapContainerName}"
        def stopCmd = "docker stop ${ldapContainerName}"

        // 2. 构造完整的 SSH 命令
        // 确保使用无密码 SSH，因此不需要 -p 参数
        def sshCommand = "ssh -o StrictHostKeyChecking=no ${remoteUser}@${remoteIP} '${stopCmd}'"

        logger.info("Executing remote command to restart LDAP on ${remoteIP}: ${sshCommand}")

        // stop ldap
        try {
            // 3. 使用 ProcessBuilder 执行命令
            def proc = new ProcessBuilder("/bin/bash", "-c", sshCommand)
                    .redirectErrorStream(true) // 重定向错误流到标准输出
                    .start()

            StringBuilder output = new StringBuilder()
            proc.getInputStream().eachLine { line ->
                output.append(line).append("\n")
                logger.info("Remote output: ${line}") // 实时输出日志
            }

            proc.waitFor() // 等待命令执行完成

            if (proc.exitValue() == 0) {
                logger.info("✅ LDAP container ${ldapContainerName} restarted successfully on ${remoteIP}.")
                logger.info("Restart command output:\n${output.toString()}")

                // 4. 【可选但推荐】执行验证命令
//                verifyLdapRestart(remoteIP, remoteUser, ldapContainerName)
            } else {
                logger.error("❌ Failed to restart LDAP container ${ldapContainerName}. Exit Code: ${proc.exitValue()}")
                logger.error("Error output:\n${output.toString()}")
                // 可以抛出异常中断执行
                throw new RuntimeException("LDAP restart failed.")
            }
        } catch (IOException e) {
            logger.error("❌ Failed to execute SSH command: ${e.getMessage()}", e)
        }


        // check testuser in cache time
    connect(testUser, testUserPlaintextPassword, url) {
        def grants = sql """show grants;"""
        logger.info("grants: " + grants)
        assertTrue(grants.toString().contains("${testGroup}"))
        def res = sql """select * from ${dbName}.${tbName}"""
        assertTrue(res.size() == 3)
        logger.info("SUCCESS: LDAP user '${testUser}' successfully logged in to Doris.")
    }

    // check testuser2 in cache time
        connect(testUser2, testUserPlaintextPassword, url) {
            def grants = sql """show grants;"""
            logger.info("grants: " + grants)
            assertTrue(grants.toString().contains("${testGroup}"))
            def res = sql """select * from ${dbName}.${tbName}"""
            assertTrue(res.size() == 3)
            res = sql """select * from ${dbName}.${tbName2}"""
            assertTrue(res.size() == 3)
            logger.info("SUCCESS: LDAP user '${testUser}' successfully logged in to Doris.")
        }

        sql """REFRESH LDAP FOR ${testUser};"""
        sql """REFRESH LDAP FOR ${testUser2};"""

    try {
        connect(testUser, testUserPlaintextPassword, url) {
            assert false
        }
    } catch (Exception e) {
        log.info("e.getMessage(): " + e.getMessage())
        assertTrue(e.getMessage().contains('Communications link failure'))
    }

    try {
        connect(testUser2, testUserPlaintextPassword, url) {
            assert false
        }
    } catch (Exception e) {
        log.info("e.getMessage(): " + e.getMessage())
        assertTrue(e.getMessage().contains('Communications link failure'))
    }

        sshCommand = "ssh -o StrictHostKeyChecking=no ${remoteUser}@${remoteIP} '${startCmd}'"

        logger.info("Executing remote command to start LDAP on ${remoteIP}: ${sshCommand}")

    // start ldap
        try {
            // 3. 使用 ProcessBuilder 执行命令
            def proc = new ProcessBuilder("/bin/bash", "-c", sshCommand)
                    .redirectErrorStream(true) // 重定向错误流到标准输出
                    .start()

            StringBuilder output = new StringBuilder()
            proc.getInputStream().eachLine { line ->
                output.append(line).append("\n")
                logger.info("Remote output: ${line}") // 实时输出日志
            }

            proc.waitFor() // 等待命令执行完成


            if (proc.exitValue() == 0) {
                logger.info("✅ LDAP container ${ldapContainerName} restarted successfully on ${remoteIP}.")
                logger.info("Restart command output:\n${output.toString()}")

                // 4. 【可选但推荐】执行验证命令
                verifyLdapRestart(remoteIP, remoteUser, ldapContainerName)
            } else {
                logger.error("❌ Failed to restart LDAP container ${ldapContainerName}. Exit Code: ${proc.exitValue()}")
                logger.error("Error output:\n${output.toString()}")
                // 可以抛出异常中断执行
                throw new RuntimeException("LDAP restart failed.")
            }
        } catch (IOException e) {
            logger.error("❌ Failed to execute SSH command: ${e.getMessage()}", e)
        }


        connect(testUser, testUserPlaintextPassword, url) {
            def grants = sql """show grants;"""
            logger.info("grants: " + grants)
            assertTrue(grants.toString().contains("${testGroup}"))
            def res = sql """select * from ${dbName}.${tbName}"""
            assertTrue(res.size() == 3)
            logger.info("SUCCESS: LDAP user '${testUser}' successfully logged in to Doris.")
        }

        connect(testUser2, testUserPlaintextPassword, url) {
            def grants = sql """show grants;"""
            logger.info("grants: " + grants)
            assertTrue(grants.toString().contains("${testGroup}"))
            def res = sql """select * from ${dbName}.${tbName}"""
            assertTrue(res.size() == 3)
            res = sql """select * from ${dbName}.${tbName2}"""
            assertTrue(res.size() == 3)
            logger.info("SUCCESS: LDAP user '${testUser}' successfully logged in to Doris.")
        }
        
    }


    // Clean up: always try to remove all created entities
    logger.info("Starting cleanup process...")
    sql "DROP ROLE '${testGroup}';"

    for (String dn in [testUserDn, testGroupDn]) {
        deleteLdapEntry("""ldap://${ldapHost}:${ldapPort}""", ldapAdminUser, ldapAdminPassword, dn)
    }
}
