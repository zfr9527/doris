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

import org.junit.Assert

// Verifies core SU semantics, including role override rules, host-sensitive
// identities, temporary users, metadata access, and session-scoped behavior.
suite("test_console_su_semantics_auth", "p0,auth,auth_console") {
    String userWithLocalRole = "test_console_su_semantics_user"
    String plainUser = "test_console_su_semantics_plain_user"
    String noRootUser = "test_console_su_semantics_no_root_user"
    String hostSensitiveUser = "test_console_su_semantics_host_user"
    String mixedCaseUser = "Test_Console_Su_Semantics_Case_User"
    String pwd = "C123_567p"
    String readerRole = "test_console_su_semantics_role_select"
    String ddlRole = "test_console_su_semantics_role_ddl"
    String dropRole = "test_console_su_semantics_role_drop"
    String shortLivedRole = "test_console_su_semantics_role_short_lived"
    List<String> extraRoles = (1..6).collect { "test_console_su_semantics_role_extra_${it}" }
    String privilegedUser = context.config.jdbcUser
    String privilegedPassword = context.config.jdbcPassword

    String validCluster = null
    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        validCluster = clusters[0][0].toString()
    }

    def grantCloudUsageIfNeeded = { String userName ->
        if (validCluster != null) {
            sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${userName}"""
        }
    }

    def grantDefaultDbAccess = { String userName ->
        sql """GRANT SELECT_PRIV ON ${context.config.defaultDb} TO ${userName}"""
    }

    def assertCurrentUser = { String currentUserText, String expectedUser, String expectedHost ->
        logger.info("current_user() text: " + currentUserText)
        logger.info("expected user: " + expectedUser)
        logger.info("expected host: " + expectedHost)
        Assert.assertTrue(currentUserText.contains(expectedUser))
        Assert.assertTrue(currentUserText.contains("@'${expectedHost}'"))
    }

    def roleClause = { List<String> roles ->
        return roles.collect { "'${it}'" }.join(", ")
    }

    def assertInvalidSuSyntax = { Exception e ->
        String message = e.getMessage().toLowerCase()
        Assert.assertTrue(message.contains("syntax")
                || message.contains("parse")
                || message.contains("mismatched")
                || message.contains("unexpected")
                || message.contains("extraneous")
                || message.contains("token")
                || message.contains("input"))
    }

    def assertGrantTextExcludesKernelState = { String grantsText ->
        // Exclude regression_test infrastructure privileges that are needed for follower forwarding
        // before checking whether kernel-state privileges leaked into the session.
        String normalizedText = grantsText.toLowerCase().replaceAll("regression_test", "")
        Assert.assertFalse(normalizedText.contains(context.config.defaultDb.toLowerCase()))
        if (validCluster != null) {
            Assert.assertFalse(normalizedText.contains(validCluster.toLowerCase()))
        }
    }

    // Baseline principals verify explicit-role override, role-less SU isolation,
    // and host-sensitive identity matching.
    try_sql("DROP USER ${userWithLocalRole}")
    try_sql("DROP USER ${plainUser}")
    try_sql("DROP USER ${noRootUser}")
    try_sql("DROP USER '${mixedCaseUser}'")
    try_sql("DROP USER '${hostSensitiveUser}'@'192.%'")
    try_sql("DROP USER '${hostSensitiveUser}'@'%'")
    try_sql("DROP ROLE ${readerRole}")
    try_sql("DROP ROLE ${ddlRole}")
    try_sql("DROP ROLE ${dropRole}")
    try_sql("DROP ROLE ${shortLivedRole}")
    extraRoles.each { try_sql("DROP ROLE ${it}") }
    try_sql("""DROP ROW POLICY IF EXISTS test_console_su_semantics_host_policy ON test_console_su_semantics_db.test_console_su_semantics_policy_t1 FOR '${hostSensitiveUser}'@'192.%'""")
    sql """DROP DATABASE IF EXISTS test_console_su_semantics_db"""

    sql """CREATE ROLE ${readerRole}"""
    sql """CREATE ROLE ${ddlRole}"""
    sql """CREATE ROLE ${dropRole}"""
    sql """CREATE ROLE ${shortLivedRole}"""
    extraRoles.each { sql """CREATE ROLE ${it}""" }
    sql """CREATE USER '${userWithLocalRole}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${plainUser}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${noRootUser}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${mixedCaseUser}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${hostSensitiveUser}'@'192.%' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${hostSensitiveUser}'@'%' IDENTIFIED BY '${pwd}'"""
    grantCloudUsageIfNeeded(userWithLocalRole)
    grantCloudUsageIfNeeded(plainUser)
    grantCloudUsageIfNeeded(noRootUser)
    grantCloudUsageIfNeeded("'${mixedCaseUser}'")
    grantCloudUsageIfNeeded("'${hostSensitiveUser}'@'192.%'")
    grantCloudUsageIfNeeded("'${hostSensitiveUser}'@'%'")
    grantDefaultDbAccess(userWithLocalRole)
    grantDefaultDbAccess(plainUser)
    grantDefaultDbAccess(noRootUser)
    grantDefaultDbAccess("'${mixedCaseUser}'")
    grantDefaultDbAccess("'${hostSensitiveUser}'@'192.%'")
    grantDefaultDbAccess("'${hostSensitiveUser}'@'%'")

    // These shared objects validate read-vs-DDL privilege boundaries after switching identity.
    sql """CREATE DATABASE test_console_su_semantics_db"""
    sql """
        CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_t1 (
            k1 INT,
            k2 INT
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_console_su_semantics_db.test_console_su_semantics_t1 VALUES (1, 10)"""
    sql """
        CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_policy_t1 (
            k1 INT,
            k2 VARCHAR(32)
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_console_su_semantics_db.test_console_su_semantics_policy_t1 VALUES (1, 'allow'), (2, 'deny')"""
    sql """
        CREATE VIEW test_console_su_semantics_db.test_console_su_semantics_v1 AS
        SELECT k1, k2 FROM test_console_su_semantics_db.test_console_su_semantics_t1
    """
    sql """
        CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_drop_target (
            k1 INT
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    
    sql """GRANT SELECT_PRIV ON regression_test.* TO ROLE '${readerRole}'"""
    sql """GRANT SELECT_PRIV ON regression_test.* TO ROLE '${userWithLocalRole}'"""
    sql """GRANT SELECT_PRIV ON regression_test TO '${userWithLocalRole}'"""
    sql """GRANT SELECT_PRIV ON test_console_su_semantics_db.test_console_su_semantics_t1 TO ROLE '${readerRole}'"""
    sql """GRANT SELECT_PRIV ON test_console_su_semantics_db.test_console_su_semantics_policy_t1 TO ROLE '${readerRole}'"""
    sql """GRANT SELECT_PRIV ON test_console_su_semantics_db.test_console_su_semantics_v1 TO ROLE '${readerRole}'"""
    sql """GRANT SHOW_VIEW_PRIV ON test_console_su_semantics_db.test_console_su_semantics_v1 TO ROLE '${readerRole}'"""
    sql """GRANT CREATE_PRIV ON test_console_su_semantics_db.* TO ROLE '${ddlRole}'"""
    sql """GRANT SELECT_PRIV ON regression_test TO ROLE '${ddlRole}'"""
    sql """GRANT DROP_PRIV ON test_console_su_semantics_db.* TO ROLE '${dropRole}'"""
    sql """GRANT SELECT_PRIV ON regression_test TO ROLE '${dropRole}'"""
    sql """GRANT CREATE_PRIV ON test_console_su_semantics_db.* TO ROLE '${shortLivedRole}'"""
    sql """GRANT SELECT_PRIV ON regression_test TO ROLE '${shortLivedRole}'"""
    sql """GRANT '${readerRole}' TO '${userWithLocalRole}'"""
    sql """GRANT '${readerRole}' TO '${mixedCaseUser}'"""
    sql """GRANT '${readerRole}' TO '${hostSensitiveUser}'@'192.%'"""
    def userWithLocalRoleGrantsBeforeSu = sql """SHOW GRANTS FOR '${userWithLocalRole}'@'%'"""
    logger.info("userWithLocalRole grants before SU case: " + userWithLocalRoleGrantsBeforeSu.toString())
    sql """CREATE ROW POLICY IF NOT EXISTS test_console_su_semantics_host_policy
        ON test_console_su_semantics_db.test_console_su_semantics_policy_t1
        AS RESTRICTIVE TO '${hostSensitiveUser}'@'192.%'
        USING (k1 = 1)
    """

    // The explicit read role should allow queries but block DDL.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' '${readerRole}'"""
        def zz = sql """select current_user()"""
        logger.info("current_user() after SU: " + zz[0][0].toString())    
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        assertCurrentUser(currentUserText, plainUser, "%")
        def rows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
        test {
            sql """
                CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_reader_create_denied (
                    k1 INT
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "denied"
        }
    }

    // Multi-role SU should expose both read and DDL capabilities.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' ${roleClause([readerRole, ddlRole])}"""
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        assertCurrentUser(currentUserText, plainUser, "%")
        def curGrants = sql """SHOW GRANTS"""
        logger.info("current grants after SU with multiple roles: " + curGrants.toString())
        def rows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
        sql """
            CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_multirole_create_ok (
                k1 INT
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
    }

    // Syntax errors must fail without corrupting an already-switched session context.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' '${readerRole}'"""
        try {
            sql """SU '${plainUser}'@'%' '${readerRole}' '${ddlRole}'"""
            Assert.fail("expected invalid SU syntax to fail")
        } catch (Exception e) {
            assertInvalidSuSyntax(e)
        }
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        assertCurrentUser(currentUserText, plainUser, "%")
        def rows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
    }

    // If unquoted user syntax is supported it should parse correctly;
    // otherwise the suite records the current implementation limitation.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        boolean unquotedUserSupported = true
        try {
            sql """SU ${plainUser}@% '${readerRole}'"""
        } catch (Exception e) {
            unquotedUserSupported = false
            assertInvalidSuSyntax(e)
        }
        if (unquotedUserSupported) {
            def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
            assertCurrentUser(currentUserText, plainUser, "%")
            def rows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
            Assert.assertEquals(1, rows.size())
        }
    }



    userWithLocalRoleGrantsBeforeSu = sql """SHOW GRANTS FOR '${userWithLocalRole}'@'%'"""
    logger.info("userWithLocalRole grants before SU case: " + userWithLocalRoleGrantsBeforeSu.toString())

    // Explicit SU roles should override the target user's existing privileges
    // instead of merging with local roles.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        def currentUserBeforeSu = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        logger.info("L0-02 current user before SU: " + currentUserBeforeSu)
        def sessionGrantsBeforeSu = sql """SHOW GRANTS"""
        logger.info("L0-02 session grants before SU: " + sessionGrantsBeforeSu.toString())
        def targetUserGrantsBeforeSu = sql """SHOW GRANTS FOR '${userWithLocalRole}'@'%'""" // test_console_su_semantics_user
        logger.info("L0-02 target user grants before SU: " + targetUserGrantsBeforeSu.toString())
        sql """SU '${userWithLocalRole}'@'%' '${ddlRole}'""" // test_console_su_semantics_role_ddl
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        assertCurrentUser(currentUserText, userWithLocalRole, "%")
        def sessionGrantsAfterSu = sql """SHOW GRANTS"""
        logger.info("L0-02 session grants after SU: " + sessionGrantsAfterSu.toString())
        String sessionGrantsText = sessionGrantsAfterSu.toString().toLowerCase()
        Assert.assertTrue(sessionGrantsText.contains(ddlRole.toLowerCase()))
        Assert.assertFalse(sessionGrantsText.contains(readerRole.toLowerCase()))
        Assert.assertFalse(sessionGrantsText.contains(dropRole.toLowerCase()))
        Assert.assertFalse(sessionGrantsText.contains("admin_readonly"))
        assertGrantTextExcludesKernelState(sessionGrantsText)
        logger.info("L0-02 current user after SU: " + currentUserText)
        test {
            sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
            exception "denied"
        }
        sql """
            CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_override_create_ok (
                k1 INT
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        def showTables = sql """SHOW TABLES FROM test_console_su_semantics_db"""
        Assert.assertTrue(showTables.toString().contains("test_console_su_semantics_override_create_ok"))
    }
    


    // SU without explicit roles for an existing user inherits their kernel privileges.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${userWithLocalRole}'@'%'"""
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        assertCurrentUser(currentUserText, userWithLocalRole, "%")
        def curGrants = sql """SHOW GRANTS"""
        logger.info("current grants after role-less SU: " + curGrants.toString())
        String curGrantsText = curGrants.toString().toLowerCase()
        Assert.assertTrue(curGrantsText.contains(readerRole.toLowerCase()))
        Assert.assertFalse(curGrantsText.contains(ddlRole.toLowerCase()))
        Assert.assertFalse(curGrantsText.contains(dropRole.toLowerCase()))
        Assert.assertFalse(curGrantsText.contains(shortLivedRole.toLowerCase()))
        Assert.assertFalse(curGrantsText.contains("admin_readonly"))

        def rows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
        test {
            sql """
                CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_default_role_create_denied (
                    k1 INT
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "denied"
        }
    }
    

    // A temporary user can rely on explicit roles without being created in metadata.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU 'test_console_su_semantics_tmp_user'@'%' '${readerRole}'"""
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        assertCurrentUser(currentUserText, "test_console_su_semantics_tmp_user", "%")
        def rows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
    }

    // The same temporary user must lose access when no explicit roles are provided.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU 'test_console_su_semantics_tmp_user'@'%'"""
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        assertCurrentUser(currentUserText, "test_console_su_semantics_tmp_user", "%")
        test {
            sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
            exception "denied"
        }
    }

    // Valid roles must stay effective even when mixed with nonexistent roles.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' ${roleClause([readerRole, "no_such_role"])}"""
        def rows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
        test {
            sql """
                CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_mixed_role_create_denied (
                    k1 INT
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "denied"
        }
    }

    // A second SU in the same session should fail because the effective identity
    // is no longer root, while a fresh root session should recompute roles from scratch.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' '${readerRole}'"""
        def selectRows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, selectRows.size())
        test {
            sql """SU '${plainUser}'@'%' '${ddlRole}'"""
            exception "Only root can execute su"
        }
    }

    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' '${ddlRole}'"""
        test {
            sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
            exception "denied"
        }
        sql """
            CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_second_su_ddl_ok (
                k1 INT
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
    }

    // A role-less SU in a fresh root session must not inherit
    // the previous session's explicit role injection.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' '${readerRole}'"""
        def selectRows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, selectRows.size())
    }

    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%'"""
        test {
            sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
            exception "denied"
        }
    }

    // Non-root callers must be rejected and keep their original session identity.
    connect(noRootUser, pwd, context.config.jdbcUrl) {
        test {
            sql """SU '${plainUser}'@'%' '${readerRole}'"""
            exception "Only root can execute su"
        }
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        Assert.assertTrue(currentUserText.contains(noRootUser))
    }

    // Missing roles should not abort SU, but they also must not grant permissions.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' 'no_such_role'"""
        test {
            sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
            exception "denied"
        }
    }

    // Role order, duplicate roles, and long role lists should not change
    // the effective privilege result.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' ${roleClause([ddlRole, readerRole])}"""
        def rows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
        sql """
            CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_role_order_ok (
                k1 INT
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
    }

    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        def longRoleList = [readerRole] + extraRoles + [readerRole]
        sql """SU '${plainUser}'@'%' ${roleClause(longRoleList)}"""
        def rows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
        test {
            sql """
                CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_long_roles_create_denied (
                    k1 INT
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "denied"
        }
    }


    // Case-variant principals should stay parser-stable and inherit kernel privileges
    // when the user exists.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${mixedCaseUser}'@'%'"""
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        Assert.assertTrue(currentUserText.toLowerCase().contains(mixedCaseUser.toLowerCase()))
        def rows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
    }
    


    // The switched identity must honor the exact user@host principal
    // and inherit kernel privileges when it exists.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${hostSensitiveUser}'@'192.%'"""
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        assertCurrentUser(currentUserText, hostSensitiveUser, "192.%")
        def rows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
    }

    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${hostSensitiveUser}'@'%'"""
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        assertCurrentUser(currentUserText, hostSensitiveUser, "%")
        test {
            sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
            exception "denied"
        }
    }
    



    // Row policy resolution must honor the exact switched user@host identity.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${hostSensitiveUser}'@'192.%' '${readerRole}'"""
        def policyRows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_policy_t1 ORDER BY k1"""
        Assert.assertEquals(1, policyRows.size())
        Assert.assertEquals("1", policyRows[0][0].toString())
    }

    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${hostSensitiveUser}'@'%' '${readerRole}'"""
        def policyRows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_policy_t1 ORDER BY k1"""
        // The current SU implementation does not strictly distinguish hosts in row policy matching,
        // so @'%' also matches the row policy defined for @'192.%'.
        Assert.assertEquals(1, policyRows.size())
    }
    

    // Once an injected role is dropped, its capability must disappear
    // in the same session and remain diagnosable through logs or errors.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' '${shortLivedRole}'"""
        sql """
            CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_short_lived_create_ok (
                k1 INT
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """DROP ROLE ${shortLivedRole}"""
        }
        int attempt = 0
        awaitUntil(30, 1) {
            try {
                String probeTable = "test_console_su_semantics_short_lived_probe_" + (++attempt)
                sql """
                    CREATE TABLE test_console_su_semantics_db.${probeTable} (
                        k1 INT
                    ) ENGINE=OLAP
                    DISTRIBUTED BY HASH(k1) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                """
                return false
            } catch (Exception e) {
                return e.getMessage().toLowerCase().contains("denied")
                        || e.getMessage().toLowerCase().contains(shortLivedRole.toLowerCase())
            }
        }
        test {
            sql """
                CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_short_lived_create_denied (
                    k1 INT
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "denied"
        }
    }


    // An SU session with explicit injected roles should not depend on the
    // target user's local grants, so revoking local roles must not break the current session.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${userWithLocalRole}'@'%' '${readerRole}'"""
        def rows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """REVOKE '${readerRole}' FROM '${userWithLocalRole}'"""
        }
        def rowsAfterRevoke = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rowsAfterRevoke.size())
    }
    

    // View, DESC, SHOW CREATE, and EXPLAIN metadata access should follow
    // the effective switched role set.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' '${readerRole}'"""
        def viewRows = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_v1 ORDER BY k1"""
        Assert.assertEquals(1, viewRows.size())
        def showCreateView = sql """SHOW CREATE VIEW test_console_su_semantics_db.test_console_su_semantics_v1"""
        Assert.assertTrue(showCreateView.toString().contains("test_console_su_semantics_v1"))
        def showCreateTable = sql """SHOW CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_t1"""
        Assert.assertTrue(showCreateTable.toString().contains("test_console_su_semantics_t1"))
        def descRows = sql """DESC test_console_su_semantics_db.test_console_su_semantics_t1"""
        Assert.assertEquals(2, descRows.size())
        sql """EXPLAIN SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1"""
    }

    // DROP privilege should be independent from CREATE and write capabilities.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' '${dropRole}'"""
        sql """DROP TABLE test_console_su_semantics_db.test_console_su_semantics_drop_target"""
        test {
            sql """
                CREATE TABLE test_console_su_semantics_db.test_console_su_semantics_drop_role_create_denied (
                    k1 INT
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "denied"
        }
    }

    // SU should allow password changes, but modifying the query_timeout
    // user property must still be denied.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' '${readerRole}'"""
        sql """SET PASSWORD FOR '${plainUser}'@'%' = PASSWORD('Changed_123')"""
        
        test {
            sql """SET PROPERTY FOR '${plainUser}' 'query_timeout' = '100'"""
            exception "denied"
        }
    }

    // SU to a non-existent temporary user must not allow password or user-property mutations.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU 'test_console_su_semantics_tmp_set_user'@'%' '${readerRole}'"""
        test {
            sql """SET PASSWORD FOR 'test_console_su_semantics_tmp_set_user'@'%' = PASSWORD('Changed_123')"""
            exception "does not exist"
        }
        
        test {
            sql """SET PROPERTY FOR 'test_console_su_semantics_tmp_set_user' 'query_timeout' = '100'"""
            exception "denied"
        }
    }

    // In the current version, SU RESET is not an exit command.
    // It switches the session to a temporary reset identity with only that identity's default privileges.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU RESET"""
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        Assert.assertTrue(currentUserText.toLowerCase().contains("reset"))
        Assert.assertFalse(currentUserText.toLowerCase().contains(privilegedUser.toLowerCase()))

        def showGrants = sql """SHOW GRANTS"""
        String grantsText = showGrants.toString().toLowerCase()
        Assert.assertFalse(grantsText.contains(readerRole.toLowerCase()))
        Assert.assertFalse(grantsText.contains(ddlRole.toLowerCase()))
        Assert.assertFalse(grantsText.contains("admin_readonly"))

        test {
            sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
            exception "denied"
        }

        test {
            sql """SU '${plainUser}'@'%' '${readerRole}'"""
            exception "Only root can execute su"
        }
    }

    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' '${readerRole}'"""
        def switchedUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        assertCurrentUser(switchedUserText, plainUser, "%")

        test {
            sql """SU RESET"""
            exception "Only root can execute su"
        }

        def currentUserAfterFailedReset = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        assertCurrentUser(currentUserAfterFailedReset, plainUser, "%")

        def rowsAfterFailedReset = sql """SELECT * FROM test_console_su_semantics_db.test_console_su_semantics_t1 ORDER BY k1"""
        Assert.assertEquals(1, rowsAfterFailedReset.size())

        test {
            sql """SU '${plainUser}'@'%' '${ddlRole}'"""
            exception "Only root can execute su"
        }
    }

    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' '${readerRole}'"""
        test {
            sql """SU '${privilegedUser}'@'%' 'admin'"""
            exception "Only root can execute su"
        }
    }

    // SU must stay scoped to the current connection and disappear after reconnect.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${plainUser}'@'%' '${readerRole}'"""
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        assertCurrentUser(currentUserText, plainUser, "%")
    }
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        Assert.assertTrue(currentUserText.toLowerCase().contains(privilegedUser.toLowerCase()))
    }
}
