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

import groovy.json.JsonOutput
import org.junit.Assert

// Verifies that SQL and HTTP SU flows emit accurate audit records, preserve
// the effective identity, and keep failure modes diagnosable.
suite("test_console_su_audit_auth", "p0,auth,nonConcurrent,auth_console") {
    String auditUser = "test_console_su_audit_user"
    String nonRootUser = "test_console_su_audit_no_root_user"
    String pwd = "C123_567p"
    String readerRole = "test_console_su_audit_reader_role"
    String nonRootAuditRole = "test_console_su_audit_non_root_role_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String suCommandAuditRole = "test_console_su_audit_su_cmd_role_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String selectMarker = "su_audit_select_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String insertMarker = "su_audit_insert_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String multiRoleMarker = "su_audit_multirole_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String infoSchemaMarker = "su_audit_infoschema_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String statsMarker = "su_audit_stats_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String variableMarker = "su_audit_variable_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String httpMarker = "su_audit_http_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String missingRoleMarker = "audit_missing_role_" + UUID.randomUUID().toString().replace("-", "").substring(0, 6)
    String mixedMissingRoleMarker = "audit_mixed_role_" + UUID.randomUUID().toString().replace("-", "").substring(0, 6)
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

    // The audit_log.user column is populated by getQualifiedUser(), which returns
    // just the username without the @host suffix.
    def assertAuditUser = { String actualText, String expectedUser ->
        String normalizedText = actualText.replace("'", "")
        logger.info("audit user text: " + actualText + ", expected: " + expectedUser)
        Assert.assertTrue(normalizedText.contains(expectedUser))
    }

    def queryAuditRecords = { List<Map<String, String>> expectations ->
        String query = expectations.collect { expectation ->
            """
                SELECT '${expectation.key}' AS record_key, user, stmt, state, error_code, error_message
                FROM (
                    SELECT user, stmt, state, error_code, error_message
                    FROM __internal_schema.audit_log
                    WHERE ${expectation.condition}
                    ORDER BY time DESC
                    LIMIT 1
                ) audit_row
            """
        }.join("\nUNION ALL\n")
        def rows = sql query
        def records = [:]
        rows.each { row ->
            records[row[0].toString()] = row.subList(1, row.size())
        }
        return records
    }

    def waitAuditRecords = { List<Map<String, String>> expectations ->
        int retry = 100
        while (true) {
            def records = queryAuditRecords(expectations)
            List<String> pending = expectations.findAll { !records.containsKey(it.key) }
                    .collect { it.description }
            if (pending.isEmpty()) {
                return records
            }
            logger.info("waiting for audit records for ${pending}...")
            if (retry-- <= 0) {
                throw new RuntimeException("failed to find audit records for ${pending}")
            }
            sql """CALL FLUSH_AUDIT_LOG()"""
            sleep(2000)
        }
    }

    def httpQuery = { String authUser, String authPwd, String suUser, String suRoles, String stmt, Closure checkFunc ->
        def requestBody = JsonOutput.toJson([stmt: stmt, is_sync: true, limit: 100])
        httpTest {
            basicAuthorization "${authUser}", "${authPwd}"
            endpoint "${context.config.feHttpAddress}"
            uri "/api/query/internal/test_console_su_audit_db"
            header("X-Doris-Stream", "false")
            if (suUser != null) {
                header("X-Doris-Su-User", suUser)
            }
            if (suRoles != null) {
                header("X-Doris-Su-Roles", suRoles)
            }
            body requestBody
            op "post"
            check checkFunc
        }
    }

    try {
        sql """SET GLOBAL enable_audit_plugin = true"""
    } catch (Exception e) {
        log.warn("skip test_console_su_audit_auth because " + e.getMessage())
        assertTrue(e.getMessage().toUpperCase().contains("ADMIN"))
        return
    }

    try {
        // Prepare one temporary target user with read-only role injection
        // and a unique audit marker for each SQL path.
        try_sql("DROP USER ${auditUser}")
        try_sql("DROP USER ${nonRootUser}")
        try_sql("DROP ROLE ${readerRole}")
        sql """DROP DATABASE IF EXISTS test_console_su_audit_db"""

        sql """CREATE ROLE ${readerRole}"""
        sql """CREATE ROLE ${nonRootAuditRole}"""
        sql """CREATE ROLE ${suCommandAuditRole}"""
        sql """CREATE USER '${auditUser}' IDENTIFIED BY '${pwd}'"""
        sql """CREATE USER '${nonRootUser}' IDENTIFIED BY '${pwd}'"""
        grantCloudUsageIfNeeded(auditUser)
        grantCloudUsageIfNeeded(nonRootUser)
        grantDefaultDbAccess(auditUser)
        grantDefaultDbAccess(nonRootUser)

        sql """CREATE DATABASE test_console_su_audit_db"""
        sql """
            CREATE TABLE test_console_su_audit_db.test_console_su_audit_t1 (
                k1 INT,
                tag VARCHAR(64)
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """INSERT INTO test_console_su_audit_db.test_console_su_audit_t1 VALUES (1, 'seed')"""
        sql """GRANT SELECT_PRIV ON test_console_su_audit_db.test_console_su_audit_t1 TO ROLE '${readerRole}'"""
        sql """GRANT SELECT_PRIV ON test_console_su_audit_db.test_console_su_audit_t1 TO ROLE '${nonRootAuditRole}'"""
        sql """GRANT SELECT_PRIV ON test_console_su_audit_db.test_console_su_audit_t1 TO ROLE '${suCommandAuditRole}'"""

        // Successful SQL after SU must be audited with the effective temporary user identity.
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """SU '${auditUser}'@'%' '${readerRole}'"""
            def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
            Assert.assertTrue(currentUserText.contains(auditUser))
            Assert.assertTrue(currentUserText.contains("@"))

            def rows = sql """
                SELECT '${selectMarker}' AS marker, tag
                FROM test_console_su_audit_db.test_console_su_audit_t1
                ORDER BY k1
            """
            Assert.assertEquals(1, rows.size())
            Assert.assertEquals(selectMarker, rows[0][0].toString())
        }

        // Multi-role audit rows should still only record the effective user,
        // not the role list.
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """SU '${auditUser}'@'%' '${readerRole}', 'admin_readonly'"""
            def rows = sql """
                SELECT '${multiRoleMarker}' AS marker, tag
                FROM test_console_su_audit_db.test_console_su_audit_t1
                ORDER BY k1
            """
            Assert.assertEquals(1, rows.size())
        }
        // Missing roles and mixed valid/invalid roles should remain diagnosable
        // through audit records and execution results even without reading FE runtime logs.
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """SU '${auditUser}'@'%' '${missingRoleMarker}'"""
            test {
                sql """
                    SELECT '${missingRoleMarker}' AS marker, tag
                    FROM test_console_su_audit_db.test_console_su_audit_t1
                    ORDER BY k1
                """
                exception "denied"
            }
        }
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """SU '${auditUser}'@'%' '${readerRole}', '${mixedMissingRoleMarker}'"""
            def rows = sql """
                SELECT '${mixedMissingRoleMarker}' AS marker, tag
                FROM test_console_su_audit_db.test_console_su_audit_t1
                ORDER BY k1
            """
            Assert.assertEquals(1, rows.size())
        }
        // Root-only rejection and SQL privilege denial must remain
        // distinguishable and diagnosable.
        String nonRootError = null
        connect(nonRootUser, pwd, context.config.jdbcUrl) {
            try {
                sql """SU '${auditUser}'@'%' '${nonRootAuditRole}'"""
                Assert.fail("expected non-root SU to be rejected")
            } catch (Exception e) {
                nonRootError = e.getMessage()
            }
        }
        Assert.assertTrue(nonRootError.contains("Only root can execute su"))

        // Even denied SQL should still be attributed to the switched temporary identity.
        String deniedSqlError = null
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """SU '${auditUser}'@'%' '${readerRole}'"""
            try {
                sql """
                    INSERT INTO test_console_su_audit_db.test_console_su_audit_t1
                    VALUES (2, '${insertMarker}')
                """
                Assert.fail("expected insert to be denied")
            } catch (Exception e) {
                deniedSqlError = e.getMessage()
            }
        }
        Assert.assertTrue(deniedSqlError.toLowerCase().contains("denied"))
        Assert.assertFalse(deniedSqlError.contains("Only root can execute su"))

        // Metadata and statistics queries should also keep the switched audit identity.
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """SU '${auditUser}'@'%' 'admin_readonly'"""
            def infoSchemaRows = sql """
                SELECT '${infoSchemaMarker}' AS marker, column_name
                FROM information_schema.columns
                WHERE table_schema = 'test_console_su_audit_db'
                  AND table_name = 'test_console_su_audit_t1'
                ORDER BY column_name
                LIMIT 1
            """
            Assert.assertEquals(1, infoSchemaRows.size())

            def statsRows = sql """
                SELECT '${statsMarker}' AS marker, COUNT(*)
                FROM internal.__internal_schema.column_statistics
            """
            Assert.assertEquals(1, statsRows.size())
        }

        // Variable queries should preserve the exact switched audit identity as well.
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """SU '${auditUser}'@'%' 'admin_readonly'"""
            def variableRows = sql """SELECT '${variableMarker}' AS marker, @@global.sql_mode"""
            Assert.assertEquals(1, variableRows.size())
        }

        // The SU command itself should also leave an audit entry.
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """SU '${auditUser}'@'%' '${suCommandAuditRole}'"""
        }

        // HTTP SU queries should be audited under the effective temporary identity too.
        httpQuery(privilegedUser, privilegedPassword, "${auditUser}@%", readerRole,
                "SELECT '${httpMarker}' AS marker, tag FROM test_console_su_audit_db.test_console_su_audit_t1 ORDER BY k1") {
            respCode, body ->
                Assert.assertEquals(200, respCode)
                def json = parseJson(body)
                Assert.assertEquals(0, json.code)
                Assert.assertEquals(1, json.data.data.size())
        }

        def auditRows = waitAuditRecords([
                [key: "selectAudit",
                 condition: """user = '${auditUser}' AND stmt LIKE '%${selectMarker}%'""",
                 description: "marker ${selectMarker}"],
                [key: "multiRoleAudit",
                 condition: """user = '${auditUser}' AND stmt LIKE '%${multiRoleMarker}%'""",
                 description: "marker ${multiRoleMarker}"],
                [key: "missingRoleAudit",
                 condition: """user = '${auditUser}' AND stmt LIKE '%${missingRoleMarker}%' AND stmt NOT LIKE 'SU %'""",
                 description: "denied query audit for missing role ${missingRoleMarker}"],
                [key: "missingRoleSuAudit",
                 condition: """stmt LIKE 'SU ''${auditUser}''@''%''%' AND stmt LIKE '%''${missingRoleMarker}''%'""",
                 description: "SU audit for missing role ${missingRoleMarker}"],
                [key: "mixedRoleAudit",
                 condition: """user = '${auditUser}' AND stmt LIKE '%${mixedMissingRoleMarker}%' AND stmt NOT LIKE 'SU %'""",
                 description: "successful query audit for mixed role marker ${mixedMissingRoleMarker}"],
                [key: "mixedRoleSuAudit",
                 condition: """stmt LIKE 'SU ''${auditUser}''@''%''%' AND stmt LIKE '%''${readerRole}''%' AND stmt LIKE '%''${mixedMissingRoleMarker}''%'""",
                 description: "SU audit for mixed role marker ${mixedMissingRoleMarker}"],
                [key: "nonRootAudit",
                 condition: """user = '${nonRootUser}' AND state = 'ERR' AND error_message LIKE '%Only root can execute su%'"""
                         + """ AND stmt = 'SU ''${auditUser}''@''%'' ''${nonRootAuditRole}'''""",
                 description: "failed SU audit for non-root user ${nonRootUser}"],
                [key: "insertAudit",
                 condition: """user = '${auditUser}' AND stmt LIKE '%${insertMarker}%'""",
                 description: "marker ${insertMarker}"],
                [key: "infoSchemaAudit",
                 condition: """user = '${auditUser}' AND stmt LIKE '%${infoSchemaMarker}%'""",
                 description: "marker ${infoSchemaMarker}"],
                [key: "statsAudit",
                 condition: """user = '${auditUser}' AND stmt LIKE '%${statsMarker}%'""",
                 description: "marker ${statsMarker}"],
                [key: "variableAudit",
                 condition: """user = '${auditUser}' AND stmt LIKE '%${variableMarker}%'""",
                 description: "marker ${variableMarker}"],
                [key: "suAuditRow",
                 condition: """stmt = 'SU ''${auditUser}''@''%'' ''${suCommandAuditRole}'''""",
                 description: "SU command audit record"],
                [key: "httpAudit",
                 condition: """user = '${auditUser}' AND stmt LIKE '%${httpMarker}%'""",
                 description: "marker ${httpMarker}"]
        ])

        def selectAudit = auditRows.selectAudit
        assertAuditUser(selectAudit[0].toString(), auditUser)
        Assert.assertTrue(selectAudit[1].toString().contains(selectMarker))

        def multiRoleAudit = auditRows.multiRoleAudit
        assertAuditUser(multiRoleAudit[0].toString(), auditUser)
        Assert.assertFalse(multiRoleAudit[0].toString().contains(readerRole))
        Assert.assertFalse(multiRoleAudit[0].toString().contains("admin_readonly"))

        def missingRoleAudit = auditRows.missingRoleAudit
        assertAuditUser(missingRoleAudit[0].toString(), auditUser)
        Assert.assertTrue(missingRoleAudit[1].toString().contains(missingRoleMarker))
        Assert.assertEquals("ERR", missingRoleAudit[2].toString())
        Assert.assertTrue(missingRoleAudit[4].toString().toLowerCase().contains("denied"))

        def missingRoleSuAudit = auditRows.missingRoleSuAudit
        Assert.assertTrue(missingRoleSuAudit[1].toString().contains(missingRoleMarker))

        def mixedRoleAudit = auditRows.mixedRoleAudit
        assertAuditUser(mixedRoleAudit[0].toString(), auditUser)
        Assert.assertTrue(mixedRoleAudit[1].toString().contains(mixedMissingRoleMarker))
        Assert.assertEquals("EOF", mixedRoleAudit[2].toString())
        Assert.assertTrue(mixedRoleAudit[4].toString().isEmpty())

        def mixedRoleSuAudit = auditRows.mixedRoleSuAudit
        Assert.assertTrue(mixedRoleSuAudit[1].toString().contains(readerRole))
        Assert.assertTrue(mixedRoleSuAudit[1].toString().contains(mixedMissingRoleMarker))

        def nonRootAudit = auditRows.nonRootAudit
        assertAuditUser(nonRootAudit[0].toString(), nonRootUser)
        Assert.assertEquals("ERR", nonRootAudit[2].toString())
        Assert.assertTrue(nonRootAudit[1].toString().contains(nonRootAuditRole))
        Assert.assertTrue(nonRootAudit[4].toString().contains("Only root can execute su"))

        def insertAudit = auditRows.insertAudit
        assertAuditUser(insertAudit[0].toString(), auditUser)
        Assert.assertTrue(insertAudit[1].toString().contains(insertMarker))

        def infoSchemaAudit = auditRows.infoSchemaAudit
        assertAuditUser(infoSchemaAudit[0].toString(), auditUser)

        def statsAudit = auditRows.statsAudit
        assertAuditUser(statsAudit[0].toString(), auditUser)

        def variableAudit = auditRows.variableAudit
        assertAuditUser(variableAudit[0].toString(), auditUser)

        def suAuditRow = auditRows.suAuditRow
        Assert.assertTrue(suAuditRow[1].toString().contains(auditUser))
        Assert.assertTrue(suAuditRow[1].toString().contains(suCommandAuditRole))

        def httpAudit = auditRows.httpAudit
        assertAuditUser(httpAudit[0].toString(), auditUser)
    } finally {
        sql """SET GLOBAL enable_audit_plugin = false"""
    }
}
