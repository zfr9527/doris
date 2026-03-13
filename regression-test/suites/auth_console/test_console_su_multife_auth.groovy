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

import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

// Verifies SU propagation across non-master FE forwarding, metadata visibility,
// processlist behavior, audit identity, and concurrent cross-FE workloads.
suite("test_console_su_multife_auth", "p0,auth,nonConcurrent,auth_console") {
    // Find a live non-master FE, prefer a follower, and fall back to an observer
    // so the suite can still cover non-master -> master forwarding semantics.
    def result = sql """SHOW FRONTENDS"""
    String nonMasterFeIp = ""
    String nonMasterFeRole = ""
    for (int i = 0; i < result.size(); i++) {
        if (result[i][8] != "false" || result[i][11] != "true") {
            continue
        }
        String candidateRole = result[i][7].toString()
        if (candidateRole == "FOLLOWER") {
            nonMasterFeIp = result[i][1].toString()
            nonMasterFeRole = candidateRole
            break
        }
        if (candidateRole == "OBSERVER" && nonMasterFeIp == "") {
            nonMasterFeIp = result[i][1].toString()
            nonMasterFeRole = candidateRole
        }
    }

    if (nonMasterFeIp == "") {
        logger.info("skip test_console_su_multife_auth because no alive non-master frontend exists")
        return
    }

    logger.info("test_console_su_multife_auth selected non-master FE {} with role {}", nonMasterFeIp, nonMasterFeRole)

    def tokens = context.config.jdbcUrl.split('/')
    // Do not set a default database here. The forwarded request may validate privileges on the current database
    // before it reaches master, and `information_schema` is not granted to the switched user in this suite.
    String nonMasterJdbcUrl = (tokens[0] + "//" + tokens[2] + "/" + "?")
            .replaceAll(/\/\/[0-9.]+:/, "//${nonMasterFeIp}:")

    String ddlUser = "test_console_su_multife_user"
    String processUser = "test_console_su_multife_process_user"
    String pwd = "C123_567p"
    String ddlRole = "test_console_su_multife_role_ddl"
    String missingRoleMarker = "test_console_su_multife_missing_role_" + UUID.randomUUID().toString().replace("-", "").substring(0, 6)
    String forwardCreateOkTable = "test_console_su_multife_forward_create_ok_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String forwardWarnDeniedTable = "test_console_su_multife_forward_warn_denied_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String privilegedUser = context.config.jdbcUser
    String privilegedPassword = context.config.jdbcPassword

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

    // The audit user field only contains the username and omits the @host suffix.
    def assertAuditUser = { String actualText, String expectedUser ->
        String normalizedText = actualText.replace("'", "")
        Assert.assertTrue(normalizedText.contains(expectedUser))
    }

    String validCluster = null
    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        validCluster = clusters[0][0].toString()
    }

    def assertGrantTextExcludesKernelState = { String grantsText ->
        String normalizedText = grantsText.toLowerCase()
        Assert.assertFalse(normalizedText.contains(context.config.defaultDb.toLowerCase()))
        if (validCluster != null) {
            Assert.assertFalse(normalizedText.contains(validCluster.toLowerCase()))
        }
    }

    def grantCloudUsageIfNeeded = { String userName ->
        if (validCluster != null) {
            sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${userName}"""
        }
    }

    def grantDefaultDbAccess = { String userName ->
        sql """GRANT SELECT_PRIV ON ${context.config.defaultDb} TO ${userName}"""
    }

    // These shared objects are used by the forwarded DDL and metadata checks.
    try_sql("DROP USER ${ddlUser}")
    try_sql("DROP USER ${processUser}")
    try_sql("DROP ROLE ${ddlRole}")
    sql """DROP DATABASE IF EXISTS test_console_su_multife_db"""

    sql """CREATE ROLE ${ddlRole}"""
    sql """CREATE USER '${ddlUser}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${processUser}' IDENTIFIED BY '${pwd}'"""
    grantCloudUsageIfNeeded(ddlUser)
    grantCloudUsageIfNeeded(processUser)
    grantDefaultDbAccess(ddlUser)
    grantDefaultDbAccess(processUser)

    sql """CREATE DATABASE test_console_su_multife_db"""
    sql """
        CREATE TABLE test_console_su_multife_db.test_console_su_multife_t1 (
            k1 INT,
            k2 INT
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_console_su_multife_db.test_console_su_multife_t1 VALUES (1, 10)"""
    sql """GRANT CREATE_PRIV ON test_console_su_multife_db.* TO ROLE '${ddlRole}'"""
    sql """ANALYZE TABLE test_console_su_multife_db.test_console_su_multife_t1 WITH SYNC"""
    def tableId = get_table_id("internal", "test_console_su_multife_db", "test_console_su_multife_t1")

    boolean auditEnabled = true
    try {
        sql """SET GLOBAL enable_audit_plugin = true"""
    } catch (Exception e) {
        auditEnabled = false
        log.warn("skip multife audit assertions because {}", e.getMessage())
    }
    onFinish {
        if (auditEnabled) {
            try {
                sql """SET GLOBAL enable_audit_plugin = false"""
            } catch (Throwable t) {
                log.warn("failed to disable audit plugin after multife suite", t)
            }
        }
    }

    connect(privilegedUser, privilegedPassword, nonMasterJdbcUrl) {
        sql """SYNC"""
    }

    // Non-master -> master forwarding must preserve the injected
    // DDL role and audit identity.
    connect(privilegedUser, privilegedPassword, nonMasterJdbcUrl) {
        sql """SU '${ddlUser}'@'%' '${ddlRole}'"""
        sql """
            CREATE TABLE test_console_su_multife_db.${forwardCreateOkTable} (
                k1 INT
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
    }
  


    def showTables = sql """SHOW TABLES FROM test_console_su_multife_db"""
    Assert.assertTrue(showTables.toString().contains(forwardCreateOkTable))
    

    // Forwarding must also preserve the denial path when no DDL role is present.
    connect(privilegedUser, privilegedPassword, nonMasterJdbcUrl) {
        sql """SU '${ddlUser}'@'%'"""
        def showGrants = sql """SHOW GRANTS"""
        String showGrantsText = showGrants.toString()
        Assert.assertFalse(showGrantsText.contains(ddlRole))
        Assert.assertFalse(showGrantsText.contains("admin_readonly"))

        test {
            sql """SELECT * FROM test_console_su_multife_db.test_console_su_multife_t1 ORDER BY k1"""
            exception "denied"
        }
        test {
            sql """
                CREATE TABLE test_console_su_multife_db.test_console_su_multife_forward_create_denied (
                    k1 INT
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "denied"
        }
    }

    // Processlist, information_schema, and catalogs should all
    // reflect the switched identity across FE hops.
    connect(privilegedUser, privilegedPassword, nonMasterJdbcUrl) {
        sql """SU '${processUser}'@'%' 'admin_readonly'"""
        // sql """SET fetch_all_fe_for_system_table = true"""
        def fullProcessList = sql """SHOW FULL PROCESSLIST"""
        Assert.assertTrue(!fullProcessList.isEmpty())

        def infoSchemaTables = sql """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'test_console_su_multife_db'
            ORDER BY table_name
        """
        Assert.assertTrue(infoSchemaTables.toString().contains(forwardCreateOkTable))

        def showCatalogs = sql """SHOW CATALOGS"""
        Assert.assertTrue(showCatalogs.toString().contains("internal"))
    }

    // SU without roles inherits kernel permissions for existing users, and
    // under the current behavior SHOW FULL PROCESSLIST should also succeed.
    connect(privilegedUser, privilegedPassword, nonMasterJdbcUrl) {
        sql """SU '${processUser}'@'%'"""
        def showGrants = sql """SHOW GRANTS"""
        String showGrantsText = showGrants.toString()
        Assert.assertFalse(showGrantsText.contains(ddlRole))
        Assert.assertFalse(showGrantsText.contains("admin_readonly"))

        test {
            sql """SELECT * FROM test_console_su_multife_db.test_console_su_multife_t1 ORDER BY k1"""
            exception "denied"
        }
        def processRows = sql """SHOW FULL PROCESSLIST"""
        Assert.assertEquals(1, processRows.size())
    }

    // Missing-role behavior on the selected non-master FE should match the
    // local behavior and remain diagnosable through audit rows plus execution results.
    connect(privilegedUser, privilegedPassword, nonMasterJdbcUrl) {
        sql """SU '${ddlUser}'@'%' '${missingRoleMarker}'"""
        test {
            sql """
                CREATE TABLE test_console_su_multife_db.${forwardWarnDeniedTable} (
                    k1 INT
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "denied"
        }
    }

    // Statistics queries forwarded across FEs must keep the switched identity too.
    connect(privilegedUser, privilegedPassword, nonMasterJdbcUrl) {
        sql """SU '${processUser}'@'%' 'admin_readonly'"""
        def statsRows = sql """
            SELECT col_id
            FROM internal.__internal_schema.column_statistics
            WHERE tbl_id = ${tableId}
            ORDER BY id
        """
        Assert.assertTrue(statsRows.size() >= 2)
    }

    // Concurrent cross-FE DDL, query, and processlist operations should not leak role context.
    CountDownLatch concurrentStartLatch = new CountDownLatch(1)
    CountDownLatch concurrentDoneLatch = new CountDownLatch(4)
    List<String> concurrentFailures = Collections.synchronizedList(new ArrayList<String>())

    (0..<4).each { idx ->
        Thread.start {
            try {
                concurrentStartLatch.await()
                connect(privilegedUser, privilegedPassword, nonMasterJdbcUrl) {
                    if (idx % 2 == 0) {
                        sql """SU '${ddlUser}'@'%' '${ddlRole}'"""
                        sql """
                            CREATE TABLE test_console_su_multife_db.test_console_su_multife_concurrent_${idx} (
                                k1 INT
                            ) ENGINE=OLAP
                            DISTRIBUTED BY HASH(k1) BUCKETS 1
                            PROPERTIES ("replication_num" = "1")
                        """
                    } else {
                        sql """SU '${processUser}'@'%' 'admin_readonly'"""
                        def processRows = sql """SHOW FULL PROCESSLIST"""
                        Assert.assertTrue(!processRows.isEmpty())
                        def queryRows = sql """
                            SELECT * FROM test_console_su_multife_db.test_console_su_multife_t1 ORDER BY k1
                        """
                        Assert.assertEquals(1, queryRows.size())
                    }
                }
            } catch (Throwable t) {
                concurrentFailures.add("multife-${idx}: ${t.getMessage()}")
            } finally {
                concurrentDoneLatch.countDown()
            }
        }
    }

    concurrentStartLatch.countDown()
    Assert.assertTrue("test_console_su_multife_concurrent timed out", concurrentDoneLatch.await(120, TimeUnit.SECONDS))
    Assert.assertTrue("test_console_su_multife_concurrent failures: ${concurrentFailures}", concurrentFailures.isEmpty())

    if (auditEnabled) {
        def auditRows = waitAuditRecords([
                [key: "ddlAudit",
                 condition: """user = '${ddlUser}' AND stmt LIKE '%${forwardCreateOkTable}%'""",
                 description: "forwarded create audit for ${forwardCreateOkTable}"],
                [key: "missingRoleSuAudit",
                 condition: """stmt = 'SU ''${ddlUser}''@''%'' ''${missingRoleMarker}'''""",
                 description: "missing-role SU audit for ${missingRoleMarker}"],
                [key: "missingRoleDdlAudit",
                 condition: """user = '${ddlUser}' AND state = 'ERR' AND stmt LIKE '%${forwardWarnDeniedTable}%'""",
                 description: "missing-role forwarded DDL audit for ${forwardWarnDeniedTable}"]
        ])

        def ddlAudit = auditRows.ddlAudit
        assertAuditUser(ddlAudit[0].toString(), ddlUser)
        Assert.assertTrue(ddlAudit[1].toString().contains(forwardCreateOkTable))

        def missingRoleSuAudit = auditRows.missingRoleSuAudit
        Assert.assertTrue(missingRoleSuAudit[1].toString().contains(missingRoleMarker))

        def missingRoleDdlAudit = auditRows.missingRoleDdlAudit
        assertAuditUser(missingRoleDdlAudit[0].toString(), ddlUser)
        Assert.assertTrue(missingRoleDdlAudit[1].toString().contains(forwardWarnDeniedTable))
        Assert.assertEquals("ERR", missingRoleDdlAudit[2].toString())
        Assert.assertTrue(missingRoleDdlAudit[4].toString().toLowerCase().contains("denied"))
    }
}
