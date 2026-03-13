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

import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

// Verifies SU stability under repeated switching, concurrency, role mutation,
// HTTP isolation, and malformed or boundary-case inputs.
suite("test_console_su_stability_auth", "p2,auth,auth_console") {
    String readerUser = "test_console_su_stability_reader_user"
    String nopermUser = "test_console_su_stability_noperm_user"
    String pwd = "C123_567p"
    String readerRole = "test_console_su_stability_reader_role"
    String flakyRole = "test_console_su_stability_flaky_role"
    String missingRoleMarker = "test_console_su_stability_missing_role_" + UUID.randomUUID().toString().replace("-", "").substring(0, 6)
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

    def httpQuery = { String authUser, String authPwd, String suUser, String suRoles, String stmt, Closure checkFunc ->
        def requestBody = JsonOutput.toJson([stmt: stmt, is_sync: true, limit: 100])
        httpTest {
            basicAuthorization "${authUser}", "${authPwd}"
            endpoint "${context.config.feHttpAddress}"
            uri "/api/query/internal/test_console_su_stability_db"
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

    def assertHttpSuccess = { int respCode, String body ->
        Assert.assertEquals(200, respCode)
        def json = parseJson(body)
        Assert.assertEquals(0, json.code)
        Assert.assertEquals(1, json.data.data.size())
        Assert.assertEquals("1", json.data.data[0][0].toString())
        Assert.assertEquals("10", json.data.data[0][1].toString())
    }

    def assertHttpCurrentUserContains = { int respCode, String body, String expectedUser ->
        Assert.assertEquals(200, respCode)
        def json = parseJson(body)
        Assert.assertEquals(0, json.code)
        Assert.assertEquals(1, json.data.data.size())
        Assert.assertTrue(json.data.data[0][0].toString().toLowerCase().contains(expectedUser.toLowerCase()))
    }

    def assertHttpDeniedByAny = { int respCode, String body, List<String> messageParts ->
        Assert.assertEquals(200, respCode)
        def json = parseJson(body)
        Assert.assertTrue(json.code != 0)
        String message = json.data.toString().toLowerCase()
        Assert.assertTrue(messageParts.any { message.contains(it.toLowerCase()) })
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

    def runConcurrent = { String name, int threadCount, Closure worker ->
        CountDownLatch startLatch = new CountDownLatch(1)
        CountDownLatch doneLatch = new CountDownLatch(threadCount)
        List<String> failures = Collections.synchronizedList(new ArrayList<String>())

        (0..<threadCount).each { idx ->
            Thread.start {
                try {
                    startLatch.await()
                    worker(idx)
                } catch (Throwable t) {
                    failures.add("${name}-${idx}: ${t.getMessage()}")
                } finally {
                    doneLatch.countDown()
                }
            }
        }

        startLatch.countDown()
        Assert.assertTrue("${name} timed out", doneLatch.await(120, TimeUnit.SECONDS))
        Assert.assertTrue("${name} failures: ${failures}", failures.isEmpty())
    }

    try_sql("DROP USER ${readerUser}")
    try_sql("DROP USER ${nopermUser}")
    try_sql("DROP ROLE ${readerRole}")
    try_sql("DROP ROLE ${flakyRole}")
    sql """DROP DATABASE IF EXISTS test_console_su_stability_db"""

    sql """CREATE ROLE ${readerRole}"""
    sql """CREATE ROLE ${flakyRole}"""
    sql """CREATE USER '${readerUser}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${nopermUser}' IDENTIFIED BY '${pwd}'"""
    grantCloudUsageIfNeeded(readerUser)
    grantCloudUsageIfNeeded(nopermUser)
    grantDefaultDbAccess(readerUser)
    grantDefaultDbAccess(nopermUser)

    sql """CREATE DATABASE test_console_su_stability_db"""
    sql """
        CREATE TABLE test_console_su_stability_db.test_console_su_stability_t1 (
            k1 INT,
            k2 INT
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_console_su_stability_db.test_console_su_stability_t1 VALUES (1, 10)"""
    sql """GRANT SELECT_PRIV ON test_console_su_stability_db.test_console_su_stability_t1 TO ROLE '${readerRole}'"""
    sql """GRANT SELECT_PRIV ON test_console_su_stability_db.test_console_su_stability_t1 TO ROLE '${flakyRole}'"""
    sql """GRANT '${readerRole}' TO '${readerUser}'"""

    // The second SU in one session should fail consistently, and repeated SU
    // across fresh root sessions should stay stable. SU without roles inherits kernel permissions for existing users.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${readerUser}'@'%'"""
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        Assert.assertTrue(currentUserText.contains(readerUser))
        def rows = sql """SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
        test {
            sql """SU '${nopermUser}'@'%' '${readerRole}'"""
            exception "Only root can execute su"
        }
    }

    (0..<12).each { idx ->
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            if (idx % 3 == 0) {
                sql """SU '${readerUser}'@'%'"""
            } else if (idx % 3 == 1) {
                sql """SU '${nopermUser}'@'%' '${readerRole}'"""
            } else {
                sql """SU '${nopermUser}'@'%'"""
            }

            def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
            if (idx % 3 == 0) {
                Assert.assertTrue(currentUserText.contains(readerUser))
                def rows = sql """SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1"""
                Assert.assertEquals(1, rows.size())
            } else if (idx % 3 == 1) {
                Assert.assertTrue(currentUserText.contains(nopermUser))
                def rows = sql """SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1"""
                Assert.assertEquals(1, rows.size())
            } else {
                Assert.assertTrue(currentUserText.contains(nopermUser))
                test {
                    sql """SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1"""
                    exception "denied"
                }
            }
        }
    }

    // Concurrent SU across sessions must keep identities isolated.
    runConcurrent("test_console_su_stability_concurrent_su", 6) { int idx ->
        (0..<5).each {
            connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
                String targetUser = idx % 2 == 0 ? readerUser : nopermUser
                sql """SU '${targetUser}'@'%' '${readerRole}'"""
                def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
                Assert.assertTrue(currentUserText.contains(targetUser))
                def rows = sql """SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1"""
                Assert.assertEquals(1, rows.size())
            }
        }
    }

    // Concurrent mixed valid/invalid roles should remain stable.
    runConcurrent("test_console_su_stability_mixed_roles", 4) { int idx ->
        (0..<6).each {
            connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
                sql """SU '${nopermUser}'@'%' '${readerRole}', '${missingRoleMarker}_${idx}'"""
                def rows = sql """SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1"""
                Assert.assertEquals(1, rows.size())
            }
        }
    }

    // Concurrent processlist queries should stay stable.
    runConcurrent("test_console_su_stability_processlist", 4) { int idx ->
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """SU '${readerUser}'@'%' 'admin_readonly'"""
            (0..<5).each {
                def rows = sql """SHOW FULL PROCESSLIST"""
                Assert.assertTrue(!rows.isEmpty())
            }
        }
    }

    // Repeated HTTP SU requests must stay request-scoped.
    (0..<5).each {
        httpQuery(privilegedUser, privilegedPassword, "${nopermUser}@%", readerRole,
                "SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1") {
            respCode, body ->
                assertHttpSuccess(respCode, body)
        }
        httpQuery(privilegedUser, privilegedPassword, "${nopermUser}@%", null,
                "SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1") {
            respCode, body ->
                assertHttpDeniedByAny(respCode, body, ["denied"])
        }
        httpQuery(privilegedUser, privilegedPassword, "${readerUser}@%", null,
                "SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1") {
            respCode, body ->
                assertHttpSuccess(respCode, body)
        }
    }

    // Concurrent role mutation and SU queries should only resolve as success
    // or permission denial, without random failures.
    CountDownLatch st06StartLatch = new CountDownLatch(1)
    CountDownLatch st06DoneLatch = new CountDownLatch(2)
    List<String> st06Failures = Collections.synchronizedList(new ArrayList<String>())

    Thread.start {
        try {
            st06StartLatch.await()
            connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
                (0..<5).each {
                    try_sql("DROP ROLE ${flakyRole}")
                    sql """CREATE ROLE ${flakyRole}"""
                    sql """GRANT SELECT_PRIV ON test_console_su_stability_db.test_console_su_stability_t1 TO ROLE '${flakyRole}'"""
                    sleep(200)
                }
            }
        } catch (Throwable t) {
            st06Failures.add("role-mutation: ${t.getMessage()}")
        } finally {
            st06DoneLatch.countDown()
        }
    }

    Thread.start {
        try {
            st06StartLatch.await()
            (0..<15).each {
                connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
                    try {
                        sql """SU '${nopermUser}'@'%' '${flakyRole}'"""
                        def rows = sql """SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1"""
                        Assert.assertEquals(1, rows.size())
                    } catch (Exception e) {
                        String message = e.getMessage().toLowerCase()
                        Assert.assertTrue(message.contains("denied")
                                || message.contains("unknown")
                                || message.contains("does not exist")
                                || message.contains(flakyRole.toLowerCase()))
                    }
                }
            }
        } catch (Throwable t) {
            st06Failures.add("su-query: ${t.getMessage()}")
        } finally {
            st06DoneLatch.countDown()
        }
    }

    st06StartLatch.countDown()
    Assert.assertTrue("test_console_su_stability_role_mutation timed out", st06DoneLatch.await(120, TimeUnit.SECONDS))
    Assert.assertTrue("test_console_su_stability_role_mutation failures: ${st06Failures}", st06Failures.isEmpty())

    // Boundary inputs must fail controllably and leave the session clean.
    String longUser = "test_console_su_stability_tmp_" + ("x" * 48)
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        try {
            sql """SU '${longUser}'@'%' '${readerRole}'"""
            def rows = sql """SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1"""
            Assert.assertEquals(1, rows.size())
        } catch (Exception e) {
            String message = e.getMessage().toLowerCase()
            Assert.assertTrue(message.contains("user")
                    || message.contains("long")
                    || message.contains("syntax")
                    || message.contains("unknown"))
        }
    }

    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        String longRoleList = (0..<24).collect { "'${readerRole}'" }.join(", ")
        sql """SU '${nopermUser}'@'%' ${longRoleList}"""
        def rows = sql """SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
    }

    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        try {
            sql """SU '${nopermUser}'@'%' '${readerRole}' ,"""
            Assert.fail("expected malformed SU syntax to fail")
        } catch (Exception e) {
            assertInvalidSuSyntax(e)
        }

        sql """SU '${nopermUser}'@'%' '${readerRole}'"""
        def recoveredRows = sql """SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1"""
        Assert.assertEquals(1, recoveredRows.size())
    }

    // The current HTTP SU path accepts a target identity that contains spaces
    // and still validates permissions according to the injected role.
    httpQuery(privilegedUser, privilegedPassword, "bad user format", readerRole,
            "SELECT CURRENT_USER()") {
        respCode, body ->
            assertHttpCurrentUserContains(respCode, body, "bad user format")
    }
    httpQuery(privilegedUser, privilegedPassword, "bad user format", readerRole,
            "SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1") {
        respCode, body ->
            assertHttpSuccess(respCode, body)
    }
    httpQuery(privilegedUser, privilegedPassword, "${readerUser}@%", readerRole,
            "SELECT * FROM test_console_su_stability_db.test_console_su_stability_t1 ORDER BY k1") {
        respCode, body ->
            assertHttpSuccess(respCode, body)
    }
}
