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

// Verifies that SU changes do not regress root behavior, direct-login users,
// role recreation semantics, or related non-SU compatibility paths.
suite("test_console_su_regression_auth", "p1,auth,auth_console") {
    String directUser = "test_console_su_regression_user"
    String localRoleUser = "test_console_su_regression_local_role_user"
    String pwd = "C123_567p"
    String readerRole = "test_console_su_regression_reader_role"
    String recycledRole = "test_console_su_regression_recycled_role"
    String privilegedUser = context.config.jdbcUser
    String privilegedPassword = context.config.jdbcPassword

    String validCluster = null
    if (isCloudMode()) {
        def clusters = sql """SHOW CLUSTERS"""
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

    try_sql("DROP USER ${directUser}")
    try_sql("DROP USER ${localRoleUser}")
    try_sql("DROP ROLE ${readerRole}")
    try_sql("DROP ROLE ${recycledRole}")
    sql """DROP DATABASE IF EXISTS test_console_su_regression_db"""

    sql """CREATE ROLE ${readerRole}"""
    sql """CREATE ROLE ${recycledRole}"""
    sql """CREATE USER '${directUser}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${localRoleUser}' IDENTIFIED BY '${pwd}'"""
    grantCloudUsageIfNeeded(directUser)
    grantCloudUsageIfNeeded(localRoleUser)
    grantDefaultDbAccess(directUser)
    grantDefaultDbAccess(localRoleUser)

    sql """CREATE DATABASE test_console_su_regression_db"""
    sql """
        CREATE TABLE test_console_su_regression_db.test_console_su_regression_t1 (
            k1 INT,
            k2 INT
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_console_su_regression_db.test_console_su_regression_t1 VALUES (1, 10)"""
    sql """GRANT SELECT_PRIV ON test_console_su_regression_db.test_console_su_regression_t1 TO ROLE '${readerRole}'"""
    sql """GRANT '${readerRole}' TO '${localRoleUser}'"""

    // Root's non-SU behavior should remain unchanged.
    def rootRows = sql """SELECT * FROM test_console_su_regression_db.test_console_su_regression_t1 ORDER BY k1"""
    Assert.assertEquals(1, rootRows.size())
    sql """
        CREATE TABLE test_console_su_regression_db.test_console_su_regression_root_create_ok (
            k1 INT
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """SHOW DATABASES"""
    sql """SHOW CATALOGS"""

    // Direct-login users and local roles should keep their original behavior.
    connect(localRoleUser, pwd, context.config.jdbcUrl) {
        def rows = sql """SELECT * FROM test_console_su_regression_db.test_console_su_regression_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
        test {
            sql """
                CREATE TABLE test_console_su_regression_db.test_console_su_regression_direct_create_denied (
                    k1 INT
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "denied"
        }
    }

    // A failed SU must not break the subsequent non-SU SQL path.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        try {
            sql """SU '${directUser}'@'%' '${readerRole}' '${recycledRole}'"""
            Assert.fail("expected invalid SU syntax to fail")
        } catch (Exception e) {
            assertInvalidSuSyntax(e)
        }
        def rows = sql """SELECT * FROM test_console_su_regression_db.test_console_su_regression_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
    }

    // Recreated roles should use the latest definition rather than the old cache.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${directUser}'@'%' '${recycledRole}'"""
        test {
            sql """SELECT * FROM test_console_su_regression_db.test_console_su_regression_t1 ORDER BY k1"""
            exception "denied"
        }
    }

    sql """DROP ROLE ${recycledRole}"""
    sql """CREATE ROLE ${recycledRole}"""
    sql """GRANT SELECT_PRIV ON test_console_su_regression_db.test_console_su_regression_t1 TO ROLE '${recycledRole}'"""

    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${directUser}'@'%' '${recycledRole}'"""
        def rows = sql """SELECT * FROM test_console_su_regression_db.test_console_su_regression_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
    }

    // LDAP login regression and FE-restart residue checks depend on extra
    // environment and are documented as environment-dependent items in the mapping file.
}
