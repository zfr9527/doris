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

// Verifies that the built-in admin_readonly role can access every supported
// read-only path while remaining blocked from write, DDL, and management actions.
suite("test_console_admin_readonly_auth", "p0,auth,auth_console") {
    String readonlyUser = "test_console_admin_readonly_user"
    String pwd = "C123_567p"
    String privilegedUser = context.config.jdbcUser
    String privilegedPassword = context.config.jdbcPassword

    String validCluster = null
    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        validCluster = clusters[0][0].toString()
    }

    def grantDefaultDbAccess = { String userName ->
        sql """GRANT SELECT_PRIV ON ${context.config.defaultDb} TO ${userName}"""
    }

    if (validCluster != null) {
        try_sql("DROP USER ${readonlyUser}")
        sql """CREATE USER '${readonlyUser}' IDENTIFIED BY '${pwd}'"""
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${readonlyUser}"""
    } else {
        try_sql("DROP USER ${readonlyUser}")
        sql """CREATE USER '${readonlyUser}' IDENTIFIED BY '${pwd}'"""
    }
    grantDefaultDbAccess(readonlyUser)

    // These shared objects verify that admin_readonly has broad query visibility but no write privileges.
    try_sql("""DROP ROW POLICY IF EXISTS test_console_admin_readonly_policy ON test_console_admin_readonly_db.test_console_admin_readonly_t1 FOR '${readonlyUser}'@'%'""")
    sql """DROP DATABASE IF EXISTS test_console_admin_readonly_db"""
    sql """DROP CATALOG IF EXISTS test_console_admin_readonly_catalog"""
    sql """CREATE DATABASE test_console_admin_readonly_db"""
    sql """
        CREATE TABLE test_console_admin_readonly_db.test_console_admin_readonly_t1 (
            k1 INT,
            k2 INT
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_console_admin_readonly_db.test_console_admin_readonly_t1 VALUES (1, 10), (2, 20)"""
    sql """
        CREATE VIEW test_console_admin_readonly_db.test_console_admin_readonly_v1 AS
        SELECT k1, k2 FROM test_console_admin_readonly_db.test_console_admin_readonly_t1
    """
    sql """CREATE CATALOG test_console_admin_readonly_catalog PROPERTIES ("type"="es", "hosts"="http://8.8.8.8:9200")"""
    sql """CREATE ROW POLICY IF NOT EXISTS test_console_admin_readonly_policy
        ON test_console_admin_readonly_db.test_console_admin_readonly_t1
        AS RESTRICTIVE TO '${readonlyUser}'@'%'
        USING (k1 = 1)
    """

    // All query-only entry points should remain available.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${readonlyUser}'@'%' 'admin_readonly'"""
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        Assert.assertTrue(currentUserText.contains(readonlyUser))

        sql """USE test_console_admin_readonly_db"""
        sql """SHOW DATABASES"""
        sql """SHOW TABLES FROM test_console_admin_readonly_db"""

        def selectRows = sql """SELECT * FROM test_console_admin_readonly_db.test_console_admin_readonly_t1 ORDER BY k1"""
        Assert.assertEquals(1, selectRows.size())
        Assert.assertEquals("1", selectRows[0][0].toString())

        // admin_readonly should still respect row policy instead of bypassing row filters.
        def viewRows = sql """SELECT * FROM test_console_admin_readonly_db.test_console_admin_readonly_v1 ORDER BY k1"""
        Assert.assertEquals(1, viewRows.size())

        def descRows = sql """DESC test_console_admin_readonly_db.test_console_admin_readonly_t1"""
        Assert.assertEquals(2, descRows.size())

        sql """SHOW CREATE TABLE test_console_admin_readonly_db.test_console_admin_readonly_t1"""
        sql """SHOW CREATE VIEW test_console_admin_readonly_db.test_console_admin_readonly_v1"""
        sql """EXPLAIN SELECT * FROM test_console_admin_readonly_db.test_console_admin_readonly_t1"""

        def informationSchemaColumns = sql """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'test_console_admin_readonly_db'
              AND table_name = 'test_console_admin_readonly_t1'
            ORDER BY column_name
        """
        Assert.assertEquals(2, informationSchemaColumns.size())

        sql """SHOW RESOURCES"""
        sql """SHOW WORKLOAD GROUPS"""

        def processList = sql """SHOW PROCESSLIST"""
        Assert.assertTrue(!processList.isEmpty())

        def fullProcessList = sql """SHOW FULL PROCESSLIST"""
        logger.info("full process list: ${fullProcessList}")
        Assert.assertTrue(!fullProcessList.isEmpty())

        def showVariables = sql """SHOW VARIABLES LIKE 'sql_mode'"""
        Assert.assertEquals("sql_mode", showVariables[0][0].toString())

        def showGlobalVariables = sql """SHOW GLOBAL VARIABLES LIKE 'sql_mode'"""
        Assert.assertEquals("sql_mode", showGlobalVariables[0][0].toString())

        sql """SELECT @@session.sql_mode"""
        sql """SELECT @@global.sql_mode"""

        // SHOW GRANTS should reflect the effective admin_readonly identity.
        def showGrants = sql """SHOW GRANTS"""
        Assert.assertTrue(showGrants.toString().contains("admin_readonly"))

        def showCatalogs = sql """SHOW CATALOGS"""
        Assert.assertTrue(showCatalogs.toString().contains("internal"))

        sql """SHOW PROC '/backends'"""
        sql """SHOW PROC '/dbs'"""
        sql """SELECT COUNT(*) FROM internal.__internal_schema.column_statistics"""
        // sql """SELECT COUNT(*) FROM internal.__internal_schema.histogram_statistics"""
        sql """SET SESSION query_timeout = 10"""
        def queryTimeout = sql """SHOW VARIABLES LIKE 'query_timeout'"""
        Assert.assertEquals("query_timeout", queryTimeout[0][0].toString())
        Assert.assertTrue(queryTimeout[0][1].toString().startsWith("10"))

        // All write, DDL, and auth-management paths must be rejected.
        test {
            sql """INSERT INTO test_console_admin_readonly_db.test_console_admin_readonly_t1 VALUES (2, 20)"""
            exception "denied"
        }

        test {
            sql """
                INSERT INTO test_console_admin_readonly_db.test_console_admin_readonly_t1
                SELECT k1 + 1, k2 + 10
                FROM test_console_admin_readonly_db.test_console_admin_readonly_t1
            """
            exception "denied"
        }

        test {
            sql """
                CREATE TABLE test_console_admin_readonly_db.test_console_admin_readonly_create_denied (
                    k1 INT
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "denied"
        }

        test {
            sql """
                CREATE TABLE test_console_admin_readonly_db.test_console_admin_readonly_ctas_denied
                PROPERTIES ("replication_num" = "1") AS
                SELECT * FROM test_console_admin_readonly_db.test_console_admin_readonly_t1
            """
            exception "denied"
        }

        test {
            sql """GRANT SELECT_PRIV ON test_console_admin_readonly_db.* TO ${readonlyUser}"""
            exception "denied"
        }

        test {
            sql """REVOKE SELECT_PRIV ON test_console_admin_readonly_db.* FROM ${readonlyUser}"""
            exception "denied"
        }

        test {
            sql """CREATE USER 'test_console_admin_readonly_denied_user' IDENTIFIED BY 'Denied_123'"""
            exception "denied"
        }

        test {
            sql """SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES'"""
            exception "denied"
        }

        test {
            sql """ALTER TABLE test_console_admin_readonly_db.test_console_admin_readonly_t1 MODIFY COLUMN k2 BIGINT"""
            exception "denied"
        }

        test {
            sql """DROP TABLE test_console_admin_readonly_db.test_console_admin_readonly_t1"""
            exception "denied"
        }

        test {
            sql """DELETE FROM test_console_admin_readonly_db.test_console_admin_readonly_t1 WHERE k1 = 1"""
            exception "denied"
        }

        test {
            sql """UPDATE test_console_admin_readonly_db.test_console_admin_readonly_t1 SET k2 = 30 WHERE k1 = 1"""
            exception "denied"
        }

        test {
            sql """TRUNCATE TABLE test_console_admin_readonly_db.test_console_admin_readonly_t1"""
            exception "denied"
        }

        // Kill operations should remain denied for admin_readonly.
        try {
            sql """KILL QUERY 99999999"""
            Assert.fail("expected KILL QUERY to be rejected for admin_readonly")
        } catch (Exception e) {
            String message = e.getMessage().toLowerCase()
            Assert.assertTrue(message.contains("denied") || message.contains("unknown"))
        }

        try {
            sql """KILL CONNECTION 99999999"""
            Assert.fail("expected KILL CONNECTION to be rejected for admin_readonly")
        } catch (Exception e) {
            String message = e.getMessage().toLowerCase()
            Assert.assertTrue(message.contains("denied") || message.contains("unknown"))
        }

        // admin_readonly is built-in and must not be dropped or replaced.
        test {
            sql """DROP ROLE admin_readonly"""
            exception "Can not drop role"
        }
    }
}
