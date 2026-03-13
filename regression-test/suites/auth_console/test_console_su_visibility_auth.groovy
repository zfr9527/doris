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

// Verifies metadata visibility, grants output, processlist filtering, catalog
// exposure, and statistics access under switched SU identities.
suite("test_console_su_visibility_auth", "p0,auth,auth_console") {
    String visibleUser = "test_console_su_visibility_user"
    String otherUser = "test_console_su_visibility_other_user"
    String pwd = "C123_567p"
    String readerRole = "test_console_su_visibility_reader_role"
    String catalogRole = "test_console_su_visibility_catalog_role"
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

    def assertDeniedOrHidden = { Closure action ->
        try {
            action()
            Assert.fail("expected access to be denied or hidden")
        } catch (Exception e) {
            String message = e.getMessage().toLowerCase()
            Assert.assertTrue(message.contains("denied") || message.contains("unknown"))
        }
    }

    def rowContainsUser = { Map row, String userName ->
        row.values().any { value -> value != null && value.toString().contains(userName) }
    }

    def roleClause = { List<String> roles ->
        roles.collect { "'${it}'" }.join(", ")
    }

    def assertGrantTextExcludesKernelState = { String grantsText ->
        String normalizedText = grantsText.toLowerCase()
        Assert.assertFalse(normalizedText.contains(context.config.defaultDb.toLowerCase()))
        if (validCluster != null) {
            Assert.assertFalse(normalizedText.contains(validCluster.toLowerCase()))
        }
    }

    // Prepare one visible set and one hidden set so metadata filters can be checked from both sides.
    try_sql("DROP USER ${visibleUser}")
    try_sql("DROP USER ${otherUser}")
    try_sql("DROP ROLE ${readerRole}")
    try_sql("DROP ROLE ${catalogRole}")
    sql """DROP DATABASE IF EXISTS test_console_su_visibility_db"""
    sql """DROP DATABASE IF EXISTS test_console_su_visibility_noauth_db"""
    sql """DROP CATALOG IF EXISTS test_console_su_visibility_catalog"""
    sql """DROP CATALOG IF EXISTS test_console_su_visibility_hidden_catalog"""

    sql """CREATE ROLE ${readerRole}"""
    sql """CREATE ROLE ${catalogRole}"""
    sql """CREATE USER '${visibleUser}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${otherUser}' IDENTIFIED BY '${pwd}'"""
    grantCloudUsageIfNeeded(visibleUser)
    grantCloudUsageIfNeeded(otherUser)
    grantDefaultDbAccess(visibleUser)
    grantDefaultDbAccess(otherUser)

    sql """CREATE DATABASE test_console_su_visibility_db"""
    sql """CREATE DATABASE test_console_su_visibility_noauth_db"""
    sql """
        CREATE TABLE test_console_su_visibility_db.test_console_su_visibility_t1 (
            k1 INT,
            k2 INT
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE test_console_su_visibility_db.test_console_su_visibility_hidden_t1 (
            k1 INT
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE test_console_su_visibility_noauth_db.test_console_su_visibility_noauth_t1 (
            k1 INT
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE VIEW test_console_su_visibility_db.test_console_su_visibility_v1 AS
        SELECT k1, k2 FROM test_console_su_visibility_db.test_console_su_visibility_t1
    """
    sql """
        CREATE VIEW test_console_su_visibility_db.test_console_su_visibility_hidden_v1 AS
        SELECT k1 FROM test_console_su_visibility_db.test_console_su_visibility_hidden_t1
    """
    sql """INSERT INTO test_console_su_visibility_db.test_console_su_visibility_t1 VALUES (1, 10)"""
    sql """CREATE CATALOG test_console_su_visibility_catalog PROPERTIES ("type"="es", "hosts"="http://8.8.8.8:9200")"""
    sql """CREATE CATALOG test_console_su_visibility_hidden_catalog PROPERTIES ("type"="es", "hosts"="http://8.8.8.8:9200")"""
    sql """GRANT SELECT_PRIV ON test_console_su_visibility_db.test_console_su_visibility_t1 TO ROLE '${readerRole}'"""
    sql """GRANT SELECT_PRIV ON test_console_su_visibility_db.test_console_su_visibility_v1 TO ROLE '${readerRole}'"""
    sql """GRANT SHOW_VIEW_PRIV ON test_console_su_visibility_db.test_console_su_visibility_v1 TO ROLE '${readerRole}'"""
    sql """GRANT SELECT_PRIV ON test_console_su_visibility_catalog.*.* TO ROLE '${catalogRole}'"""
    sql """GRANT '${readerRole}' TO '${visibleUser}'"""
    sql """GRANT '${catalogRole}' TO '${visibleUser}'"""
    sql """ANALYZE TABLE test_console_su_visibility_db.test_console_su_visibility_t1 WITH SYNC"""

    boolean histogramReady = true
    try {
        sql """ANALYZE TABLE test_console_su_visibility_db.test_console_su_visibility_t1 UPDATE HISTOGRAM WITH SYNC"""
    } catch (Exception e) {
        histogramReady = false
        log.info("skip histogram materialization because {}", e.getMessage())
    }

    boolean tableStatisticsSupported = true
    try {
        sql """SELECT COUNT(*) FROM internal.__internal_schema.table_statistics"""
    } catch (Exception e) {
        tableStatisticsSupported = false
        log.info("skip table_statistics validation because {}", e.getMessage())
    }

    def visibleTableId = get_table_id("internal", "test_console_su_visibility_db", "test_console_su_visibility_t1")

    // Metadata listings must reflect
    // the switched identity instead of root.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${visibleUser}'@'%' ${roleClause([readerRole, catalogRole])}"""
        def currentUserText = sql("""SELECT CURRENT_USER()""")[0][0].toString()
        Assert.assertTrue(currentUserText.contains(visibleUser))

        def showDatabases = sql """SHOW DATABASES"""
        Assert.assertTrue(showDatabases.toString().contains("test_console_su_visibility_db"))
        Assert.assertFalse(showDatabases.toString().contains("test_console_su_visibility_noauth_db"))

        def showTables = sql """SHOW TABLES FROM test_console_su_visibility_db"""
        Assert.assertTrue(showTables.toString().contains("test_console_su_visibility_t1"))
        Assert.assertTrue(showTables.toString().contains("test_console_su_visibility_v1"))
        Assert.assertFalse(showTables.toString().contains("test_console_su_visibility_hidden_t1"))
        Assert.assertFalse(showTables.toString().contains("test_console_su_visibility_hidden_v1"))

        // Without explicit information_schema privileges, the switched
        // identity must not query these system tables.
        def curGrants = sql """SHOW GRANTS"""
        log.info("SHOW GRANTS result: {}", curGrants)
        test {
            sql """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'test_console_su_visibility_db'
                ORDER BY table_name
            """
            exception "denied"
        }

        test {
            sql """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'test_console_su_visibility_db'
                  AND table_name = 'test_console_su_visibility_t1'
                ORDER BY column_name
            """
            exception "denied"
        }

        // Visible views and table metadata should succeed,
        // while hidden objects stay inaccessible.
        def viewRows = sql """SELECT * FROM test_console_su_visibility_db.test_console_su_visibility_v1 ORDER BY k1"""
        Assert.assertEquals(1, viewRows.size())

        def showCreateView = sql """SHOW CREATE VIEW test_console_su_visibility_db.test_console_su_visibility_v1"""
        Assert.assertTrue(showCreateView.toString().contains("test_console_su_visibility_v1"))

        def showCreateTable = sql """SHOW CREATE TABLE test_console_su_visibility_db.test_console_su_visibility_t1"""
        Assert.assertTrue(showCreateTable.toString().contains("test_console_su_visibility_t1"))

        def descRows = sql """DESC test_console_su_visibility_db.test_console_su_visibility_t1"""
        Assert.assertEquals(2, descRows.size())

        def showGrants = sql """SHOW GRANTS"""
        String showGrantsText = showGrants.toString()
        Assert.assertTrue(showGrantsText.contains(readerRole))
        Assert.assertTrue(showGrantsText.contains(catalogRole))
        Assert.assertFalse(showGrantsText.contains("admin_readonly"))
        assertGrantTextExcludesKernelState(showGrantsText)

        // The self property view should stay scoped to the
        // effective switched identity.
        def showProperty = sql """SHOW PROPERTY LIKE 'max_query_instances'"""
        Assert.assertEquals("max_query_instances", showProperty[0][0].toString())

        def showCatalogs = sql """SHOW CATALOGS"""
        Assert.assertTrue(showCatalogs.toString().contains("test_console_su_visibility_catalog"))
        Assert.assertFalse(showCatalogs.toString().contains("test_console_su_visibility_hidden_catalog"))

        // Statistics system tables should be denied
        // without explicit __internal_schema privileges.
        test {
            sql """
                SELECT col_id, `count`
                FROM internal.__internal_schema.column_statistics
                WHERE tbl_id = ${visibleTableId}
                ORDER BY id
            """
            exception "denied"
        }
        if (histogramReady) {
            test {
                sql """SELECT * FROM internal.__internal_schema.histogram_statistics"""
                exception "denied"
            }
        }
        if (tableStatisticsSupported) {
            test {
                sql """SELECT * FROM internal.__internal_schema.table_statistics WHERE tbl_id = ${visibleTableId}"""
                exception "denied"
            }
        }

        // Ordinary users should only see processlist rows that belong to their visible scope.
        def processList = sql_return_maparray """SHOW PROCESSLIST"""
        Assert.assertEquals(1, processList.size())
        Assert.assertTrue(processList.every { row -> rowContainsUser(row, visibleUser) })

        // Hidden metadata and other users'
        // authorization info must not leak.
        test {
            sql """SELECT * FROM test_console_su_visibility_noauth_db.test_console_su_visibility_noauth_t1"""
            exception "denied"
        }

        assertDeniedOrHidden {
            sql """SHOW CREATE TABLE test_console_su_visibility_db.test_console_su_visibility_hidden_t1"""
        }

        assertDeniedOrHidden {
            sql """DESC test_console_su_visibility_db.test_console_su_visibility_hidden_t1"""
        }

        assertDeniedOrHidden {
            sql """SHOW CREATE VIEW test_console_su_visibility_db.test_console_su_visibility_hidden_v1"""
        }

        test {
            sql """SHOW GRANTS FOR '${otherUser}'@'%'"""
            exception "denied"
        }

        test {
            sql """SHOW GRANTS FOR '${privilegedUser}'@'%'"""
            exception "denied"
        }

        test {
            sql """SHOW PROPERTY FOR '${otherUser}' LIKE 'max_query_instances'"""
            exception "denied"
        }

        test {
            sql """SHOW PROC '/backends'"""
            exception "denied"
        }
    }

    // Different switched identities should only observe their own grants.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${otherUser}'@'%'"""
        def showGrants = sql """SHOW GRANTS"""
        String showGrantsText = showGrants.toString()
        logger.info("SHOW GRANTS for ${otherUser}: " + showGrantsText)
        Assert.assertFalse(showGrantsText.contains(readerRole))
        Assert.assertFalse(showGrantsText.contains(catalogRole))
        Assert.assertFalse(showGrantsText.contains("admin_readonly"))

        def showDatabases = sql """SHOW DATABASES"""
        Assert.assertFalse(showDatabases.toString().contains("test_console_su_visibility_db"))
        Assert.assertFalse(showDatabases.toString().contains("test_console_su_visibility_noauth_db"))

        def showCatalogs = sql """SHOW CATALOGS"""
        Assert.assertFalse(showCatalogs.toString().contains("test_console_su_visibility_catalog"))
        Assert.assertFalse(showCatalogs.toString().contains("test_console_su_visibility_hidden_catalog"))
    }

    // Without explicit roles, SU for an existing user inherits their kernel-level grants.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${visibleUser}'@'%'"""
        def showGrants = sql """SHOW GRANTS"""
        String showGrantsText = showGrants.toString()
        logger.info("SHOW GRANTS for ${visibleUser}: " + showGrantsText)
        Assert.assertTrue(showGrantsText.contains(readerRole))
        Assert.assertTrue(showGrantsText.contains(catalogRole))
        Assert.assertFalse(showGrantsText.contains("admin_readonly"))

        def showDatabases = sql """SHOW DATABASES"""
        Assert.assertTrue(showDatabases.toString().contains("test_console_su_visibility_db"))
        Assert.assertFalse(showDatabases.toString().contains("test_console_su_visibility_noauth_db"))

        def showCatalogs = sql """SHOW CATALOGS"""
        Assert.assertTrue(showCatalogs.toString().contains("test_console_su_visibility_catalog"))
        Assert.assertFalse(showCatalogs.toString().contains("test_console_su_visibility_hidden_catalog"))
    }

    // Explicit SU user-role should override the target user's existing local roles
    // instead of merging with them.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${visibleUser}'@'%' 'admin_readonly'"""
        def showGrants = sql """SHOW GRANTS"""
        String showGrantsText = showGrants.toString()
        Assert.assertTrue(showGrantsText.contains("admin_readonly"))
        Assert.assertFalse(showGrantsText.contains(readerRole))
        Assert.assertFalse(showGrantsText.contains(catalogRole))
    }

    // Temporary users should only see the grants injected for the current session.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU 'test_console_su_visibility_tmp_user'@'%' '${readerRole}'"""
        def showGrants = sql """SHOW GRANTS"""
        String showGrantsText = showGrants.toString()
        Assert.assertTrue(showGrantsText.contains(readerRole))
        Assert.assertFalse(showGrantsText.contains(catalogRole))
        Assert.assertFalse(showGrantsText.contains("admin_readonly"))
        assertGrantTextExcludesKernelState(showGrantsText)
    }
}
