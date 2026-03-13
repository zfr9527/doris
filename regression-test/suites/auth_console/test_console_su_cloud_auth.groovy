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

// Verifies SU behavior in cloud mode, especially compute group reuse,
// default compute group switching, and non-SU cloud regression coverage.
suite("test_console_su_cloud_auth", "p0,auth,auth_console") {
    if (!isCloudMode()) {
        log.info("skip test_console_su_cloud_auth because current mode is not cloud")
        return
    }

    String cloudReuseUser = "test_console_su_cloud_reuse_user"
    String cloudAOnlyUser = "test_console_su_cloud_a_only_user"
    String cloudDefaultBUser = "test_console_su_cloud_default_b_user"
    String pwd = "Cloud123_567p"
    String privilegedUser = context.config.jdbcUser
    String privilegedPassword = context.config.jdbcPassword

    def grantDefaultDbAccess = { String userName ->
        sql """GRANT SELECT_PRIV ON ${context.config.defaultDb} TO ${userName}"""
    }

    def assertCurrentCluster = { String expectedCluster ->
        def showClusters = sql_return_maparray """SHOW CLUSTERS"""
        def currentCluster = showClusters.find { row -> row.is_current == "TRUE" }
        Assert.assertTrue("SHOW CLUSTERS should still list all visible clusters, and exactly one row must be marked current: ${showClusters}",
                currentCluster != null)
        Assert.assertEquals("unexpected current cluster in SHOW CLUSTERS: ${showClusters}",
                expectedCluster, currentCluster.cluster)
    }

    def clusters = sql """SHOW CLUSTERS"""
    assertTrue(!clusters.isEmpty())
    String clusterA = clusters[0][0].toString()
    String clusterB = clusters.size() > 1 ? clusters[1][0].toString() : null

    try_sql("DROP USER ${cloudReuseUser}")
    try_sql("DROP USER ${cloudAOnlyUser}")
    try_sql("DROP USER ${cloudDefaultBUser}")
    sql """DROP DATABASE IF EXISTS test_console_su_cloud_db"""

    sql """CREATE USER '${cloudReuseUser}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${cloudAOnlyUser}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${cloudDefaultBUser}' IDENTIFIED BY '${pwd}'"""
    grantDefaultDbAccess(cloudReuseUser)
    grantDefaultDbAccess(cloudAOnlyUser)
    grantDefaultDbAccess(cloudDefaultBUser)

    sql """GRANT USAGE_PRIV ON CLUSTER `${clusterA}` TO ${cloudAOnlyUser}"""
    if (clusterB != null) {
        sql """GRANT USAGE_PRIV ON CLUSTER `${clusterB}` TO ${cloudDefaultBUser}"""
        sql """SET PROPERTY FOR '${cloudDefaultBUser}' 'default_compute_group' = '${clusterB}'"""
    } else {
        sql """GRANT USAGE_PRIV ON CLUSTER `${clusterA}` TO ${cloudDefaultBUser}"""
        sql """SET PROPERTY FOR '${cloudDefaultBUser}' 'default_compute_group' = '${clusterA}'"""
    }

    sql """CREATE DATABASE test_console_su_cloud_db"""
    sql """
        CREATE TABLE test_console_su_cloud_db.test_console_su_cloud_t1 (
            k1 INT,
            k2 INT
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_console_su_cloud_db.test_console_su_cloud_t1 VALUES (1, 10)"""

    // The existing non-SU cloud path should remain intact.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """USE @${clusterA}"""
        def rows = sql """SELECT * FROM test_console_su_cloud_db.test_console_su_cloud_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
        sql """SHOW BACKENDS"""
        sql """SHOW COMPUTE GROUPS"""
        sql """SHOW CLUSTERS"""
        sql """SELECT * FROM mv_infos("database"="test_console_su_cloud_db")"""
    }

    // When the current session already has a compute group, SU to a user without
    // a default compute group and without cluster privileges must not trigger infrastructure permission errors.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """USE @${clusterA}"""
        sql """SU '${cloudReuseUser}'@'%' 'admin_readonly'"""
        def rows = sql """SELECT * FROM test_console_su_cloud_db.test_console_su_cloud_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
        sql """SHOW BACKENDS"""
        sql """SHOW COMPUTE GROUPS"""
        sql """SHOW CLUSTERS"""
        sql """SELECT * FROM mv_infos("database"="test_console_su_cloud_db")"""
    }


    // A target user with a default compute group should switch
    // the current cluster marker in SHOW CLUSTERS to that group.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """USE @${clusterA}"""
        sql """SU '${cloudDefaultBUser}'@'%' 'admin_readonly'"""
        def rows = sql """SELECT * FROM test_console_su_cloud_db.test_console_su_cloud_t1 ORDER BY k1"""
        Assert.assertEquals(1, rows.size())
        if (clusterB != null) {
            assertCurrentCluster(clusterB)
        }
    }
    

    // The second SU in one session should fail consistently, and a fresh
    // root session must not inherit the previous session's current cluster.
    if (clusterB != null) {
        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """USE @${clusterA}"""
            sql """SU '${cloudDefaultBUser}'@'%' 'admin_readonly'"""
            def firstRows = sql """SELECT * FROM test_console_su_cloud_db.test_console_su_cloud_t1 ORDER BY k1"""
            Assert.assertEquals(1, firstRows.size())
            test {
                sql """SU '${cloudAOnlyUser}'@'%' 'admin_readonly'"""
                exception "Only root can execute su"
            }
        }

        connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
            sql """USE @${clusterA}"""
            sql """SU '${cloudAOnlyUser}'@'%' 'admin_readonly'"""
            def secondRows = sql """SELECT * FROM test_console_su_cloud_db.test_console_su_cloud_t1 ORDER BY k1"""
            Assert.assertEquals(1, secondRows.size())
            assertCurrentCluster(clusterA)
        }
    } else {
        log.info("skip second-switch cluster validation because only one compute group is available")
    }
}
