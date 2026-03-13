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

// Verifies HTTP-side SU behavior, including role injection, request isolation,
// error handling, and header parsing edge cases.
suite("test_console_http_su_auth", "p0,auth,auth_console") {
    String analystUser = "test_console_http_su_analyst"
    String nopermUser = "test_console_http_su_noperm_user"
    String specialUser = "Test.Console-Http_Su_User"
    String pwd = "C123_567p"
    String readerRole = "test_console_http_su_reader_role"
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

    def describeRolesHeader = { String suRoles ->
        if (suRoles == null) {
            return "<null>"
        }
        if (suRoles.trim().isEmpty()) {
            return "<blank>"
        }
        return suRoles
    }

    def logHttpResponse = { String caseName, int respCode, String body ->
        def json = parseJson(body)
        log.info("${caseName} response summary: respCode=${respCode}, code=${json.code}, msg=${json.msg}, data=${json.data}")
        return json
    }

    def httpQuery = { String caseName, String authUser, String authPwd, String suUser, String suRoles, String stmt, Closure checkFunc ->
        def requestBody = JsonOutput.toJson([stmt: stmt, is_sync: true, limit: 100])
        log.info("${caseName} request summary: authUser=${authUser}, suUser=${suUser == null ? "<null>" : suUser}, suRoles=${describeRolesHeader(suRoles)}, stmt=${stmt}")
        httpTest {
            basicAuthorization "${authUser}", "${authPwd}"
            endpoint "${context.config.feHttpAddress}"
            uri "/api/query/internal/test_console_http_su_auth_db"
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

    def assertSuccess = { String caseName, int respCode, String body ->
        Assert.assertEquals(200, respCode)
        def json = logHttpResponse(caseName, respCode, body)
        Assert.assertEquals(0, json.code)
        Assert.assertEquals("success", json.msg)
        Assert.assertEquals("result_set", json.data.type)
        Assert.assertEquals(1, json.data.data.size())
        Assert.assertEquals("1", json.data.data[0][0].toString())
        Assert.assertEquals("10", json.data.data[0][1].toString())
    }

    def assertCurrentUserContains = { String caseName, int respCode, String body, String expectedUser ->
        Assert.assertEquals(200, respCode)
        def json = logHttpResponse(caseName, respCode, body)
        Assert.assertEquals(0, json.code)
        Assert.assertEquals("success", json.msg)
        Assert.assertEquals("result_set", json.data.type)
        Assert.assertEquals(1, json.data.data.size())
        Assert.assertTrue(json.data.data[0][0].toString().toLowerCase().contains(expectedUser.toLowerCase()))
    }

    def assertDenied = { String caseName, int respCode, String body, String messagePart ->
        Assert.assertEquals(200, respCode)
        def json = logHttpResponse(caseName, respCode, body)
        Assert.assertTrue(json.code != 0)
        Assert.assertTrue(json.data.toString().toLowerCase().contains(messagePart.toLowerCase()))
    }

    def assertDeniedByAny = { String caseName, int respCode, String body, List<String> messageParts ->
        Assert.assertEquals(200, respCode)
        def json = logHttpResponse(caseName, respCode, body)
        Assert.assertTrue(json.code != 0)
        String message = json.data.toString().toLowerCase()
        Assert.assertTrue(messageParts.any { message.contains(it.toLowerCase()) })
    }

    // Prepare one HTTP target user with a readable role and one target user that needs temporary role injection.
    try_sql("DROP USER ${analystUser}")
    try_sql("DROP USER ${nopermUser}")
    try_sql("DROP USER '${specialUser}'")
    try_sql("DROP ROLE ${readerRole}")
    sql """DROP DATABASE IF EXISTS test_console_http_su_auth_db"""

    sql """CREATE ROLE ${readerRole}"""
    sql """CREATE USER '${analystUser}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${nopermUser}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${specialUser}' IDENTIFIED BY '${pwd}'"""
    grantCloudUsageIfNeeded(analystUser)
    grantCloudUsageIfNeeded(nopermUser)
    grantCloudUsageIfNeeded("'${specialUser}'")
    grantDefaultDbAccess(analystUser)
    grantDefaultDbAccess(nopermUser)
    grantDefaultDbAccess("'${specialUser}'")
    sql """CREATE DATABASE test_console_http_su_auth_db"""
    sql """
        CREATE TABLE test_console_http_su_auth_db.test_console_http_su_auth_t1 (
            k1 INT,
            k2 INT
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_console_http_su_auth_db.test_console_http_su_auth_t1 VALUES (1, 10)"""
    sql """GRANT SELECT_PRIV ON test_console_http_su_auth_db.test_console_http_su_auth_t1 TO ROLE '${readerRole}'"""
    sql """GRANT '${readerRole}' TO '${analystUser}'"""
    sql """GRANT '${readerRole}' TO '${specialUser}'"""

    // SQL and HTTP should observe the same effective identity and return the same data.
    connect(privilegedUser, privilegedPassword, context.config.jdbcUrl) {
        sql """SU '${analystUser}'@'%' '${readerRole}'"""
        def sqlRows = sql """SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1"""
        Assert.assertEquals(1, sqlRows.size())
        Assert.assertEquals("1", sqlRows[0][0].toString())
        Assert.assertEquals("10", sqlRows[0][1].toString())
    }

    // Root with HTTP SU headers should succeed and expose the injected role immediately.
    httpQuery("root_http_su_success", privilegedUser, privilegedPassword, "${analystUser}@%", readerRole,
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertSuccess("root_http_su_success", respCode, body)
    }

    // Non-root callers must not be able to use the HTTP SU headers.
    httpQuery("non_root_http_su_denied", analystUser, pwd, "${nopermUser}@%", readerRole,
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertDenied("non_root_http_su_denied", respCode, body, "root")
    }

    // Switching to a user without default roles must fail when no roles header is supplied.
    httpQuery("missing_roles_header_denied", privilegedUser, privilegedPassword, "${nopermUser}@%", null,
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertDenied("missing_roles_header_denied", respCode, body, "denied")
    }

    // A request with only nonexistent roles should not fail the SU parse path,
    // but the query must still be denied.
    httpQuery("nonexistent_roles_header_denied", privilegedUser, privilegedPassword, "${nopermUser}@%", "no_such_role",
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertDenied("nonexistent_roles_header_denied", respCode, body, "denied")
    }

    // Omitting the roles header for an existing Doris user inherits their kernel permissions.
    def analystUserGrantsBeforeImplicitRoleInheritance = sql """SHOW GRANTS FOR '${analystUser}'@'%'"""
    log.info("Existing user grants before HTTP SU without roles header: {}", analystUserGrantsBeforeImplicitRoleInheritance)
    httpQuery("omit_roles_header_inherits_existing_user_privileges", privilegedUser, privilegedPassword, "${analystUser}@%", null,
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertSuccess("omit_roles_header_inherits_existing_user_privileges", respCode, body)
    }
    

    // Mixed valid and invalid roles should keep the valid role effective.
    httpQuery("mixed_roles_keep_valid_role", privilegedUser, privilegedPassword, "${nopermUser}@%", "${readerRole},no_such_role",
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertSuccess("mixed_roles_keep_valid_role", respCode, body)
    }

    // Request-scoped role injection must not leak into the next request.
    httpQuery("request_scoped_role_injection_succeeds", privilegedUser, privilegedPassword, "${nopermUser}@%", readerRole,
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertSuccess("request_scoped_role_injection_succeeds", respCode, body)
    }
    httpQuery("next_request_without_roles_is_denied", privilegedUser, privilegedPassword, "${nopermUser}@%", null,
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertDenied("next_request_without_roles_is_denied", respCode, body, "denied")
    }

    // A valid user name with mixed case, dot, hyphen, and underscore should be accepted by HTTP SU.
    httpQuery("special_character_user_name_supported", privilegedUser, privilegedPassword, "${specialUser}@%", readerRole,
            "SELECT CURRENT_USER()") {
        respCode, body ->
            assertCurrentUserContains("special_character_user_name_supported", respCode, body, specialUser)
    }

    // Passing only the target user with a blank roles header is equivalent to no roles;
    // existing users inherit kernel permissions.
    httpQuery("blank_roles_header_inherits_existing_user_privileges", privilegedUser, privilegedPassword, "${analystUser}@%", "   ",
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertSuccess("blank_roles_header_inherits_existing_user_privileges", respCode, body)
    }

    // A literal "null" roles header is discarded by FE as an invalid value,
    // which is equivalent to omitting the roles header. Existing users inherit their kernel permissions.
    httpQuery("literal_null_roles_header_is_ignored", privilegedUser, privilegedPassword, "${analystUser}@%", "null",
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertSuccess("literal_null_roles_header_is_ignored", respCode, body)
    }

    httpQuery("explicit_role_header_control", privilegedUser, privilegedPassword, "${analystUser}@%", readerRole,
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertSuccess("explicit_role_header_control", respCode, body)
    }

    // Spaces and empty elements in the roles header should not corrupt role parsing.
    httpQuery("sparse_roles_header_parses_successfully", privilegedUser, privilegedPassword, "${nopermUser}@%", " ${readerRole}, , ${readerRole}, ",
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertSuccess("sparse_roles_header_parses_successfully", respCode, body)
    }

    // The framework stores headers in a Map, so simulate duplicate headers
    // with a gateway-merged multi-value string.
    httpQuery("merged_duplicate_roles_header_parses_successfully", privilegedUser, privilegedPassword, "${nopermUser}@%", "${readerRole},${readerRole}",
            "SELECT * FROM test_console_http_su_auth_db.test_console_http_su_auth_t1 ORDER BY k1") {
        respCode, body ->
            assertSuccess("merged_duplicate_roles_header_parses_successfully", respCode, body)
    }
}
