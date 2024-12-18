
suite("test_unicode_character_auth") {

    String user1 = 'test_unicode_character_auth_userA'
    String user2 = 'test_unicode_character_auth_userΩ'
    String user3 = 'test_unicode_character_auth_user.,!?©'
    String user4 = 'test_unicode_character_auth_user一$'
    String user5 = 'test_unicode_character_auth_user+-*÷》>=≥'
    String user6 = 'test_unicode_character_auth_user😊😢🎉 '
    String user7 = 'test_unicode_character_auth_user𝓐Δ'
    String user8 = 'test_unicode_character_auth_user𝔸⌘'


    String role1 = 'test_unicode_character_auth_roleA'
    String role2 = 'test_unicode_character_auth_roleΩ'
    String role3 = 'test_unicode_character_auth_role.,!?©'
    String role4 = 'test_unicode_character_auth_role一$'
    String role5 = 'test_unicode_character_auth_role+-*÷》>=≥'
    String role6 = 'test_unicode_character_auth_role😊😢🎉 '
    String role7 = 'test_unicode_character_auth_role𝓐Δ'
    String role8 = 'test_unicode_character_auth_role𝔸⌘'


    String pwd = 'C123_567p'
    String dbName = 'test_unicode_character_auth_db'
    String tableName = 'test_unicode_character_auth_tb'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}"""
    }

    try_sql("DROP USER ${user1}")
    try_sql("DROP USER ${user2}")
    try_sql("DROP USER ${user3}")
    try_sql("DROP USER ${user4}")
    try_sql("DROP USER ${user5}")
    try_sql("DROP USER ${user6}")
    try_sql("DROP USER ${user7}")
    try_sql("DROP USER ${user8}")

    try_sql """drop database if exists ${dbName}"""

    sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user2}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user3}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user4}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user5}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user6}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user7}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user8}' IDENTIFIED BY '${pwd}'"""

    sql """grant select_priv on regression_test to ${user1}"""
    sql """grant select_priv on regression_test to ${user2}"""
    sql """grant select_priv on regression_test to ${user3}"""
    sql """grant select_priv on regression_test to ${user4}"""
    sql """grant select_priv on regression_test to ${user5}"""
    sql """grant select_priv on regression_test to ${user6}"""
    sql """grant select_priv on regression_test to ${user7}"""
    sql """grant select_priv on regression_test to ${user8}"""

    sql """create database ${dbName}"""
    sql """create table ${dbName}.${tableName} (
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );"""

    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user1}"""
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
        def res = sql """show grants"""
        assertTrue(res == )
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${user1}"""

    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user2}"""
    connect(user2, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${user2}"""

    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user3}"""
    connect(user3, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${user3}"""

    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user4}"""
    connect(user4, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${user4}"""

    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user5}"""
    connect(user5, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${user5}"""

    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user6}"""
    connect(user6, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${user6}"""

    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user7}"""
    connect(user7, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${user7}"""

    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user8}"""
    connect(user8, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${user8}"""



    try_sql("DROP role ${role1}")
    try_sql("DROP role ${role2}")
    try_sql("DROP role ${role3}")
    try_sql("DROP role ${role4}")
    try_sql("DROP role ${role5}")
    try_sql("DROP role ${role6}")
    try_sql("DROP role ${role7}")
    try_sql("DROP role ${role8}")
    sql """CREATE ROLE ${role1}"""
    sql """CREATE ROLE ${role2}"""
    sql """CREATE ROLE ${role3}"""
    sql """CREATE ROLE ${role4}"""
    sql """CREATE ROLE ${role5}"""
    sql """CREATE ROLE ${role6}"""
    sql """CREATE ROLE ${role7}"""
    sql """CREATE ROLE ${role8}"""


    sql """GRANT '${role1}' TO ${user1};"""
    sql """grant SELECT_PRIV on ${dbName}.${tableName} to role ${role1}"""
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
        def res = sql """show grants"""
        assertTrue(res == )
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${role1}"""






}
