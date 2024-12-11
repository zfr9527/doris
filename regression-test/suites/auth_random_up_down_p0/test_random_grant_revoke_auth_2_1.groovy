suite("test_random_upgrade_downgrade_compatibility_auth_2_1","p0,auth,restart_fe") {

    String user1 = 'test_random_up_down_auth_user1'
    String role1 = 'test_random_up_down_auth_role1'
    String pwd = 'C123_567p'

    String dbName = 'test_auth_up_down_db'
    String tableName1 = 'test_auth_up_down_table1'
    String tableName2 = 'test_auth_up_down_table2'

    connect(user1, "${pwd}", context.config.jdbcUrl) {
        logger.info("url:" + context.config.jdbcUrl)
        def grants_res = sql """show grants;"""
        logger.info("grants_res: " + grants_res)

        assertTrue(grants_res[0][3] == "")
        assertTrue(grants_res[0][6] == "hive2_test_random_up_down_auth_ctl.test_auth_up_down_db: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv,Show_view_priv; hive3_test_random_up_down_auth_ctl.test_auth_up_down_db: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv,Show_view_priv; internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv; internal.test_auth_up_down_db: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv,Show_view_priv")
        assertTrue(grants_res[0][7] == "hive2_test_random_up_down_auth_ctl.test_auth_up_down_db.test_auth_up_down_table1: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv,Show_view_priv; hive3_test_random_up_down_auth_ctl.test_auth_up_down_db.test_auth_up_down_table1: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv,Show_view_priv; internal.test_auth_up_down_db.test_auth_up_down_table1: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv,Show_view_priv")
        assertTrue(grants_res[0][8] == "hive2_test_random_up_down_auth_ctl.test_auth_up_down_db.test_auth_up_down_table1: Select_priv[id]; hive3_test_random_up_down_auth_ctl.test_auth_up_down_db.test_auth_up_down_table1: Select_priv[id]; internal.test_auth_up_down_db.test_auth_up_down_table1: Select_priv[id]")
    }


    sql """revoke select_priv on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke select_priv on ${dbName} from ${user1}"""

    sql """revoke LOAD_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke LOAD_PRIV on ${dbName} from ${user1}"""

    sql """revoke ALTER_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke ALTER_PRIV on ${dbName} from ${user1}"""

    sql """revoke CREATE_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke CREATE_PRIV on ${dbName} from ${user1}"""

    sql """revoke DROP_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke DROP_PRIV on ${dbName} from ${user1}"""

    sql """revoke SHOW_VIEW_PRIV on ${dbName}.${tableName1} from ${user1}"""
    sql """revoke SHOW_VIEW_PRIV on ${dbName} from ${user1}"""

    sql """revoke select_priv(id) on ${dbName}.${tableName1} from ${user1}"""


    for (String hivePrefix : ["hive2", "hive3"]) {
        String catalog_name = "${hivePrefix}_test_random_up_down_auth_ctl"

        sql """revoke select_priv on ${catalog_name}.${dbName}.${tableName1} from ${user1}"""
        sql """revoke select_priv on ${catalog_name}.${dbName}.* from ${user1}"""

        sql """revoke LOAD_PRIV on ${catalog_name}.${dbName}.${tableName1} from ${user1}"""
        sql """revoke LOAD_PRIV on ${catalog_name}.${dbName}.* from ${user1}"""

        sql """revoke ALTER_PRIV on ${catalog_name}.${dbName}.${tableName1} from ${user1}"""
        sql """revoke ALTER_PRIV on ${catalog_name}.${dbName}.* from ${user1}"""

        sql """revoke CREATE_PRIV on ${catalog_name}.${dbName}.${tableName1} from ${user1}"""
        sql """revoke CREATE_PRIV on ${catalog_name}.${dbName}.* from ${user1}"""

        sql """revoke DROP_PRIV on ${catalog_name}.${dbName}.${tableName1} from ${user1}"""
        sql """revoke DROP_PRIV on ${catalog_name}.${dbName}.* from ${user1}"""

        sql """revoke SHOW_VIEW_PRIV on ${catalog_name}.${dbName}.${tableName1} from ${user1}"""
        sql """revoke SHOW_VIEW_PRIV on ${catalog_name}.${dbName}.* from ${user1}"""

        sql """revoke select_priv(id) on ${catalog_name}.${dbName}.${tableName1} from ${user1}"""

    }

    sleep(10 * 1000)
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        logger.info("url:" + context.config.jdbcUrl)
        def grants_res = sql """show grants;"""
        logger.info("grants_res: " + grants_res)

        assertTrue(grants_res[0][3] == "")
        assertTrue(grants_res[0][6] == "internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv")
        assertTrue(grants_res[0][7] == null)
        assertTrue(grants_res[0][8] == null)

    }

    sql """grant '${role1}' to '${user1}'"""
    sleep(10 * 1000)
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        logger.info("url:" + context.config.jdbcUrl)
        def grants_res = sql """show grants;"""
        logger.info("grants_res: " + grants_res)
        assertTrue(grants_res[0][3] == "test_random_up_down_auth_role1")
        assertTrue(grants_res[0][6] == "hive2_test_random_up_down_auth_ctl.test_auth_up_down_db: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv,Show_view_priv; hive3_test_random_up_down_auth_ctl.test_auth_up_down_db: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv,Show_view_priv; internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv; internal.test_auth_up_down_db: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv,Show_view_priv")
        assertTrue(grants_res[0][7] == "hive2_test_random_up_down_auth_ctl.test_auth_up_down_db.test_auth_up_down_table1: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv,Show_view_priv; hive3_test_random_up_down_auth_ctl.test_auth_up_down_db.test_auth_up_down_table1: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv,Show_view_priv; internal.test_auth_up_down_db.test_auth_up_down_table1: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv,Show_view_priv")
        assertTrue(grants_res[0][8] == "hive2_test_random_up_down_auth_ctl.test_auth_up_down_db.test_auth_up_down_table1: Select_priv[id]; hive3_test_random_up_down_auth_ctl.test_auth_up_down_db.test_auth_up_down_table1: Select_priv[id]; internal.test_auth_up_down_db.test_auth_up_down_table1: Select_priv[id]")
    }
    sql """revoke '${role1}' from '${user1}'"""
    sleep(10 * 1000)
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        logger.info("url:" + context.config.jdbcUrl)
        def grants_res = sql """show grants;"""
        logger.info("grants_res: " + grants_res)
        assertTrue(grants_res[0][3] == "")
        assertTrue(grants_res[0][6] == "internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv")
        assertTrue(grants_res[0][7] == null)
        assertTrue(grants_res[0][8] == null)
    }

    sql """grant select_priv on ${dbName}.${tableName1} to ${user1}"""
    sql """grant select_priv on ${dbName} to ${user1}"""

    sql """grant LOAD_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant LOAD_PRIV on ${dbName} to ${user1}"""

    sql """grant ALTER_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant ALTER_PRIV on ${dbName} to ${user1}"""

    sql """grant CREATE_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant CREATE_PRIV on ${dbName} to ${user1}"""

    sql """grant DROP_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant DROP_PRIV on ${dbName} to ${user1}"""

    sql """grant SHOW_VIEW_PRIV on ${dbName}.${tableName1} to ${user1}"""
    sql """grant SHOW_VIEW_PRIV on ${dbName} to ${user1}"""

    sql """grant select_priv(id) on ${dbName}.${tableName1} to ${user1}"""



    for (String hivePrefix : ["hive2", "hive3"]) {
        String catalog_name = "${hivePrefix}_test_random_up_down_auth_ctl"

        sql """grant select_priv on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
        sql """grant select_priv on ${catalog_name}.${dbName}.* to ${user1}"""

        sql """grant LOAD_PRIV on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
        sql """grant LOAD_PRIV on ${catalog_name}.${dbName}.* to ${user1}"""

        sql """grant ALTER_PRIV on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
        sql """grant ALTER_PRIV on ${catalog_name}.${dbName}.* to ${user1}"""

        sql """grant CREATE_PRIV on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
        sql """grant CREATE_PRIV on ${catalog_name}.${dbName}.* to ${user1}"""

        sql """grant DROP_PRIV on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
        sql """grant DROP_PRIV on ${catalog_name}.${dbName}.* to ${user1}"""

        sql """grant SHOW_VIEW_PRIV on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""
        sql """grant SHOW_VIEW_PRIV on ${catalog_name}.${dbName}.* to ${user1}"""

        sql """grant select_priv(id) on ${catalog_name}.${dbName}.${tableName1} to ${user1}"""

    }
}
