suite("ldap_test") {

    logger.info("ldap_test")
    
    String enabled = context.config.otherConfigs.get("enableLdapTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        
        String ldapHost = context.config.otherConfigs.get("ldapHost")
        String ldapPort = context.config.otherConfigs.get("ldapPort")
        String ldapUser = context.config.otherConfigs.get("ldapUser")
        String ldapPassword = context.config.otherConfigs.get("ldapPassword")
        String ldapBaseDn = context.config.otherConfigs.get("ldapBaseDn")
        String ldapFilter = context.config.otherConfigs.get("ldapFilter")
        String ldapFilePath = context.config.otherConfigs.get("ldapFilePath")

        String ldapJdbcUrl = "ldap://${ldapHost}:${ldapPort}"
        String ldapJdbcUser = ldapUser
        String ldapJdbcPassword = ldapPassword
        
        logger.info("ldapJdbcUrl: ${ldapJdbcUrl}")
        logger.info("ldapJdbcUser: ${ldapJdbcUser}")
        logger.info("ldapJdbcPassword: ${ldapJdbcPassword}")
        logger.info("ldapBaseDn: ${ldapBaseDn}")
        logger.info("ldapFilter: ${ldapFilter}")
        logger.info("ldapFilePath: ${ldapFilePath}")
    
    }
    
    
}