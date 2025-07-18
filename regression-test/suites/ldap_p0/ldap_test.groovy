suite("ldap_test") {

    logger.info("ldap_test")
    
    String enabled = context.config.otherConfigs.get("enableLdapTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        
        String ldapHost = config.otherConfigs.get("ldapHost")
        String ldapPort = config.otherConfigs.get("ldapPort")
        String ldapUser = config.otherConfigs.get("ldapUser")
        String ldapPassword = config.otherConfigs.get("ldapPassword")
        String ldapBaseDn = config.otherConfigs.get("ldapBaseDn")
        String ldapFilter = config.otherConfigs.get("ldapFilter")
        String ldapFilePath = config.otherConfigs.get("ldapFilePath")

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