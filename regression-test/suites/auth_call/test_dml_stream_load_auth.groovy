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

import org.junit.Assert;
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_dml_stream_load_auth","p0,auth") {
    String user = 'test_dml_stream_load_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_dml_stream_load_auth_db'
    String tableName = 'test_dml_stream_load_auth_tb'

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""
    sql """create table ${dbName}.${tableName} (
                id BIGINT,
                username VARCHAR(20)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );"""

    def jdbcUrl = context.config.jdbcUrl
    def urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    def sql_port
    if (urlWithoutSchema.indexOf("/") >= 0) {
        // e.g: jdbc:mysql://locahost:8080/?a=b
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
    } else {
        // e.g: jdbc:mysql://locahost:8080
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
    }
    String feHttpAddress = context.config.feHttpAddress
    def http_port = feHttpAddress.substring(feHttpAddress.indexOf(":") + 1)

    def path_file = "${context.file.parent}/../../data/auth_call/stream_load_data.csv"

    def cm = """curl --location-trusted -u ${user}:${pwd} -H "column_separator:," -T ${path_file} http://${sql_ip}:${http_port}/api/${dbName}/${tableName}/_stream_load"""
    logger.info("cm: " + cm)
    try {
        def process = cm.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
    } catch (Exception e) {
        log.info(e.getMessage())
        logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
    }

    sql """grant load_priv on ${dbName}.${tableName} to ${user}"""

    def process = cm.toString().execute()
    def code = process.waitFor()
    def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    def out = process.getText()
    logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)

//    sql """drop database if exists ${dbName}"""
//    try_sql("DROP USER ${user}")
}
