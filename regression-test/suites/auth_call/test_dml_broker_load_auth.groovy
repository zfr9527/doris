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

suite("test_dml_broker_load_auth","p0,auth") {

    UUID uuid = UUID.randomUUID()
    String randomValue = uuid.toString()
    int hashCode = randomValue.hashCode()
    hashCode = hashCode > 0 ? hashCode : hashCode * (-1)

    String user = 'test_dml_broker_load_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_dml_broker_load_auth_db'
    String tableName = 'test_dml_broker_load_auth_tb'
    String loadLabelName = 'test_dml_broker_load_auth_label' + hashCode.toString()

    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = getS3BucketName()

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""

    sql """CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 32"""
    sql """
            LOAD LABEL ${loadLabelName} (
                DATA INFILE("s3://${bucket}/regression/tpch/sf0.01/customer.csv.gz")
                INTO TABLE ${dbName}.${tableName}
                COLUMNS TERMINATED BY "|"
                (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp)
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "$endpoint",
                "AWS_REGION" = "$region",
                "provider" = "${getS3Provider()}",
                "compress_type" = "GZ"
            )
            properties(
                "timeout" = "28800",
                "exec_mem_limit" = "8589934592",
            )
            """

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
