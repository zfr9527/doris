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

/*
Build the basic information. When conceiving upgrade and downgrade test cases, you should firmly grasp the object
representation of the function in the FE meta.
Taking MTMV as an example, the main function points are creation, refresh, and rewriting, and the involved entities
are the base table and MTMV.
1.When creating an MTMV, check if the rewriting meets the expectations.
2.When refreshing an MTMV, check if the rewriting meets the expectations.
3.When deleting an MTMV, check if the rewriting meets the expectations.
4.When deleting a base table, check if the rewriting meets the expectations; then trigger a refresh and check if the
rewriting meets the expectations.
5.When deleting a partition of a base table, check if the rewriting meets the expectations; then trigger a refresh and
check if the rewriting meets the expectations.
6.Design a slightly more complex scenario. For example: Build an MTMV with two base tables. When deleting one of the
base tables, check if the refresh of the MTMV meets the expectations and if the rewriting meets the expectations;
create an MTMV with the undeleted base table and check if it can be created and refreshed normally, and if the
corresponding rewriting meets the expectations.
 */
suite("test_upgrade_downgrade_prepare_olap_mtmv_zfr_hive_tmp","p0,mtmv,restart_fe") {
    String suiteName = "mtmv_up_down_olap_hive"
    String ctlName = "${suiteName}_ctl"
    String dbName = context.config.getDbNameByFile(context.file)

//    sql """create catalog if not exists ${ctlName} properties (
//        "type"="hms",
//        'hive.metastore.uris' = 'thrift://172.20.48.119:9383',
//        'fs.defaultFS' = 'hdfs://172.20.48.119:8320',
//        'hadoop.username' = 'hadoop',
//        'enable.auto.analyze' = 'false'
//        );"""
//    sql """switch ${ctlName}"""
//    sql """create database if not exists ${dbName}"""
//    sql """use ${dbName}"""

    String hivePrefix = "hive3"
    setHivePrefix(hivePrefix)

    hive_docker """ set hive.stats.column.autogather = false; """


    String tableName1 = """${suiteName}_tb1"""
    String tableName2 = """${suiteName}_tb2"""
    String tableName3 = """${suiteName}_tb3"""
    String tableName4 = """${suiteName}_tb4"""
    String tableName5 = """${suiteName}_tb5"""
    String tableName6 = """${suiteName}_tb6"""
    String tableName7 = """${suiteName}_tb7"""
    String tableName8 = """${suiteName}_tb8"""
    String tableName9 = """${suiteName}_tb9"""
    String tableName10 = """${suiteName}_tb10"""
    String mtmvName1 = """${suiteName}_mtmv1"""
    String mtmvName2 = """${suiteName}_mtmv2"""
    String mtmvName3 = """${suiteName}_mtmv3"""
    String mtmvName4 = """${suiteName}_mtmv4"""
    String mtmvName5 = """${suiteName}_mtmv5"""
    String mtmvName6 = """${suiteName}_mtmv6"""

    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2018-01-15') values (13,13)"""
    hive_docker """ set hive.stats.column.autogather = true; """

}
