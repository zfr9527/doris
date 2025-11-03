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

suite("test_negative_create_multi_pct_mtmv","mtmv") {
    String suiteName = "test_multi_pct_mtmv"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String tableName3 = "${suiteName}_table3"
    String mvName = "${suiteName}_mv"
    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop table if exists `${tableName3}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY RANGE(`k1`, `k2`)
        (
            PARTITION `p201701` VALUES [("2017-01-01", 1),  ("2017-02-01", 1)),
            PARTITION `p201702` VALUES [("2017-02-01", 1), ("2017-03-01", 1))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName1} values("2017-01-01",1);
        insert into ${tableName1} values("2017-02-01",2);
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION `p201702` VALUES [("2017-02-01"),  ("2017-03-01")),
            PARTITION `p201703` VALUES [("2017-03-01"), ("2017-04-01"))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName2} values("2017-02-01",3);
        insert into ${tableName2} values("2017-03-01",4);
        """

    sql """
        CREATE TABLE ${tableName3}
        (
            k3 INT not null,
            k4 INT not null
        )
        DISTRIBUTED BY HASH(k3) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        insert into ${tableName3} values(1,1);
        insert into ${tableName3} values(2,2);
        insert into ${tableName3} values(3,3);
        insert into ${tableName3} values(4,4);
        """
    String mvSql = "SELECT t1.k1,t1.k2,t2.k2 as k3,t3.k4 from ${tableName1} t1 inner join ${tableName2} t2 on t1.k1=t2.k1 left join ${tableName3} t3 on t1.k2=t3.k3;"
    test {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        ${mvSql}
        """
        exception "java.lang.IllegalArgumentException"
    }

    mvSql = "SELECT t1.k1,t1.k2,t2.k2 as k3,t3.k4 from ${tableName2} t1 inner join ${tableName1} t2 on t1.k1=t2.k1 left join ${tableName3} t3 on t1.k2=t3.k3;";
    test {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        ${mvSql}
        """
        exception "java.lang.IllegalArgumentException"
    }

    mvSql = "SELECT t1.k1, t1.k2, t2.k3 as k3 from ${tableName2}  t1 inner join ${tableName3} t2 on t1.k2=t2.k3 left join ${tableName1} t3 on t1.k1=t3.k1;";
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        ${mvSql}
        """
    sql """drop MATERIALIZED VIEW ${mvName}"""

    mvSql = "SELECT t1.k1,t1.k2,t2.k2 as k3,t3.k4 from ${tableName1} t1 join ${tableName1} t2 on t1.k1=t2.k1 left join ${tableName3} t3 on t1.k2=t3.k3;"
    test {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        ${mvSql}
        """
        exception "Partition values number is more than partition column number"
    }
    mvSql = "SELECT t1.k1,t1.k2 from ${tableName1} t1 union all select t2.k1, t2.k2 from ${tableName1} t2"
    test {
        sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        ${mvSql}
        """
        exception "Partition values number is more than partition column number"
    }

    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop table if exists `${tableName3}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY list(`k1`, `k2`)
        (
            PARTITION `p201701` VALUES in (("2017-01-01", 1),  ("2017-01-01", 2)),
            PARTITION `p201702` VALUES in (("2017-02-01", 1), ("2017-02-01", 2))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName1} values("2017-01-01",1);
        insert into ${tableName1} values("2017-02-01",2);
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY list(`k1`)
        (
            PARTITION `p201702` VALUES in (("2017-02-01")),
            PARTITION `p201703` VALUES in (("2017-03-01"))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName2} values("2017-02-01",3);
        insert into ${tableName2} values("2017-03-01",4);
        """

    sql """
        CREATE TABLE ${tableName3}
        (
            k3 INT not null,
            k4 INT not null
        )
        DISTRIBUTED BY HASH(k3) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        insert into ${tableName3} values(1,1);
        insert into ${tableName3} values(2,2);
        insert into ${tableName3} values(3,3);
        insert into ${tableName3} values(4,4);
        """

    mvSql = "SELECT t1.k1,t1.k2,t2.k2 as k3,t3.k4 from ${tableName1} t1 inner join ${tableName2} t2 on t1.k1=t2.k1 left join ${tableName3} t3 on t1.k2=t3.k3;";

    sql """
    CREATE MATERIALIZED VIEW ${mvName}
    BUILD DEFERRED REFRESH AUTO ON MANUAL
    partition by(k1)
    DISTRIBUTED BY RANDOM BUCKETS 2
    PROPERTIES (
    'replication_num' = '1'
    )
    AS
    ${mvSql}
    """
    sql """drop MATERIALIZED VIEW ${mvName}"""

    mvSql = "SELECT t1.k1,t1.k2,t2.k2 as k3,t3.k4 from ${tableName2} t1 inner join ${tableName1} t2 on t1.k1=t2.k1 left join ${tableName3} t3 on t1.k2=t3.k3;";

    sql """
    CREATE MATERIALIZED VIEW ${mvName}
    BUILD DEFERRED REFRESH AUTO ON MANUAL
    partition by(k1)
    DISTRIBUTED BY RANDOM BUCKETS 2
    PROPERTIES (
    'replication_num' = '1'
    )
    AS
    ${mvSql}
    """
    sql """drop MATERIALIZED VIEW ${mvName}"""

    mvSql = "SELECT t1.k1, t1.k2, t2.k3 as k3 from ${tableName2}  t1 inner join ${tableName3} t2 on t1.k2=t2.k3 left join ${tableName1} t3 on t1.k1=t3.k1;";
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        ${mvSql}
        """
    sql """drop MATERIALIZED VIEW ${mvName}"""

    mvSql = "SELECT t1.k1,t1.k2,t2.k2 as k3,t3.k4 from ${tableName1} t1 join ${tableName1} t2 on t1.k1=t2.k1 left join ${tableName3} t3 on t1.k2=t3.k3;"
    sql """
    CREATE MATERIALIZED VIEW ${mvName}
    BUILD DEFERRED REFRESH AUTO ON MANUAL
    partition by(k1)
    DISTRIBUTED BY RANDOM BUCKETS 2
    PROPERTIES (
    'replication_num' = '1'
    )
    AS
    ${mvSql}
    """
    sql """drop MATERIALIZED VIEW ${mvName}"""

    mvSql = "SELECT t1.k1,t1.k2 from ${tableName1} t1 union all select t2.k1, t2.k2 from ${tableName1} t2"
    sql """
    CREATE MATERIALIZED VIEW ${mvName}
    BUILD DEFERRED REFRESH AUTO ON MANUAL
    partition by(k1)
    DISTRIBUTED BY RANDOM BUCKETS 2
    PROPERTIES (
    'replication_num' = '1'
    )
    AS
    ${mvSql}
    """

}
