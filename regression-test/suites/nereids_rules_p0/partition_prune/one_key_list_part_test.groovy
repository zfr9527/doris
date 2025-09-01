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

suite("one_key_list_part_test") {

    String dbName = context.config.getDbNameByFile(context.file)
    sql """set partition_pruning_expand_threshold=1000;"""

    sql """create table key_1_fixed_list_int_part (a int, dt datetime, c varchar(100)) duplicate key(a)
        PARTITION BY LIST(a)
        (
            PARTITION `p_NULL` VALUES IN ((NULL)),
            PARTITION `p_1` VALUES IN ("1"),
            PARTITION `p_2` VALUES IN ("2"),
            PARTITION `p_3` VALUES IN ("3"),
            PARTITION `p_4` VALUES IN ("4"),
            PARTITION `p_5` VALUES IN ("5"),
            PARTITION `p_6` VALUES IN ("6"),
            PARTITION `p_7` VALUES IN ("7"),
            PARTITION `p_8` VALUES IN ("8"),
            PARTITION `p_9` VALUES IN ("9"),
            PARTITION `p_10` VALUES IN ("10"),
            PARTITION `p_11` VALUES IN ("11"),
            PARTITION `p_12` VALUES IN ("12")
        ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_fixed_list_int_part values 
            (1, "2023-01-01 00:00:00", "111"),
            (2, "2023-02-01 00:00:00", "222"),
            (3, "2023-03-01 00:00:00", "333"),
            (4, "2023-04-01 00:00:00", "444"),
            (5, "2023-05-01 00:00:00", "555"),
            (6, "2023-06-01 00:00:00", "666"),
            (6, "2023-06-15 10:00:00", "666"),
            (7, "2023-07-01 00:00:00", "777"),
            (8, "2023-08-01 00:00:00", "888"),
            (9, "2023-09-01 00:00:00", "999"),
            (10, "2023-10-01 00:00:00", "jjj"),
            (11, "2023-11-01 00:00:00", "qqq"),
            (12, "2023-12-01 00:00:00", "kkk"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            ("null", null, "zzz");"""
    sql """analyze table key_1_fixed_list_int_part with sync;"""

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains "1/13 (p_5)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a IN (1, 3, 8);")
        contains "3/13 (p_1,p_3,p_8) "
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a IN (2, 12);")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a IS NULL;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;")
        contains ""
    }


    /*
-- 查询单个分区，期望只扫描 p_5
SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5;

-- 查询多个分区，期望只扫描 p_1, p_3, p_8
SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a IN (1, 3, 8);

-- 查询跨度较大的多个分区，期望只扫描 p_2, p_12
SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a IN (2, 12);

-- 查询分区中的 NULL 值，期望只扫描 p_NULL
SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a IS NULL;

-- a 加上 5 等于 10，那么 a 应该是 5，期望扫描 p_5
SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a + 5 = 10;

-- 在 CASE WHEN 表达式中，分区裁剪通常会失效，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = CASE WHEN c = 'test' THEN 10 ELSE 5 END;

// 这个剔除了NULL分区没扫描。默认null分区不被扫描
空分区有1个意思：一个是分区没有数据，一行数据都没有
NULL分区就是用来存包含NULL值的分区。里面到底有没有数据不一定

-- `MOD` 运算，结果依赖于 a 的值，通常无法进行分区裁剪，期望全表扫描。  这个没有问题。  这个函数不好用，因为没有支持常量折叠
SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE MOD(a, 3) = 1;

-- 可裁剪条件和不可裁剪条件通过 AND 连接，期望只扫描 p_1, p_2, p_3
SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a IN (1, 2, 3) AND c = 'test_value';

-- OR 的一个分支可裁剪，另一个分支不可裁剪，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a = 5 OR c LIKE 'pattern%';

// null也没有扫描
-- NOT IN 相当于一个非常大的 IN 列表，期望扫描除 p_5 之外的所有分区
SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a NOT IN (5);

// null也没有扫描
-- != 同样相当于一个排除性的 IN 列表，期望扫描除 p_6 之外的所有分区
SELECT a, dt, c FROM key_1_fixed_list_int_part WHERE a != 6;

// 实际p1没有扫描， 这个没有问题
-- 查询同时满足 a 在特定列表中，且a+5满足特定条件，且c不为空
-- 期望只扫描 p_1, p_2, p_3, p_4, p_5
SELECT a, dt, c FROM key_1_fixed_list_int_part
WHERE a IN (1, 2, 3, 4, 5)
  AND (a + 5 > 6)
  AND c IS NOT NULL;


-- 复杂的 OR 组合，其中一个分支可以裁剪，另一个无法裁剪
-- 期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_int_part
WHERE (a IN (1, 5, 9) OR c = 'unknown')
  AND dt IS NOT NULL;


-- `CASE WHEN` 与 `IN` 混合，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_int_part
WHERE a IN (
    CASE WHEN a > 5 THEN 7 ELSE 3 END,
    CASE WHEN c = 'special' THEN 11 ELSE 1 END
);

-- 期望扫描 p_1, p_5, p_9, p_12
SELECT a, dt, c FROM key_1_fixed_list_int_part
WHERE (a IN (1, 5, 9, 12)) AND (MOD(a, 2) = 1 OR a > 10);


-- 3/13 (p_10,p_11,p_12)
SELECT *
FROM key_1_fixed_list_int_part
WHERE CASE WHEN a > 9 THEN 1 ELSE 0 END = 1;

-- 4/13 (p_1,p_2,p_11,p_12)
SELECT *
FROM key_1_fixed_list_int_part
WHERE IF(a IN (1, 2) OR a > 10, TRUE, FALSE);

-- 12/13 (p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12)
-- 看上去没有裁剪，mark2
SELECT *
FROM key_1_fixed_list_int_part
WHERE (CASE WHEN a > 5 THEN a ELSE a * 2 END) % 4 = 0;
a > 5 and a % 4 = 0
or
a <= 5 and a % 2 = 0
     */


    sql """create table key_1_fixed_list_date_part (a int, dt datetime, c varchar(100)) duplicate key(a)
        PARTITION BY LIST(dt)
        (
            PARTITION `p_NULL` VALUES IN ((NULL)),
            PARTITION `p_1` VALUES IN ("2023-01-01 00:00:00"),
            PARTITION `p_2` VALUES IN ("2023-02-01 00:00:00"),
            PARTITION `p_3` VALUES IN ("2023-03-01 00:00:00"),
            PARTITION `p_4` VALUES IN ("2023-04-01 00:00:00"),
            PARTITION `p_5` VALUES IN ("2023-05-01 00:00:00"),
            PARTITION `p_6` VALUES IN ("2023-06-01 00:00:00"),
            PARTITION `p_7` VALUES IN ("2023-07-01 00:00:00"),
            PARTITION `p_8` VALUES IN ("2023-08-01 00:00:00"),
            PARTITION `p_9` VALUES IN ("2023-09-01 00:00:00"),
            PARTITION `p_10` VALUES IN ("2023-10-01 00:00:00"),
            PARTITION `p_11` VALUES IN ("2023-11-01 00:00:00"),
            PARTITION `p_12` VALUES IN ("2023-12-01 00:00:00")
        ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_fixed_list_date_part values 
            (1, "2023-01-01 00:00:00", "111"),
            (2, "2023-02-01 00:00:00", "222"),
            (3, "2023-03-01 00:00:00", "333"),
            (4, "2023-04-01 00:00:00", "444"),
            (5, "2023-05-01 00:00:00", "555"),
            (6, "2023-06-01 00:00:00", "666"),
            (7, "2023-07-01 00:00:00", "777"),
            (8, "2023-08-01 00:00:00", "888"),
            (9, "2023-09-01 00:00:00", "999"),
            (10, "2023-10-01 00:00:00", "jjj"),
            (11, "2023-11-01 00:00:00", "qqq"),
            (12, "2023-12-01 00:00:00", "kkk"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            (null, "null", "zzz");"""
    sql """analyze table key_1_fixed_list_date_part with sync;"""

    /*
    -- 查询单个分区，期望只扫描 p_5
SELECT a, dt, c FROM key_1_fixed_list_date_part WHERE dt = '2023-05-01 00:00:00';

-- 查询多个分区，期望只扫描 p_1, p_3, p_8
SELECT a, dt, c FROM key_1_fixed_list_date_part WHERE dt IN ('2023-01-01 00:00:00', '2023-03-01 00:00:00', '2023-08-01 00:00:00');

-- 查询跨度较大的多个分区，期望只扫描 p_2, p_12
SELECT a, dt, c FROM key_1_fixed_list_date_part WHERE dt IN ('2023-02-01 00:00:00', '2023-12-01 00:00:00');

-- 查询分区中的 NULL 值，期望只扫描 p_NULL
SELECT a, dt, c FROM key_1_fixed_list_date_part WHERE dt IS NULL;

-- dt 加上 1 个月后是 '2023-06-01'，那么 dt 应该是 '2023-05-01'，期望扫描 p_5
SELECT a, dt, c FROM key_1_fixed_list_date_part WHERE date_add(dt, INTERVAL 1 MONTH) = '2023-06-01 00:00:00';

-- 在 CASE WHEN 表达式中，分区裁剪通常会失效，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_date_part WHERE dt = CASE WHEN a > 5 THEN '2023-10-01 00:00:00' ELSE '2023-03-01 00:00:00' END;


-- 将 dt 转换为日期，只比较年月日，期望扫描 p_4
SELECT a, dt, c FROM key_1_fixed_list_date_part WHERE date(dt) = '2023-04-01';

-- ToDate 函数，期望扫描 p_11
SELECT a, dt, c FROM key_1_fixed_list_date_part WHERE to_date(dt) = '2023-11-01';

-- 可裁剪条件和不可裁剪条件通过 AND 连接，期望只扫描 p_1, p_2, p_3
SELECT a, dt, c FROM key_1_fixed_list_date_part WHERE dt IN ('2023-01-01 00:00:00', '2023-02-01 00:00:00', '2023-03-01 00:00:00') AND c = 'test_value';

-- OR 的一个分支可裁剪，另一个分支不可裁剪，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_date_part WHERE dt = '2023-05-01 00:00:00' OR c LIKE 'pattern%';

// null分区也查不出来
-- NOT IN 相当于一个非常大的 IN 列表，期望扫描除 p_5 之外的所有分区
SELECT a, dt, c FROM key_1_fixed_list_date_part WHERE dt NOT IN ('2023-05-01 00:00:00');

// null分区也查不出来
-- != 同样相当于一个排除性的 IN 列表，期望扫描除 p_6 之外的所有分区
SELECT a, dt, c FROM key_1_fixed_list_date_part WHERE dt != '2023-06-01 00:00:00';

-- 查询同时满足 dt 在特定列表中，且a+5满足特定条件，且c不为空
-- 期望只扫描 p_1, p_2, p_3, p_4, p_5
SELECT a, dt, c FROM key_1_fixed_list_date_part
WHERE dt IN ('2023-01-01 00:00:00', '2023-02-01 00:00:00', '2023-03-01 00:00:00', '2023-04-01 00:00:00', '2023-05-01 00:00:00')
  AND (a + 5 > 6)
  AND c IS NOT NULL;

-- 复杂的 OR 组合，其中一个分支可以裁剪，另一个无法裁剪
-- 期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_date_part
WHERE (dt IN ('2023-01-01 00:00:00', '2023-05-01 00:00:00', '2023-09-01 00:00:00') OR c = 'unknown')
  AND a IS NOT NULL;

-- `CASE WHEN` 与 `IN` 混合，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_date_part
WHERE dt IN (
    CASE WHEN a > 5 THEN '2023-07-01 00:00:00' ELSE '2023-03-01 00:00:00' END,
    CASE WHEN c = 'special' THEN '2023-11-01 00:00:00' ELSE '2023-01-01 00:00:00' END
);

-- 期望扫描 p_1, p_5, p_9, p_12
SELECT a, dt, c FROM key_1_fixed_list_date_part
WHERE (dt IN ('2023-01-01 00:00:00', '2023-05-01 00:00:00', '2023-09-01 00:00:00', '2023-12-01 00:00:00')) AND (a = 1 OR a > 10);

-- 2/13 (p_3,p_5)
SELECT *
FROM key_1_fixed_list_date_part
WHERE IF(dt IN ('2023-03-01 00:00:00', '2023-05-01 00:00:00'), TRUE, FALSE);

-- 4/13 (p_1,p_2,p_11,p_12)
SELECT *
FROM key_1_fixed_list_date_part
WHERE IF(dt IN ('2023-01-01 00:00:00', '2023-02-01 00:00:00') OR dt > '2023-10-01 00:00:00', TRUE, FALSE);

-- 2/13 (p_NULL,p_5)
SELECT *
FROM key_1_fixed_list_date_part
WHERE IF(dt IS NULL OR dt = '2023-05-01 00:00:00', TRUE, FALSE);
     */



    sql """create table key_1_fixed_list_varchar_part (a int, dt datetime, c varchar(100)) duplicate key(a)
        PARTITION BY LIST(c)
        (
            PARTITION `p_NULL` VALUES IN ((NULL)), 
            PARTITION `p_NULL_2` VALUES IN ("NULL"),
            PARTITION `p_1` VALUES IN ("111"),
            PARTITION `p_2` VALUES IN ("222"),
            PARTITION `p_3` VALUES IN ("333"),
            PARTITION `p_4` VALUES IN ("4444"),
            PARTITION `p_5` VALUES IN ("555"),
            PARTITION `p_6` VALUES IN ("666"),
            PARTITION `p_7` VALUES IN ("7777"),
            PARTITION `p_8` VALUES IN ("888"),
            PARTITION `p_9` VALUES IN ("999"),
            PARTITION `p_10` VALUES IN ("jjj"),
            PARTITION `p_11` VALUES IN ("qqq"),
            PARTITION `p_12` VALUES IN ("kkk")
        ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_fixed_list_varchar_part values 
            (100, "2023-01-01 00:00:00", "111"),
            (200, "2023-02-01 00:00:00", "222"),
            (300, "2023-03-01 00:00:00", "333"),
            (400, "2023-04-01 00:00:00", "4444"),
            (500, "2023-05-01 00:00:00", "555"),
            (600, "2023-06-01 00:00:00", "666"),
            (700, "2023-07-01 00:00:00", "7777"),
            (800, "2023-08-01 00:00:00", "888"),
            (900, "2023-09-01 00:00:00", "999"),
            (1000, "2023-10-01 00:00:00", "jjj"),
            (500000, "2024-12-01 00:00:00", "qqq"),
            (1000000, "2024-12-01 00:00:00", "kkk"),
            (null, null, "NULL"),
            (1, null, "NULL"),
            (null, "2023-01-01 00:00:00", null),
            (null, null, "kkk");"""
    sql """analyze table key_1_fixed_list_varchar_part with sync;"""
    /*
-- 查询单个分区，期望只扫描 p_5
SELECT a, dt, c FROM key_1_fixed_list_varchar_part WHERE c = '555';

-- 查询多个分区，期望只扫描 p_1, p_3, p_8
SELECT a, dt, c FROM key_1_fixed_list_varchar_part WHERE c IN ('111', '333', '888');

-- 查询跨度较大的多个分区，期望只扫描 p_2, p_12
SELECT a, dt, c FROM key_1_fixed_list_varchar_part WHERE c IN ('222', 'kkk');


-- 查询分区中的 NULL 值，期望只扫描 p_NULL
SELECT a, dt, c FROM key_1_fixed_list_varchar_part WHERE c IS NULL;

// 实际只扫描了一个分区，upper支持了常量折叠，所以可以实现分区裁剪
-- 将c转换为大写后进行匹配，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_varchar_part WHERE UPPER(c) = 'JJJ';

// 实际只扫描了分区名称长度为3的分区，这个也没有问题
-- 检查字符串长度，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_varchar_part WHERE LENGTH(c) = 3;

// 实际上只扫描了一个分区
-- 字符串拼接后进行匹配，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_varchar_part WHERE CONCAT(c, 'suffix') = '111suffix';

-- 可裁剪条件和不可裁剪条件通过 AND 连接，期望只扫描 p_1, p_2, p_3
SELECT a, dt, c FROM key_1_fixed_list_varchar_part WHERE c IN ('111', '222', '333') AND a = 10;

-- OR 的一个分支可裁剪，另一个分支不可裁剪，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_varchar_part WHERE c = '555' OR dt IS NULL;

// 没有扫描null分区
-- NOT IN 相当于一个非常大的 IN 列表，期望扫描除 p_5 之外的所有分区
SELECT a, dt, c FROM key_1_fixed_list_varchar_part WHERE c NOT IN ('555');

// 没有扫描null分区
-- != 同样相当于一个排除性的 IN 列表，期望扫描除 p_6 之外的所有分区
SELECT a, dt, c FROM key_1_fixed_list_varchar_part WHERE c != '666';


-- 查询同时满足 c 在特定列表中，且a满足特定条件
-- 期望只扫描 p_1, p_2, p_3, p_4, p_5
SELECT a, dt, c FROM key_1_fixed_list_varchar_part
WHERE c IN ('111', '222', '333', '444', '555')
  AND a > 1
  AND dt IS NOT NULL;

-- 复杂的 OR 组合，其中一个分支可以裁剪，另一个无法裁剪
-- 期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_varchar_part
WHERE (c IN ('111', '555', '999') OR dt IS NULL)
  AND a IS NOT NULL;

-- `CASE WHEN` 与 `IN` 混合，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_list_varchar_part
WHERE c IN (
    CASE WHEN a > 5 THEN '777' ELSE '333' END,
    CASE WHEN a = 10 THEN 'kkk' ELSE '111' END
);


-- 期望扫描 p_1, p_5, p_9, p_12
SELECT a, dt, c FROM key_1_fixed_list_varchar_part
WHERE (c IN ('111', '555', '999', 'kkk')) AND (a = 1 OR a > 10);

-- 3/14 (p_10,p_11,p_12)
SELECT *
FROM key_1_fixed_list_varchar_part
WHERE CASE WHEN c > 'j' THEN 1 ELSE 0 END = 1;


-- 5/14 (p_1,p_2,p_10,p_11,p_12)
SELECT *
FROM key_1_fixed_list_varchar_part
WHERE IF(c IN ('111', '222') OR c > 'i', TRUE, FALSE);

-- 3/14 (p_NULL_2,p_4,p_7)
SELECT *
FROM key_1_fixed_list_varchar_part
WHERE CASE WHEN LENGTH(c) = 4 THEN TRUE ELSE FALSE END;
     */

}
