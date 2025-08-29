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

suite("key_1_range_part_update_test") {

//    DateAddSub/DateCeilFloor/DateDiff/FromSecond/Date/LastDay/Microsecond/ToDate/UnixTimestamp

    sql """set partition_pruning_expand_threshold=1000;"""
    String dbName = context.config.getDbNameByFile(context.file)

    sql """create table key_1_special_fixed_range_date_part (a int, dt datetime, c varchar(100)) duplicate key(a)
    partition by range(dt) (
        PARTITION p_min VALUES LESS THAN ("2023-01-01 00:00:00"),
        PARTITION p_202301 VALUES [('2023-01-01 00:00:00'), ('2023-02-01 00:00:00')),
        PARTITION p_202302 VALUES [('2023-02-01 00:00:00'), ('2023-03-01 00:00:00')),
        PARTITION p_202304 VALUES [('2023-04-01 00:00:00'), ('2023-05-01 00:00:00')),
        PARTITION p_202305 VALUES [('2023-05-01 00:00:00'), ('2023-06-01 00:00:00')),
        PARTITION p_202306 VALUES [('2023-06-01 00:00:00'), ('2023-08-01 00:00:00')),
        PARTITION p_202308 VALUES [('2023-08-01 00:00:00'), ('2023-09-01 00:00:00')),
        PARTITION p_202309 VALUES [('2023-09-01 00:00:00'), ('2023-10-01 00:00:00')),
        PARTITION p_202310 VALUES [('2023-10-01 00:00:00'), ('2023-12-01 00:00:00')),
        PARTITION p_202312 VALUES [('2023-12-01 00:00:00'), ('2024-01-01 00:00:00')),
        PARTITION p_max VALUES [('2024-01-01 00:00:00'), ('9999-12-31 23:59:59'))
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_special_fixed_range_date_part values 
            (0, "2021-01-01 00:00:00", "000"),
            (1, "2023-01-01 00:00:00", "111"),
            (2, "2023-02-01 00:00:00", "222"),
            (4, "2023-04-01 00:00:00", "444"),
            (5, "2023-05-01 00:00:00", "555"),
            (6, "2023-06-01 00:00:00", "666"),
            (8, "2023-08-01 00:00:00", "888"),
            (9, "2023-09-01 00:00:00", "999"),
            (10, "2023-10-01 00:00:00", "jjj"),
            (12, "2023-12-01 00:00:00", "kkk"),
            (13, "2024-12-01 00:00:00", "aaa"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            (null, null, "zzz");"""
    sql """analyze table key_1_special_fixed_range_date_part with sync;"""

    /*

    -- 查询单个分区，期望只扫描 p_202308
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt = '2023-08-15 10:00:00';

-- 2/11 (p_202306,p_202308)
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt >= '2023-06-01 00:00:00' AND dt <= '2023-08-01 00:00:00';

-- 3/11 (p_202308,p_202309,p_202310)
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt BETWEEN '2023-08-05 00:00:00' AND '2023-11-10 00:00:00';

-- 1/11 (p_202308)
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt = '2023-08-01 00:00:00';

-- 2/11 (p_202306,p_202308)
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt <= '2023-08-01 00:00:00' AND dt >= '2023-07-31 23:59:59';

-- 2/11 (p_202306,p_202308)  mark
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_trunc('month', dt) = '2023-08-01 00:00:00';

-- 3/11 (p_202305,p_202306,p_202308)  mark
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_trunc('month', dt) BETWEEN '2023-06-01 00:00:00' AND '2023-08-01 00:00:00';

-- 2/11 (p_202308,p_max)  mark
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_add(dt, INTERVAL 1 MONTH) = '2023-09-15 10:00:00';

-- 8/11 (p_min,p_202305,p_202306,p_202308,p_202309,p_202310,p_202312,p_max)  mark
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_sub(dt, INTERVAL 2 MONTH) > '2023-03-10 00:00:00';

// 实际上扫描了全表
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE datediff(dt, '2023-08-01 00:00:00') = 10;

-- 1/11 (p_202308)
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt = from_unixtime(unix_timestamp('2023-08-20 12:00:00'));

-- 1/11 (p_202308)
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date(dt) = '2023-08-05';

-- 1/11 (p_202308)
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE to_date(dt) = '2023-08-20';

-- 2/11 (p_202306,p_202308) mark
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE last_day(dt) = '2023-08-31 00:00:00';

-- OR 的一个分支可裁剪，另一个分支不可裁剪，期望全表扫描
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_trunc('month', dt) = '2023-08-01' OR a > 100;


-- 4/11 (p_202305,p_202306,p_202308,p_max) mark
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_add(dt, INTERVAL 1 MONTH) BETWEEN '2023-07-01' AND '2023-09-01';


-- 2/11 (p_202306,p_202308)
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt != '2023-07-05 12:00:00' AND dt > '2023-07-01' AND dt <= '2023-08-01';


-- 7/11 (p_min,p_202301,p_202302,p_202304,p_202305,p_202306,p_202308)
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE (dt IS NULL OR dt < '2023-09-01') AND NOT (dt IS NULL);

-- =2/11 (p_202306,p_202308) mark
explain sELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE CAST(dt AS DATE) = '2023-08-01' AND unix_timestamp(dt) >= 1685548800;

-- 嵌套 OR，期望全表扫描，因为 OR 逻辑无法精确裁剪
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE (dt BETWEEN '2023-08-01' AND '2023-08-31' OR a = 1) AND (dt > '2023-06-15' OR c LIKE 'pattern');


-- 1/11 (p_202308)
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE (dt BETWEEN '2023-08-01' AND '2023-08-31') AND (dt > '2023-06-15' OR c LIKE 'pattern');


     */




    sql """create table key_1_special_fixed_range_int_part (a int, dt datetime, c varchar(100)) duplicate key(a)
    partition by range(a) (
        PARTITION p_min VALUES [(-2147483648), (0)),
        PARTITION p_0_10 VALUES [(0), (10)),
        PARTITION p_10_20 VALUES [(10), (20)),
        PARTITION p_30_60 VALUES [(30), (60)),
        PARTITION p_60_70 VALUES [(60), (70)),
        PARTITION p_70_80 VALUES [(70), (90)),
        PARTITION p_90_100 VALUES [(90), (100)),
        PARTITION p_100_110 VALUES [(100), (110)),
        PARTITION p_120_130 VALUES [(120), (130)),
        PARTITION p_max VALUES [(130), (2147483647))
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_special_fixed_range_int_part values 
            (-10000, "2021-01-01 00:00:00", "000"),
            (0, "2021-01-01 00:00:00", "000"),
            (10, "2023-01-01 00:00:00", "111"),
            (30, "2023-02-01 00:00:00", "222"),
            (60, "2023-06-01 00:00:00", "666"),
            (70, "2023-03-01 00:00:00", "333"),
            (90, "2023-04-01 00:00:00", "444"),
            (100, "2023-05-01 00:00:00", "555"),
            (120, "2023-06-01 00:00:00", "666"),
            (500000, "2024-12-01 00:00:00", "aaa"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            (null, null, "zzz");"""
    sql """analyze table key_1_special_fixed_range_int_part with sync;"""

    /*
    -- 1/10 (p_60_70)
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a = 65;

-- 2/10 (p_30_60,p_60_70)
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a >= 30 AND a < 70;

-- 3/10 (p_70_80,p_90_100,p_100_110)
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a BETWEEN 75 AND 105;


-- 1/10 (p_60_70)
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a = 60;

-- 2/10 (p_60_70,p_70_80)
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a < 90 AND a >= 69;

-- 4/10 (p_min,p_30_60,p_70_80,p_max)  mark   a = 85   这个没看懂
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a + 10 = 95;

-- 7/10 (p_min,p_60_70,p_70_80,p_90_100,p_100_110,p_120_130,p_max)  mark
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a - 5 > 60;


-- 在 CASE WHEN 表达式中，分区裁剪通常会失效，期望全表扫描
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE CASE WHEN c = 'test' THEN a > 90 ELSE a < 10 END;

// 全表扫描
EXPLAIN SELECT * FROM key_1_special_fixed_range_int_part
WHERE a > IF(c IS NULL, 50, 10)
AND dt IS NOT NULL;

// 9/10 (p_min,p_0_10,p_10_20,p_30_60,p_60_70,p_70_80,p_90_100,p_120_130,p_max)
EXPLAIN SELECT * FROM key_1_special_fixed_range_int_part
WHERE a < (CASE WHEN a > 50 THEN 100 ELSE 20 END)
   OR a <=> 125

( ( a > 50 and a < 100 ) or (a < 20) )

-- `MOD` 运算，结果依赖于 a 的值，通常无法进行分区裁剪，期望全表扫描
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE MOD(a, 10) = 5;


-- 3/10 (p_min,p_0_10,p_max)
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE ABS(a) < 10;

-- 6/10 (p_min,p_0_10,p_10_20,p_30_60,p_60_70,p_70_80)
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE (a > 75 OR c = 'something') AND a < 85;

-- OR 的一个分支可裁剪，另一个分支不可裁剪，期望全表扫描
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a >= 70 OR c LIKE 'test%';


-- 2/10 (p_30_60,p_60_70)
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a != 45 AND a >= 30 AND a <= 60;


-- 5/10 (p_min,p_0_10,p_10_20,p_30_60,p_60_70)
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE (a IS NULL OR a < 70) AND NOT (a IS NULL);

-- 1/10 (p_60_70)
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a BETWEEN 60 AND 69;

-- 1/10 (p_60_70)
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a = 65;

-- 5/10 (p_60_70,p_70_80,p_90_100,p_100_110,p_120_130)
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a BETWEEN 65 AND 125;

     */





}
