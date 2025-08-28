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

suite("key_1_range_part_test") {

//    DateAddSub/DateCeilFloor/DateDiff/FromSecond/Date/LastDay/Microsecond/ToDate/UnixTimestamp

    String dbName = context.config.getDbNameByFile(context.file)

    sql """create table key_1_fixed_range_date_part (a int, dt datetime, c varchar(100)) duplicate key(a)
    partition by range(dt) (
        PARTITION p_min VALUES LESS THAN ("2023-01-01 00:00:00"),
        PARTITION p_202301 VALUES [('2023-01-01 00:00:00'), ('2023-02-01 00:00:00')),
        PARTITION p_202302 VALUES [('2023-02-01 00:00:00'), ('2023-03-01 00:00:00')),
        PARTITION p_202303 VALUES [('2023-03-01 00:00:00'), ('2023-04-01 00:00:00')),
        PARTITION p_202304 VALUES [('2023-04-01 00:00:00'), ('2023-05-01 00:00:00')),
        PARTITION p_202305 VALUES [('2023-05-01 00:00:00'), ('2023-06-01 00:00:00')),
        PARTITION p_202306 VALUES [('2023-06-01 00:00:00'), ('2023-07-01 00:00:00')),
        PARTITION p_202307 VALUES [('2023-07-01 00:00:00'), ('2023-08-01 00:00:00')),
        PARTITION p_202308 VALUES [('2023-08-01 00:00:00'), ('2023-09-01 00:00:00')),
        PARTITION p_202309 VALUES [('2023-09-01 00:00:00'), ('2023-10-01 00:00:00')),
        PARTITION p_202310 VALUES [('2023-10-01 00:00:00'), ('2023-11-01 00:00:00')),
        PARTITION p_202311 VALUES [('2023-11-01 00:00:00'), ('2023-12-01 00:00:00')),
        PARTITION p_202312 VALUES [('2023-12-01 00:00:00'), ('2024-01-01 00:00:00')),
        PARTITION p_max VALUES [('2024-01-01 00:00:00'), ('9999-12-31 23:59:59'))
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_fixed_range_date_part values 
            (0, "2021-01-01 00:00:00", "000"),
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
            (13, "2024-12-01 00:00:00", "aaa"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            (null, null, "zzz");"""
    sql """analyze table key_1_fixed_range_date_part with sync;"""

    /*

    -- 查询单个分区，期望只扫描 p_202306
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt = '2023-06-15 10:00:00';


-- 查询一个完整分区，期望只扫描 p_202307
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt >= '2023-07-01 00:00:00' AND dt < '2023-08-01 00:00:00';

-- 查询两个完整分区，期望扫描 p_202309 和 p_202310
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt BETWEEN '2023-09-01 00:00:00' AND '2023-10-10 00:00:00';

-- 查询分区 p_202308 的下边界，期望扫描 p_202308
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt = '2023-08-01 00:00:00';

-- 查询分区 p_202308 的上边界（不包含），期望扫描 p_202308
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt < '2023-09-01 00:00:00' AND dt >= '2023-08-31 23:59:59';


-- 截断到月，查询 2023年3月的数据，期望扫描 p_202303
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date_trunc('month', dt) = '2023-03-01 00:00:00';

-- 结合 BETWEEN，查询 2023年4月到5月的数据，期望扫描 p_202304 和 p_202305
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date_trunc('month', dt) BETWEEN '2023-04-01 00:00:00' AND '2023-05-01 00:00:00';

// 这两个都多了max和min分区
-- dt 加上一个月后等于 2023-11-15，那么 dt 应该是 2023-10-15，期望扫描 p_202310
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date_add(dt, INTERVAL 1 MONTH) = '2023-11-15 10:00:00';

-- dt 减去两个月后大于 2023-03-10，那么 dt 应该大于 2023-05-10，期望扫描 p_202305, p_202306, ..., p_max
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date_sub(dt, INTERVAL 2 MONTH) > '2023-03-10 00:00:00';

//这个是继承了monotonic类的函数，但是不知道为什么扫描了所有分区而不是单个分区
-- dt 和 2023-04-01 的天数差为 10，那么 dt 应该为 2023-04-11，期望扫描 p_202304
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE datediff(dt, '2023-04-01 00:00:00') = 10;

-- Unix 时间戳转换，期望扫描 p_202305
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt = from_unixtime(unix_timestamp('2023-05-20 12:00:00'));


-- 将 dt 转换为日期，只比较年月日，期望扫描 p_202309
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date(dt) = '2023-09-05';

-- ToDate 函数，期望扫描 p_202301
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE to_date(dt) = '2023-01-20';

// 这个也扫描了两个分区
-- 查询 last_day(dt) 等于 2023年3月的最后一天，期望扫描 p_202303
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE last_day(dt) = '2023-03-31 00:00:00';


-- OR 的一个分支可裁剪，另一个分支不可裁剪，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date_trunc('month', dt) = '2023-08-01' OR a > 100;

// 这个比预期多扫描了一个6月
-- dt 的范围由两个函数决定，期望扫描 p_202307 和 p_202308
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date_add(dt, INTERVAL 1 MONTH) BETWEEN '2023-08-01' AND '2023-09-01';


-- `!=` 不等于条件，结合日期函数，期望只裁剪到分区 p_202307
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt != '2023-07-05 12:00:00' AND dt > '2023-07-01' AND dt < '2023-08-01';

// 这个只扫描了min分区。有个问题是前后两个条件交换之后仍然只扫描了一个分区，所以程序内部是有一个什么判断的因子仍然可以定位到满足dt的只有一个分区？
-- 复杂的 IS NULL / IS NOT NULL 条件，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE (dt IS NULL OR dt < '2023-01-01') AND NOT (dt IS NULL);
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE NOT (dt IS NULL) and (dt IS NULL OR dt < '2023-01-01');

SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE (dt IS NULL OR dt < '2023-01-01');
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE NOT (dt IS NULL);


-- `CAST` 强制类型转换，期望p_202308
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE CAST(dt AS DATE) = '2023-08-15' AND unix_timestamp(dt) > 1690848000;


-- 嵌套 OR，期望全表扫描，这个我不太确定
SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE (dt BETWEEN '2023-05-01' AND '2023-05-31' OR a = 1) AND (dt > '2023-06-15' OR c LIKE 'pattern');

     */


    sql """create table key_1_special_fixed_range_date_part (a int, dt datetime, c varchar(100)) duplicate key(a)
    partition by range(dt) (
        PARTITION p_min VALUES LESS THAN ("2023-01-01 00:00:00"),
        PARTITION p_202301 VALUES [('2023-01-01 00:00:00'), ('2023-02-01 00:00:00')),
        PARTITION p_202302 VALUES [('2023-02-01 00:00:00'), ('2023-03-01 00:00:00')),
        PARTITION p_202304 VALUES [('2023-04-01 00:00:00'), ('2023-05-01 00:00:00')),
        PARTITION p_202305 VALUES [('2023-05-01 00:00:00'), ('2023-06-01 00:00:00')),
        PARTITION p_202306 VALUES [('2023-06-01 00:00:00'), ('2023-08-01 00:00:00')),
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

    -- 查询单个分区，期望只扫描 p_202306 (因为2023-07-15现在属于这个分区)
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt = '2023-07-15 10:00:00';

-- 查询一个完整分区，期望只扫描 p_202306
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt >= '2023-06-01 00:00:00' AND dt < '2023-08-01 00:00:00';

-- 查询两个完整分区，期望扫描 p_202309 和 p_202310
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt BETWEEN '2023-09-05 00:00:00' AND '2023-11-10 00:00:00';

-- 查询分区 p_202306 的下边界，期望扫描 p_202306
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt = '2023-06-01 00:00:00';

-- 查询分区 p_202306 的上边界（不包含），期望扫描 p_202306
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt < '2023-08-01 00:00:00' AND dt >= '2023-07-31 23:59:59';

// 这个为什么世纪扫描的是p_202302分区
-- 截断到月，查询 2023年3月的数据，期望扫描 p_202303
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_trunc('month', dt) = '2023-03-01 00:00:00';

// 这个实际上扫描了5月和6月
-- 结合 BETWEEN，查询 2023年6月和7月的数据，期望扫描 p_202306和07
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_trunc('month', dt) BETWEEN '2023-06-01 00:00:00' AND '2023-07-01 00:00:00';

// 多扫描了max分区
-- dt 加上一个月后等于 2023-11-15，那么 dt 应该是 2023-10-15，期望扫描 p_202310
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_add(dt, INTERVAL 1 MONTH) = '2023-11-15 10:00:00';

// 多扫描了min分区
-- dt 减去两个月后大于 2023-03-10，那么 dt 应该大于 2023-05-10，期望扫描 p_202305, p_202306, ..., p_max
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_sub(dt, INTERVAL 2 MONTH) > '2023-03-10 00:00:00';

// 这个也跟前表一样，实际上扫描了全表
-- dt 和 2023-07-01 的天数差为 10，那么 dt 应该为 2023-07-11，期望扫描 p_202306
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE datediff(dt, '2023-07-01 00:00:00') = 10;

-- Unix 时间戳转换，期望扫描 p_202310
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt = from_unixtime(unix_timestamp('2023-11-20 12:00:00'));

-- 将 dt 转换为日期，只比较年月日，期望扫描 p_202309
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date(dt) = '2023-09-05';

-- ToDate 函数，期望扫描 p_202301
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE to_date(dt) = '2023-01-20';

-- 查询 last_day(dt) 等于 2023年7月的最后一天，期望扫描 p_202306
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE last_day(dt) = '2023-07-31 00:00:00';

-- OR 的一个分支可裁剪，另一个分支不可裁剪，期望全表扫描
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_trunc('month', dt) = '2023-08-01' OR a > 100;

// 实际上为啥扫描的是5和6，还有max
-- dt 的范围由两个函数决定，期望扫描 p_202306 和 p_202309
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_add(dt, INTERVAL 1 MONTH) BETWEEN '2023-07-01' AND '2023-09-01';


-- `!=` 不等于条件，结合日期函数，期望只裁剪到分区 p_202306
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt != '2023-07-05 12:00:00' AND dt > '2023-07-01' AND dt < '2023-08-01';

// 这个只扫描了min分区
-- 复杂的 IS NULL / IS NOT NULL 条件，期望全表扫描
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE (dt IS NULL OR dt < '2023-01-01') AND NOT (dt IS NULL);

// 多扫描了5月
-- `CAST` 强制类型转换，扫描6月
explain sELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE CAST(dt AS DATE) = '2023-06-01' AND unix_timestamp(dt) >= 1685548800;

-- 嵌套 OR，期望全表扫描，因为 OR 逻辑无法精确裁剪
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE (dt BETWEEN '2023-05-01' AND '2023-05-31' OR a = 1) AND (dt > '2023-06-15' OR c LIKE 'pattern');

// 实际扫描了5月
-- 新增用例：嵌套 AND，期望扫描 ？
SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE (dt BETWEEN '2023-05-01' AND '2023-05-31') AND (dt > '2023-06-15' OR c LIKE 'pattern');


     */



    sql """create table key_1_fixed_range_int_part (a int, dt datetime, c varchar(100)) duplicate key(a)
    partition by range(a) (
        PARTITION p_min VALUES [(-2147483648), (0)),
        PARTITION p_0_100 VALUES [(0), (100)),
        PARTITION p_100_200 VALUES [(100), (200)),
        PARTITION p_200_300 VALUES [(200), (300)),
        PARTITION p_300_400 VALUES [(300), (400)),
        PARTITION p_400_500 VALUES [(400), (500)),
        PARTITION p_500_600 VALUES [(500), (600)),
        PARTITION p_600_700 VALUES [(600), (700)),
        PARTITION p_700_800 VALUES [(700), (800)),
        PARTITION p_800_900 VALUES [(800), (900)),
        PARTITION p_900_1000 VALUES [(900), (1000)),
        PARTITION p_1000_1100 VALUES [(1000), (1100)),
        PARTITION p_max VALUES [(1300), (2147483647))
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_fixed_range_int_part values 
            (-10000, "2021-01-01 00:00:00", "000"),
            (0, "2021-01-01 00:00:00", "000"),
            (100, "2023-01-01 00:00:00", "111"),
            (200, "2023-02-01 00:00:00", "222"),
            (300, "2023-03-01 00:00:00", "333"),
            (400, "2023-04-01 00:00:00", "444"),
            (500, "2023-05-01 00:00:00", "555"),
            (600, "2023-06-01 00:00:00", "666"),
            (700, "2023-07-01 00:00:00", "777"),
            (800, "2023-08-01 00:00:00", "888"),
            (900, "2023-09-01 00:00:00", "999"),
            (1000, "2023-10-01 00:00:00", "jjj"),
            (500000, "2024-12-01 00:00:00", "aaa"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            (null, null, "zzz");"""
    sql """analyze table key_1_fixed_range_int_part with sync;"""
    /*


    -- 查询单个分区，期望只扫描 p_200_300
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a = 250;

-- 查询一个完整分区，期望只扫描 p_500_600
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a >= 500 AND a < 600;

-- 查询两个完整分区，期望扫描 p_400_500 和 p_500_600
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a BETWEEN 450 AND 550;

-- 查询分区 p_100_200 的下边界，期望扫描 p_100_200
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a = 100;

-- 查询分区 p_100_200 的上边界（不包含），期望扫描 p_100_200
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a < 200 AND a >= 199;

// 这个实际上是全表扫描？
-- a 加上 100 等于 350，那么 a 应该是 250，期望扫描 p_200_300
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a + 100 = 350;

// 这个也是全表扫描？
-- a 减去 50 大于 800，那么 a 应该大于 850，期望扫描 p_800_900, p_900_1000, p_1000_1100, p_max
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a - 50 > 800;


-- 在 CASE WHEN 表达式中，分区裁剪通常会失效，期望全表扫描？
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE CASE WHEN c = 'test' THEN a > 900 ELSE a < 100 END;

// 全表扫描？
EXPLAIN SELECT * FROM key_1_fixed_range_int_part
WHERE a > IF(c IS NULL, 500, 100)
AND dt IS NOT NULL;

// 10/13 (p_min,p_0_100,p_100_200,p_500_600,p_600_700,p_700_800,p_800_900,p_900_1000,p_1000_1100,p_max) ？？？
EXPLAIN SELECT * FROM key_1_fixed_range_int_part
WHERE a < (CASE WHEN a > 500 THEN 1000 ELSE 200 END)
   OR a <=> 1250

-- `MOD` 运算，结果依赖于 a 的值，通常无法进行分区裁剪，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE MOD(a, 100) = 50;

// 实际上是全表扫描？
-- `ABS` 函数，期望扫描 p_min 和 p_0_100
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE ABS(a) < 100;

// 实际上扫描了partitions=8/13 (p_min,p_0_100,p_100_200,p_200_300,p_300_400,p_400_500,p_500_600,p_600_700)
-- 两个可裁剪条件通过 AND 连接，期望扫描 p_500_600
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE (a > 550 OR c = 'something') AND a < 650;

-- OR 的一个分支可裁剪，另一个分支不可裁剪，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a > 900 OR c LIKE 'test%';

-- `!=` 不等于条件，结合算术运算，期望只裁剪到分区 p_200_300
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a != 250 AND a >= 200 AND a < 300;

// 这个sql只扫描了min分区
-- 复杂的 IS NULL / IS NOT NULL 条件，期望全表扫描
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE (a IS NULL OR a < 0) AND NOT (a IS NULL);

-- 查询分区范围空隙中的值，期望不扫描任何分区（空集）
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a BETWEEN 1100 AND 1299;

-- 查询跨越分区空隙的值，期望扫描 p_1000_1100 和 p_max
SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a BETWEEN 1050 AND 1350;

     */




    sql """create table key_1_special_fixed_range_int_part (a int, dt datetime, c varchar(100)) duplicate key(a)
    partition by range(a) (
        PARTITION p_min VALUES [(-2147483648), (0)),
        PARTITION p_0_100 VALUES [(0), (100)),
        PARTITION p_100_200 VALUES [(100), (200)),
        PARTITION p_300_600 VALUES [(300), (600)),
        PARTITION p_700_800 VALUES [(700), (900)),
        PARTITION p_900_1000 VALUES [(900), (1000)),
        PARTITION p_1000_1100 VALUES [(1000), (1100)),
        PARTITION p_1200_1300 VALUES [(1200), (1300)),
        PARTITION p_max VALUES [(1300), (2147483647))
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_special_fixed_range_int_part values 
            (-10000, "2021-01-01 00:00:00", "000"),
            (0, "2021-01-01 00:00:00", "000"),
            (100, "2023-01-01 00:00:00", "111"),
            (300, "2023-02-01 00:00:00", "222"),
            (700, "2023-03-01 00:00:00", "333"),
            (900, "2023-04-01 00:00:00", "444"),
            (1000, "2023-05-01 00:00:00", "555"),
            (1200, "2023-06-01 00:00:00", "666"),
            (500000, "2024-12-01 00:00:00", "aaa"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            (null, null, "zzz");"""
    sql """analyze table key_1_special_fixed_range_int_part with sync;"""

    /*
    -- 查询单个分区，期望只扫描 p_300_600
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a = 450;

-- 查询一个完整分区，期望只扫描 p_300_600
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a >= 300 AND a < 600;

-- 查询两个完整分区，期望扫描 p_900_1000 和 p_1000_1100
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a BETWEEN 950 AND 1050;


-- 查询分区 p_300_600 的下边界，期望扫描 p_300_600
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a = 300;

-- 查询分区 p_700_800 的上边界（不包含），期望扫描 p_700_800
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a < 900 AND a >= 899;

// 实际上全表扫描？
-- a 加上 100 等于 450，那么 a 应该是 350，期望扫描 p_300_600
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a + 100 = 450;

// 实际上全表扫描？
-- a 减去 50 大于 800，那么 a 应该大于 850，期望扫描 p_700_800, p_900_1000, p_1000_1100, p_1200_1300, p_max
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a - 50 > 800;


-- 在 CASE WHEN 表达式中，分区裁剪通常会失效，期望全表扫描
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE CASE WHEN c = 'test' THEN a > 900 ELSE a < 100 END;

// 全表扫描？
EXPLAIN SELECT * FROM key_1_special_fixed_range_int_part
WHERE a > IF(c IS NULL, 500, 100)
AND dt IS NOT NULL;

// 全表扫描？
EXPLAIN SELECT * FROM key_1_special_fixed_range_int_part
WHERE a < (CASE WHEN a > 500 THEN 1000 ELSE 200 END)
   OR a <=> 1250

-- `MOD` 运算，结果依赖于 a 的值，通常无法进行分区裁剪，期望全表扫描
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE MOD(a, 100) = 50;

// 实际上全表扫描？
-- `ABS` 函数，期望扫描 p_min 和 p_0_100
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE ABS(a) < 100;

// 实际上扫描了五个分区，5/9 (p_min,p_0_100,p_100_200,p_300_600,p_700_800)
-- 两个可裁剪条件通过 AND 连接，期望扫描 p_700_800
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE (a > 750 OR c = 'something') AND a < 850;

-- OR 的一个分支可裁剪，另一个分支不可裁剪，期望全表扫描
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a > 900 OR c LIKE 'test%';


-- `!=` 不等于条件，结合算术运算，期望只裁剪到分区 p_300_600
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a != 450 AND a >= 300 AND a < 600;

// 实际上只扫描了min分区？
-- 复杂的 IS NULL / IS NOT NULL 条件，期望全表扫描
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE (a IS NULL OR a < 0) AND NOT (a IS NULL);

-- 查询分区范围空隙中的值，期望不扫描任何分区（空集）
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a BETWEEN 600 AND 699;

-- 查询分区范围空隙中的值，期望不扫描任何分区（空集）
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a = 650;

-- 查询跨越分区空隙的值，期望扫描 p_1000_1100 和 p_1200_1300
SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a BETWEEN 1050 AND 1250;

     */





}
