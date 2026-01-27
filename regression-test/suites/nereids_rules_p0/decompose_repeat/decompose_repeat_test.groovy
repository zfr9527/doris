suite("decompose_repeat_test") {

    String db_name = context.config.getDbNameByFile(context.file)

    sql """CREATE TABLE test_rollup_rewrite (
    a INT,
    b INT,
    c INT,
    d INT,
    e INT,
    f DECIMAL(10, 2),
    -- 为了测试更复杂的 SQL，可以增加一个辅助列
    g VARCHAR(255)
) ENGINE=OLAP 
DUPLICATE KEY(a, b, c)
DISTRIBUTED BY HASH(a) BUCKETS 10
properties('replication_num'='1');"""

    sql """INSERT INTO test_rollup_rewrite (a, b, c, d, e, f, g) VALUES
-- 常规数据
(1, 10, 100, 1000, 10000, 10.5, 'group1'),
(1, 10, 100, 1000, 20000, 20.0, 'group1'),
(1, 20, 200, 2000, 30000, 5.0,  'group2'),

-- 边界测试：包含 NULL 值的维度列（脑图：数据设计-注意null）
(NULL, 10, 100, 1000, 10000, 50.0, 'null_dim_a'),
(2, NULL, 200, 2000, 20000, 30.0, 'null_dim_b'),
(2, 20, NULL, NULL, NULL, 15.5, 'multi_nulls'),

-- 极端数据：所有维度均为 NULL
(NULL, NULL, NULL, NULL, NULL, 100.0, 'all_nulls'),

-- 重复行测试计算正确性
(1, 10, 100, 1000, 10000, 5.0, 'duplicate_row'),
(1, 10, 100, 1000, 10000, 5.0, 'duplicate_row');"""

    sql """-- 5个维度，触发重写。验证 sum, count, min, max, any_value
SELECT a, b, c, d, e, 
       SUM(f), COUNT(f), MIN(f), MAX(f), ANY_VALUE(g)
FROM test_rollup_rewrite 
GROUP BY ROLLUP(a, b, c, d, e)
ORDER BY a, b, c, d, e;"""

    sql """-- 4个维度，触发重写。验证 CUBE 的全组合是否正确
SELECT a, b, c, d, SUM(f)
FROM test_rollup_rewrite 
GROUP BY CUBE(a, b, c, d)
ORDER BY a, b, c, d;"""

}
