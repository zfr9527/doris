suite("decompose_repeat_test") {

    String db_name = context.config.getDbNameByFile(context.file)
    String tb_name = "decompose_repeat_test_table"

    def compare_res = { def stmt ->
        sql "set disable_nereids_rules='DECOMPOSE_REPEAT';"
        def no_rewrite_res = sql stmt
        logger.info("no_rewrite_res: " + no_rewrite_res)
        sql "set disable_nereids_rules='';"
        def rewrite_res = sql stmt
        logger.info("rewrite_res: " + rewrite_res)
        assertEquals(no_rewrite_res.toString(), rewrite_res.toString())

//        assertTrue((rewrite_res == [] && no_rewrite_res == []) || (rewrite_res.size() == no_rewrite_res.size()))
//        for (int row = 0; row < rewrite_res.size(); row++) {
//            assertTrue(rewrite_res[row].size() == no_rewrite_res[row].size())
//            for (int col = 0; col < rewrite_res[row].size(); col++) {
//                assertTrue(rewrite_res[row][col] == no_rewrite_res[row][col])
//            }
//        }
    }

    def judge_explain = { def stmt, def res ->
        sql "set disable_nereids_rules='DECOMPOSE_REPEAT';"
        def no_rewrite_explain_res = sql "explain shape plan" + stmt
        sql "set disable_nereids_rules='';"
        def rewrite_explain_res = sql "explain shape plan" + stmt
        if (res) {
            assertNotEquals(no_rewrite_explain_res.toString(), rewrite_explain_res.toString())
        } else {
            assertEquals(no_rewrite_explain_res.toString(), rewrite_explain_res.toString())
        }

    }

    sql """drop table if exists ${tb_name}"""

    sql """CREATE TABLE ${tb_name} (
            a INT,
            b INT,
            c INT,
            d INT,
            e INT,
            f DECIMAL(10, 2),
            g VARCHAR(255)
        ) ENGINE=OLAP 
        DUPLICATE KEY(a, b, c)
        DISTRIBUTED BY HASH(a) BUCKETS 10
        properties('replication_num'='1');"""

    sql """INSERT INTO ${tb_name} (a, b, c, d, e, f, g) VALUES
            (1, 10, 100, 1000, 10000, 10.5, 'group1'),
            (1, 10, 100, 1000, 20000, 20.0, 'group1'),
            (1, 20, 200, 2000, 30000, 5.0,  'group2'),
            
            (NULL, 10, 100, 1000, 10000, 50.0, 'null_dim_a'),
            (2, NULL, 200, 2000, 20000, 30.0, 'null_dim_b'),
            (2, 20, NULL, NULL, NULL, 15.5, 'multi_nulls'),
            
            (NULL, NULL, NULL, NULL, NULL, 100.0, 'all_nulls'),
            
            (1, 10, 100, 1000, 10000, 5.0, 'duplicate_row'),
            (1, 10, 100, 1000, 10000, 5.0, 'duplicate_row');"""

    def sql_str = """-- 5个维度，触发重写。验证 sum, count, min, max, any_value
            SELECT a, b, c, d, e, 
                   SUM(f), COUNT(f), MIN(f), MAX(f)
            FROM ${tb_name} 
            GROUP BY ROLLUP(a, b, c, d, e)
            ORDER BY 1,2,3,4,5,6,7,8,9,10;"""
    judge_explain(sql_str, true)
    compare_res(sql_str)

    sql_str = """-- 4个维度，触发重写。验证 CUBE 的全组合是否正确
            SELECT a, b, c, d, SUM(f)
            FROM ${tb_name} 
            GROUP BY CUBE(a, b, c, d)
            ORDER BY 1,2,3,4,5;"""
    judge_explain(sql_str, true)
    compare_res(sql_str)

    sql_str =  """-- 验证手动指定的多个高维度组合
            SELECT a, b, c, d, e, SUM(f)
            FROM ${tb_name} 
            GROUP BY GROUPING SETS (
                (a, b, c, d, e),
                (a, b, c, d),
                (a, b),
                ()
            )
            ORDER BY 1,2,3,4,5,6;"""
    judge_explain(sql_str, true)
    compare_res(sql_str)


    sql_str =  """-- 场景：子查询内做高维聚合，外层做 Join
            -- 验证：优化器是否能穿透子查询进行改写
            SELECT t1.total, t2.g 
            FROM (
                SELECT a, b, c, d, e, SUM(f) as total 
                FROM ${tb_name} 
                GROUP BY CUBE(a, b, c, d, e)
            ) t1
            JOIN ${tb_name} t2 ON t1.a = t2.a
            WHERE t1.total > 10
            ORDER BY t1.total, t2.g;"""
    judge_explain(sql_str, true)
    compare_res(sql_str)


    sql_str =  """-- 子查询中包含 ROLLUP
            WITH cte1 AS (
                SELECT a, b, c, d, e, SUM(f) as sum_f
                FROM ${tb_name}
                GROUP BY ROLLUP(a, b, c, d, e)
            )
            SELECT * FROM cte1 JOIN ${tb_name} t2 ON cte1.a = t2.a WHERE cte1.sum_f > 0
            ORDER BY cte1.a, cte1.b, cte1.c, cte1.d, cte1.e, cte1.sum_f, t2.b, t2.c, t2.d, t2.e, t2.f, t2.g;"""
    judge_explain(sql_str, true)
    compare_res(sql_str)


    sql_str =  """-- 场景：聚合的数据源本身就是多个 CTE Join 的结果
            -- 验证：脑图中“with a as (...) select * from a t1 join a t2” 场景
            WITH complex_source AS (
                SELECT t1.a, t1.b, t2.c, t2.d, t1.e, t1.f
                FROM ${tb_name} t1
                JOIN ${tb_name} t2 ON t1.a = t2.b
                WHERE t1.f > 0
            )
            SELECT a, b, c, d, e, SUM(f)
            FROM complex_source
            GROUP BY ROLLUP(a, b, c, d, e)
            ORDER BY a, b, c, d, e;"""
    judge_explain(sql_str, true)
    compare_res(sql_str)

    sql_str =  """-- 场景：SELECT 中有两个独立的聚合子查询，且都符合改写条件
            SELECT * FROM 
                (SELECT a, b, c, d, e, SUM(f) FROM ${tb_name} GROUP BY ROLLUP(a, b, c, d, e)) temp1,
                (SELECT a, b, c, d, e, MIN(f) FROM ${tb_name} GROUP BY CUBE(a, b, c, d, e)) temp2
            WHERE temp1.a = temp2.a
            ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12;"""
    judge_explain(sql_str, true)
    compare_res(sql_str)

//    sql """-- 场景：在子查询中计算 GROUPING_ID，外层通过该 ID 进行过滤
//            SELECT * FROM (
//                SELECT a, b, c, d, e,
//                       GROUPING_ID(a, b, c, d, e) as gid,
//                       SUM(f) as s
//                FROM ${tb_name}
//                GROUP BY ROLLUP(a, b, c, d, e)
//            ) t
//            WHERE t.gid < 16 -- 过滤掉某些层级的聚合结果
//            ORDER BY t.a, t.b, t.c, t.d, t.e, t.gid, t.s;"""

//    sql """SELECT a, b, c, d, e, GROUPING(a), GROUPING_ID(a,b,c,d,e), SUM(f) FROM ${tb_name} GROUP BY ROLLUP(a,b,c,d,e)
//            ORDER BY a, b, c, d, e;"""
//    sql """SELECT a, b, c, d, GROUPING(a), GROUPING(d), GROUPING_ID(a,b,c,d), SUM(f) FROM ${tb_name} GROUP BY CUBE(a,b,c,d)
//            ORDER BY a, b, c, d;"""
//    sql """SELECT a, b, c, d, e, GROUPING_ID(a,e), SUM(f) FROM ${tb_name} GROUP BY GROUPING SETS ((a,b,c,d,e), (a,b), ())
//            ORDER BY a, b, c, d, e;"""

    sql_str =  """-- 脑图场景：with a as (...) select * from a
            WITH agg_cte AS (
                SELECT a, b, c, d, e, 
                       GROUPING(a) as is_a_null,
                       GROUPING_ID(a, b, c, d, e) as gid,
                       MAX(f) as max_f
                FROM ${tb_name}
                GROUP BY CUBE(a, b, c, d, e)
            )
            SELECT * FROM agg_cte WHERE is_a_null = 0 AND gid > 0
            ORDER BY a, b, c, d, e, is_a_null, gid, max_f;"""
    judge_explain(sql_str, true)
    compare_res(sql_str)

//    sql """-- 场景：维度列参与运算后进行 ROLLUP
//            -- 验证：GROUPING 函数是否能正确识别表达式维度的汇总状态
//            SELECT (a + 1) as a_new, b, c, d, e,
//                   GROUPING(a_new),
//                   GROUPING_ID(a_new, b, c, d, e),
//                   COUNT(*)
//            FROM ${tb_name}
//            GROUP BY ROLLUP(a_new, b, c, d, e)
//            ORDER BY a_new, b, c, d, e;"""

    sql_str =  """-- 验证在同一个 SQL 中对同一列多次调用 GROUPING 是否正常
            SELECT a, b, c, d, e, GROUPING(a) as g1, GROUPING(a) as g2, SUM(f)
            FROM ${tb_name}
            GROUP BY ROLLUP(a, b, c, d, e)
            ORDER BY a, b, c, d, e, g1, g2;"""
    judge_explain(sql_str, true)
    compare_res(sql_str)

//    sql """-- 验证当数据源为空表时，重写后的 GROUPING_ID 是否依然返回预期的全汇总 ID
//            SELECT a, b, c, d, e, GROUPING_ID(a,b,c,d,e), SUM(f)
//            FROM ${tb_name} WHERE 1=0
//            GROUP BY ROLLUP(a,b,c,d,e)
//            ORDER BY a, b, c, d, e;"""

    sql_str =  """-- 场景：脑图中的“一个consumer和2个consumer”
            SELECT * FROM (
                WITH rollup_cte AS (
                    SELECT a, b, c, d, e, SUM(f) as total
                    FROM ${tb_name}
                    GROUP BY ROLLUP(a, b, c, d, e)
                )
                SELECT * FROM rollup_cte WHERE a = 1
                UNION ALL
                SELECT * FROM rollup_cte WHERE a = 2
            ) t
            ORDER BY a, b, c, d, e, total;"""
    judge_explain(sql_str, true)
    compare_res(sql_str)

    sql_str =  """-- 场景：将改写后的聚合结果与其自身进行 Join
            WITH agg_result AS (
                SELECT a, b, c, d, e, SUM(f) as s
                FROM ${tb_name}
                GROUP BY ROLLUP(a, b, c, d, e)
            )
            -- 比较不同分组层级之间的差异
            SELECT r1.a, r1.s - r2.s 
            FROM agg_result r1
            JOIN agg_result r2 ON r1.a = r2.a
            WHERE r1.b IS NOT NULL AND r2.b IS NULL
            ORDER BY r1.a, (r1.s - r2.s);"""
    judge_explain(sql_str, true)
    compare_res(sql_str)


    sql_str =  """-- 场景：数据中本身有 NULL，且触发 CUBE 改写
            -- 验证：改写后的 Union All 逻辑是否会因为 Null 导致结果集膨胀或丢失
            SELECT a, b, c, d, e, SUM(f)
            FROM ${tb_name} 
            WHERE a IS NULL 
            GROUP BY CUBE(a, b, c, d, e)
            ORDER BY a, b, c, d, e;"""
    judge_explain(sql_str, true)
    compare_res(sql_str)


    sql_str =  """-- 场景：在一个 SQL 中同时写 SUM（支持）和 AVG（不支持）
            -- 预期：由于包含 AVG，整个 Repeat 算子不应触发改写
            SELECT a, b, c, d, e, SUM(f), AVG(f)
            FROM ${tb_name}
            GROUP BY ROLLUP(a, b, c, d, e)
            ORDER BY a, b, c, d, e;"""
    judge_explain(sql_str, false)
    compare_res(sql_str)

//    sql """-- 场景：有5个维度 a,b,c,d,e，但 GROUPING SETS 中不包含 (a,b,c,d,e)
//            -- 预期：不触发重写，计划中仍为传统的 LogicalRepeat
//            SELECT a, b, c, d, e, SUM(f)
//            FROM ${tb_name}
//            GROUP BY GROUPING SETS (
//                (a, b, c, d),
//                (a, b, c),
//                (a, b),
//                (a)
//            )
//            ORDER BY a, b, c, d, e;"""

    sql_str =  """-- 场景：有5个维度 a,b,c,d,e，但 GROUPING SETS 中不包含 (a,b,c,d,e)
            -- 预期：触发重写，
            SELECT a, b, c, d, e, SUM(f)
            FROM decompose_repeat_test_table
            GROUP BY GROUPING SETS (
                (a, b, c, d, e),
                (a, b, c, d),
                (a, b, c),
                (a, b),
                (a)
            )
            ORDER BY a, b, c, d, e;"""
    judge_explain(sql_str, true)
    compare_res(sql_str)

    sql_str =  """-- 场景：存在两个较大的分组组合，但它们互不包含，且没有全集
            -- 验证：优化器是否会尝试拆分（通常该规则要求必须有一个唯一的最高层级聚合作为入口）
            SELECT a, b, c, d, e, SUM(f)
            FROM ${tb_name}
            GROUP BY GROUPING SETS (
                (a, b, c, d),  -- 缺少 e
                (b, c, d, e),  -- 缺少 a
                (a, e)
            )
            ORDER BY a, b, c, d, e;"""
    judge_explain(sql_str, false)
    compare_res(sql_str)

    sql_str =  """-- 场景：高维度无全集 + GROUPING_ID
            -- 预期：验证在规则未触发时，Doris 原生的聚合逻辑是否正确
            SELECT a, b, c, d, e, 
                   GROUPING_ID(a, b, c, d, e) as gid, 
                   SUM(f)
            FROM ${tb_name}
            GROUP BY GROUPING SETS (
                (a, b, c, d),
                (e)
            )
            ORDER BY a, b, c, d, e, gid;"""
    judge_explain(sql_str, false)
    compare_res(sql_str)

    sql_str =  """-- 场景：有最大组 (a,b,c,d,e)，但使用了 DISTINCT（脑图明确不支持 distinct）
            -- 预期：异常拦截，不触发改写
            SELECT a, b, c, d, e, COUNT(DISTINCT f)
            FROM ${tb_name}
            GROUP BY ROLLUP(a, b, c, d, e)
            ORDER BY a, b, c, d, e;"""
    judge_explain(sql_str, false)
    compare_res(sql_str)



    // mtmv


    // sql cache


    // query cache


}
