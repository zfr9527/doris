import java.util.concurrent.TimeUnit

suite("decompose_repeat_query_cache_test") {

    def setSessionVariables = {
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set enable_sql_cache=false"
        sql "set enable_query_cache=true"
        sql """set enable_materialized_view_nest_rewrite=true;"""
    }

    def assertHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                assertTrue(profileString.contains("HitCache:  1")) && assertFalse(profileString.contains("HitCache:  0"))
            }
        }
    }

    def assertNoCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                assertTrue(profileString.contains("HitCache:  0")) && assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    def compare_res = { def stmt ->
        sql "set disable_nereids_rules='DECOMPOSE_REPEAT';"
        def no_rewrite_res = sql stmt
        logger.info("no_rewrite_res: " + no_rewrite_res)
        sql "set disable_nereids_rules='';"
        def rewrite_res = sql stmt
        logger.info("rewrite_res: " + rewrite_res)
        assertEquals(no_rewrite_res.toString(), rewrite_res.toString())
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

    String dbName = context.config.getDbNameByFile(context.file)
    sql "ADMIN SET FRONTEND CONFIG ('cache_last_version_interval_second' = '0')"

    combineFutures(
            extraThread("testRenameMtmv", {

                def prefix_str = "decompose_repeat_query_cache_"
                def tb_name = prefix_str + "table"

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

                setSessionVariables()

                def sql_str = """
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

                assertNoCache sql_str
                judge_explain(sql_str, true)
                compare_res(sql_str)

                assertHasCache sql_str
                judge_explain(sql_str, true)
                compare_res(sql_str)

            })
    ).get()

}
