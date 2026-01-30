import java.util.concurrent.TimeUnit

class CanRetryException extends IllegalStateException {
    CanRetryException() {
    }

    CanRetryException(String var1) {
        super(var1)
    }

    CanRetryException(String var1, Throwable var2) {
        super(var1, var2)
    }

    CanRetryException(Throwable var1) {
        super(var1)
    }
}

suite("decompose_repeat_sql_cache_test") {


    withGlobalLock("cache_last_version_interval_second") {

        sql """ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '0');"""
        sql """ADMIN SET ALL FRONTENDS CONFIG ('sql_cache_manage_num' = '100000')"""

        def assertHasCache = { String sqlStr ->
            explain {
                sql ("physical plan ${sqlStr}")
                contains("PhysicalSqlCache")
            }

            judge_res(sqlStr)
        }

        def assertNoCache = { String sqlStr ->
            explain {
                sql ("physical plan ${sqlStr}")
                notContains("PhysicalSqlCache")
            }
        }

        def retryTestSqlCache = { int executeTimes, long intervalMillis, Closure<Integer> closure ->
            Throwable throwable = null
            for (int i = 1; i <= executeTimes; ++i) {
                try {
                    return closure(i)
                } catch (CanRetryException t) {
                    logger.warn("Retry failed: $t", t)
                    throwable = t.getCause()
                    Uninterruptibles.sleepUninterruptibly(intervalMillis, TimeUnit.MILLISECONDS)
                } catch (Throwable t) {
                    throwable = t
                    break
                }
            }
            if (throwable != null) {
                throw throwable
            }
            return null
        }

        String dbName = context.config.getDbNameByFile(context.file)

        for (def __ in 0..3) {
            combineFutures(
                    extraThread("testDecomposeRepeatSqlCache", {
                        retryTestSqlCache(3, 1000) {
                            def prefix_str = "decompose_repeat_sql_cache_"
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

                            sleep(10 * 1000)

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

                            sql "set enable_nereids_planner=true"
                            sql "set enable_fallback_to_original_planner=false"
                            sql "set enable_sql_cache=true"
                            sql "set enable_strong_consistency_read=true"

                            // Direct Query
                            assertNoCache sql_str
                            retryUntilHasSqlCache sql_str
                            assertHasCache sql_str

                        }
                    })
            ).get()
        }

    }

}
