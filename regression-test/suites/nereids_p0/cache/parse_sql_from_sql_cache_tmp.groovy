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

import java.util.stream.Collectors

suite("parse_sql_from_sql_cache_tmp") {
    def assertHasCache = { String sqlStr ->
        explain {
            sql ("physical plan ${sqlStr}")
            contains("PhysicalSqlCache")
        }
    }

    def assertNoCache = { String sqlStr ->
        explain {
            sql ("physical plan ${sqlStr}")
            notContains("PhysicalSqlCache")
        }
    }

    def dbName = (sql "select database()")[0][0].toString()
    sql "ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '10')"

    // make sure if the table has been dropped, the cache should invalidate,
    // so we should retry multiple times to check
    for (def __ in 0..3) {
        combineFutures(

            extraThread("testAddPartitionAndInsertOverwrite", {
                def tb_name = "test_insert_overwrite_use_plan_cache2"
                createTestTable tb_name

                // after partition changed 10s, the sql cache can be used
                sleep(10000)

                sql "set enable_nereids_planner=true"
                sql "set enable_fallback_to_original_planner=false"
                sql "set enable_sql_cache=true"

                assertNoCache "select * from ${tb_name}"
                sql "select * from ${tb_name}"
                assertHasCache "select * from ${tb_name}"

                // insert overwrite data can not use cache
                sql "INSERT OVERWRITE table ${tb_name} PARTITION(p5) VALUES (5, 6);"
                sleep(10 * 1000)
                assertNoCache "select * from ${tb_name}"
                sql "select * from ${tb_name}"
                assertHasCache "select * from ${tb_name}"

                // NOTE: in cloud mode, add empty partition can not use cache, because the table version already update,
                //       but in native mode, add empty partition can use cache
                sql "alter table ${tb_name} add partition p6 values[('6'),('7'))"
                if (isCloudMode()) {
                    assertNoCache "select * from ${tb_name}"
                } else {
                    assertHasCache "select * from ${tb_name}"
                }

                // insert overwrite data can not use cache
                sql "INSERT OVERWRITE table ${tb_name} PARTITION(p6) VALUES (6, 6);"
                assertNoCache "select * from ${tb_name}"
            })
        ).get()
    }
}
