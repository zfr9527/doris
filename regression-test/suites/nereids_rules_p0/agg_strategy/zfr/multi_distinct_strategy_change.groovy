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

suite("multi_distinct_strategy_change") {
    sql "SET ignore_shape_nodes='PhysicalProject'"
    sql "set enable_parallel_result_sink=false"
    sql "set global enable_auto_analyze=false"
    sql "set runtime_filter_mode=OFF"
    sql "set be_number_for_test=1"
    sql "set parallel_pipeline_task_num=1"

    multi_sql """
        drop stats t_md_hash_id;
        drop stats t_md_hash_ab;
        drop stats t_md_lineage;
    """

    // T1: strategy decision is based on distinct keys instead of group-by keys.
    explain {
        sql """
            logical plan
            select g_low, count(distinct id)
            from t_md_hash_id
            group by g_low
        """
        notContains "multi_distinct_count"
    }
    explain {
        sql """
            logical plan
            select id, count(distinct dst_high)
            from t_md_hash_id
            group by id
        """
        contains "multi_distinct_count"
    }

    // T2-F: distinct function coverage under the same distribution check.
    explain {
        sql """
            logical plan
            select g_low, sum(distinct id)
            from t_md_hash_id
            group by g_low
        """
        notContains "multi_distinct_sum"
    }
    explain {
        sql """
            logical plan
            select g_low, sum(distinct dst_high)
            from t_md_hash_id
            group by g_low
        """
        contains "multi_distinct_sum"
    }
    explain {
        sql """
            logical plan
            select g_low, sum0(distinct id)
            from t_md_hash_id
            group by g_low
        """
        notContains "multi_distinct_sum0"
    }
    explain {
        sql """
            logical plan
            select g_low, sum0(distinct dst_high)
            from t_md_hash_id
            group by g_low
        """
        contains "multi_distinct_sum0"
    }
    explain {
        sql """
            logical plan
            select g_low, group_concat(distinct id, '|')
            from t_md_hash_id
            group by g_low
        """
        notContains "multi_distinct_group_concat"
    }
    explain {
        sql """
            logical plan
            select g_low, group_concat(distinct dst_high, '|')
            from t_md_hash_id
            group by g_low
        """
        contains "multi_distinct_group_concat"
    }

    // T3: hash distribution must be matched completely instead of partially.
    explain {
        sql """
            logical plan
            select g, count(distinct b)
            from t_md_hash_ab
            group by g
        """
        contains "multi_distinct_count"
    }

    // T4: distribution lineage can pass through project/filter, but not expression rewrite.
    explain {
        sql """
            logical plan
            select g, count(distinct k1_alias)
            from (
                select k1 as k1_alias, g
                from t_md_lineage
                where k2 > 3
            ) t
            group by g
        """
        notContains "multi_distinct_count"
    }
    explain {
        sql """
            logical plan
            select g, count(distinct k1_alias + 1)
            from (
                select k1 as k1_alias, g
                from t_md_lineage
                where k2 > 3
            ) t
            group by g
        """
        contains "multi_distinct_count"
    }

    sql "analyze table t_md_hash_id with sync"

    // T5: once distribution is satisfied, stats/ndv must not override split.
    explain {
        sql """
            logical plan
            select g_low, count(distinct id)
            from t_md_hash_id
            group by g_low
        """
        notContains "multi_distinct_count"
    }
    explain {
        sql """
            logical plan
            select g_low, count(distinct dst_high)
            from t_md_hash_id
            group by g_low
        """
        contains "multi_distinct_count"
    }
}
