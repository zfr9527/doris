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

#pragma once

#include <bthread/mutex.h>
#include <bvar/bvar.h>
#include <bvar/latency_recorder.h>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>

/**
 * Manage bvars that with similar names (identical prefix)
 * ${module}_${name}_${tag}
 * where `tag` is added automatically when calling `get` or `put`
 */
template <typename Bvar, bool is_status = false>
class BvarWithTag {
public:
    BvarWithTag(std::string module, std::string name)
            : module_(std::move(module)), name_(std::move(name)) {}

    template <typename ValType>
        requires std::is_integral_v<ValType>
    void put(const std::string& tag, ValType value) {
        std::shared_ptr<Bvar> instance = nullptr;
        {
            std::lock_guard<bthread::Mutex> l(mutex_);
            auto it = bvar_map_.find(tag);
            if (it == bvar_map_.end()) {
                instance = std::make_shared<Bvar>(module_, name_ + "_" + tag, ValType());
                bvar_map_[tag] = instance;
            } else {
                instance = it->second;
            }
        }
        // FIXME(gavin): check bvar::Adder and more
        if constexpr (std::is_same_v<Bvar, bvar::LatencyRecorder>) {
            (*instance) << value;
        } else if constexpr (is_status) {
            instance->set_value(value);
        } else {
            // This branch mean to be unreachable, add an assert(false) here to
            // prevent missing branch match.
            // Postpone deduction of static_assert by evaluating sizeof(T)
            static_assert(!sizeof(Bvar), "all types must be matched with if constexpr");
        }
    }

    std::shared_ptr<Bvar> get(const std::string& tag) {
        std::shared_ptr<Bvar> instance = nullptr;
        std::lock_guard<bthread::Mutex> l(mutex_);

        auto it = bvar_map_.find(tag);
        if (it == bvar_map_.end()) {
            instance = std::make_shared<Bvar>(module_, name_ + "_" + tag);
            bvar_map_[tag] = instance;
            return instance;
        }
        return it->second;
    }

    void remove(const std::string& tag) {
        std::lock_guard<bthread::Mutex> l(mutex_);
        bvar_map_.erase(tag);
    }

private:
    bthread::Mutex mutex_;
    std::string module_;
    std::string name_;
    std::map<std::string, std::shared_ptr<Bvar>> bvar_map_;
};

using BvarLatencyRecorderWithTag = BvarWithTag<bvar::LatencyRecorder>;

template <typename T>
    requires std::is_integral_v<T>
using BvarStatusWithTag = BvarWithTag<bvar::Status<T>, true>;

// meta-service's bvars
extern BvarLatencyRecorderWithTag g_bvar_ms_begin_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_precommit_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_commit_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_commit_txn_eventually;
extern BvarLatencyRecorderWithTag g_bvar_ms_abort_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_current_max_txn_id;
extern BvarLatencyRecorderWithTag g_bvar_ms_check_txn_conflict;
extern BvarLatencyRecorderWithTag g_bvar_ms_abort_txn_with_coordinator;
extern BvarLatencyRecorderWithTag g_bvar_ms_begin_sub_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_abort_sub_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_clean_txn_label;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_version;
extern BvarLatencyRecorderWithTag g_bvar_ms_batch_get_version;
extern BvarLatencyRecorderWithTag g_bvar_ms_create_tablets;
extern BvarLatencyRecorderWithTag g_bvar_ms_update_tablet;
extern BvarLatencyRecorderWithTag g_bvar_ms_update_tablet_schema;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_tablet;
extern BvarLatencyRecorderWithTag g_bvar_ms_prepare_rowset;
extern BvarLatencyRecorderWithTag g_bvar_ms_commit_rowset;
extern BvarLatencyRecorderWithTag g_bvar_ms_update_tmp_rowset;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_rowset;
extern BvarLatencyRecorderWithTag g_bvar_ms_drop_index;
extern BvarLatencyRecorderWithTag g_bvar_ms_prepare_index;
extern BvarLatencyRecorderWithTag g_bvar_ms_commit_index;
extern BvarLatencyRecorderWithTag g_bvar_ms_prepare_partition;
extern BvarLatencyRecorderWithTag g_bvar_ms_commit_partition;
extern BvarLatencyRecorderWithTag g_bvar_ms_drop_partition;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_tablet_stats;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_obj_store_info;
extern BvarLatencyRecorderWithTag g_bvar_ms_alter_obj_store_info;
extern BvarLatencyRecorderWithTag g_bvar_ms_alter_storage_vault;
extern BvarLatencyRecorderWithTag g_bvar_ms_create_instance;
extern BvarLatencyRecorderWithTag g_bvar_ms_alter_instance;
extern BvarLatencyRecorderWithTag g_bvar_ms_alter_cluster;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_cluster;
extern BvarLatencyRecorderWithTag g_bvar_ms_create_stage;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_stage;
extern BvarLatencyRecorderWithTag g_bvar_ms_drop_stage;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_iam;
extern BvarLatencyRecorderWithTag g_bvar_ms_update_ak_sk;
extern BvarLatencyRecorderWithTag g_bvar_ms_alter_iam;
extern BvarLatencyRecorderWithTag g_bvar_ms_alter_ram_user;
extern BvarLatencyRecorderWithTag g_bvar_ms_begin_copy;
extern BvarLatencyRecorderWithTag g_bvar_ms_finish_copy;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_copy_job;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_copy_files;
extern BvarLatencyRecorderWithTag g_bvar_ms_filter_copy_files;
extern BvarLatencyRecorderWithTag g_bvar_ms_start_tablet_job;
extern BvarLatencyRecorderWithTag g_bvar_ms_finish_tablet_job;
extern BvarLatencyRecorderWithTag g_bvar_ms_update_delete_bitmap;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_delete_bitmap;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_delete_bitmap_update_lock;
extern BvarLatencyRecorderWithTag g_bvar_ms_remove_delete_bitmap;
extern BvarLatencyRecorderWithTag g_bvar_ms_remove_delete_bitmap_update_lock;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_cluster_status;
extern BvarLatencyRecorderWithTag g_bvar_ms_set_cluster_status;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_instance;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_rl_task_commit_attach;
extern BvarLatencyRecorderWithTag g_bvar_ms_reset_rl_progress;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_txn_id;
extern BvarLatencyRecorderWithTag g_bvar_ms_check_kv;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_schema_dict;
extern bvar::Adder<int64_t> g_bvar_update_delete_bitmap_fail_counter;
extern bvar::Adder<int64_t> g_bvar_get_delete_bitmap_fail_counter;

// recycler's bvars
extern BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_index_earlest_ts;
extern BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_partition_earlest_ts;
extern BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_rowset_earlest_ts;
extern BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_tmp_rowset_earlest_ts;
extern BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_expired_txn_label_earlest_ts;

// txn_kv's bvars
extern bvar::LatencyRecorder g_bvar_txn_kv_get;
extern bvar::LatencyRecorder g_bvar_txn_kv_range_get;
extern bvar::LatencyRecorder g_bvar_txn_kv_put;
extern bvar::LatencyRecorder g_bvar_txn_kv_commit;
extern bvar::LatencyRecorder g_bvar_txn_kv_atomic_set_ver_key;
extern bvar::LatencyRecorder g_bvar_txn_kv_atomic_set_ver_value;
extern bvar::LatencyRecorder g_bvar_txn_kv_atomic_add;
extern bvar::LatencyRecorder g_bvar_txn_kv_remove;
extern bvar::LatencyRecorder g_bvar_txn_kv_range_remove;
extern bvar::LatencyRecorder g_bvar_txn_kv_get_read_version;
extern bvar::LatencyRecorder g_bvar_txn_kv_get_committed_version;
extern bvar::LatencyRecorder g_bvar_txn_kv_batch_get;

extern bvar::Adder<int64_t> g_bvar_txn_kv_commit_error_counter;
extern bvar::Adder<int64_t> g_bvar_txn_kv_commit_conflict_counter;
extern bvar::Adder<int64_t> g_bvar_txn_kv_get_count_normalized;

extern bvar::Adder<int64_t> g_bvar_delete_bitmap_lock_txn_put_conflict_counter;
extern bvar::Adder<int64_t> g_bvar_delete_bitmap_lock_txn_remove_conflict_by_fail_counter;
extern bvar::Adder<int64_t> g_bvar_delete_bitmap_lock_txn_remove_conflict_by_load_counter;
extern bvar::Adder<int64_t>
        g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_commit_counter;
extern bvar::Adder<int64_t>
        g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_lease_counter;
extern bvar::Adder<int64_t>
        g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_abort_counter;

extern const int64_t BVAR_FDB_INVALID_VALUE;
extern bvar::Status<int64_t> g_bvar_fdb_client_count;
extern bvar::Status<int64_t> g_bvar_fdb_configuration_coordinators_count;
extern bvar::Status<int64_t> g_bvar_fdb_configuration_usable_regions;
extern bvar::Status<int64_t> g_bvar_fdb_coordinators_unreachable_count;
extern bvar::Status<int64_t> g_bvar_fdb_fault_tolerance_count;
extern bvar::Status<int64_t> g_bvar_fdb_data_average_partition_size_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_log_server_space_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_moving_data_highest_priority;
extern bvar::Status<int64_t> g_bvar_fdb_data_moving_data_in_flight_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_moving_data_in_queue_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_moving_total_written_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_partition_count;
extern bvar::Status<int64_t> g_bvar_fdb_data_storage_server_space_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_state_min_replicas_remaining;
extern bvar::Status<int64_t> g_bvar_fdb_data_total_kv_size_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_total_disk_used_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_generation;
extern bvar::Status<int64_t> g_bvar_fdb_incompatible_connections;
extern bvar::Status<int64_t> g_bvar_fdb_latency_probe_transaction_start_ns;
extern bvar::Status<int64_t> g_bvar_fdb_latency_probe_commit_ns;
extern bvar::Status<int64_t> g_bvar_fdb_latency_probe_read_ns;
extern bvar::Status<int64_t> g_bvar_fdb_machines_count;
extern bvar::Status<int64_t> g_bvar_fdb_process_count;
extern bvar::Status<int64_t> g_bvar_fdb_qos_worst_data_lag_storage_server_ns;
extern bvar::Status<int64_t> g_bvar_fdb_qos_worst_durability_lag_storage_server_ns;
extern bvar::Status<int64_t> g_bvar_fdb_qos_worst_log_server_queue_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_qos_worst_storage_server_queue_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_workload_conflict_rate_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_location_rate_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_keys_read_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_read_bytes_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_read_rate_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_write_rate_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_written_bytes_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_transactions_started_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_transactions_committed_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_transactions_rejected_hz;
extern bvar::Status<int64_t> g_bvar_fdb_client_thread_busyness_percent;

// checker
extern BvarStatusWithTag<long> g_bvar_checker_num_scanned;
extern BvarStatusWithTag<long> g_bvar_checker_num_scanned_with_segment;
extern BvarStatusWithTag<long> g_bvar_checker_num_check_failed;
extern BvarStatusWithTag<long> g_bvar_checker_check_cost_s;
extern BvarStatusWithTag<long> g_bvar_checker_enqueue_cost_s;
extern BvarStatusWithTag<long> g_bvar_checker_last_success_time_ms;
extern BvarStatusWithTag<long> g_bvar_checker_instance_volume;
extern BvarStatusWithTag<long> g_bvar_inverted_checker_num_scanned;
extern BvarStatusWithTag<long> g_bvar_inverted_checker_num_check_failed;

extern BvarStatusWithTag<int64_t> g_bvar_inverted_checker_leaked_delete_bitmaps;
extern BvarStatusWithTag<int64_t> g_bvar_inverted_checker_abnormal_delete_bitmaps;
extern BvarStatusWithTag<int64_t> g_bvar_inverted_checker_delete_bitmaps_scanned;
