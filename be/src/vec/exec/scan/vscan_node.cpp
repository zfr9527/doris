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

#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/scan/vscanner.h"

#include "runtime/runtime_filter_mgr.h"
#include "util/thread.h"
#include "util/priority_thread_pool.hpp"
#include "olap/tablet.h"

namespace doris::vectorized {

Status VScanNode::init(const TPlanNode &tnode, RuntimeState *state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    _direct_conjunct_size = _conjunct_ctxs.size();
    _runtime_state = state;

    const TQueryOptions& query_options = state->query_options();
    if (query_options.__isset.max_scan_key_num) {
        _max_scan_key_num = query_options.max_scan_key_num;
    } else {
        _max_scan_key_num = config::doris_max_scan_key_num;
    }
    if (query_options.__isset.max_pushdown_conditions_per_column) {
        _max_pushdown_conditions_per_column = query_options.max_pushdown_conditions_per_column;
    } else {
        _max_pushdown_conditions_per_column = config::max_pushdown_conditions_per_column;
    }

    _max_scanner_queue_size_bytes = query_options.mem_limit / 20;

    RETURN_IF_ERROR(_register_runtime_filter());

    return Status::OK();
}

Status VScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    RETURN_IF_ERROR(_init_profile());

    // init profile for runtime filter
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        IRuntimeFilter* runtime_filter = nullptr;
        state->runtime_filter_mgr()->get_consume_filter(_runtime_filter_descs[i].filter_id,
                                                        &runtime_filter);
        DCHECK(runtime_filter != nullptr);
        runtime_filter->init_profile(_runtime_profile.get());
    }
    return Status::OK();
}

Status VScanNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VScanNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    RETURN_IF_ERROR(_acquire_runtime_filter());

    RETURN_IF_ERROR(_process_conjuncts());
    RETURN_IF_ERROR(_init_scanners());
    RETURN_IF_ERROR(_start_scanners());
    return Status::OK();
}

//
Status VScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if (state->is_cancelled()) {
        std::unique_lock<std::mutex> l(_queue_lock);
        if (update_status(Status::Cancelled("Cancelled"))) {
            _queue_writer_cond.notify_all();
        }
    }

    if (_scan_finished.load()) {
        *eos = true;
        return Status::OK();
    }

    Block* scan_block = nullptr;
    {
        std::unique_lock<std::mutex> l(_queue_lock);
        // wait for block from queue
        while (_process_status.ok() && !_runtime_state->is_cancelled() &&
               _num_running_scanners > 0 && _block_queue.empty()) {
            _queue_reader_cond.wait_for(l, std::chrono::seconds(1));
        }
        if (!_process_status.ok()) {
            return _process_status;
        }
        if (_runtime_state->is_cancelled()) {
            if (update_status(Status::Cancelled("Cancelled"))) {
                _queue_writer_cond.notify_all();
            }
            return _process_status;
        }
        if (!_block_queue.empty()) {
            scan_block = _block_queue.front();
            _block_queue.pop_front();
        }
    }

    // return block
    if (scan_block != nullptr) {
        // notify scanner
        _queue_writer_cond.notify_one();
        // get scanner's block memory
        block->swap(*scan_block);
        reached_limit(block, eos);
        // reach scan node limit
        if (*eos) {
            _scan_finished.store(true);
            _queue_reader_cond.notify_all();
        }

        {
            // ReThink whether the SpinLock Better
            std::lock_guard<std::mutex> l(_free_blocks_lock);
            _free_blocks.emplace_back(scan_block);
        }
        return Status::OK();
    }

    return Status::OK();
}

Status VScanNode::_init_scanners() {
    return Status::OK();
}

Status VScanNode::_start_scanners() {
    std::list<VScanner*> scanners;
    int assigned_thread_num = _running_thread;
    size_t max_thread = config::doris_scanner_queue_size;
    // if batch size is 4k, max_thread will be 12
    if (config::doris_scanner_row_num > _runtime_state->batch_size()) {
        max_thread /= config::doris_scanner_row_num / _runtime_state->batch_size();
        if (max_thread <= 0) {
            max_thread = 1;
        }
    }

    // calculate the thread num
    size_t thread_slot_num = 0;
    if (_scan_row_batches_bytes < _max_scanner_queue_size_bytes / 2) {
        std::lock_guard<std::mutex> l(_free_blocks_lock);
        // _block_per_scanner is 4, _free_blocks.size() is 1024
        thread_slot_num = _free_blocks.size() / _block_per_scanner;
        thread_slot_num += (_free_blocks.size() % _block_per_scanner != 0);
        thread_slot_num = std::min(thread_slot_num, max_thread - assigned_thread_num);
        if (thread_slot_num <= 0) {
            thread_slot_num = 1;
        }
    } else {
        std::lock_guard<std::mutex> l(_queue_lock);
        if (_block_queue.empty()) {
            if (assigned_thread_num == 0) {
                thread_slot_num = 1;
            }
        }
    }

    {
        std::lock_guard<std::mutex> l(_scanners_lock);
        thread_slot_num = std::min(thread_slot_num, _scanners.size());
        for (int i = 0; i < thread_slot_num && !_scanners.empty();) {
            auto scanner = _scanners.front();
            _scanners.pop_front();
            scanners.push_back(scanner);
            assigned_thread_num++;
            i++;
        }
    }

    // submit scanners to thread-pool
    auto cur_span = opentelemetry::trace::Tracer::GetCurrentSpan();
    ThreadPoolToken* thread_token = nullptr;
    if (_limit > -1 && _limit < 1024) {
        thread_token = _runtime_state->get_query_fragments_ctx()->get_serial_token();
    } else {
        thread_token = _runtime_state->get_query_fragments_ctx()->get_token();
    }
    PriorityThreadPool* thread_pool = _runtime_state->exec_env()->scan_thread_pool();
    PriorityThreadPool* remote_thread_pool = _runtime_state->exec_env()->remote_scan_thread_pool();

    auto iter = scanners.begin();
    while (iter != scanners.end()) {
        auto s = _submit_scanner(_runtime_state, thread_token, thread_pool, remote_thread_pool, *iter,
                                 cur_span);
        if (s.ok()) {
            scanners.erase(iter++);
        } else {
            LOG(FATAL) << "Failed to assign scanner task to thread pool! " << s.get_error_msg();
        }
    }
    return Status::OK();
}

Status VScanNode::_submit_scanner(RuntimeState* state, ThreadPoolToken* thread_token,
                                  PriorityThreadPool* thread_pool,
                                  PriorityThreadPool* remote_thread_pool, VScanner* scanner,
                                  const OpentelemetrySpan& cur_span) {
    Status s = Status::OK();
    if (thread_token != nullptr) {
        s = thread_token->submit_func([this, scanner, parent_span = cur_span] {
            opentelemetry::trace::Scope scope {parent_span};
            this->scanner_thread(scanner);
        });
    } else {
        PriorityThreadPool::Task task;
        task.work_function = [this, scanner, parent_span = cur_span] {
            opentelemetry::trace::Scope scope {parent_span};
            this->scanner_thread(scanner);
        };
        // task.priority = _nice;
        // task.queue_id = state->exec_env()->store_path_to_index((scanner)->scan_disk());

        TabletStorageType type = TabletStorageType::STORAGE_TYPE_LOCAL;
        bool ret;
        if (type == TabletStorageType::STORAGE_TYPE_LOCAL) {
            ret = thread_pool->offer(task);
        } else {
            ret = remote_thread_pool->offer(task);
        }

        if (!ret) {
            s = Status::InternalError("Failed to schedule olap scanner");
        }
    }

    if (s.ok()) {
        // scanner->start_wait_worker_timer();
        // COUNTER_UPDATE(_scanner_sched_counter, 1);
        ++_running_thread;
        // ++_total_assign_num;
    }
    return s;
}

void VScanNode::scanner_thread(VScanner* scanner) {
    START_AND_SCOPE_SPAN(scanner->runtime_state()->get_tracer(), span,
                         "VScanNode::scanner_thread");
    SCOPED_ATTACH_TASK(_runtime_state);
    Thread::set_self_name("vscanner");
    // int64_t wait_time = scanner->update_wait_worker_timer();
    // Do not use ScopedTimer. There is no guarantee that, the counter
    // (_scan_cpu_timer, the class member) is not destroyed after `_running_thread==0`.
    ThreadCpuStopWatch cpu_watch;
    cpu_watch.start();
    Status status = Status::OK();
    bool eos = false;
    RuntimeState *state = scanner->runtime_state();
    DCHECK(nullptr != state);
    if (!scanner->is_open()) {
        status = scanner->open(state);
        if (!status.ok()) {
            std::lock_guard<SpinLock> guard(_status_mutex);
            _process_status = status;
            eos = true;
            return;
        }
    }

    std::vector<Block*> blocks;
    // Because we use thread pool to scan data from storage. One scanner can't
    // use this thread too long, this can starve other query's scanner. So, we
    // need yield this thread when we do enough work. However, OlapStorage read
    // data in pre-aggregate mode, then we can't use storage returned data to
    // judge if we need to yield. So we record all raw data read in this round
    // scan, if this exceed row number or bytes threshold, we yield this thread.
    int64_t raw_rows_read = scanner->raw_rows_read();
    int64_t raw_rows_threshold = raw_rows_read + config::doris_scanner_row_num;
    int64_t raw_bytes_read = 0;
    int64_t raw_bytes_threshold = config::doris_scanner_row_bytes;
    bool get_free_block = true;
    bool reached_limit = false;
    int num_rows_in_block = 0;

    // Has to wait at least one full block, or it will cause a lot of schedule task in priority
    // queue, it will affect query latency and query concurrency for example ssb 3.3.
    while (!eos && raw_bytes_read < raw_bytes_threshold &&
           ((raw_rows_read < raw_rows_threshold && get_free_block) ||
            num_rows_in_block < _runtime_state->batch_size())) {
        if (UNLIKELY(state->is_cancelled())) {
            eos = true;
            status = Status::Cancelled("Cancelled");
            break;
        }

        auto block = _alloc_block(&get_free_block);
        status = scanner->get_block(_runtime_state, block, &eos);
        VLOG_ROW << "VOlapScanNode input rows: " << block->rows();
        if (!status.ok()) {
            LOG(WARNING) << "Scan thread read VOlapScanner failed: " << status.to_string();
            // Add block ptr in blocks, prevent mem leak in read failed
            blocks.push_back(block);
            eos = true;
            break;
        }

        raw_bytes_read += block->bytes();
        num_rows_in_block += block->rows();
        // 4. if status not ok, change status_.
        if (UNLIKELY(block->rows() == 0)) {
            std::lock_guard<std::mutex> l(_free_blocks_lock);
            _free_blocks.emplace_back(block);
        } else {
            if (!blocks.empty() &&
                blocks.back()->rows() + block->rows() <= _runtime_state->batch_size()) {
                MutableBlock(blocks.back()).merge(*block);
                block->clear_column_data();
                std::lock_guard<std::mutex> l(_free_blocks_lock);
                _free_blocks.emplace_back(block);
            } else {
                blocks.push_back(block);
            }
        }
        raw_rows_read = scanner->raw_rows_read();
        if (_limit != -1 and raw_rows_read >= _limit) {
            eos = true;
            reached_limit = true;
            break;
        }
    } // end while

    {
        // if we failed, check status.
        if (UNLIKELY(!status.ok())) {
            std::lock_guard<SpinLock> guard(_status_mutex);
            if (LIKELY(_process_status.ok())) {
                _process_status = status;
            }
        }

        bool global_status_ok = false;
        {
            std::lock_guard<SpinLock> guard(_status_mutex);
            global_status_ok = _process_status.ok();
        }
        if (UNLIKELY(!global_status_ok)) {
            eos = true;
            std::for_each(blocks.begin(), blocks.end(), std::default_delete<Block>());
        } else {
            std::unique_lock<std::mutex> l(_queue_lock);
            _block_queue.insert(_block_queue.end(), blocks.begin(), blocks.end());
            for (auto b: blocks) {
                _scan_row_batches_bytes += b->allocated_bytes();
            }
            _queue_writer_cond.notify_one();
        }

        ThreadPoolToken *thread_token = nullptr;
        if (_limit > -1 && _limit < 1024) {
            thread_token = state->get_query_fragments_ctx()->get_serial_token();
        } else {
            thread_token = state->get_query_fragments_ctx()->get_token();
        }
        PriorityThreadPool *thread_pool = state->exec_env()->scan_thread_pool();
        PriorityThreadPool *remote_thread_pool = state->exec_env()->remote_scan_thread_pool();
        // If eos is true, we will process out of this lock block.
        // 1. no more data for this scanner: global_status_ok = true or false
        // 2. read return error for this scanner: global_status_ok = false
        // 3. query is canceled: global_status_ok = false
        // 4. error occurred for any other scanners: global_status_ok = false
        // 5. reach limit: global_status_ok = true
        if (eos) {
            scanner->close(state);
            if (global_status_ok && !reached_limit) {
                std::lock_guard<std::mutex> l(_scanners_lock);
                if (!_scanners.empty() &&
                    _scan_row_batches_bytes < _max_scanner_queue_size_bytes / 2) {
                    auto cur_span = opentelemetry::trace::Tracer::GetCurrentSpan();
                    auto s = _submit_scanner(state, thread_token, thread_pool, remote_thread_pool,
                                             _scanners.front(), cur_span);
                    if (!s.ok()) {
                        LOG(FATAL) << "Failed to assign scanner task to thread pool! "
                                   << s.get_error_msg();
                    }
                    _scanners.pop_front();
                }
            }
        } else {
            // status is ok and more data to read
            if (_scan_row_batches_bytes < _max_scanner_queue_size_bytes / 2) {
                auto cur_span = opentelemetry::trace::Tracer::GetCurrentSpan();
                auto s = _submit_scanner(state, thread_token, thread_pool, remote_thread_pool,
                                         scanner, cur_span);
                if (!s.ok()) {
                    LOG(FATAL) << "Failed to assign scanner task to thread pool! "
                               << s.get_error_msg();
                }
            } else {
                std::lock_guard<std::mutex> l(_scanners_lock);
                _scanners.push_front(scanner);
            }
        }
    }
    if (eos) {
        std::lock_guard<std::mutex> l(_queue_lock);
        // _progress.update(1);
        // if (_progress.done()) {
        //     // this is the right out
        //     _scanner_done = true;
        // }
    }
    // _scan_cpu_timer->update(cpu_watch.elapsed_time());
    // _scanner_wait_worker_timer->update(wait_time);

    std::unique_lock<std::mutex> l(_queue_lock);
    _running_thread--;

    // The transfer thead will wait for `_running_thread==0`, to make sure all scanner threads won't access class members.
    // Do not access class members after this code.
    _queue_reader_cond.notify_one();
    _scan_thread_exit_cv.notify_one();
}

Block* VScanNode::_alloc_block(bool* get_free_block) {
    {
        std::lock_guard<std::mutex> l(_free_blocks_lock);
        if (!_free_blocks.empty()) {
            auto block = _free_blocks.back();
            _free_blocks.pop_back();
            return block;
        }
    }

    *get_free_block = false;
    auto block = new Block(_tuple_desc->slots(), _runtime_state->batch_size());
    // _buffered_bytes += block->allocated_bytes();
    return block;
}

Status VScanNode::_register_runtime_filter() {
    int filter_size = _runtime_filter_descs.size();
    _runtime_filter_ctxs.resize(filter_size);
    _runtime_filter_ready_flag.resize(filter_size);
    for (int i = 0; i < filter_size; ++i) {
        IRuntimeFilter* runtime_filter = nullptr;
        const auto& filter_desc = _runtime_filter_descs[i];
        RETURN_IF_ERROR(_runtime_state->runtime_filter_mgr()->regist_filter(
                RuntimeFilterRole::CONSUMER, filter_desc, _runtime_state->query_options(), id()));
        RETURN_IF_ERROR(_runtime_state->runtime_filter_mgr()->get_consume_filter(filter_desc.filter_id,
                                                                        &runtime_filter));
        _runtime_filter_ctxs[i].runtime_filter = runtime_filter;
        _runtime_filter_ready_flag[i] = false;
        _rf_locks.push_back(std::make_unique<std::mutex>());
    }
    return Status::OK();
}

Status VScanNode::_acquire_runtime_filter() {
    _runtime_filter_ctxs.resize(_runtime_filter_descs.size());
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        auto& filter_desc = _runtime_filter_descs[i];
        IRuntimeFilter* runtime_filter = nullptr;
        _runtime_state->runtime_filter_mgr()->get_consume_filter(filter_desc.filter_id, &runtime_filter);
        DCHECK(runtime_filter != nullptr);
        if (runtime_filter == nullptr) {
            continue;
        }
        bool ready = runtime_filter->is_ready();
        if (!ready) {
            ready = runtime_filter->await();
        }
        if (ready) {
            std::list<ExprContext*> expr_context;
            RETURN_IF_ERROR(runtime_filter->get_push_expr_ctxs(&expr_context));
            _runtime_filter_ctxs[i].apply_mark = true;
            _runtime_filter_ctxs[i].runtime_filter = runtime_filter;
            for (auto ctx : expr_context) {
                ctx->prepare(_runtime_state, row_desc());
                ctx->open(_runtime_state);
                int index = _conjunct_ctxs.size();
                _conjunct_ctxs.push_back(ctx);
                _conjunct_id_to_runtime_filter_ctxs[index] = &_runtime_filter_ctxs[i];
            }
        }
    }

    return Status::OK();
}

}
