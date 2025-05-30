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

#include "runtime/memory/thread_mem_tracker_mgr.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group_manager.h"

namespace doris {

class ThreadMemTrackerMgrTest : public testing::Test {
public:
    ThreadMemTrackerMgrTest() = default;
    ~ThreadMemTrackerMgrTest() override = default;

    void SetUp() override { _wg_manager = std::make_unique<WorkloadGroupMgr>(); }

    void TearDown() override { _wg_manager.reset(); }

protected:
    std::unique_ptr<WorkloadGroupMgr> _wg_manager;
};

TEST_F(ThreadMemTrackerMgrTest, ConsumeMemory) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "UT-ConsumeMemory");
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t);

    int64_t size1 = 4 * 1024;
    int64_t size2 = 4 * 1024 * 1024;

    thread_context->attach_task(rc);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    // size1 < config::mem_tracker_consume_min_size_bytes, not consume mem tracker.
    EXPECT_EQ(t->consumption(), 0);

    thread_context->thread_mem_tracker_mgr->consume(size2);
    // size1 + size2 > onfig::mem_tracker_consume_min_size_bytes, consume mem tracker.
    EXPECT_EQ(t->consumption(), size1 + size2);

    thread_context->thread_mem_tracker_mgr->consume(-size1);
    // std::abs(-size1) < config::mem_tracker_consume_min_size_bytes, not consume mem tracker.
    EXPECT_EQ(t->consumption(), size1 + size2);

    thread_context->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_EQ(t->consumption(), size2);

    thread_context->thread_mem_tracker_mgr->consume(-size2);
    // std::abs(-size2) > onfig::mem_tracker_consume_min_size_bytes, consume mem tracker.
    EXPECT_EQ(t->consumption(), 0);

    thread_context->thread_mem_tracker_mgr->consume(-size2);
    EXPECT_EQ(t->consumption(), -size2);

    thread_context->thread_mem_tracker_mgr->consume(-size1);
    EXPECT_EQ(t->consumption(), -size2);

    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size2 * 2);
    thread_context->thread_mem_tracker_mgr->consume(size2 * 10);
    thread_context->thread_mem_tracker_mgr->consume(size2 * 100);
    thread_context->thread_mem_tracker_mgr->consume(size2 * 1000);
    thread_context->thread_mem_tracker_mgr->consume(size2 * 10000);
    thread_context->thread_mem_tracker_mgr->consume(-size2 * 2);
    thread_context->thread_mem_tracker_mgr->consume(-size2 * 10);
    thread_context->thread_mem_tracker_mgr->consume(-size2 * 100);
    thread_context->thread_mem_tracker_mgr->consume(-size2 * 1000);
    thread_context->thread_mem_tracker_mgr->consume(-size2 * 10000);
    thread_context->detach_task();
    EXPECT_EQ(t->consumption(), 0); // detach automatic call flush_untracked_mem.
}

TEST_F(ThreadMemTrackerMgrTest, Boundary) {
    // TODO, Boundary check may not be necessary, add some `IF` maybe increase cost time.
}

TEST_F(ThreadMemTrackerMgrTest, NestedSwitchMemTracker) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t1 = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedSwitchMemTracker1");
    std::shared_ptr<MemTrackerLimiter> t2 = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedSwitchMemTracker2");
    std::shared_ptr<MemTrackerLimiter> t3 = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedSwitchMemTracker3");
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t1);

    int64_t size1 = 4 * 1024;
    int64_t size2 = 4 * 1024 * 1024;

    thread_context->attach_task(rc);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t1->consumption(), size1 + size2);

    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->attach_limiter_tracker(t2);
    EXPECT_EQ(t1->consumption(),
              size1 + size2 + size1); // attach automatic call flush_untracked_mem.

    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1); // not changed, now consume t2
    EXPECT_EQ(t2->consumption(), size1 + size2);

    thread_context->thread_mem_tracker_mgr->detach_limiter_tracker(); // detach
    EXPECT_EQ(t2->consumption(),
              size1 + size2 + size1); // detach automatic call flush_untracked_mem.

    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2);
    EXPECT_EQ(t2->consumption(), size1 + size2 + size1); // not changed, now consume t1

    thread_context->thread_mem_tracker_mgr->attach_limiter_tracker(t2);
    thread_context->thread_mem_tracker_mgr->consume(-size1);
    thread_context->thread_mem_tracker_mgr->attach_limiter_tracker(t3);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2);
    EXPECT_EQ(t2->consumption(), size1 + size2); // attach automatic call flush_untracked_mem.
    EXPECT_EQ(t3->consumption(), size1 + size2);

    thread_context->thread_mem_tracker_mgr->consume(-size1);
    thread_context->thread_mem_tracker_mgr->consume(-size2);
    thread_context->thread_mem_tracker_mgr->consume(-size1);
    EXPECT_EQ(t3->consumption(), size1);

    thread_context->thread_mem_tracker_mgr->detach_limiter_tracker(); // detach
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2);
    EXPECT_EQ(t2->consumption(), size1 + size2);
    EXPECT_EQ(t3->consumption(), 0);

    thread_context->thread_mem_tracker_mgr->consume(-size1);
    thread_context->thread_mem_tracker_mgr->consume(-size2);
    thread_context->thread_mem_tracker_mgr->consume(-size1);
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2);
    EXPECT_EQ(t2->consumption(), 0);

    thread_context->thread_mem_tracker_mgr->detach_limiter_tracker(); // detach
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2);
    EXPECT_EQ(t2->consumption(), -size1);

    thread_context->thread_mem_tracker_mgr->consume(-t1->consumption());
    thread_context->detach_task(); // detach t1
    EXPECT_EQ(t1->consumption(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, MultiMemTracker) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t1 =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "UT-MultiMemTracker1");
    std::shared_ptr<MemTracker> t2 = std::make_shared<MemTracker>("UT-MultiMemTracker2");
    std::shared_ptr<MemTracker> t3 = std::make_shared<MemTracker>("UT-MultiMemTracker3");
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t1);

    int64_t size1 = 4 * 1024;
    int64_t size2 = 4 * 1024 * 1024;

    thread_context->attach_task(rc);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    EXPECT_EQ(t1->consumption(), size1 + size2);

    bool rt = thread_context->thread_mem_tracker_mgr->push_consumer_tracker(t2.get());
    EXPECT_EQ(rt, true);
    EXPECT_EQ(t1->consumption(), size1 + size2); // _untracked_mem = size1
    EXPECT_EQ(t2->consumption(), 0);

    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2);
    EXPECT_EQ(t2->consumption(), size2);

    rt = thread_context->thread_mem_tracker_mgr->push_consumer_tracker(t2.get());
    EXPECT_EQ(rt, false);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2);
    EXPECT_EQ(t2->consumption(), size2 + size2);

    rt = thread_context->thread_mem_tracker_mgr->push_consumer_tracker(t3.get());
    EXPECT_EQ(rt, true);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(-size1); // _untracked_mem = -size1
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2 + size1 + size2);
    EXPECT_EQ(t2->consumption(), size2 + size2 + size2);
    EXPECT_EQ(t3->consumption(), size2);

    thread_context->thread_mem_tracker_mgr->pop_consumer_tracker();
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2 + size1 + size2);
    EXPECT_EQ(t2->consumption(), size2 + size2 + size2);
    EXPECT_EQ(t3->consumption(), size2);

    thread_context->thread_mem_tracker_mgr->consume(-size2);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(-size2);
    thread_context->thread_mem_tracker_mgr->pop_consumer_tracker();
    EXPECT_EQ(t1->consumption(),
              size1 + size2 + size1 + size2 + size2 + size1 + size2 - size1 - size2);
    EXPECT_EQ(t2->consumption(), size2 + size2);
    EXPECT_EQ(t3->consumption(), size2);

    thread_context->thread_mem_tracker_mgr->consume(-t1->consumption());
    thread_context->detach_task(); // detach t1
    EXPECT_EQ(t1->consumption(), 0);
    EXPECT_EQ(t2->consumption(), size2 + size2);
    EXPECT_EQ(t3->consumption(), size2);
}

TEST_F(ThreadMemTrackerMgrTest, ReserveMemory) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "UT-ReserveMemory");
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t);

    int64_t size1 = 4 * 1024;
    int64_t size2 = 4 * 1024 * 1024;
    int64_t size3 = size2 * 2;

    thread_context->attach_task(rc);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t->consumption(), size1 + size2);

    auto st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3);

    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(-size2);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->consume(-size1);
    thread_context->thread_mem_tracker_mgr->consume(-size1);
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    // std::abs(-size1 - size1) < SYNC_PROC_RESERVED_INTERVAL_BYTES, not update process_reserved_memory.
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size1 + size1);

    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    // reserved memory used done
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    // no reserved memory, normal memory consumption
    EXPECT_EQ(t->consumption(), size1 + size2 + size3 + size1 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

    thread_context->thread_mem_tracker_mgr->consume(-size3);
    thread_context->thread_mem_tracker_mgr->consume(-size1);
    thread_context->thread_mem_tracker_mgr->consume(-size2);
    EXPECT_EQ(t->consumption(), size1 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

    st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3);

    thread_context->thread_mem_tracker_mgr->consume(-size1);
    // ThreadMemTrackerMgr _reserved_mem = size3 + size1
    // ThreadMemTrackerMgr _untracked_mem = -size1
    thread_context->thread_mem_tracker_mgr->consume(size3);
    // ThreadMemTrackerMgr _reserved_mem = size1
    // ThreadMemTrackerMgr _untracked_mem = -size1 + size3
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(),
              size1); // size3 + size1 - size3

    thread_context->thread_mem_tracker_mgr->consume(-size3);
    // ThreadMemTrackerMgr _reserved_mem = size1 + size3
    // ThreadMemTrackerMgr _untracked_mem = 0, std::abs(-size3) > SYNC_PROC_RESERVED_INTERVAL_BYTES,
    // so update process_reserved_memory.
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size1 + size3);

    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    // ThreadMemTrackerMgr _reserved_mem = size1 + size3 - size1 - size2 - size1 = size3 - size2 - size1
    // ThreadMemTrackerMgr _untracked_mem = size1
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    // size1 + size3 - (size1 + size2)
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->shrink_reserved();
    // size1 + size2 + size3 - _reserved_mem, size1 + size2 + size3 - (size3 - size2 - size1)
    EXPECT_EQ(t->consumption(), size1 + size2 + size1 + size2);
    // size3 - size2 - (_reserved_mem + _untracked_mem) = 0, size3 - size2 - ((size3 - size2 - size1) + (size1)) = 0
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

    thread_context->detach_task();
    EXPECT_EQ(t->consumption(), size1 + size2 + size1 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, NestedReserveMemory) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedReserveMemory");
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t);

    int64_t size2 = 4 * 1024 * 1024;
    int64_t size3 = size2 * 2;

    thread_context->attach_task(rc);
    auto st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(t->consumption(), size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3);

    thread_context->thread_mem_tracker_mgr->consume(size2);
    // ThreadMemTrackerMgr _reserved_mem = size3 - size2
    // ThreadMemTrackerMgr _untracked_mem = 0, size2 > SYNC_PROC_RESERVED_INTERVAL_BYTES,
    // update process_reserved_memory.
    EXPECT_EQ(t->consumption(), size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    st = thread_context->thread_mem_tracker_mgr->try_reserve(size2);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // ThreadMemTrackerMgr _reserved_mem = size3 - size2 + size2
    // ThreadMemTrackerMgr _untracked_mem = 0
    EXPECT_EQ(t->consumption(), size3 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(),
              size3); // size3 - size2 + size2

    st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    thread_context->thread_mem_tracker_mgr->consume(size3);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size3);
    // ThreadMemTrackerMgr _reserved_mem = size3 - size2
    // ThreadMemTrackerMgr _untracked_mem = 0
    EXPECT_EQ(t->consumption(), size3 + size2 + size3 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->shrink_reserved();
    // size3 + size2 + size3 + size3 - _reserved_mem, size3 + size2 + size3 + size3 - (size3 - size2)
    EXPECT_EQ(t->consumption(), size3 + size2 + size3 + size2);
    // size3 - size2 - (_reserved_mem + _untracked_mem) = 0, size3 - size2 - ((size3 - size2 - size1) + (size1)) = 0
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

    thread_context->detach_task();
    EXPECT_EQ(t->consumption(), size3 + size2 + size3 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, NestedSwitchMemTrackerReserveMemory) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t1 = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedSwitchMemTrackerReserveMemory1");
    std::shared_ptr<MemTrackerLimiter> t2 = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedSwitchMemTrackerReserveMemory2");
    std::shared_ptr<MemTrackerLimiter> t3 = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedSwitchMemTrackerReserveMemory3");
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t1);

    int64_t size1 = 4 * 1024;
    int64_t size2 = 4 * 1024 * 1024;
    int64_t size3 = size2 * 2;

    thread_context->attach_task(rc);
    auto st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t1->consumption(), size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->attach_limiter_tracker(t2);
    st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(t1->consumption(), size3);
    EXPECT_EQ(t2->consumption(), size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2 + size3);

    thread_context->thread_mem_tracker_mgr->consume(size2 + size3); // reserved memory used done
    EXPECT_EQ(t1->consumption(), size3);
    EXPECT_EQ(t2->consumption(), size3 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->attach_limiter_tracker(t3);
    st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(t1->consumption(), size3);
    EXPECT_EQ(t2->consumption(), size3 + size2);
    EXPECT_EQ(t3->consumption(), size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2 + size3);

    thread_context->thread_mem_tracker_mgr->consume(-size2);
    thread_context->thread_mem_tracker_mgr->consume(-size1);
    // ThreadMemTrackerMgr _reserved_mem = size3 + size2 + size1
    // ThreadMemTrackerMgr _untracked_mem = -size1
    EXPECT_EQ(t1->consumption(), size3);
    EXPECT_EQ(t2->consumption(), size3 + size2);
    EXPECT_EQ(t3->consumption(), size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(),
              size3 - size2 + size3 + size2);

    thread_context->thread_mem_tracker_mgr->detach_limiter_tracker(); // detach
    EXPECT_EQ(t1->consumption(), size3);
    EXPECT_EQ(t2->consumption(), size3 + size2);
    EXPECT_EQ(t3->consumption(), -size1 - size2); // size3 - _reserved_mem
    //  size3 - size2 + size3 + size2 - (_reserved_mem + _untracked_mem)
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->detach_limiter_tracker(); // detach
    EXPECT_EQ(t1->consumption(), size3);
    // not changed, reserved memory used done.
    EXPECT_EQ(t2->consumption(), size3 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->detach_task();
    EXPECT_EQ(t1->consumption(), size2); // size3 - _reserved_mem
    // size3 - size2 - (_reserved_mem + _untracked_mem)
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, ReserveMemoryFailed) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-ReserveMemory", 1024);
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t);
    {
        WorkloadGroupInfo wg_info {.id = 1,
                                   .memory_limit = 2048,
                                   .enable_memory_overcommit = false,
                                   .memory_high_watermark = 100};
        auto wg = _wg_manager->get_or_create_workload_group(wg_info);
        rc->set_workload_group(wg);
        thread_context->attach_task(rc);

        EXPECT_EQ(t->consumption(), 0);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);

        auto st = thread_context->thread_mem_tracker_mgr->try_reserve(1024);
        EXPECT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(t->consumption(), 1024);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 1024);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 1024);

        st = thread_context->thread_mem_tracker_mgr->try_reserve(1024);
        EXPECT_EQ(st.code(), ErrorCode::QUERY_MEMORY_EXCEEDED) << st.to_string();
        EXPECT_EQ(t->consumption(), 1024);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 1024);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 1024);

        t->set_limit(1024L * 1024);
        st = thread_context->thread_mem_tracker_mgr->try_reserve(1024);
        EXPECT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(t->consumption(), 2048);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 2048);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 2048);

        st = thread_context->thread_mem_tracker_mgr->try_reserve(1024);
        EXPECT_EQ(st.code(), ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED) << st.to_string();
        EXPECT_EQ(t->consumption(), 2048);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 2048);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 2048);

        thread_context->thread_mem_tracker_mgr->shrink_reserved();
        EXPECT_EQ(t->consumption(), 0);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

        thread_context->detach_task();
        EXPECT_EQ(t->consumption(), 0);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);
    }

    {
        WorkloadGroupInfo wg_info {.id = 2, .memory_limit = 1024, .enable_memory_overcommit = true};
        auto wg = _wg_manager->get_or_create_workload_group(wg_info);
        rc->set_workload_group(wg);
        thread_context->attach_task(rc);

        auto st = thread_context->thread_mem_tracker_mgr->try_reserve(1024L * 1024);
        EXPECT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(t->consumption(), 1024L * 1024);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 1024L * 1024);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 1024L * 1024);

        st = thread_context->thread_mem_tracker_mgr->try_reserve(1024);
        EXPECT_EQ(st.code(), ErrorCode::QUERY_MEMORY_EXCEEDED) << st.to_string();
        EXPECT_EQ(t->consumption(), 1024L * 1024);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 1024L * 1024);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 1024L * 1024);

        t->set_limit(-1);
        st = thread_context->thread_mem_tracker_mgr->try_reserve(1024L * 1024 * 1024);
        EXPECT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(t->consumption(), 1024L * 1024 + 1024L * 1024 * 1024);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 1024L * 1024 + 1024L * 1024 * 1024);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(),
                  1024L * 1024 + 1024L * 1024 * 1024);

        thread_context->thread_mem_tracker_mgr->shrink_reserved();
        EXPECT_EQ(t->consumption(), 0);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

        thread_context->detach_task();
        EXPECT_EQ(t->consumption(), 0);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);
    }
}

} // end namespace doris
