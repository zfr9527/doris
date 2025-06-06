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

package org.apache.doris.nereids;

/**
 * optimize plan process has some phase, such as analyze, rewrite, optimize, generate physical plan
 * and so on, this hook give a chance to do something in the planning process.
 * For example: after analyze plan when query or explain, we should generate materialization context.
 */
public interface PlannerHook {

    /**
     * the hook before analyze
     */
    default void beforeAnalyze(NereidsPlanner planner) {
    }

    /**
     * the hook after analyze
     */
    default void afterAnalyze(NereidsPlanner planner) {
    }

    /**
     * the hook before rewrite
     */
    default void beforeRewrite(NereidsPlanner planner) {
    }

    /**
     * the hook after rewrite
     */
    default void afterRewrite(NereidsPlanner planner) {
    }
}
