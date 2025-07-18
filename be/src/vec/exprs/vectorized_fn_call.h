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
#include <gen_cpp/Types_types.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "udf/udf.h"
#include "vec/core/column_numbers.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/function.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class Block;
class VExprContext;

class VectorizedFnCall : public VExpr {
    ENABLE_FACTORY_CREATOR(VectorizedFnCall);

public:
#ifdef BE_TEST
    VectorizedFnCall() = default;
#endif
    VectorizedFnCall(const TExprNode& node);
    Status execute(VExprContext* context, Block* block, int* result_column_id) override;
    Status execute_runtime_filter(doris::vectorized::VExprContext* context,
                                  doris::vectorized::Block* block, int* result_column_id,
                                  ColumnNumbers& args) override;
    Status evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) override;
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override;
    const std::string& expr_name() const override;
    std::string debug_string() const override;
    bool is_constant() const override {
        if (!_function->is_use_default_implementation_for_constants() ||
            // udf function with no argument, can't sure it's must return const column
            (_fn.binary_type == TFunctionBinaryType::JAVA_UDF && _children.empty())) {
            return false;
        }
        return VExpr::is_constant();
    }
    static std::string debug_string(const std::vector<VectorizedFnCall*>& exprs);

    bool can_push_down_to_index() const override;
    bool equals(const VExpr& other) override;

    size_t estimate_memory(const size_t rows) override;

protected:
    FunctionBasePtr _function;
    std::string _expr_name;
    std::string _function_name;

private:
    Status _do_execute(doris::vectorized::VExprContext* context, doris::vectorized::Block* block,
                       int* result_column_id, ColumnNumbers& args);
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
