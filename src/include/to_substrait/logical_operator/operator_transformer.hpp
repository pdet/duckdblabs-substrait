//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/logical_operator/operator_transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "substrait/algebra.pb.h"
#include <string>
#include <unordered_map>
#include "substrait/plan.pb.h"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function.hpp"
#include "to_substrait/plan_transformer.hpp"
#include "to_substrait/type_transformer.hpp"

namespace duckdb {
//! Base class that transforms Logical DuckDB Plans to Substrait Relations
class OperatorTransformer {
public:
	explicit OperatorTransformer(LogicalOperator &input_p, PlanTransformer &plan_p)
	    : input(input_p), plan_transformer(plan_p) {};

	//! Converts from input to result
	virtual void Wololo() = 0;

	//! Flat representation table column ids of substrait - interesting choice.
	vector<idx_t> reference_ids;
	//! Maps DuckDB operator emit to reference id
	unordered_map<idx_t, idx_t> emit_map;
	//! Resulting substrait relation
	substrait::Rel *result = nullptr;
	//! Original DuckDB Logical Operator
	LogicalOperator &input;
	//! Original DuckDB Logical Operator
	PlanTransformer &plan_transformer;

private:
	static void AllocateFunctionArgument(substrait::Expression_ScalarFunction *scalar_fun,
	                                     substrait::Expression *value);
};

} // namespace duckdb
