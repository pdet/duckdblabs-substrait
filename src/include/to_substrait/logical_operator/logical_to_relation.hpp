//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/logical_operator/logical_to_relation.hpp
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

namespace duckdb {
//! Base class that transforms Logical DuckDB Plans to Substrait Relations
class LogicalToRelation {
public:
	explicit LogicalToRelation(LogicalOperator &input_p) : input(input_p) {};

	//! Converts from input to result
	virtual void Wololo() = 0;

	//! Creates a Conjunction
	template <typename T, typename FUNC>
	substrait::Expression *CreateConjunction(T &source, FUNC f) {
		substrait::Expression *res = nullptr;
		for (auto &ele : source) {
			auto child_expression = f(ele);
			if (!res) {
				res = child_expression;
			} else {
				auto temp_expr = new substrait::Expression();
				auto scalar_fun = temp_expr->mutable_scalar_function();
				scalar_fun->set_function_reference(RegisterFunction("and"));
				LogicalType boolean_type(LogicalTypeId::BOOLEAN);
				*scalar_fun->mutable_output_type() = DuckToSubstraitType(boolean_type);
				AllocateFunctionArgument(scalar_fun, res);
				AllocateFunctionArgument(scalar_fun, child_expression);
				res = temp_expr;
			}
		}
		return res;
	}

	//! Flat representation table column ids of substrait - interesting choice.
	vector<idx_t> reference_ids;
	//! Maps DuckDB operator emit to reference id
	unordered_map<idx_t, idx_t> emit_map;
	//! Resulting substrait relation
	substrait::Rel *result = nullptr;
	//! Original DuckDB Logical Operator
	LogicalOperator &input;
};

} // namespace duckdb
