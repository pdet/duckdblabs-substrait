//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/plan_transformer.hpp
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
class PlanTransformer {
public:
	PlanTransformer(ClientContext &context_p, LogicalOperator &root_op);

private:
	//! Variables used to register functions
	std::unordered_map<std::string, uint64_t> functions_map;
	//! Remapped DuckDB functions names to Substrait compatible function names
	static const unordered_map<std::string, std::string> function_names_remap;

	uint64_t last_function_id = 1;

	//! The substrait Plan
	substrait::Plan plan;
	ClientContext &context;

	uint64_t max_string_length = 1;
};
} // namespace duckdb