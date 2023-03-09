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
#include "to_substrait/type_transformer.hpp"
// #include "to_substrait/expression/conjunction_factory.hpp"

namespace duckdb {
//! Transforms Logical DuckDB Plans to Substrait Relations
class ConjunctionFactory;

class PlanTransformer {
public:
	PlanTransformer(ClientContext &context_p, LogicalOperator &root_op);

	uint64_t RegisterFunction(const string &name);

	//! Remapped DuckDB functions names to Substrait compatible function names
	static const std::unordered_map<std::string, std::string> function_names_remap;

	ClientContext &GetContext();

	unique_ptr<ConjunctionFactory> conjunction_factory;

private:
	//! Variables used to register functions
	std::unordered_map<std::string, uint64_t> functions_map;

	uint64_t last_function_id = 1;

	//! The substrait Plan
	substrait::Plan plan;
	ClientContext &context;

	uint64_t max_string_length = 1;

	//! In this function, we start to traverse the DuckDB Plan and transform each logical node into a substrait
	//! relation.
	static substrait::RelRoot *TransformRoot(const LogicalOperator &dop);

	//! In addition, we must identify the node closest to the root that containt the aliases
	//!  Since these must be stored in the Substait's root relation.
	static void FillRootAliases(const LogicalOperator &duck_root, substrait::RelRoot *substrait_root);
};
} // namespace duckdb