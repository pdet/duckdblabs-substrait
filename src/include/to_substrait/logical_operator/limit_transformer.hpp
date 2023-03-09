//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/logical_operator/limit_transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "to_substrait/logical_operator/operator_transformer.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

#include <string>
#include <unordered_map>

namespace duckdb {
//! Transforms Limit Logical Operator from DuckDB to Fetch Relation of Substrait
class LimitTransformer : public OperatorTransformer {
public:
	explicit LimitTransformer(LogicalOperator &op, PlanTransformer &plan_p);

	void Wololo() override;

private:
	LogicalLimit &logical_limit;
};
} // namespace duckdb
