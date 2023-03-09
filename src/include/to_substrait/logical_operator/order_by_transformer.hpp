//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/logical_operator/order_by_transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "to_substrait/logical_operator/operator_transformer.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

#include <string>
#include <unordered_map>

namespace duckdb {
//! Transforms Order By Logical Operator from DuckDB to Sort Relation of Substrait
class OrderByTransformer : public OperatorTransformer {
public:
	explicit OrderByTransformer(LogicalOperator &op, PlanTransformer &plan_p);

	void Wololo() override;

private:
	LogicalOrder &logical_order;
};
} // namespace duckdb
