//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/logical_operator/cross_product_transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "to_substrait/logical_operator/operator_transformer.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"

#include <string>
#include <unordered_map>

namespace duckdb {
//! Transforms Cross Product Logical Operator from DuckDB to Fetch Relation of Substrait
class CrossProductTransformer : public OperatorTransformer {
public:
	explicit CrossProductTransformer(LogicalOperator &op, PlanTransformer &plan_p);

	void Wololo() override;

private:
	LogicalCrossProduct &logical_cross_product;
};
} // namespace duckdb
