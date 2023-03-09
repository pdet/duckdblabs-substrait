//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/logical_operator/top_n_transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "to_substrait/logical_operator/operator_transformer.hpp"

#include <string>
#include <unordered_map>

namespace duckdb {
//! Transforms TopN Logical Operator from DuckDB to Fetch Relation of Substrait
class TopNTransformer : public OperatorTransformer {
public:
	explicit TopNTransformer(LogicalOperator &op, PlanTransformer &plan_p);

	void Wololo() override;

private:
	LogicalTopN &logical_top_n;
};
} // namespace duckdb
