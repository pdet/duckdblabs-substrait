//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/logical_operator/projection_transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "to_substrait/logical_operator/operator_transformer.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <string>
#include <unordered_map>

namespace duckdb {
//! Transforms Projection Logical Operator from DuckDB to Project Relation of Substrait
class ProjectionTransformer : public OperatorTransformer {
public:
	explicit ProjectionTransformer(LogicalOperator &op, PlanTransformer &plan_p);

	void Wololo() override;

private:
	LogicalProjection &logical_projection;
};
} // namespace duckdb
