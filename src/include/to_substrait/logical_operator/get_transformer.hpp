//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/logical_operator/get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "to_substrait/logical_operator/operator_transformer.hpp"

#include <string>
#include <unordered_map>

namespace duckdb {
//! Transforms Get Logical Operator from DuckDB to Read Relation of Substrait
class GetTransformer : OperatorTransformer {
public:
	explicit GetTransformer(LogicalOperator &op, PlanTransformer &plan_p)
	    : OperatorTransformer(op, plan_p), dget((LogicalGet &)op) {
		D_ASSERT(op.type == LogicalOperatorType::LOGICAL_GET);
		result = new substrait::Rel();
	};
	void Wololo() override;

private:
	LogicalGet &dget;
	substrait::ReadRel *sget = nullptr;
	BindInfo *bind_info = nullptr;
	FunctionData *bind_data = nullptr;

	//! Methods to transform different LogicalGet Types (e.g., Table, Parquet)
	//! To Substrait;
	//! Internal function that transforms a Table Scan to a ReadRel
	void TransformTableScan();
	//! Internal function that transforms a Parquet Scan to a ReadRel
	void TransformParquetScan();
};
} // namespace duckdb
