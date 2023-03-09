//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/logical_operator/get_transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "to_substrait/logical_operator/operator_transformer.hpp"

#include <string>
#include <unordered_map>

namespace duckdb {
//! Transforms Get Logical Operator from DuckDB to Read Relation of Substrait
class GetTransformer : public OperatorTransformer {
public:
	explicit GetTransformer(LogicalOperator &op, PlanTransformer &plan_p);

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
