//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/logical_operator/get.hpp
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

#include "duckdb/planner/operator/logical_get.hpp"
namespace duckdb {
//! Base class that transforms Logical DuckDB Plans to Substrait Relations
class LogicalToRelation {
public:
	explicit LogicalToRelation(LogicalOperator &input_p) : input(input_p) {};
	//! Flat representation table column ids of substrait - interesting choice.
	vector<idx_t> reference_ids;
	//! Maps DuckDB operator emit to reference id
	unordered_map<idx_t, idx_t> emit_map;
	//! Resulting substrait relation
	substrait::Rel *result = nullptr;
	//! Original DuckDB Logical Operator
	LogicalOperator &input;

	//! Converts from input to result
	virtual void Wololo() = 0;
};
//! Transforms Get Logical Operator from DuckDB to Read Relation of Substrait
class GetToRead : LogicalToRelation {
public:
	explicit GetToRead(LogicalOperator &op, ClientContext &context_p)
	    : LogicalToRelation(op), context(context_p), dget((LogicalGet &)op) {
		D_ASSERT(op.type == LogicalOperatorType::LOGICAL_GET);
		result = new substrait::Rel();
	};
	void Wololo() override;

private:
	ClientContext &context;
	LogicalGet &dget;
	substrait::ReadRel *sget;
	BindInfo *bind_info;
	FunctionData *bind_data;
	//! Internal function that transforms a Table Scan to a ReadRel
	void TransformTableScan();
	//! Internal function that transforms a Parquet Scan to a ReadRel
	void TransformParquetScan();
};
} // namespace duckdb
