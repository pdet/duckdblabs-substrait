//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/expression/constant_transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "to_substrait/logical_operator/operator_transformer.hpp"

#include <string>
#include <unordered_map>
namespace duckdb {
//! Transforms Logical DuckDB Plans to Substrait Relations
class ConstantTransformer {
public:
	static ::substrait::Type Wololo(const LogicalType &type, idx_t *max_string_length = nullptr,
	                                BaseStatistics *column_statistics = nullptr, bool not_null = true);

	//! Methods to transform DuckDBConstants to Substrait Expressions
	void TransformConstant(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformInteger(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformDouble(Value &dval, substrait::Expression &sexpr);
	void TransformBigInt(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformDate(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformVarchar(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformBoolean(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformDecimal(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformHugeInt(Value &dval, substrait::Expression &sexpr);
	void TransformSmallInt(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformFloat(Value &dval, substrait::Expression &sexpr);
	void TransformTime(Value &dval, substrait::Expression &sexpr);
	void TransformInterval(Value &dval, substrait::Expression &sexpr);
	void TransformTimestamp(Value &dval, substrait::Expression &sexpr);
	void TransformEnum(duckdb::Value &dval, substrait::Expression &sexpr);
};
} // namespace duckdb