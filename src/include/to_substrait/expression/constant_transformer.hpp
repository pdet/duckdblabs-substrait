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
	// TODO: this can create the substrait expression internally and just return the pointer, gets cleaner imo
	ConstantTransformer(Value &dval, substrait::Expression &sexpr);

	//! DuckDB Constant Value
	Value &dval;
	//! Substrait expression of a constant value
	substrait::Expression &sexpr;

	//! Perform the actual conversion
	substrait::Expression *Wololo();

private:
	//! Methods to transform DuckDBConstants to Substrait Expressions
	void TransformInteger();
	void TransformDouble();
	void TransformBigInt();
	void TransformDate();
	void TransformVarchar();
	void TransformBoolean();
	void TransformDecimal();
	void TransformHugeInt();
	void TransformSmallInt();
	void TransformFloat();
	void TransformTime();
	void TransformInterval();
	void TransformTimestamp();
	void TransformEnum();

	//! Gets bytes of the value
	string GetRawValue(hugeint_t value);
};
} // namespace duckdb