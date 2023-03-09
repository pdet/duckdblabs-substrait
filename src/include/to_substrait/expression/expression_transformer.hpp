//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/expression/expression_transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {
//! Transforms Logical DuckDB Plans to Substrait Relations
class ExpressionTransformer {
public:
	ExpressionTransformer() = default;

	virtual substrait::Expression *Wololo() = 0;
};
} // namespace duckdb