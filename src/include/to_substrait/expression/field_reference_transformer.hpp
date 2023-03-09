//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/expression/field_reference_transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "to_substrait/expression/expression_transformer.hpp"

#include <substrait/algebra.pb.h>

namespace duckdb {
//! Transforms A DuckDB Filter Expression to a Substrait Filter Expression
class FieldReferenceTransformer : ExpressionTransformer {
public:
	explicit FieldReferenceTransformer(substrait::Expression *expr, uint64_t col_idx);

	//! Perform the actual conversion
	substrait::Expression *Wololo() override;

private:
	const uint64_t col_idx;
	substrait::Expression *expr;
};
} // namespace duckdb
