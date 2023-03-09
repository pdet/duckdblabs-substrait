//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/expression/filter_transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "to_substrait/logical_operator/operator_transformer.hpp"
#include "to_substrait/expression/expression_transformer.hpp"
#include <string>
#include <unordered_map>

namespace duckdb {
//! Transforms A DuckDB Filter Expression to a Substrait Filter Expression
class FilterTransformer : ExpressionTransformer {
public:
	explicit FilterTransformer(uint64_t col_idx, TableFilter &dfilter, const LogicalType &return_type,
	                           PlanTransformer &plan_p);

	//! Perform the actual conversion
	substrait::Expression *Wololo() override;

private:
	const uint64_t col_idx;
	const TableFilter &dfilter;
	const LogicalType &return_type;
	PlanTransformer &plan;
	substrait::Expression *filter;

	//! Transforms Is Not Null
	void TransformIsNotNullFilter();
	//! Transforms Conjunction And
	void TransformConjunctionAndFilter();
	//! Transforms Comparisons with Constants
	void TransformConstantComparisonFilter();
};
} // namespace duckdb
