//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/type_transformer.hpp
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

namespace duckdb {
//! Transforms Logical DuckDB Plans to Substrait Relations
class TypeTransformer {
public:
	static ::substrait::Type Wololo(const LogicalType &type, idx_t *max_string_length = nullptr,
	                                BaseStatistics *column_statistics = nullptr, bool not_null = true);
};
} // namespace duckdb