//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait.hpp
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

namespace duckdb {
class DuckDBToSubstrait {
public:
	explicit DuckDBToSubstrait(ClientContext &context, duckdb::LogicalOperator &dop) : context(context) {
		TransformPlan(dop);
	};

	~DuckDBToSubstrait() {
		plan.Clear();
	}
	//! Serializes the substrait plan to a string
	string SerializeToString();
	string SerializeToJson();
	//! Transforms a DuckDB Logical Type into a Substrait Type
	static ::substrait::Type DuckToSubstraitType(const LogicalType &type, BaseStatistics *column_statistics = nullptr,
	                                             bool not_null = false);
	//! Creates a Conjunction
	template <typename T, typename FUNC>
	substrait::Expression *CreateConjunction(T &source, FUNC f) {
		substrait::Expression *res = nullptr;
		for (auto &ele : source) {
			auto child_expression = f(ele);
			if (!res) {
				res = child_expression;
			} else {
				auto temp_expr = new substrait::Expression();
				auto scalar_fun = temp_expr->mutable_scalar_function();
				scalar_fun->set_function_reference(RegisterFunction("and"));
				LogicalType boolean_type(LogicalTypeId::BOOLEAN);
				*scalar_fun->mutable_output_type() = DuckToSubstraitType(boolean_type);
				AllocateFunctionArgument(scalar_fun, res);
				AllocateFunctionArgument(scalar_fun, child_expression);
				res = temp_expr;
			}
		}
		return res;
	}

	//! Methods to Transform Logical Operators to Substrait Relations
	substrait::Rel *TransformOp(duckdb::LogicalOperator &dop);
	static substrait::Rel *TransformFilter(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformProjection(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformTopN(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformLimit(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformOrderBy(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformComparisonJoin(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformAggregateGroup(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformGet(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformCrossProduct(duckdb::LogicalOperator &dop);

private:
	//! Transform DuckDB Plan to Substrait Plan
	void TransformPlan(duckdb::LogicalOperator &dop);
	//! Registers a function
	uint64_t RegisterFunction(const std::string &name);
	//! Creates a reference to a table column
	void CreateFieldRef(substrait::Expression *expr, uint64_t col_idx);

	//! Transforms Relation Root
	substrait::RelRoot *TransformRootOp(LogicalOperator &dop);

	//! Methods to transform different LogicalGet Types (e.g., Table, Parquet)
	//! To Substrait;
	void TransformTableScanToSubstrait(LogicalGet &dget, substrait::ReadRel *sget);
	void TransformParquetScanToSubstrait(LogicalGet &dget, substrait::ReadRel *sget, BindInfo &bind_info,
	                                     FunctionData &bind_data);

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

	//! Methods to transform a DuckDB Expression to a Substrait Expression
	void TransformExpr(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset = 0);
	void TransformBoundRefExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformCastExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformFunctionExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformConstantExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr);
	void TransformComparisonExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr);
	void TransformConjunctionExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformNotNullExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformIsNullExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformNotExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformCaseExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr);
	void TransformInExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr);

	//! Methods to transform DuckDB Filters to Substrait Expression
	substrait::Expression *TransformFilter(uint64_t col_idx, duckdb::TableFilter &dfilter, LogicalType &return_type);
	substrait::Expression *TransformIsNotNullFilter(uint64_t col_idx, duckdb::TableFilter &dfilter,
	                                                LogicalType &return_type);
	substrait::Expression *TransformConjuctionAndFilter(uint64_t col_idx, duckdb::TableFilter &dfilter,
	                                                    LogicalType &return_type);
	substrait::Expression *TransformConstantComparisonFilter(uint64_t col_idx, duckdb::TableFilter &dfilter,
	                                                         LogicalType &return_type);

	//! Transforms DuckDB Join Conditions to Substrait Expression
	substrait::Expression *TransformJoinCond(duckdb::JoinCondition &dcond, uint64_t left_ncol);
	//! Transforms DuckDB Sort Order to Substrait Sort Order
	void TransformOrder(duckdb::BoundOrderByNode &dordf, substrait::SortField &sordf);

	void AllocateFunctionArgument(substrait::Expression_ScalarFunction *scalar_fun, substrait::Expression *value);
	std::string &RemapFunctionName(std::string &function_name);

	//! Variables used to register functions
	std::unordered_map<std::string, uint64_t> functions_map;
	//! Remapped DuckDB functions names to Substrait compatible function names
	static const unordered_map<std::string, std::string> function_names_remap;
	uint64_t last_function_id = 1;

	//! The substrait Plan
	substrait::Plan plan;
	ClientContext &context;

	uint64_t max_string_length = 1;
};
} // namespace duckdb
