#include "to_substrait.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "google/protobuf/util/json_util.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "to_substrait/logical_operator/get_transformer.hpp"

namespace duckdb {

std::string &DuckDBToSubstrait::RemapFunctionName(std::string &function_name) {
	auto it = function_names_remap.find(function_name);
	if (it != function_names_remap.end()) {
		function_name = it->second;
	}
	return function_name;
}

string DuckDBToSubstrait::SerializeToString() {
	string serialized;
	if (!plan.SerializeToString(&serialized)) {
		throw InternalException("It was not possible to serialize the substrait plan");
	}
	return serialized;
}

string DuckDBToSubstrait::SerializeToJson() {
	string serialized;
	auto success = google::protobuf::util::MessageToJsonString(plan, &serialized);
	if (!success.ok()) {
		throw InternalException("It was not possible to serialize the substrait plan");
	}
	return serialized;
}

void DuckDBToSubstrait::AllocateFunctionArgument(substrait::Expression_ScalarFunction *scalar_fun,
                                                 substrait::Expression *value) {
	auto function_argument = new substrait::FunctionArgument();
	function_argument->set_allocated_value(value);
	scalar_fun->mutable_arguments()->AddAllocated(function_argument);
}

void DuckDBToSubstrait::TransformBoundRefExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                    uint64_t col_offset) {
	auto &dref = (BoundReferenceExpression &)dexpr;
	CreateFieldRef(&sexpr, dref.index + col_offset);
}

void DuckDBToSubstrait::TransformCastExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	auto &dcast = (BoundCastExpression &)dexpr;
	auto scast = sexpr.mutable_cast();
	TransformExpr(*dcast.child, *scast->mutable_input(), col_offset);
	*scast->mutable_type() = DuckToSubstraitType(dcast.return_type);
}

void DuckDBToSubstrait::TransformFunctionExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                    uint64_t col_offset) {
	auto &dfun = (BoundFunctionExpression &)dexpr;
	auto sfun = sexpr.mutable_scalar_function();

	sfun->set_function_reference(RegisterFunction(RemapFunctionName(dfun.function.name)));

	for (auto &darg : dfun.children) {
		auto sarg = sfun->add_arguments();
		TransformExpr(*darg, *sarg->mutable_value(), col_offset);
	}
	auto output_type = sfun->mutable_output_type();
	*output_type = DuckToSubstraitType(dfun.return_type);
}

void DuckDBToSubstrait::TransformConstantExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &dconst = (BoundConstantExpression &)dexpr;
	TransformConstant(dconst.value, sexpr);
}

void DuckDBToSubstrait::TransformComparisonExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &dcomp = (BoundComparisonExpression &)dexpr;

	string fname;
	switch (dexpr.type) {
	case ExpressionType::COMPARE_EQUAL:
		fname = "equal";
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		fname = "lt";
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		fname = "lte";
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		fname = "gt";
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		fname = "gte";
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		fname = "not_equal";
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		fname = "is_not_distinct_from";
		break;
	default:
		throw InternalException(ExpressionTypeToString(dexpr.type));
	}

	auto scalar_fun = sexpr.mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction(fname));
	auto sarg = scalar_fun->add_arguments();
	TransformExpr(*dcomp.left, *sarg->mutable_value(), 0);
	sarg = scalar_fun->add_arguments();
	TransformExpr(*dcomp.right, *sarg->mutable_value(), 0);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dcomp.return_type);
}

void DuckDBToSubstrait::TransformConjunctionExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                       uint64_t col_offset) {
	auto &dconj = (BoundConjunctionExpression &)dexpr;
	string fname;
	switch (dexpr.type) {
	case ExpressionType::CONJUNCTION_AND:
		fname = "and";
		break;
	case ExpressionType::CONJUNCTION_OR:
		fname = "or";
		break;
	default:
		throw InternalException(ExpressionTypeToString(dexpr.type));
	}

	auto scalar_fun = sexpr.mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction(fname));
	for (auto &child : dconj.children) {
		auto s_arg = scalar_fun->add_arguments();
		TransformExpr(*child, *s_arg->mutable_value(), col_offset);
	}
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dconj.return_type);
}

void DuckDBToSubstrait::TransformNotNullExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                   uint64_t col_offset) {
	auto &dop = (BoundOperatorExpression &)dexpr;
	auto scalar_fun = sexpr.mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction("is_not_null"));
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dop.children[0], *s_arg->mutable_value(), col_offset);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dop.return_type);
}

void DuckDBToSubstrait::TransformCaseExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &dcase = (BoundCaseExpression &)dexpr;
	auto scase = sexpr.mutable_if_then();
	for (auto &dcheck : dcase.case_checks) {
		auto sif = scase->mutable_ifs()->Add();
		TransformExpr(*dcheck.when_expr, *sif->mutable_if_());
		auto then_expr = new substrait::Expression();
		TransformExpr(*dcheck.then_expr, *then_expr);
		// Push a Cast
		auto then = sif->mutable_then();
		auto scast = new substrait::Expression_Cast();
		*scast->mutable_type() = DuckToSubstraitType(dcase.return_type);
		scast->set_allocated_input(then_expr);
		then->set_allocated_cast(scast);
	}
	auto else_expr = new substrait::Expression();
	TransformExpr(*dcase.else_expr, *else_expr);
	// Push a Cast
	auto mutable_else = scase->mutable_else_();
	auto scast = new substrait::Expression_Cast();
	*scast->mutable_type() = DuckToSubstraitType(dcase.return_type);
	scast->set_allocated_input(else_expr);
	else_expr = (substrait::Expression *)scast;
	mutable_else->set_allocated_cast(scast);
}

void DuckDBToSubstrait::TransformInExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &duck_in_op = (BoundOperatorExpression &)dexpr;
	auto subs_in_op = sexpr.mutable_singular_or_list();

	// Get the expression
	TransformExpr(*duck_in_op.children[0], *subs_in_op->mutable_value());

	// Get the values
	for (idx_t i = 1; i < duck_in_op.children.size(); i++) {
		subs_in_op->add_options();
		TransformExpr(*duck_in_op.children[i], *subs_in_op->mutable_options(i - 1));
	}
}

void DuckDBToSubstrait::TransformIsNullExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                  uint64_t col_offset) {
	auto &dop = (BoundOperatorExpression &)dexpr;
	auto scalar_fun = sexpr.mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction("is_null"));
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dop.children[0], *s_arg->mutable_value(), col_offset);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dop.return_type);
}

void DuckDBToSubstrait::TransformNotExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	auto &dop = (BoundOperatorExpression &)dexpr;
	auto scalar_fun = sexpr.mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction("not"));
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dop.children[0], *s_arg->mutable_value(), col_offset);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dop.return_type);
}

void DuckDBToSubstrait::TransformExpr(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	switch (dexpr.type) {
	case ExpressionType::BOUND_REF:
		TransformBoundRefExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_CAST:
		TransformCastExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::BOUND_FUNCTION:
		TransformFunctionExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::VALUE_CONSTANT:
		TransformConstantExpression(dexpr, sexpr);
		break;
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		TransformComparisonExpression(dexpr, sexpr);
		break;
	case ExpressionType::CONJUNCTION_AND:
	case ExpressionType::CONJUNCTION_OR:
		TransformConjunctionExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		TransformNotNullExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::CASE_EXPR:
		TransformCaseExpression(dexpr, sexpr);
		break;
	case ExpressionType::COMPARE_IN:
		TransformInExpression(dexpr, sexpr);
		break;
	case ExpressionType::OPERATOR_IS_NULL:
		TransformIsNullExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_NOT:
		TransformNotExpression(dexpr, sexpr, col_offset);
		break;
	default:
		throw InternalException(ExpressionTypeToString(dexpr.type));
	}
}

void DuckDBToSubstrait::CreateFieldRef(substrait::Expression *expr, uint64_t col_idx) {
	auto selection = new ::substrait::Expression_FieldReference();
	selection->mutable_direct_reference()->mutable_struct_field()->set_field((int32_t)col_idx);
	auto root_reference = new ::substrait::Expression_FieldReference_RootReference();
	selection->set_allocated_root_reference(root_reference);
	D_ASSERT(selection->root_type_case() == substrait::Expression_FieldReference::RootTypeCase::kRootReference);
	expr->set_allocated_selection(selection);
	D_ASSERT(expr->has_selection());
}

substrait::Expression *DuckDBToSubstrait::TransformJoinCond(JoinCondition &dcond, uint64_t left_ncol) {
	auto expr = new substrait::Expression();
	string join_comparision;
	switch (dcond.comparison) {
	case ExpressionType::COMPARE_EQUAL:
		join_comparision = "equal";
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		join_comparision = "gt";
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		join_comparision = "is_not_distinct_from";
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		join_comparision = "gte";
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		join_comparision = "lte";
		break;
	default:
		throw InternalException("Unsupported join comparison");
	}
	auto scalar_fun = expr->mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction(join_comparision));
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dcond.left, *s_arg->mutable_value());
	s_arg = scalar_fun->add_arguments();
	TransformExpr(*dcond.right, *s_arg->mutable_value(), left_ncol);
	LogicalType bool_type = LogicalType::BOOLEAN;
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(bool_type);
	return expr;
}

void DuckDBToSubstrait::TransformOrder(BoundOrderByNode &dordf, substrait::SortField &sordf) {
	switch (dordf.type) {
	case OrderType::ASCENDING:
		switch (dordf.null_order) {
		case OrderByNullType::NULLS_FIRST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST);
			break;
		case OrderByNullType::NULLS_LAST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST);

			break;
		default:
			throw InternalException("Unsupported ordering type");
		}
		break;
	case OrderType::DESCENDING:
		switch (dordf.null_order) {
		case OrderByNullType::NULLS_FIRST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST);
			break;
		case OrderByNullType::NULLS_LAST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST);

			break;
		default:
			throw InternalException("Unsupported ordering type");
		}
		break;
	default:
		throw InternalException("Unsupported ordering type");
	}
	TransformExpr(*dordf.expression, *sordf.mutable_expr());
}

substrait::Rel *DuckDBToSubstrait::TransformFilter(LogicalOperator &dop) {

	auto &dfilter = (LogicalFilter &)dop;

	auto res = TransformOp(*dop.children[0]);

	if (!dfilter.expressions.empty()) {
		auto filter = new substrait::Rel();
		filter->mutable_filter()->set_allocated_input(res);
		filter->mutable_filter()->set_allocated_condition(
		    CreateConjunction(dfilter.expressions, [&](unique_ptr<Expression> &in) {
			    auto expr = new substrait::Expression();
			    TransformExpr(*in, *expr);
			    return expr;
		    }));
		res = filter;
	}

	if (!dfilter.projection_map.empty()) {
		auto projection = new substrait::Rel();
		projection->mutable_project()->set_allocated_input(res);
		for (auto col_idx : dfilter.projection_map) {
			CreateFieldRef(projection->mutable_project()->add_expressions(), col_idx);
		}
		res = projection;
	}
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformProjection(LogicalOperator &dop) {
	auto res = new substrait::Rel();
	auto &dproj = (LogicalProjection &)dop;
	auto sproj = res->mutable_project();
	sproj->set_allocated_input(TransformOp(*dop.children[0]));

	for (auto &dexpr : dproj.expressions) {
		TransformExpr(*dexpr, *sproj->add_expressions());
	}
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformTopN(LogicalOperator &dop) {
	auto &dtopn = (LogicalTopN &)dop;
	auto res = new substrait::Rel();
	auto stopn = res->mutable_fetch();

	auto sord_rel = new substrait::Rel();
	auto sord = sord_rel->mutable_sort();
	sord->set_allocated_input(TransformOp(*dop.children[0]));

	for (auto &dordf : dtopn.orders) {
		TransformOrder(dordf, *sord->add_sorts());
	}

	stopn->set_allocated_input(sord_rel);
	stopn->set_offset(dtopn.offset);
	stopn->set_count(dtopn.limit);
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformLimit(LogicalOperator &dop) {
	auto &dlimit = (LogicalLimit &)dop;
	auto res = new substrait::Rel();
	auto stopn = res->mutable_fetch();
	stopn->set_allocated_input(TransformOp(*dop.children[0]));

	stopn->set_offset(dlimit.offset_val);
	stopn->set_count(dlimit.limit_val);
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformOrderBy(LogicalOperator &dop) {
	auto res = new substrait::Rel();
	auto &dord = (LogicalOrder &)dop;
	auto sord = res->mutable_sort();

	sord->set_allocated_input(TransformOp(*dop.children[0]));

	for (auto &dordf : dord.orders) {
		TransformOrder(dordf, *sord->add_sorts());
	}
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformComparisonJoin(LogicalOperator &dop) {
	auto res = new substrait::Rel();
	auto sjoin = res->mutable_join();
	auto &djoin = (LogicalComparisonJoin &)dop;
	sjoin->set_allocated_left(TransformOp(*dop.children[0]));
	sjoin->set_allocated_right(TransformOp(*dop.children[1]));

	auto left_col_count = dop.children[0]->types.size();
	if (dop.children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto child_join = (LogicalComparisonJoin *)dop.children[0].get();
		left_col_count = child_join->left_projection_map.size() + child_join->right_projection_map.size();
	}
	sjoin->set_allocated_expression(
	    CreateConjunction(djoin.conditions, [&](JoinCondition &in) { return TransformJoinCond(in, left_col_count); }));

	switch (djoin.join_type) {
	case JoinType::INNER:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_INNER);
		break;
	case JoinType::LEFT:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT);
		break;
	case JoinType::RIGHT:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT);
		break;
	case JoinType::SINGLE:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SINGLE);
		break;
	case JoinType::SEMI:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SEMI);
		break;
	default:
		throw InternalException("Unsupported join type " + JoinTypeToString(djoin.join_type));
	}
	// somewhat odd semantics on our side
	if (djoin.left_projection_map.empty()) {
		for (uint64_t i = 0; i < dop.children[0]->types.size(); i++) {
			djoin.left_projection_map.push_back(i);
		}
	}
	if (djoin.right_projection_map.empty()) {
		for (uint64_t i = 0; i < dop.children[1]->types.size(); i++) {
			djoin.right_projection_map.push_back(i);
		}
	}
	auto proj_rel = new substrait::Rel();
	auto projection = proj_rel->mutable_project();
	for (auto left_idx : djoin.left_projection_map) {
		CreateFieldRef(projection->add_expressions(), left_idx);
	}

	for (auto right_idx : djoin.right_projection_map) {
		CreateFieldRef(projection->add_expressions(), right_idx + left_col_count);
	}
	projection->set_allocated_input(res);
	return proj_rel;
}

substrait::Rel *DuckDBToSubstrait::TransformAggregateGroup(LogicalOperator &dop) {
	auto res = new substrait::Rel();
	auto &daggr = (LogicalAggregate &)dop;
	auto saggr = res->mutable_aggregate();
	saggr->set_allocated_input(TransformOp(*dop.children[0]));
	// we only do a single grouping set for now
	auto sgrp = saggr->add_groupings();
	for (auto &dgrp : daggr.groups) {
		if (dgrp->type != ExpressionType::BOUND_REF) {
			// TODO push projection or push substrait to allow expressions here
			throw InternalException("No expressions in groupings yet");
		}
		TransformExpr(*dgrp, *sgrp->add_grouping_expressions());
	}
	for (auto &dmeas : daggr.expressions) {
		auto smeas = saggr->add_measures()->mutable_measure();
		if (dmeas->type != ExpressionType::BOUND_AGGREGATE) {
			// TODO push projection or push substrait, too
			throw InternalException("No non-aggregate expressions in measures yet");
		}
		auto &daexpr = (BoundAggregateExpression &)*dmeas;

		smeas->set_function_reference(RegisterFunction(RemapFunctionName(daexpr.function.name)));

		*smeas->mutable_output_type() = DuckToSubstraitType(daexpr.return_type);
		for (auto &darg : daexpr.children) {
			auto s_arg = smeas->add_arguments();
			TransformExpr(*darg, *s_arg->mutable_value());
		}
	}
	return res;
}

::substrait::Type DuckDBToSubstrait::DuckToSubstraitType(const LogicalType &type, BaseStatistics *column_statistics,
                                                         bool not_null) {
	::substrait::Type s_type;
	substrait::Type_Nullability type_nullability;
	if (not_null) {
		type_nullability = substrait::Type_Nullability::Type_Nullability_NULLABILITY_REQUIRED;
	} else {
		type_nullability = substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		auto bool_type = new substrait::Type_Boolean;
		bool_type->set_nullability(type_nullability);
		s_type.set_allocated_bool_(bool_type);
		return s_type;
	}

	case LogicalTypeId::TINYINT: {
		auto integral_type = new substrait::Type_I8;
		integral_type->set_nullability(type_nullability);
		s_type.set_allocated_i8(integral_type);
		return s_type;
	}
		// Substrait ppl think unsigned types are not common, so we have to upcast
		// these beauties Which completely borks the optimization they are created
		// for
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::SMALLINT: {
		auto integral_type = new substrait::Type_I16;
		integral_type->set_nullability(type_nullability);
		s_type.set_allocated_i16(integral_type);
		return s_type;
	}
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::INTEGER: {
		auto integral_type = new substrait::Type_I32;
		integral_type->set_nullability(type_nullability);
		s_type.set_allocated_i32(integral_type);
		return s_type;
	}
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::BIGINT: {
		auto integral_type = new substrait::Type_I64;
		integral_type->set_nullability(type_nullability);
		s_type.set_allocated_i64(integral_type);
		return s_type;
	}
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT: {
		// FIXME: Support for hugeint types?
		auto s_decimal = new substrait::Type_Decimal();
		s_decimal->set_scale(0);
		s_decimal->set_precision(38);
		s_decimal->set_nullability(type_nullability);
		s_type.set_allocated_decimal(s_decimal);
		return s_type;
	}
	case LogicalTypeId::DATE: {
		auto date_type = new substrait::Type_Date;
		date_type->set_nullability(type_nullability);
		s_type.set_allocated_date(date_type);
		return s_type;
	}
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIME: {
		auto time_type = new substrait::Type_Time;
		time_type->set_nullability(type_nullability);
		s_type.set_allocated_time(time_type);
		return s_type;
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC: {
		// FIXME: Shouldn't this have a precision?
		auto timestamp_type = new substrait::Type_Timestamp;
		timestamp_type->set_nullability(type_nullability);
		s_type.set_allocated_timestamp(timestamp_type);
		return s_type;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto timestamp_type = new substrait::Type_TimestampTZ;
		timestamp_type->set_nullability(type_nullability);
		s_type.set_allocated_timestamp_tz(timestamp_type);
		return s_type;
	}
	case LogicalTypeId::INTERVAL: {
		auto interval_type = new substrait::Type_IntervalDay();
		interval_type->set_nullability(type_nullability);
		s_type.set_allocated_interval_day(interval_type);
		return s_type;
	}
	case LogicalTypeId::FLOAT: {
		auto float_type = new substrait::Type_FP32;
		float_type->set_nullability(type_nullability);
		s_type.set_allocated_fp32(float_type);
		return s_type;
	}
	case LogicalTypeId::DOUBLE: {
		auto double_type = new substrait::Type_FP64;
		double_type->set_nullability(type_nullability);
		s_type.set_allocated_fp64(double_type);
		return s_type;
	}
	case LogicalTypeId::DECIMAL: {
		auto decimal_type = new substrait::Type_Decimal;
		decimal_type->set_nullability(type_nullability);
		decimal_type->set_precision(DecimalType::GetWidth(type));
		decimal_type->set_scale(DecimalType::GetScale(type));
		s_type.set_allocated_decimal(decimal_type);
		return s_type;
	}
	case LogicalTypeId::VARCHAR: {
		auto varchar_type = new substrait::Type_VarChar;
		varchar_type->set_nullability(type_nullability);
		if (column_statistics) {
			auto string_statistics = (StringStatistics *)column_statistics;
			if (max_string_length < string_statistics->max_string_length) {
				max_string_length = string_statistics->max_string_length;
			}
			varchar_type->set_length(string_statistics->max_string_length);
		} else {
			// FIXME: Have to propagate the statistics to here somehow
			varchar_type->set_length(max_string_length);
		}
		s_type.set_allocated_varchar(varchar_type);
		return s_type;
	}
	case LogicalTypeId::BLOB: {
		auto binary_type = new substrait::Type_Binary;
		binary_type->set_nullability(type_nullability);
		s_type.set_allocated_binary(binary_type);
		return s_type;
	}
	case LogicalTypeId::UUID: {
		auto uuid_type = new substrait::Type_UUID;
		uuid_type->set_nullability(type_nullability);
		s_type.set_allocated_uuid(uuid_type);
		return s_type;
	}
	case LogicalTypeId::ENUM: {
		auto enum_type = new substrait::Type_UserDefined;
		enum_type->set_nullability(type_nullability);
		s_type.set_allocated_user_defined(enum_type);
		return s_type;
	}
	default:
		throw NotImplementedException("Logical Type " + type.ToString() +
		                              " not implemented as Substrait Schema Result.");
	}
}

substrait::Rel *DuckDBToSubstrait::TransformCrossProduct(LogicalOperator &dop) {
	auto rel = new substrait::Rel();
	auto sub_cross_prod = rel->mutable_cross();
	auto &djoin = (LogicalCrossProduct &)dop;
	sub_cross_prod->set_allocated_left(TransformOp(*dop.children[0]));
	sub_cross_prod->set_allocated_right(TransformOp(*dop.children[1]));
	auto bindings = djoin.GetColumnBindings();
	return rel;
}

} // namespace duckdb
