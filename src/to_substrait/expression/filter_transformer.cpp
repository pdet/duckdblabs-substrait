#include "to_substrait/expression/filter_transformer.hpp"

#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "to_substrait/type_transformer.hpp"
#include "to_substrait/expression/constant_transformer.hpp"
using namespace duckdb;

FilterTransformer::FilterTransformer(uint64_t col_idx_p, TableFilter &dfilter_p, LogicalType &return_type_p,
                                     PlanTransformer &plan_p)
    : col_idx(col_idx_p), dfilter(dfilter_p), return_type(return_type_p), plan(plan_p), filter(nullptr) {
	switch (dfilter.filter_type) {
	case TableFilterType::IS_NOT_NULL:
		TransformIsNotNullFilter();
		break;
	case TableFilterType::CONJUNCTION_AND:
		TransformConjunctionAndFilter();
		break;
	case TableFilterType::CONSTANT_COMPARISON:
		TransformConstantComparisonFilter();
		break;
	default:
		throw NotImplementedException("Unsupported table filter type");
	}
}

void FilterTransformer::TransformIsNotNullFilter() {
	filter = new substrait::Expression();
	auto scalar_fun = filter->mutable_scalar_function();
	scalar_fun->set_function_reference(plan.RegisterFunction("is_not_null"));
	auto s_arg = scalar_fun->add_arguments();
	CreateFieldRef(s_arg->mutable_value(), col_idx);
	*scalar_fun->mutable_output_type() =  TypeTransformer::Wololo((return_type);
}

void FilterTransformer::TransformConjunctionAndFilter() {
	auto &conjunction_filter = (ConjunctionAndFilter &)dfilter;
	filter = CreateConjunction(conjunction_filter.child_filters,
	                           [&](unique_ptr<TableFilter> &in) { return TransformFilter(col_idx, *in, return_type); });
}

void FilterTransformer::TransformConstantComparisonFilter() {
	filter = new substrait::Expression();
	auto s_scalar = filter->mutable_scalar_function();
	auto &constant_filter = (ConstantFilter &)dfilter;
	*s_scalar->mutable_output_type() = TypeTransformer::Wololo(return_type);
	auto s_arg = s_scalar->add_arguments();
	CreateFieldRef(s_arg->mutable_value(), col_idx);
	s_arg = s_scalar->add_arguments();
	ConstantTransformer constant(constant_filter.constant, *s_arg->mutable_value());
	constant.Wololo();
	uint64_t function_id;
	switch (constant_filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		function_id = plan.RegisterFunction("equal");
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		function_id = plan.RegisterFunction("lte");
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		function_id = plan.RegisterFunction("lt");
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		function_id = plan.RegisterFunction("gt");
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		function_id = plan.RegisterFunction("gte");
		break;
	default:
		throw InternalException(ExpressionTypeToString(constant_filter.comparison_type));
	}
	s_scalar->set_function_reference(function_id);
}
