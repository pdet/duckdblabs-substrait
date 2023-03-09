#include "to_substrait/logical_operator/aggregate_transformer.hpp"
#include "to_substrait/logical_operator/cross_product_transformer.hpp"
#include "to_substrait/logical_operator/filter_transformer.hpp"
#include "to_substrait/logical_operator/get_transformer.hpp"
#include "to_substrait/logical_operator/join_transformer.hpp"
#include "to_substrait/logical_operator/limit_transformer.hpp"
#include "to_substrait/logical_operator/order_by_transformer.hpp"
#include "to_substrait/logical_operator/projection_transformer.hpp"
#include "to_substrait/logical_operator/top_n_transformer.hpp"

using namespace duckdb;
OperatorTransformer::OperatorTransformer(LogicalOperator &input_p, PlanTransformer &plan_p)
    : result(new substrait::Rel()), input(input_p), plan_transformer(plan_p) {
}

unique_ptr<OperatorTransformer> OperatorTransformer::Wololo(LogicalOperator &dop, PlanTransformer &plan) {
	switch (dop.type) {
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto transformer = make_unique<FilterTransformer>(dop, plan);
		transformer->Wololo();
		return std::move(transformer);
	}
	case LogicalOperatorType::LOGICAL_TOP_N: {
		auto transformer = make_unique<TopNTransformer>(dop, plan);
		transformer->Wololo();
		return std::move(transformer);
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto transformer = make_unique<LimitTransformer>(dop, plan);
		transformer->Wololo();
		return std::move(transformer);
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto transformer = make_unique<OrderByTransformer>(dop, plan);
		transformer->Wololo();
		return std::move(transformer);
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto transformer = make_unique<ProjectionTransformer>(dop, plan);
		transformer->Wololo();
		return std::move(transformer);
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto transformer = make_unique<JoinTransformer>(dop, plan);
		transformer->Wololo();
		return std::move(transformer);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto transformer = make_unique<AggregateTransformer>(dop, plan);
		transformer->Wololo();
		return std::move(transformer);
	}
	case LogicalOperatorType::LOGICAL_GET: {
		auto transformer = make_unique<GetTransformer>(dop, plan);
		transformer->Wololo();
		return std::move(transformer);
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		auto transformer = make_unique<CrossProductTransformer>(dop, plan);
		transformer->Wololo();
		return std::move(transformer);
	}
	default:
		throw NotImplementedException("Logical Operator Conversion not implemented yet. Operator: " +
		                              LogicalOperatorToString(dop.type));
	}
}
