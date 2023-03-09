#include "to_substrait/logical_operator/order_by_transformer.hpp"

using namespace duckdb;

OrderByTransformer::OrderByTransformer(LogicalOperator &op, PlanTransformer &plan_p)
    : OperatorTransformer(op, plan_p), logical_order((LogicalOrder &)op) {
	D_ASSERT(op.type == LogicalOperatorType::LOGICAL_ORDER_BY);
}

void OrderByTransformer::Wololo() {

	auto sord = result->mutable_sort();

	children.emplace_back(OperatorTransformer::Wololo(*logical_order.children[0], plan_transformer));
	auto &child = *children.back();
	emit_map = child.emit_map;
	reference_ids = child.reference_ids;

	sord->set_allocated_input(child.result);

	for (auto &dordf : logical_order.orders) {
		TransformOrder(dordf, *sord->add_sorts());
	}
}
