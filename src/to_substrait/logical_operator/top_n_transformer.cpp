#include "to_substrait/logical_operator/top_n_transformer.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

using namespace duckdb;

TopNTransformer::TopNTransformer(LogicalOperator &op, PlanTransformer &plan_p)
    : OperatorTransformer(op, plan_p), logical_top_n((LogicalTopN &)op) {
	D_ASSERT(op.type == LogicalOperatorType::LOGICAL_TOP_N);
}

void TopNTransformer::Wololo() {
	auto stopn = result->mutable_fetch();

	auto sord_rel = new substrait::Rel();
	auto sord = sord_rel->mutable_sort();

	children.emplace_back(OperatorTransformer::Wololo(*logical_top_n.children[0], plan_transformer));
	auto &child = *children.back();
	emit_map = child.emit_map;
	reference_ids = child.reference_ids;

	sord->set_allocated_input(child.result);

	for (auto &dordf : logical_top_n.orders) {
		TransformOrder(dordf, *sord->add_sorts());
	}

	stopn->set_allocated_input(sord_rel);
	stopn->set_offset(logical_top_n.offset);
	stopn->set_count(logical_top_n.limit);
}
