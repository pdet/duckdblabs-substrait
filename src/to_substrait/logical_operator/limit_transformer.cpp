#include "to_substrait/logical_operator/limit_transformer.hpp"

using namespace duckdb;

LimitTransformer::LimitTransformer(LogicalOperator &op, PlanTransformer &plan_p)
    : OperatorTransformer(op, plan_p), logical_limit((LogicalLimit &)op) {
	D_ASSERT(op.type == LogicalOperatorType::LOGICAL_LIMIT);
}

void LimitTransformer::Wololo() {
	auto stopn = result->mutable_fetch();
	children.emplace_back(OperatorTransformer::Wololo(*logical_limit.children[0], plan_transformer));
	auto &child = *children.back();
	emit_map = child.emit_map;
	reference_ids = child.reference_ids;

	stopn->set_allocated_input(child.result);

	stopn->set_offset(logical_limit.offset_val);
	stopn->set_count(logical_limit.limit_val);
}
