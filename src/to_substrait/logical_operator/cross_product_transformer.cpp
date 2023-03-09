#include "to_substrait/logical_operator/cross_product_transformer.hpp"

using namespace duckdb;

CrossProductTransformer::CrossProductTransformer(LogicalOperator &op, PlanTransformer &plan_p)
    : OperatorTransformer(op, plan_p), logical_cross_product((LogicalCrossProduct &)op) {
	D_ASSERT(op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
}

void CrossProductTransformer::Wololo() {
	auto sub_cross_prod = result->mutable_cross();

	children.emplace_back(OperatorTransformer::Wololo(*logical_cross_product.children[0], plan_transformer));
	children.emplace_back(OperatorTransformer::Wololo(*logical_cross_product.children[1], plan_transformer));
	auto &left_child = *children[0];
	auto &right_child = *children[1];

	sub_cross_prod->set_allocated_left(left_child.result);
	sub_cross_prod->set_allocated_right(right_child.result);

	reference_ids = left_child.reference_ids;
	reference_ids.insert(reference_ids.end(), right_child.reference_ids.begin(), right_child.reference_ids.end());

	emit_map = left_child.emit_map;
	emit_map.insert(emit_map.end(), right_child.emit_map.begin(), right_child.emit_map.end() ;
}