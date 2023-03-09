#include "to_substrait/logical_operator/projection_transformer.hpp"

using namespace duckdb;

ProjectionTransformer::ProjectionTransformer(LogicalOperator &op, PlanTransformer &plan_p)
    : OperatorTransformer(op, plan_p), logical_projection((LogicalProjection &)op) {
	D_ASSERT(op.type == LogicalOperatorType::LOGICAL_PROJECTION);
}

void ProjectionTransformer::Wololo() {
	auto sproj = result->mutable_project();

	children.emplace_back(OperatorTransformer::Wololo(*logical_projection.children[0], plan_transformer));
	auto &child = *children.back();
	reference_ids = child.reference_ids;

	sproj->set_allocated_input(child.result);

	for (auto &dexpr : logical_projection.expressions) {
		// FIXME: emit Ids must go throuhg transform expr
		//	emit_map = child.emit_map;
		TransformExpr(*dexpr, *sproj->add_expressions());
	}
}
