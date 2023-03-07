#include "to_substrait/plan_transformer.hpp"

using namespace duckdb;

PlanTransformer::PlanTransformer(ClientContext &context_p, LogicalOperator &root_op) : context(context_p) {
	plan.add_relations()->set_allocated_root(TransformRootOp(root_op));

	// Set version number of substrait used to generate this plan
	auto version = plan.mutable_version();
	version->set_major_number(0);
	version->set_minor_number(24);
	version->set_patch_number(0);

	// Set that duckdb produced it
	auto *producer_name = new string();
	*producer_name = "DuckDB";
	version->set_allocated_producer(producer_name);
}