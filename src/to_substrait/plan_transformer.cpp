#include "to_substrait/plan_transformer.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "to_substrait/expression/conjunction_factory.hpp"
using namespace duckdb;

const std::unordered_map<std::string, std::string> PlanTransformer::function_names_remap = {
    {"mod", "modulus"},       {"stddev", "std_dev"},      {"prefix", "starts_with"}, {"suffix", "ends_with"},
    {"substr", "substring"},  {"length", "char_length"},  {"isnan", "is_nan"},       {"isfinite", "is_finite"},
    {"isinf", "is_infinite"}, {"sum_no_overflow", "sum"}, {"count_star", "count"},   {"~~", "like"}};

PlanTransformer::PlanTransformer(ClientContext &context_p, LogicalOperator &root_op) : context(context_p) {
	plan.add_relations()->set_allocated_root(TransformRoot(root_op));

	// Set version number of substrait used to generate this plan
	auto version = plan.mutable_version();
	version->set_major_number(0);
	version->set_minor_number(24);
	version->set_patch_number(0);

	// Set that duckdb produced it
	auto *producer_name = new string();
	*producer_name = "DuckDB";
	version->set_allocated_producer(producer_name);
	conjunction_factory = make_unique<ConjunctionFactory>(*this);
}

ClientContext &PlanTransformer::GetContext() {
	return context;
}

void PlanTransformer::FillRootAliases(const LogicalOperator &duck_root, substrait::RelRoot *substrait_root) {
	// Find aliases and store them in root node
	const LogicalOperator *operator_with_aliases = &duck_root;
	bool is_projection_root = operator_with_aliases->type == LogicalOperatorType::LOGICAL_PROJECTION;
	bool is_child_topk = operator_with_aliases->children[0]->type == LogicalOperatorType::LOGICAL_TOP_N;
	if (is_projection_root) {
		// Our root node is a projection
		operator_with_aliases = &duck_root;
		if (is_child_topk) {
			// Projection is put on top of a top-k but the actual aliases are on the projection below the top-k still.
			auto &dproj = (LogicalProjection &)*operator_with_aliases;
			operator_with_aliases = operator_with_aliases->children[0].get();
			for (auto &expression : duck_root.expressions) {
				if (expression->type != ExpressionType::BOUND_REF) {
					throw InternalException("This expression should be a Bound Ref");
				}
				D_ASSERT(expression->type == ExpressionType::BOUND_REF);
				auto b_expr = (BoundReferenceExpression *)expression.get();
				substrait_root->add_names(dproj.expressions[b_expr->index]->GetName());
				return;
			}
		}
	} else {
		// If the root operator is not a projection, we must go down until we find the
		// first projection to get the aliases
		while (operator_with_aliases->type != LogicalOperatorType::LOGICAL_PROJECTION) {
			if (operator_with_aliases->children.size() != 1) {
				throw InternalException("Root node has more than 1, or 0 children (%d) up to "
				                        "reaching a projection node. Type %d",
				                        operator_with_aliases->children.size(), operator_with_aliases->type);
			}
			operator_with_aliases = operator_with_aliases->children[0].get();
		}
	}
	if (operator_with_aliases->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		throw InternalException("This node should be a projection");
	}
	auto &dproj = (LogicalProjection &)*operator_with_aliases;
	for (auto &expression : dproj.expressions) {
		substrait_root->add_names(expression->GetName());
	}
}

uint64_t PlanTransformer::RegisterFunction(const string &name) {
	if (name.empty()) {
		throw InternalException("Missing function name");
	}
	if (functions_map.find(name) == functions_map.end()) {
		auto function_id = last_function_id++;
		// FIXME: We have to do some URI YAML File shenanigans
		//		auto uri = plan.add_extension_uris();
		//		uri->set_extension_uri_anchor(function_id);
		auto sfun = plan.add_extensions()->mutable_extension_function();
		sfun->set_function_anchor(function_id);
		sfun->set_name(name);
		//		sfun->set_extension_uri_reference(function_id);

		functions_map[name] = function_id;
	}
	return functions_map[name];
}

substrait::RelRoot *PlanTransformer::TransformRoot(const LogicalOperator &dop) {

	auto root_rel = new substrait::RelRoot();
	// Start transformation of nodes
	root_rel->set_allocated_input(TransformOp(dop));
	// Fill Aliases
	FillRootAliases(dop, root_rel);
	return root_rel;
}
