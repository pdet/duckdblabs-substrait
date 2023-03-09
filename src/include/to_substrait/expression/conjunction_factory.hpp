//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/expression/conjunction_factory.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

using namespace duckdb;
//! Creates a Conjunction
template <typename T, typename FUNC>
substrait::Expression *CreateConjunction(T &source, FUNC f) {
	substrait::Expression *res = nullptr;
	for (auto &ele : source) {
		auto child_expression = f(ele);
		if (!res) {
			res = child_expression;
		} else {
			auto temp_expr = new substrait::Expression();
			auto scalar_fun = temp_expr->mutable_scalar_function();
			scalar_fun->set_function_reference(plan_transformer.RegisterFunction("and"));
			LogicalType boolean_type(LogicalTypeId::BOOLEAN);
			*scalar_fun->mutable_output_type() = TypeTransformer::Wololo(boolean_type);
			AllocateFunctionArgument(scalar_fun, res);
			AllocateFunctionArgument(scalar_fun, child_expression);
			res = temp_expr;
		}
	}
	return res;
}