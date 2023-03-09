//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait/expression/conjunction_factory.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "to_substrait/plan_transformer.hpp"

namespace duckdb {
//! Creates a Conjunction
class ConjunctionFactory {
public:
	ConjunctionFactory(PlanTransformer &plan_transformer) : plan(plan_transformer) {};

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
				scalar_fun->set_function_reference(plan.RegisterFunction("and"));
				LogicalType boolean_type(LogicalTypeId::BOOLEAN);
				*scalar_fun->mutable_output_type() = TypeTransformer::Wololo(boolean_type);
				AllocateFunctionArgument(scalar_fun, res);
				AllocateFunctionArgument(scalar_fun, child_expression);
				res = temp_expr;
			}
		}
		return res;
	}

private:
	PlanTransformer &plan;
	static void AllocateFunctionArgument(substrait::Expression_ScalarFunction *scalar_fun,
	                                     substrait::Expression *value) {
		auto function_argument = new substrait::FunctionArgument();
		function_argument->set_allocated_value(value);
		scalar_fun->mutable_arguments()->AddAllocated(function_argument);
	}
};
} // namespace duckdb