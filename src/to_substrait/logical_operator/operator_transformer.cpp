#include "to_substrait/logical_operator/operator_transformer.hpp"

using namespace duckdb;

void OperatorTransformer::AllocateFunctionArgument(substrait::Expression_ScalarFunction *scalar_fun,
                                                   substrait::Expression *value) {
	auto function_argument = new substrait::FunctionArgument();
	function_argument->set_allocated_value(value);
	scalar_fun->mutable_arguments()->AddAllocated(function_argument);
}