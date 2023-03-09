#include "to_substrait/expression/field_reference_transformer.hpp"
#include "duckdb/common/assert.hpp"

using namespace duckdb;

FieldReferenceTransformer::FieldReferenceTransformer(substrait::Expression *expr_p, uint64_t col_idx_p)
    : col_idx(col_idx_p), expr(expr_p) {
}

//! Perform the actual conversion
substrait::Expression *FieldReferenceTransformer::Wololo() {
	auto selection = new ::substrait::Expression_FieldReference();
	selection->mutable_direct_reference()->mutable_struct_field()->set_field((int32_t)col_idx);
	auto root_reference = new ::substrait::Expression_FieldReference_RootReference();
	selection->set_allocated_root_reference(root_reference);
	D_ASSERT(selection->root_type_case() == substrait::Expression_FieldReference::RootTypeCase::kRootReference);
	expr->set_allocated_selection(selection);
	D_ASSERT(expr->has_selection());
	return expr;
}
