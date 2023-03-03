#include "to_substrait/logical_operator/get.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "to_substrait.hpp"

using namespace duckdb;

set<idx_t> GetNotNullConstraintCol(TableCatalogEntry &tbl) {
	set<idx_t> not_null;
	for (auto &constraint : tbl.GetConstraints()) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			not_null.insert(((NotNullConstraint *)constraint.get())->index.index);
		}
	}
	return not_null;
}

void GetToRead::TransformTableScan() {
	auto &table_scan_bind_data = (TableScanBindData &)*dget.bind_data;
	sget->mutable_named_table()->add_names(table_scan_bind_data.table->name);
	auto base_schema = new ::substrait::NamedStruct();
	auto type_info = new substrait::Type_Struct();
	type_info->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
	auto not_null_constraint = GetNotNullConstraintCol(*table_scan_bind_data.table);
	for (idx_t i = 0; i < dget.names.size(); i++) {
		auto cur_type = dget.returned_types[i];
		if (cur_type.id() == LogicalTypeId::STRUCT) {
			throw std::runtime_error("Structs are not yet accepted in table scans");
		}
		base_schema->add_names(dget.names[i]);
		auto column_statistics = dget.function.statistics(context, &table_scan_bind_data, i);
		bool not_null = not_null_constraint.find(i) != not_null_constraint.end();
		auto new_type = type_info->add_types();
		*new_type = DuckDBToSubstrait::DuckToSubstraitType(cur_type, column_statistics.get(), not_null);
	}
	base_schema->set_allocated_struct_(type_info);
	sget->set_allocated_base_schema(base_schema);
}

void GetToRead::TransformParquetScan() {
	auto files_path = bind_info->GetOptionList<string>("file_path");
	if (files_path.size() != 1) {
		throw NotImplementedException("Substrait Parquet Reader only supports single file");
	}

	auto parquet_item = sget->mutable_local_files()->add_items();
	// FIXME: should this be uri or file ogw
	auto *path = new string();
	*path = files_path[0];
	parquet_item->set_allocated_uri_file(path);
	parquet_item->mutable_parquet();

	auto base_schema = new ::substrait::NamedStruct();
	auto type_info = new substrait::Type_Struct();
	type_info->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
	for (idx_t i = 0; i < dget.names.size(); i++) {
		auto cur_type = dget.returned_types[i];
		if (cur_type.id() == LogicalTypeId::STRUCT) {
			throw std::runtime_error("Structs are not yet accepted in table scans");
		}
		base_schema->add_names(dget.names[i]);
		auto column_statistics = dget.function.statistics(context, bind_data, i);
		auto new_type = type_info->add_types();
		*new_type = DuckDBToSubstrait::DuckToSubstraitType(cur_type, column_statistics.get(), false);
	}
	base_schema->set_allocated_struct_(type_info);
	sget->set_allocated_base_schema(base_schema);
}

void GetToRead::Wololo() {
	if (!dget.function.get_batch_info) {
		throw NotImplementedException("This Scanner Type can't be used in substrait because a get batch info "
		                              "is not yet implemented");
	}
	auto bind = dget.function.get_batch_info(dget.bind_data.get());
	bind_info = &bind;
	auto s_read = result->mutable_read();

	if (!dget.table_filters.filters.empty()) {
		// Pushdown filter
		//		auto filter =
		//		    DuckDBToSubstrait::CreateConjunction(duck_get.table_filters.filters, [&](std::pair<const idx_t,
		//unique_ptr<TableFilter>> &in) { 			    auto col_idx = in.first; 			    auto return_type = duck_get.returned_types[col_idx];
		//			    auto &filter = *in.second;
		//			    return DuckDBToSubstrait::TransformFilter(col_idx, filter, return_type);
		//		    });
		//		s_read->set_allocated_filter(filter);
	}

	if (!dget.projection_ids.empty()) {
		// Projection Pushdown
		auto projection = new substrait::Expression_MaskExpression();
		// fixme: whatever this means
		projection->set_maintain_singular_struct(true);
		auto select = new substrait::Expression_MaskExpression_StructSelect();
		for (auto col_idx : dget.projection_ids) {
			auto struct_item = select->add_struct_items();
			struct_item->set_field((int32_t)dget.column_ids[col_idx]);
			// FIXME do we need to set the child? if yes, to what?
		}
		projection->set_allocated_select(select);
		s_read->set_allocated_projection(projection);
	}

	// Add Table Schema
	switch (bind_info->type) {
	case ScanType::TABLE:
		TransformTableScan();
		break;
	case ScanType::PARQUET:
		TransformParquetScan();
		break;
	default:
		throw NotImplementedException("This Scan Type is not yet implement for the to_substrait function");
	}
}