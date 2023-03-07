#include "to_substrait/type_transformer.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

using namespace duckdb;

::substrait::Type TypeTransformer::Wololo(const LogicalType &type, idx_t *max_string_length,
                                          BaseStatistics *column_statistics, bool not_null) {
	::substrait::Type s_type;
	substrait::Type_Nullability type_nullability;
	if (not_null) {
		type_nullability = substrait::Type_Nullability::Type_Nullability_NULLABILITY_REQUIRED;
	} else {
		type_nullability = substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		auto bool_type = new substrait::Type_Boolean;
		bool_type->set_nullability(type_nullability);
		s_type.set_allocated_bool_(bool_type);
		return s_type;
	}

	case LogicalTypeId::TINYINT: {
		auto integral_type = new substrait::Type_I8;
		integral_type->set_nullability(type_nullability);
		s_type.set_allocated_i8(integral_type);
		return s_type;
	}
		// Substraiters think unsigned types are not common, so we have to upcast
		// these beauties Which completely borks the optimization they are created
		// for
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::SMALLINT: {
		auto integral_type = new substrait::Type_I16;
		integral_type->set_nullability(type_nullability);
		s_type.set_allocated_i16(integral_type);
		return s_type;
	}
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::INTEGER: {
		auto integral_type = new substrait::Type_I32;
		integral_type->set_nullability(type_nullability);
		s_type.set_allocated_i32(integral_type);
		return s_type;
	}
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::BIGINT: {
		auto integral_type = new substrait::Type_I64;
		integral_type->set_nullability(type_nullability);
		s_type.set_allocated_i64(integral_type);
		return s_type;
	}
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT: {
		// FIXME: Support for hugeint types?
		auto s_decimal = new substrait::Type_Decimal();
		s_decimal->set_scale(0);
		s_decimal->set_precision(38);
		s_decimal->set_nullability(type_nullability);
		s_type.set_allocated_decimal(s_decimal);
		return s_type;
	}
	case LogicalTypeId::DATE: {
		auto date_type = new substrait::Type_Date;
		date_type->set_nullability(type_nullability);
		s_type.set_allocated_date(date_type);
		return s_type;
	}
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIME: {
		auto time_type = new substrait::Type_Time;
		time_type->set_nullability(type_nullability);
		s_type.set_allocated_time(time_type);
		return s_type;
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC: {
		// FIXME: Shouldn't this have a precision?
		auto timestamp_type = new substrait::Type_Timestamp;
		timestamp_type->set_nullability(type_nullability);
		s_type.set_allocated_timestamp(timestamp_type);
		return s_type;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto timestamp_type = new substrait::Type_TimestampTZ;
		timestamp_type->set_nullability(type_nullability);
		s_type.set_allocated_timestamp_tz(timestamp_type);
		return s_type;
	}
	case LogicalTypeId::INTERVAL: {
		auto interval_type = new substrait::Type_IntervalDay();
		interval_type->set_nullability(type_nullability);
		s_type.set_allocated_interval_day(interval_type);
		return s_type;
	}
	case LogicalTypeId::FLOAT: {
		auto float_type = new substrait::Type_FP32;
		float_type->set_nullability(type_nullability);
		s_type.set_allocated_fp32(float_type);
		return s_type;
	}
	case LogicalTypeId::DOUBLE: {
		auto double_type = new substrait::Type_FP64;
		double_type->set_nullability(type_nullability);
		s_type.set_allocated_fp64(double_type);
		return s_type;
	}
	case LogicalTypeId::DECIMAL: {
		auto decimal_type = new substrait::Type_Decimal;
		decimal_type->set_nullability(type_nullability);
		decimal_type->set_precision(DecimalType::GetWidth(type));
		decimal_type->set_scale(DecimalType::GetScale(type));
		s_type.set_allocated_decimal(decimal_type);
		return s_type;
	}
	case LogicalTypeId::VARCHAR: {
		auto varchar_type = new substrait::Type_VarChar;
		varchar_type->set_nullability(type_nullability);
		if (max_string_length && column_statistics && StringStats::HasMaxStringLength(*column_statistics)) {
			auto stats_max_len = StringStats::MaxStringLength(*column_statistics);
			if (*max_string_length < stats_max_len) {
				*max_string_length = stats_max_len;
			}
			varchar_type->set_length((int)stats_max_len);
		} else {
			// FIXME: Have to propagate the statistics to here somehow
			varchar_type->set_length((int)*max_string_length);
		}
		s_type.set_allocated_varchar(varchar_type);
		return s_type;
	}
	case LogicalTypeId::BLOB: {
		auto binary_type = new substrait::Type_Binary;
		binary_type->set_nullability(type_nullability);
		s_type.set_allocated_binary(binary_type);
		return s_type;
	}
	case LogicalTypeId::UUID: {
		auto uuid_type = new substrait::Type_UUID;
		uuid_type->set_nullability(type_nullability);
		s_type.set_allocated_uuid(uuid_type);
		return s_type;
	}
	case LogicalTypeId::ENUM: {
		auto enum_type = new substrait::Type_UserDefined;
		enum_type->set_nullability(type_nullability);
		s_type.set_allocated_user_defined(enum_type);
		return s_type;
	}
	default:
		throw NotImplementedException("Logical Type " + type.ToString() + " can not be converted to a Substrait Type.");
	}
}