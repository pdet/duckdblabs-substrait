#include "to_substrait/expression/constant_transformer.hpp"

using namespace duckdb;

ConstantTransformer::ConstantTransformer(Value &dval_p, substrait::Expression &sexpr_p) : dval(dval_p), sexpr(sexpr_p) {
}

substrait::Expression *ConstantTransformer::Wololo() {
	if (dval.IsNull()) {
		sexpr.mutable_literal()->mutable_null();
		return &sexpr;
	}
	auto &duckdb_type = dval.type();
	switch (duckdb_type.id()) {
	case LogicalTypeId::DECIMAL:
		TransformDecimal();
		return &sexpr;
	case LogicalTypeId::INTEGER:
		TransformInteger();
		return &sexpr;
	case LogicalTypeId::SMALLINT:
		TransformSmallInt();
		return &sexpr;
	case LogicalTypeId::BIGINT:
		TransformBigInt();
		return &sexpr;
	case LogicalTypeId::HUGEINT:
		TransformHugeInt();
		return &sexpr;
	case LogicalTypeId::DATE:
		TransformDate();
		return &sexpr;
	case LogicalTypeId::TIME:
		TransformTime();
		return &sexpr;
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP:
		TransformTimestamp();
		return &sexpr;
	case LogicalTypeId::INTERVAL:
		TransformInterval();
		return &sexpr;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		TransformVarchar();
		return &sexpr;
	case LogicalTypeId::BOOLEAN:
		TransformBoolean();
		return &sexpr;
	case LogicalTypeId::DOUBLE:
		TransformDouble();
		return &sexpr;
	case LogicalTypeId::FLOAT:
		TransformFloat();
		return &sexpr;
	case LogicalTypeId::ENUM:
		TransformEnum();
		return &sexpr;
	default:
		throw InternalException("Transform constant of type %s", duckdb_type.ToString());
	}
}
void ConstantTransformer::TransformInteger() {
	auto &sval = *sexpr.mutable_literal();
	sval.set_i32(dval.GetValue<int32_t>());
}
void ConstantTransformer::TransformDouble() {
	auto &sval = *sexpr.mutable_literal();
	sval.set_fp64(dval.GetValue<double>());
}
void ConstantTransformer::TransformBigInt() {
	auto &sval = *sexpr.mutable_literal();
	sval.set_i64(dval.GetValue<int64_t>());
}
void ConstantTransformer::TransformDate() {
	auto &sval = *sexpr.mutable_literal();
	sval.set_date(dval.GetValue<date_t>().days);
}
void ConstantTransformer::TransformVarchar() {
	auto &sval = *sexpr.mutable_literal();
	string duck_str = dval.GetValue<string>();
	sval.set_string(dval.GetValue<string>());
}
void ConstantTransformer::TransformBoolean() {
	auto &sval = *sexpr.mutable_literal();
	sval.set_boolean(dval.GetValue<bool>());
}

void ConstantTransformer::TransformDecimal() {
	auto &sval = *sexpr.mutable_literal();
	auto *allocated_decimal = new ::substrait::Expression_Literal_Decimal();
	uint8_t scale, width;
	hugeint_t hugeint_value;
	Value mock_value;
	// alright time for some dirty switcharoo
	switch (dval.type().InternalType()) {
	case PhysicalType::INT8: {
		auto internal_value = dval.GetValueUnsafe<int8_t>();
		mock_value = Value::TINYINT(internal_value);
		break;
	}

	case PhysicalType::INT16: {
		auto internal_value = dval.GetValueUnsafe<int16_t>();
		mock_value = Value::SMALLINT(internal_value);
		break;
	}
	case PhysicalType::INT32: {
		auto internal_value = dval.GetValueUnsafe<int32_t>();
		mock_value = Value::INTEGER(internal_value);
		break;
	}
	case PhysicalType::INT64: {
		auto internal_value = dval.GetValueUnsafe<int64_t>();
		mock_value = Value::BIGINT(internal_value);
		break;
	}
	case PhysicalType::INT128: {
		auto internal_value = dval.GetValueUnsafe<hugeint_t>();
		mock_value = Value::HUGEINT(internal_value);
		break;
	}
	default:
		throw InternalException("Not accepted internal type for decimal");
	}
	hugeint_value = mock_value.GetValue<hugeint_t>();
	auto raw_value = GetRawValue(hugeint_value);

	dval.type().GetDecimalProperties(width, scale);

	allocated_decimal->set_scale(scale);
	allocated_decimal->set_precision(width);
	auto *decimal_value = new string();
	*decimal_value = raw_value;
	allocated_decimal->set_allocated_value(decimal_value);
	sval.set_allocated_decimal(allocated_decimal);
}
void ConstantTransformer::TransformHugeInt() {
	auto &sval = *sexpr.mutable_literal();
	auto *allocated_decimal = new ::substrait::Expression_Literal_Decimal();
	auto hugeint = dval.GetValueUnsafe<hugeint_t>();
	auto raw_value = GetRawValue(hugeint);
	allocated_decimal->set_scale(0);
	allocated_decimal->set_precision(38);

	auto *decimal_value = new string();
	*decimal_value = raw_value;
	allocated_decimal->set_allocated_value(decimal_value);
	sval.set_allocated_decimal(allocated_decimal);
}
void ConstantTransformer::TransformSmallInt() {
	auto &sval = *sexpr.mutable_literal();
	sval.set_i16(dval.GetValue<int16_t>());
}

void ConstantTransformer::TransformFloat() {
	auto &sval = *sexpr.mutable_literal();
	sval.set_fp32(dval.GetValue<float>());
}
void ConstantTransformer::TransformTime() {
	auto &sval = *sexpr.mutable_literal();
	sval.set_time(dval.GetValue<dtime_t>().micros);
}
void ConstantTransformer::TransformInterval() {
	// Substrait supports two types of INTERVAL (interval_year and interval_day)
	// whereas DuckDB INTERVAL combines both in one type. Therefore intervals
	// containing both months and days or seconds will lose some data
	// unfortunately. This implementation opts to set the largest interval value.
	auto &sval = *sexpr.mutable_literal();
	auto months = dval.GetValue<interval_t>().months;
	if (months != 0) {
		auto interval_year = make_unique<substrait::Expression_Literal_IntervalYearToMonth>();
		interval_year->set_months(months);
		sval.set_allocated_interval_year_to_month(interval_year.release());
	} else {
		auto interval_day = make_unique<substrait::Expression_Literal_IntervalDayToSecond>();
		interval_day->set_days(dval.GetValue<interval_t>().days);
		interval_day->set_microseconds(dval.GetValue<interval_t>().micros);
		sval.set_allocated_interval_day_to_second(interval_day.release());
	}
}

void ConstantTransformer::TransformTimestamp() {
	auto &sval = *sexpr.mutable_literal();
	sval.set_string(dval.ToString());
}
void ConstantTransformer::TransformEnum() {
	auto &sval = *sexpr.mutable_literal();
	sval.set_string(dval.ToString());
}

string ConstantTransformer::GetRawValue(hugeint_t value) {
	std::string str;
	str.reserve(16);
	auto *byte = (uint8_t *)&value.lower;
	for (idx_t i = 0; i < 8; i++) {
		str.push_back((char)byte[i]);
	}
	byte = (uint8_t *)&value.upper;
	for (idx_t i = 0; i < 8; i++) {
		str.push_back((char)byte[i]);
	}

	return str;
}