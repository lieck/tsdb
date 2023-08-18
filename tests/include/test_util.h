#pragma once
#include "db/table.h"

namespace LindormContest {

enum TestSchemaType {
    Basic,
};

TestSchemaType current_schema_type = TestSchemaType::Basic;

auto GenerateSchema(TestSchemaType type) -> Schema {
    Schema schema;
    if(type == TestSchemaType::Basic) {
        schema.columnTypeMap["c1"] = LindormContest::ColumnType::COLUMN_TYPE_INTEGER;
    }

    return schema;
}

auto GenerateVin(int64_t key) -> Vin {
    Vin ret;
    for(char & i : ret.vin) {
        i = 'x';
    }
    ret.vin[16] = '\0';

    auto k = "key-" + std::to_string(key);
    k.copy(ret.vin, k.size());
    return ret;
}


auto GenerateKey(int32_t key) -> InternalKey {
    InternalKey ret;
    ret.timestamp_ = 0;
    ret.vin_ = GenerateVin(key);
    return ret;
}

auto GenerateRow(int64_t key) -> Row {
    Row row;
    row.vin = GenerateVin(key);
    row.timestamp = 0;
    row.columns["c1"] = ColumnValue(static_cast<int32_t>(key & 0x7fffffff));
    return row;
}





}  // namespace LindormContest