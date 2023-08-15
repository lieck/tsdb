#pragma once

#include "common/namespace.h"
#include "db/table.h"

namespace LindormContest {

auto GenerateVin(int32_t key) -> Vin {
    Vin ret;
    for(char & i : ret.vin) {
        i = 'x';
    }

    auto k = "key-" + std::to_string(key);
    k.copy(ret.vin, k.size());
    return ret;
}


auto GenerateKey(int32_t key) -> InternalKey {
    InternalKey ret;
    ret.timestamp_ = 0;
    for(char & i : ret.vin_.vin) {
        i = 'x';
    }

    auto k = "key-" + std::to_string(key);
    k.copy(ret.vin_.vin, k.size());
    return ret;
}


auto GenerateTable() -> Table {
    auto options = NewDBOptions();

    Schema schema;
    schema.columnTypeMap["c1"] = LindormContest::ColumnType::COLUMN_TYPE_INTEGER;

    return Table("test", schema, options);
}

auto GenerateWriteRequest() -> WriteRequest {
    WriteRequest wr;

    return wr;

}

} // namespace ljdb