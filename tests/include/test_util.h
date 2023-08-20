#pragma once
#include "db/table.h"
#include "TSDBEngineImpl.h"
#include <filesystem>

namespace LindormContest {

enum TestSchemaType {
    Basic,
    Complex,
    Long
};

auto GenerateSchema(TestSchemaType type) -> Schema {
    Schema schema;
    if(type == TestSchemaType::Basic) {
        schema.columnTypeMap["c1"] = LindormContest::ColumnType::COLUMN_TYPE_INTEGER;
    } else if(type == TestSchemaType::Complex) {
        schema.columnTypeMap["c1"] = LindormContest::ColumnType::COLUMN_TYPE_INTEGER;
        schema.columnTypeMap["c2"] = LindormContest::ColumnType::COLUMN_TYPE_STRING;
        schema.columnTypeMap["c3"] = LindormContest::ColumnType::COLUMN_TYPE_DOUBLE_FLOAT;
    } else if(type == TestSchemaType::Long) {
        schema.columnTypeMap["col_integer_1"] = LindormContest::ColumnType::COLUMN_TYPE_INTEGER;
        schema.columnTypeMap["col_integer_2"] = LindormContest::ColumnType::COLUMN_TYPE_INTEGER;
        schema.columnTypeMap["col_integer_3"] = LindormContest::ColumnType::COLUMN_TYPE_INTEGER;
        schema.columnTypeMap["col_string_1"] = LindormContest::ColumnType::COLUMN_TYPE_STRING;
        schema.columnTypeMap["col_string_2"] = LindormContest::ColumnType::COLUMN_TYPE_STRING;
        schema.columnTypeMap["col_string_3"] = LindormContest::ColumnType::COLUMN_TYPE_STRING;
        schema.columnTypeMap["col_double_1"] = LindormContest::ColumnType::COLUMN_TYPE_DOUBLE_FLOAT;
        schema.columnTypeMap["col_double_2"] = LindormContest::ColumnType::COLUMN_TYPE_DOUBLE_FLOAT;
        schema.columnTypeMap["col_double_3"] = LindormContest::ColumnType::COLUMN_TYPE_DOUBLE_FLOAT;
    }

    return schema;
}


auto GetRequestedColumns(TestSchemaType type) -> std::set<std::string> {
    std::set<std::string> ret;
    if(type == TestSchemaType::Basic) {
        ret.insert("c1");
    } else if(type == TestSchemaType::Complex) {
        ret.insert("c1");
        ret.insert("c2");
        ret.insert("c3");
    } else if(type == TestSchemaType::Long) {
        ret.insert("col_integer_1");
        ret.insert("col_integer_2");
        ret.insert("col_integer_3");
        ret.insert("col_string_1");
        ret.insert("col_string_2");
        ret.insert("col_string_3");
        ret.insert("col_double_1");
        ret.insert("col_double_2");
        ret.insert("col_double_3");
    }

    return ret;
}

auto GenerateTestRow(Row &row, TestSchemaType type) {
    if(type == TestSchemaType::Basic) {
        row.columns["c1"] = ColumnValue(1);
    } else if(type == TestSchemaType::Complex) {
        row.columns["c1"] = ColumnValue(1);
        row.columns["c2"] = ColumnValue("string_value_1_1");
        row.columns["c3"] = ColumnValue(3.0);
    } else if(type == TestSchemaType::Long) {
        row.columns["col_integer_1"] = ColumnValue(1);
        row.columns["col_integer_2"] = ColumnValue(2);
        row.columns["col_integer_3"] = ColumnValue(3);
        row.columns["col_string_1"] = ColumnValue("string_value_1_1");
        row.columns["col_string_2"] = ColumnValue("string_value_1_2");
        row.columns["col_string_3"] = ColumnValue("string_value_1_3");
        row.columns["col_double_1"] = ColumnValue(1.0);
        row.columns["col_double_2"] = ColumnValue(2.0);
        row.columns["col_double_3"] = ColumnValue(3.0);
    }
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

void CreateDirectory(const std::string &directory) {
    namespace fs = std::filesystem;
    try {
        if (fs::create_directory(directory)) {
            std::cout << "Directory created successfully." << std::endl;
        } else {
            std::cout << "Failed to create directory." << std::endl;
        }
    } catch (const std::exception& ex) {
        std::cerr << "An error occurred: " << ex.what() << std::endl;
    }
}

void RemoveDirectory(const std::string& directory) {
    namespace fs = std::filesystem;
    try {
        fs::remove_all(directory);
        std::cout << "Directory and its contents have been deleted." << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "An error occurred: " << ex.what() << std::endl;
    }
}

auto DirectoryExists(const std::string& directory) -> bool {
    namespace fs = std::filesystem;
    return fs::exists(directory) && fs::is_directory(directory);
}

auto CreateTestTSDBEngine() -> TSDBEngine* {
    std::string test_directory = "./db";
    if(DirectoryExists(test_directory)) {
        RemoveDirectory(test_directory);
    }
    CreateDirectory(test_directory);

    auto engine = new TSDBEngineImpl(test_directory);
    return engine;
}



}  // namespace LindormContest