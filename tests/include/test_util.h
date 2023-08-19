#pragma once
#include "db/table.h"
#include "TSDBEngineImpl.h"
#include <filesystem>

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
    std::string test_directory = "./db_test";
    if(DirectoryExists(test_directory)) {
        RemoveDirectory(test_directory);
    }
    CreateDirectory(test_directory);

    auto engine = new TSDBEngineImpl(test_directory);
    return engine;
}



}  // namespace LindormContest