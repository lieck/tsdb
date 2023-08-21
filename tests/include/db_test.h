#include "TSDBEngineImpl.h"
#include "common/logger.h"
#include "test_util.h"
#include "test_tsdb_engine.h"
#include "memory_engine.h"

namespace LindormContest {

class TestSchema {
public:
    TestSchema(TestSchemaType type) : schema_(GenerateSchema(type)) {}

    auto GetSchema() const -> const Schema & {
        return schema_;
    }

    // 生成 500 行的数据
    auto GenerateUpsetRequest() -> WriteRequest {
        WriteRequest write_request;
        write_request.tableName = "test";

        for(int i = 0; i < 500; i++) {
            int64_t key = rand() % 10000;
            int64_t timestamp = rand() % 100000000;

            Row row;
            row.timestamp = timestamp;
            row.vin = GenerateVin(key);
            GenerateRandomRow(schema_, row);

            write_request.rows.emplace_back(std::move(row));
        }


        return write_request;
    }

    auto GenerateLatestQueryRequest(int query_count) -> LatestQueryRequest {
        LatestQueryRequest latest_query_request;
        latest_query_request.tableName = "test";
        latest_query_request.requestedColumns = GetRandomColumns(schema_);

        for(int i = 0; i < query_count; i++) {
            latest_query_request.vins.push_back(GenerateVin(rand() % 10000));
        }

        return latest_query_request;
    }

    auto GenerateRangeQueryRequest() -> TimeRangeQueryRequest {
        TimeRangeQueryRequest range_query_request;
        range_query_request.tableName = "test";
        range_query_request.requestedColumns = GetRandomColumns(schema_);

        range_query_request.vin = GenerateVin(rand() % 10000);

        int64_t start = rand() % 100000000;
        int64_t end = start + rand() % 10000;

        range_query_request.timeLowerBound = start;
        range_query_request.timeUpperBound = end;
        return range_query_request;
    }



private:
    Schema schema_;
};


enum CheckDBType {
    Memory,
    Disk,
};

// 目前仅支持对一张表测试
class DBTest {
public:
    std::string db_dir_ = "./test_db_data";
    std::string db_check_dir_ = "./check_db_data";

    explicit DBTest(CheckDBType type)  {
        if(DirectoryExists(db_check_dir_)) {
            RemoveDirectory(db_check_dir_);
        }
        CreateDirectory(db_check_dir_);

        if(DirectoryExists(db_dir_)) {
            RemoveDirectory(db_dir_);
        }
        CreateDirectory(db_dir_);


        if(type == CheckDBType::Disk) {
            check_engine_ = new TestTSDBEngine(db_check_dir_);
        } else if(type == CheckDBType::Memory) {
            check_engine_ = new MemoryEngine(db_check_dir_);
        }

        test_engine_ = new TSDBEngineImpl(db_dir_);
    }

    void Init(TestSchemaType type) {
        ASSERT_EQ(test_engine_->connect(), 0);
        ASSERT_EQ(check_engine_->connect(), 0);

        ASSERT_EQ(test_engine_->createTable("test", GenerateSchema(type)), 0);
        ASSERT_EQ(check_engine_->createTable("test", GenerateSchema(type)), 0);
    }

    void Upsert(const WriteRequest &writeRequest) {
        std::unique_lock<std::shared_mutex> lock(rw_lock_);
        ASSERT_EQ(check_engine_->upsert(writeRequest), 0);

        auto start_time = std::chrono::high_resolution_clock::now();
        ASSERT_EQ(test_engine_->upsert(writeRequest), 0);
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

        write_time_ += duration;
        write_row_count_ += writeRequest.rows.size();
    }

    void ExecuteLatestQuery(const LatestQueryRequest &pReadReq) {
        std::shared_lock<std::shared_mutex> lock(rw_lock_);

        std::vector<Row> check_results;
        ASSERT_EQ(check_engine_->executeLatestQuery(pReadReq, check_results), 0);

        auto start_time = std::chrono::high_resolution_clock::now();
        std::vector<Row> test_results;
        ASSERT_EQ(test_engine_->executeLatestQuery(pReadReq, test_results), 0);
        auto end_time = std::chrono::high_resolution_clock::now();

        ASSERT_EQ(check_results.size(), test_results.size());

        std::map<std::pair<Vin, int64_t>, Row> check_map;
        for(auto & check_result : check_results) {
            check_map[std::make_pair(check_result.vin, check_result.timestamp)] = check_result;
        }

        for(int i = 0; i < test_results.size(); ++i) {
            auto row = check_map[std::make_pair(test_results[i].vin, test_results[i].timestamp)];
            ASSERT_EQ(test_results[i].vin, row.vin);
            ASSERT_EQ(test_results[i].timestamp, row.timestamp);
            // ASSERT_EQ(check_results[i].columns, row.columns);
        }

        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
        query_time_ += duration;
    }

    void ExecuteTimeRangeQuery(const TimeRangeQueryRequest &trReadReq) {
        std::shared_lock<std::shared_mutex> lock(rw_lock_);

        std::vector<Row> check_results;
        ASSERT_EQ(check_engine_->executeTimeRangeQuery(trReadReq, check_results), 0);

        auto start_time = std::chrono::high_resolution_clock::now();
        std::vector<Row> test_results;
        ASSERT_EQ(test_engine_->executeTimeRangeQuery(trReadReq, test_results), 0);
        auto end_time = std::chrono::high_resolution_clock::now();

        // 检查结果
        ASSERT_EQ(check_results.size(), test_results.size());

        std::map<std::pair<Vin, int64_t>, Row> check_map;
        for(auto & check_result : check_results) {
            check_map[std::make_pair(check_result.vin, check_result.timestamp)] = check_result;
        }

        for(int i = 0; i < test_results.size(); ++i) {
            auto row = check_map[std::make_pair(test_results[i].vin, test_results[i].timestamp)];
            ASSERT_EQ(test_results[i].vin, row.vin);
            ASSERT_EQ(test_results[i].timestamp, row.timestamp);
            // ASSERT_EQ(check_results[i].columns, row.columns);
        }


        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
        query_time_ += duration;
    }

    void Restart() {
        Shutdown();
        delete test_engine_;
        test_engine_ = new TSDBEngineImpl(db_dir_);
        ASSERT_EQ(test_engine_->connect(), 0);
    }

    void Shutdown() {
        auto start_time = std::chrono::high_resolution_clock::now();
        ASSERT_EQ(test_engine_->shutdown(), 0);
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
        LOG_INFO("shutdown time: %ld ms", duration);
    }

    void EndTest() {
        Shutdown();
        ASSERT_EQ(check_engine_->shutdown(), 0);

        delete test_engine_;
        delete check_engine_;

        std::cout << "write row count: " << write_row_count_ << std::endl;
        std::cout << "write time: " << write_time_ << std::endl;
        std::cout << "query time: " << query_time_ << std::endl;
    }

private:
    std::shared_mutex rw_lock_{};

    int64_t write_row_count_{0};

    int64_t write_time_{0};
    int64_t query_time_{0};


    TSDBEngine *check_engine_;
    TSDBEngine *test_engine_;
};




} // namespace LindormContest