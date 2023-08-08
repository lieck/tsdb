#pragma once


#include <mutex>
#include <condition_variable>
#include <shared_mutex>
#include "common/macros.h"
#include "TSDBEngine.hpp"
#include "mem_table/mem_table.h"
#include "common/namespace.h"
#include "format.h"
#include "table_meta_data.h"
#include "background.h"
#include "compaction.h"

namespace ljdb {

class Table {
private:
    struct RangeQueryRequest {
        const Vin *vin_;
        int64_t time_lower_bound_;
        int64_t time_upper_bound_;

        const std::set<std::string> *columns_;
        std::vector<Row> *result_;

        std::set<int64_t> time_set_;

        RangeQueryRequest(const Vin *vin, int64_t time_lower_bound, int64_t time_upper_bound,
                          const std::set<std::string> *columns, std::vector<Row> *result)
         : vin_(vin), time_lower_bound_(time_lower_bound), time_upper_bound_(time_upper_bound), columns_(columns),
         result_(result) {}
    };

    struct QueryRequest {
        std::set<Vin> vin_;
        std::set<std::string> *columns_;
        std::vector<Row> *result_;

        QueryRequest(std::vector<Vin> &vins, std::set<std::string> *columns, std::vector<Row> *result)
         : columns_(columns), result_(result) {
            for(auto &vin : vins) {
                vin_.insert(vin);
            }
        }
    };

public:
    explicit Table(std::string &tableName, const Schema &schema, BackgroundTask *bgTask);
    ~Table();

    DISALLOW_COPY_AND_MOVE(Table);

    auto Upsert(const WriteRequest &wReq) -> int;

    auto Get(Vin vin, std::set<std::string> &columns, Row &row) -> bool;

    auto ExecuteLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) -> int;

    auto ExecuteTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) -> int;

private:
    auto MemTableQuery(QueryRequest &req, const std::shared_ptr<MemTable>& mem) -> void;

    auto MemTableRangeQuery(Table::RangeQueryRequest &req, const std::shared_ptr<MemTable>& mem) -> void;

    auto SSTableQuery(size_t level, QueryRequest &req) -> void;

    auto SSTableRangeQuery(size_t level, const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) -> void;

    // 后台线程任务
    static void BGWork(void* table);
    void BackgroundCall();

    // 要求：持有锁
    void MaybeScheduleCompaction();

    // 要求：持有锁
    auto BackgroundCompaction(std::unique_lock<std::mutex> &lock) -> void;

    // 执行 Manual Compaction
    auto DoManualCompaction(CompactionTask* task) -> bool;

    // 执行 Minor Compaction
    auto CompactMemTable(const std::shared_ptr<MemTable>& mem) -> FileMetaData*;

    std::string table_name_;
    Schema schema_;

    DBMetaData *db_meta_data_;

    std::mutex mutex_;
    std::condition_variable cv_;
    TableMetaData table_meta_data_;
    std::shared_ptr<MemTable> mem_;
    std::vector<std::shared_ptr<MemTable>> imm_;
    bool is_compaction_running_{false};

    std::queue<const WriteRequest*> write_queue_;
};

}; // namespace ljdb