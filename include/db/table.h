#pragma once


#include <mutex>
#include <condition_variable>
#include <shared_mutex>
#include <utility>
#include "common/macros.h"
#include "TSDBEngine.hpp"
#include "mem_table/mem_table.h"
#include "format.h"
#include "table_meta_data.h"
#include "background.h"
#include "compaction.h"

namespace LindormContest {

class Table {
private:
    struct RangeQueryRequest {
        const Vin vin_;
        int64_t time_lower_bound_;
        int64_t time_upper_bound_;

        InternalKey lower_bound_;
        InternalKey upper_bound_;

        const std::set<std::string> *columns_;
        std::vector<Row> *result_;

        std::set<int64_t> time_set_;

        RangeQueryRequest(const Vin vin, int64_t time_lower_bound, int64_t time_upper_bound,
                          const std::set<std::string> *columns, std::vector<Row> *result)
         : vin_(vin), time_lower_bound_(time_lower_bound), time_upper_bound_(time_upper_bound), columns_(columns),
         result_(result) {
            lower_bound_ = InternalKey(vin_, time_upper_bound);
            upper_bound_ = InternalKey(vin_, time_lower_bound);
        }
    };

    struct QueryRequest {
        const std::vector<Vin> *vins_;
        std::map<Vin, Row> vin_map_{};
        std::set<std::string> columns_;
        std::vector<Row> *result_;

        QueryRequest(const std::vector<Vin> *vins, std::set<std::string> columns, std::vector<Row> *result)
         : columns_(std::move(columns)), result_(result), vins_(vins) {

        }
    };

public:
    explicit Table(std::string tableName, Schema schema, DBOptions *options);
    ~Table() = default;

    DISALLOW_COPY_AND_MOVE(Table);

    auto GetTableName() -> std::string { return table_name_; }

    auto Upsert(const WriteRequest &wReq) -> int;

    auto ExecuteLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) -> int;

    auto ExecuteTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) -> int;

    auto Shutdown() -> int;

    // 从文件中读取元数据
    auto ReadMetaData(std::ifstream &file) -> void;

    // 写入元数据
    auto WriteMetaData(std::ofstream &file) const -> void;

    auto TestGetTableMetaData() -> TableMetaData& { return table_meta_data_; }

    void EraseSSTableFile();

private:
    // 查询 memtable 内符合时间范围的元素
    auto MemTableRangeQuery(Table::RangeQueryRequest &req, const std::shared_ptr<MemTable>& memtable) -> void;

    void FileTableRangeQuery(const FileMetaDataPtr& fileMetaData, Table::RangeQueryRequest &req);

    // 后台线程任务
    static void BGWork(void* table);
    void BackgroundCall();

    // 要求：持有锁
    void MaybeScheduleCompaction();

    // 要求：持有锁
    auto BackgroundCompaction(std::unique_lock<std::mutex> &lock) -> void;

    // 执行 Manual Compaction
    auto DoManualCompaction(CompactionTask* task) -> bool;

    void StartMemTableCompaction(const std::shared_ptr<MemTable> mem);

    // 执行 Minor Compaction
    auto CompactMemTable(const std::shared_ptr<MemTable>& mem) -> FileMetaDataPtr;

    std::string table_name_{};
    Schema schema_{};
    DBOptions *options_{nullptr};

    TableCache *table_cache_{nullptr};

    std::atomic_bool is_shutting_down_{false};

    // 下述是需要锁保护的变量
    std::mutex mutex_;
    std::condition_variable cv_;
    TableMetaData table_meta_data_;
    std::shared_ptr<MemTable> mem_;
    std::vector<std::shared_ptr<MemTable>> imm_;
    std::queue<const WriteRequest*> write_queue_;   // 写请求队列

    // 当前正在压缩的线程数量
    int32_t compaction_thread_count_{0};
};

}  // namespace LindormContest