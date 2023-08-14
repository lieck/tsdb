//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include "TSDBEngineImpl.h"

namespace LindormContest {

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    TSDBEngineImpl::TSDBEngineImpl(const std::string &dataDirPath)
            : TSDBEngine(dataDirPath) {

    }

    auto TSDBEngineImpl::connect() -> int {
        // 读取元数据文件
        ljdb::SSTable meta_data_file(0, 0, nullptr);
        auto iter = meta_data_file.NewIterator();
        for(iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            auto table_name = iter->GetKey();
            auto table_meta_data = ljdb::TableMetaData::Decode(iter->value().ToString());
            tables_.emplace(table_name, new ljdb::Table(table_name, table_meta_data.schema_, table_meta_data));
        }


        if(db_option_ == nullptr) {
            db_option_ = new ljdb::DBOptions();
            db_option_->table_cache_ = new ljdb::TableCache(1024);
            db_option_->block_cache_ = new ljdb::Cache<ljdb::Block>(1 << 30);
            db_option_->bg_task_ = new ljdb::BackgroundTask();
            db_option_->next_file_number_ = 0;
        }
        return 0;
    }

    auto TSDBEngineImpl::createTable(const std::string &tableName, const Schema &schema) -> int {
        std::unique_lock<std::mutex> lock(mutex_);
        if(tables_.count(tableName) == 1) {
            return -1;
        }
        tables_.emplace(tableName, ljdb::Table(tableName, schema, nullptr));
        return 0;
    }

    auto TSDBEngineImpl::shutdown() -> int {


        return 0;
    }

    auto TSDBEngineImpl::upsert(const WriteRequest &writeRequest) -> int {
        std::unique_lock<std::mutex> lock(mutex_);
        auto table = tables_.find(writeRequest.tableName);
        if(table == tables_.end()) {
            return -1;
        }
        lock.unlock();

        return table->second->Upsert(writeRequest);
    }

    auto TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) -> int {
        std::unique_lock<std::mutex> lock(mutex_);
        auto table = tables_.find(pReadReq.tableName);
        if(table == tables_.end()) {
            return -1;
        }
        lock.unlock();

        return table->second->ExecuteLatestQuery(pReadReq, pReadRes);
    }

    auto TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) -> int {
        std::unique_lock<std::mutex> lock(mutex_);
        auto table = tables_.find(trReadReq.tableName);
        if(table == tables_.end()) {
            return -1;
        }
        lock.unlock();

        return table->second->ExecuteTimeRangeQuery(trReadReq, trReadRes);
    }

    TSDBEngineImpl::~TSDBEngineImpl() {
        delete db_option_;
    }

} // End namespace LindormContest.