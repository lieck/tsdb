//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include "TSDBEngineImpl.h"
#include "common/exception.h"
#include "common/logger.h"
#include <unistd.h>
#include <fcntl.h>


namespace LindormContest {

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    TSDBEngineImpl::TSDBEngineImpl(const std::string &dataDirPath)
            : TSDBEngine(dataDirPath) {
        LOG_INFO("db engine construct");

        // 获取初始工作目录的文件描述符
        initialDirFd_ = open(".", O_RDONLY);
        if (initialDirFd_ == -1) {
            std::cerr << "Failed to open the initial working directory." << std::endl;
            std::terminate();
        }


        int result = chdir(dataDirPath.c_str());
        if(result != 0) {
            LOG_ERROR("chdir failed");
            throw Exception(ExceptionType::IO, "chdir failed");
        }
    }

    auto TSDBEngineImpl::connect() -> int {
        LOG_INFO("db engine connect");

        if(shutdown_.load(std::memory_order_acquire)) {
            LOG_INFO("connect() called after shutdown");
            return -1;
        }

        std::scoped_lock<std::mutex> lock(mutex_);
        if(db_option_ == nullptr) {
            db_option_ = NewDBOptions();
        }

        if(tables_.empty()) {
            std::ifstream file;
            try {
                file = DiskManager::OpenFile("manifest_file");
            } catch (Exception &e) {
                if(e.Type() == ExceptionType::IO) {
                    LOG_INFO("manifest_file not found");
                    return 0;
                }
                LOG_ERROR("manifest_file open failed");
                return -1;
            }


            try {
                file.seekg(0, std::ios::end);
                auto file_size = file.tellg();
                file.seekg(0, std::ios::beg);

                // 读取 file number
                char buffer[4];
                file.read(buffer, 4);
                db_option_->next_file_number_.store(CodingUtil::DecodeUint32(buffer), std::memory_order_release);

                while(file.tellg() < file_size) {
                    auto table = new Table("", Schema(), db_option_);
                    table->ReadMetaData(file);
                    tables_.emplace(table->GetTableName(), table);
                }
            } catch (Exception &e) {
                file.close();
                return -1;
            }
            file.close();
        }
        return 0;
    }

    auto TSDBEngineImpl::createTable(const std::string &tableName, const Schema &schema) -> int {
        if(shutdown_.load(std::memory_order_acquire)) {
            return -1;
        }

        std::unique_lock<std::mutex> lock(mutex_);
        if(tables_.count(tableName) == 1) {
            return -1;
        }

        if(tables_.find(tableName) != tables_.end()) {
            return -1;
        }

        tables_.emplace(tableName, new Table(tableName, schema, db_option_));
        return 0;
    }

    auto TSDBEngineImpl::shutdown() -> int {
        LOG_INFO("Shutting down");

        // 通知关闭
        for(auto &table : tables_) {
            table.second->Shutdown();
        }

        // 等待后台任务退出
        db_option_->bg_task_->WaitForEmptyQueue();

        // 写入元数据
        try {
            std::ofstream file("manifest_file", std::ofstream::trunc);
            if(!file.is_open()) {
                LOG_ERROR("manifest_file open failed");
                return -1;
            }

            // 写入 file number
            char buffer[4];
            CodingUtil::EncodeValue(buffer, db_option_->next_file_number_.load(std::memory_order_acquire));
            file.write(buffer, 4);

            for(auto &table : tables_) {
                table.second->WriteMetaData(file);
            }
            file.flush();
            file.close();
        } catch (Exception &e) {
            LOG_ERROR("manifest_file write failed");
            return -1;
        }

        // 恢复默认工作目录
        if (fchdir(initialDirFd_) != 0) {
            std::cerr << "Failed to restore the initial working directory." << std::endl;
            std::terminate();
        }
        close(initialDirFd_);

        return 0;
    }

    auto TSDBEngineImpl::upsert(const WriteRequest &writeRequest) -> int {
        if(shutdown_.load(std::memory_order_acquire)) {
            return -1;
        }

        std::unique_lock<std::mutex> lock(mutex_);
        auto table = tables_.find(writeRequest.tableName);
        if(table == tables_.end()) {
            return -1;
        }

        int result = table->second->Upsert(writeRequest);
        return result;
    }

    auto TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) -> int {
        LOG_INFO("executeLatestQuery");
        if(shutdown_.load(std::memory_order_acquire)) {
            return -1;
        }

        std::unique_lock<std::mutex> lock(mutex_);
        auto table = tables_.find(pReadReq.tableName);
        if(table == tables_.end()) {
            return -1;
        }

        int result = table->second->ExecuteLatestQuery(pReadReq, pReadRes);
        return result;
    }

    auto TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) -> int {
        LOG_INFO("executeTimeRangeQuery");
        if(shutdown_.load(std::memory_order_acquire)) {
            return -1;
        }

        std::unique_lock<std::mutex> lock(mutex_);
        auto table = tables_.find(trReadReq.tableName);
        if(table == tables_.end()) {
            return -1;
        }

        int result = table->second->ExecuteTimeRangeQuery(trReadReq, trReadRes);
        return result;
    }

    TSDBEngineImpl::~TSDBEngineImpl() {
        delete db_option_;
    }

} // End namespace LindormContest.