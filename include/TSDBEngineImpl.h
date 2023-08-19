//
// You should modify this file.
//
// Refer TSDBEngineSample.h to ensure that you have understood
// the interface semantics correctly.
//

#ifndef LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
#define LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H

#include <unordered_map>
#include <mutex>
#include "TSDBEngine.hpp"
#include "db/background.h"
#include "db/table.h"

namespace LindormContest {

class TSDBEngineImpl : public TSDBEngine {
public:
    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    explicit TSDBEngineImpl(const std::string &dataDirPath);

    auto connect() -> int override;

    auto createTable(const std::string &tableName, const Schema &schema) -> int override;

    auto shutdown() -> int override;

    auto upsert(const WriteRequest &wReq) -> int override;

    auto executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) -> int override;

    auto executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) -> int override;

    ~TSDBEngineImpl() override;

private:
    DBOptions *db_option_{};

    std::atomic_bool shutdown_{false};

    std::mutex mutex_;
    std::unordered_map<std::string, Table*> tables_;
}; // End class TSDBEngineImpl.

}; // End namespace LindormContest.

#endif //LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
