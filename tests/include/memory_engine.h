
#include "TSDBEngine.hpp"

#include <fstream>
#include <sstream>
#include <filesystem>

namespace LindormContest {

    class MemoryEngine : public TSDBEngine {
    public:

        explicit MemoryEngine(const std::string &dataDirPath) : TSDBEngine(dataDirPath) {}

        int connect() override {
            return 0;
        }

        int createTable(const std::string &tableName, const Schema &schema) override {
            return 0;
        }

        int shutdown() override {
            return 0;
        }

        int upsert(const WriteRequest &wReq) override {
            std::scoped_lock<std::mutex> lock(lock_);
            for(auto &row : wReq.rows) {
                data_[row.vin][row.timestamp] = 1;
            }
            return 0;
        }

        int executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) override {
            std::scoped_lock<std::mutex> lock(lock_);
            for(auto &vin : pReadReq.vins) {
                auto &vin_data = data_[vin];
                if(vin_data.empty()) {
                    continue;
                }

                auto it = vin_data.rbegin();
                if(it != vin_data.rend()) {
                    Row row;
                    row.vin = vin;
                    row.timestamp = it->first;
                    pReadRes.push_back(row);
                }

            }
            return 0;
        }

        int executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) override {
            std::scoped_lock<std::mutex> lock(lock_);
            auto &vin_data = data_[trReadReq.vin];
            auto it = vin_data.lower_bound(trReadReq.timeLowerBound);
            while(it != vin_data.end() && it->first < trReadReq.timeUpperBound) {
                Row row;
                row.vin = trReadReq.vin;
                row.timestamp = it->first;
                trReadRes.push_back(row);
                it++;
            }
            return 0;
        }

        ~MemoryEngine() override = default;

    private:
        std::mutex lock_;
        std::map<Vin, std::map<int64_t, int>> data_;
    };
}
