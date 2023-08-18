#include <unordered_map>
#include <utility>
#include <algorithm>


namespace LindormContest {

    using timestamp_t = int64_t;

    class TestTableOperator {
        friend Table;

    public:
        explicit TestTableOperator(std::string name, TestSchemaType type) : name_(std::move(name)), type_(type) {
            if(type == TestSchemaType::Basic) {
                schema_.columnTypeMap["c1"] = LindormContest::ColumnType::COLUMN_TYPE_INTEGER;
            }
        }

        ~TestTableOperator() {
            delete table_;
        }

        auto GenerateTable(DBOptions *options) -> Table* {
            table_ = new Table("test", schema_, options);
            return table_;
        }

        auto GenerateWriteRequest(int64_t start_key, int limit, timestamp_t timestamp) -> WriteRequest {
            std::scoped_lock<std::mutex> lock(mutex_);

            WriteRequest wr;
            wr.tableName = "test";
            for(int64_t i = 0; i < limit; i++) {
                Row row;
                row.vin = GenerateVin(start_key + i);
                row.timestamp = timestamp;

                if(current_schema_type == TestSchemaType::Basic) {
                    row.columns["c1"] = ColumnValue(static_cast<int32_t>((start_key + i) & 0x7fffffff));
                }

                wr.rows.push_back(row);

                data_[row.vin][row.timestamp] = row;
            }

            return wr;
        }


        auto GenerateLatestQueryRequest(int64_t start_key, int limit) -> LatestQueryRequest {
            LatestQueryRequest qr;
            qr.tableName = name_;
            for(int64_t i = 0; i < limit; i++) {
                qr.vins.push_back(GenerateVin(start_key + i));
            }

            if(type_ == TestSchemaType::Basic) {
                qr.requestedColumns = {"c1"};
            }

            return qr;
        }


        auto GenerateTimeRangeQueryRequest(int64_t key, int64_t start_time, int64_t end_time) -> TimeRangeQueryRequest {
            std::scoped_lock<std::mutex> lock(mutex_);

            TimeRangeQueryRequest qr;
            qr.tableName = name_;
            qr.vin = GenerateVin(key);
            qr.timeLowerBound = start_time;
            qr.timeUpperBound = end_time;

            if(type_ == TestSchemaType::Basic) {
                qr.requestedColumns = {"c1"};
            }

            return qr;
        }

        void CheckQuery(std::vector<Row> & results, int64_t start_key, int limit, timestamp_t timestamp = 0) {
            std::scoped_lock<std::mutex> lock(mutex_);

            ASSERT(limit > 0, "limit must be positive");
            ASSERT_EQ(results.size(), limit) << "results.size() = " << results.size() << ", limit = " << limit;

            sort(results.begin(), results.end(), [](const Row & a, const Row & b) {
                return a.vin < b.vin;
            });

            std::set<Vin> check_vins;
            for(int i = 0; i < limit; i++) {
                check_vins.insert(GenerateVin(start_key + i));
            }

            size_t i = 0;
            for(const auto &vin : check_vins) {
                ASSERT_TRUE(i < results.size());
                auto & row = results[i];
                ASSERT_EQ(row.vin, vin)
                    << "vin = " << row.vin.vin << ", expected = " << vin.vin;
                if(timestamp != 0) {
                    ASSERT_EQ(row.timestamp, timestamp);
                }

                if(data_[row.vin].count(row.timestamp) == 0) {
                    ASSERT_TRUE(false) << "vin = " << row.vin.vin << ", timestamp = " << row.timestamp
                                       << " not found in data_";
                } else {
                    auto check_row = data_[row.vin][row.timestamp];
                    ASSERT_EQ(row.vin, check_row.vin);
                    ASSERT_EQ(row.timestamp, check_row.timestamp);
                    ASSERT_EQ(row.columns.size(), check_row.columns.size());
                    for(auto & column : row.columns) {
                        ASSERT_EQ(column.second, check_row.columns[column.first]);
                    }
                }
                i++;
            }
        }

        void CheckRangeQuery(const std::vector<Row> &result, int64_t key, int64_t start_time, int64_t end_time) {
            ASSERT_EQ(result.size(), end_time - start_time);
            std::set<int64_t> check_timestamps;
            for(auto & row : result) {
                ASSERT_EQ(row.vin, GenerateVin(key));
                ASSERT_GE(row.timestamp, start_time);
                ASSERT_LE(row.timestamp, end_time);
                ASSERT_TRUE(check_timestamps.count(row.timestamp) == 0);
                check_timestamps.insert(row.timestamp);
            }
        }

        void CheckLastQuery(std::vector<Row> & results, LatestQueryRequest &rq, bool check_value) {
            std::scoped_lock<std::mutex> lock(mutex_);

            std::set<Vin> keys;
            for(auto & vin : rq.vins) {
                if(data_.count(vin) != 0 && data_[vin].size() > 0) {
                    keys.insert(vin);
                }
            }

            ASSERT_EQ(results.size(), keys.size());
            for(auto & row : results) {
                ASSERT_TRUE(keys.count(row.vin) != 0);

                // check timestamp
                ASSERT_EQ(row.timestamp, data_[row.vin].rbegin()->first);

                if(check_value) {
                    ASSERT_EQ(row.columns.size(), rq.requestedColumns.size());

                    auto check_row = data_[row.vin][row.timestamp];
                    for(auto & column : row.columns) {
                        ASSERT_EQ(column.second, check_row.columns[column.first]);
                    }
                }
            }
        }

        void CheckRangeQuery(const std::vector<Row> &results, TimeRangeQueryRequest &qr, bool check_value) {
            std::scoped_lock<std::mutex> lock(mutex_);
            if(data_.count(qr.vin) == 0) {
                ASSERT_EQ(results.size(), 0);
                return;
            }

            auto mp = data_[qr.vin];

            std::map<int64_t, Row> mp_results;
            for(size_t i = 0; i < results.size(); i++) {
                mp_results[results[i].timestamp] = results[i];
            }

            int check_count = 0;
            for(auto &r : mp) {
                if(r.first >= qr.timeLowerBound && r.first < qr.timeUpperBound) {
                    check_count++;
                }
            }

            ASSERT_EQ(check_count, results.size());

            auto iter = mp_results.begin();


            auto check_iter = mp.lower_bound(qr.timeLowerBound);
            for(int i = 0; i < check_count; i++) {

                ASSERT_EQ(iter->second.vin, qr.vin) << "vin = " << iter->second.vin.vin
                                                     << ", expected = " << qr.vin.vin;
                ASSERT_EQ(iter->second.timestamp, check_iter->first);

                if(check_value) {
                    ASSERT_EQ(iter->second.columns.size(), qr.requestedColumns.size());
                    auto check_row = check_iter->second;
                    for(auto & column : iter->second.columns) {
                        ASSERT_EQ(column.second, check_row.columns[column.first]);
                    }
                }

                check_iter++;
                iter++;
            }

        }

    private:
        std::string name_;
        TestSchemaType type_;
        Schema schema_;
        Table *table_;

        std::mutex mutex_;

        std::map<Vin, std::map<timestamp_t, Row>> data_{};
    };


} // namespace LindormContest