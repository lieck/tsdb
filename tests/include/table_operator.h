#include <unordered_map>
#include <utility>


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

        auto GenerateTable(DBOptions *options) -> Table {
            return Table("test", schema_, options);
        }

        auto GenerateWriteRequest(int64_t start_key, int limit, timestamp_t timestamp) -> WriteRequest {
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

    private:
        std::string name_;
        TestSchemaType type_;
        Schema schema_;

        std::map<Vin, std::unordered_map<timestamp_t, Row>> data_{};
    };


} // namespace LindormContest