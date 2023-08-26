//#include <gtest/gtest.h>
//#include "mem_table/mem_table.h"
//#include "test_util.h"
//#include "table_operator.h"
//#include "common/logger.h"
//
//namespace LindormContest {
//
//
//    TEST(MemTableTest, BasicAssertions) {
//        MemTable mem_table;
//        for (int64_t i = 0; i < 100; i++) {
//            mem_table.Insert(GenerateRow(i));
//        }
//
//        auto iter = mem_table.NewIterator();
//
//        std::set<Vin> vins;
//        for (int64_t i = 0; i < 100; i++) {
//            vins.insert(GenerateVin(i));
//        }
//
//        iter->SeekToFirst();
//        for (const auto &vin: vins) {
//            ASSERT_EQ(iter->Valid(), true);
//            ASSERT_EQ(iter->GetKey().vin_, vin)
//                                        << "got " << iter->GetKey().vin_.vin << " expected " << vin.vin;
//            iter->Next();
//        }
//    }
//
//
//    TEST(MemTableTest, WriteTable) {
//        auto options = NewDBOptions();
//
//        TestTableOperator test("test", TestSchemaType::Basic);
//        auto table = test.GenerateTable(options);
//
//        auto wr = test.GenerateWriteRequest(0, 100, 12);
//        ASSERT_EQ(table.Upsert(wr), 0) << "Upsert failed";
//
//        auto qr = test.GenerateLatestQueryRequest(0, 100);
//        std::vector<Row> results;
//        ASSERT_EQ(table.ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
//
//        test.CheckQuery(results, 0, 100);
//    }
//
//    TEST(MemTableTest, MultipleWrite) {
//        LOG_INFO("MultipleWrite");
//
//        auto options = NewDBOptions();
//
//        TestTableOperator test("test", TestSchemaType::Basic);
//        auto table = test.GenerateTable(options);
//
//        auto wr1 = test.GenerateWriteRequest(0, 100, 1);
//        ASSERT_EQ(table.Upsert(wr1), 0) << "Upsert failed";
//
//        auto wr2 = test.GenerateWriteRequest(0, 100, 15);
//        ASSERT_EQ(table.Upsert(wr2), 0) << "Upsert failed";
//
//        auto wr3 = test.GenerateWriteRequest(0, 100, 10);
//        ASSERT_EQ(table.Upsert(wr3), 0) << "Upsert failed";
//
//        auto qr = test.GenerateLatestQueryRequest(0, 100);
//        std::vector<Row> results;
//        ASSERT_EQ(table.ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
//
//        test.CheckQuery(results, 0, 100, 15);
//    }
//
//
//} // End namespace LindormContest.