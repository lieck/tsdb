
#include <fstream>
#include "db/table.h"
#include "util/coding.h"
#include "common/config.h"
#include "disk/disk_manager.h"
#include "sstable/sstable_builder.h"
#include "db/background.h"

namespace ljdb {

Table::Table(std::string &tableName, const Schema &schema, BackgroundTask *bgTask) : table_name_(tableName), schema_(schema), bg_task_(bgTask) {
    // 计算 row 大约占据的空间
    for(auto &s : schema.columnTypeMap) {
        if(s.second == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
            approximate_row_size_ += 8;
        } else if(s.second == LindormContest::COLUMN_TYPE_INTEGER) {
            approximate_row_size_ += 4;
        }
    }
}



auto Table::Upsert(const WriteRequest &wReq) -> int {
    std::lock_guard<std::mutex> lock(mutex_);

    // 若其他线程正在写入, 则等待


    // 将数据写入 memtable
    for(auto &row : wReq.rows) {
        // 判断 mem 是否写满
        if(mem_->ApproximateSize() >= K_MEM_TABLE_SIZE_THRESHOLD) {
            while(imm_ != nullptr) {
                // 触发压缩

            }

            imm_ = std::move(mem_);
            mem_ = std::make_unique<MemTable>();
        }

        mem_->Insert(row);
    }

    return 0;
}

auto Table::ExecuteLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) -> int {

    return 0;
}

auto Table::ExecuteTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) -> int {
    auto query = RangeQueryRequest(&trReadReq.vin, trReadReq.timeLowerBound, trReadReq.timeUpperBound,
                                   &trReadReq.requestedColumns, &trReadRes);


    return 0;
}


auto Table::MemTableQuery(Table::QueryRequest &req, const std::shared_ptr<MemTable>& mem) -> void {
//    auto vin_iter = req.vin_.begin();
//    auto iter = mem->NewIterator();
//    iter->Seek(InternalKey(*vin_iter, MAX_TIMESTAMP));
//
//    while(iter->Valid() && vin_iter != req.vin_.end()) {
//        if(iter->GetKey().vin_ == *vin_iter) {
//            Row row = CodingUtil::DecodeRow(iter->GetValue().data(), schema_, req.columns_);
//            row.vin = iter->GetKey().vin_;
//            row.timestamp = iter->GetKey().timestamp_;
//            req.result_->push_back(row);
//
//            auto tmp = vin_iter;
//            ++vin_iter;
//            req.vin_.erase(tmp);
//        }
//
//        iter->Next();
//    }
}

auto Table::MemTableRangeQuery(Table::RangeQueryRequest &req, const std::shared_ptr<MemTable>& mem) -> void {
//    auto iter = mem->NewIterator();
//    iter->Seek(InternalKey(*req.vin_, req.time_upper_bound_));
//    while(iter->Valid()) {
//        auto key = iter->GetKey();
//        if(key.vin_ != *req.vin_ || key.timestamp_ < req.time_lower_bound_) {
//            break;
//        }
//
//        if(req.time_set_.count(key.timestamp_) == 0) {
//            req.time_set_.insert(key.timestamp_);
//
//            Row row = CodingUtil::DecodeRow(iter->GetValue().data(), schema_, req.columns_);
//            row.vin = key.vin_;
//            row.timestamp = key.timestamp_;
//            req.result_->push_back(row);
//        }
//
//        iter->Next();
//    }
}

auto Table::SSTableQuery(size_t level, Table::QueryRequest &req) -> void {
    // 搜索 L0
    if(level == 0) {
        // TODO 默认搜索全部 L0 文件, 但可以搜索指定的 L0 文件
        auto l0_set = table_meta_data_.GetFileMetaData(0);
        for(auto iter = l0_set.rbegin(); iter != l0_set.rend(); iter++) {
            auto file = DiskManager::OpenSSTableFile((*iter)->file_number_);
            
        }
    }
}


void Table::MaybeScheduleCompaction() {
    bg_task_->Schedule(&Table::BGWork, this);
}

void Table::BGWork(void *table) {
    reinterpret_cast<Table*>(table)->BackgroundCall();
}

void Table::BackgroundCall() {

}

auto Table::BackgroundCompaction() -> void {
    if(imm_ != nullptr) {
        CompactMemTable();
    }



}

auto Table::CompactMemTable() -> void {
    ASSERT(imm_ != nullptr, "imm_ is nullptr");
    auto file_number = table_meta_data_.NextFileNumber();

    SStableBuilder builder(file_number);
    auto iter = imm_->NewIterator();
    iter->SeekToFirst();

    InternalKey start_key = iter->GetKey();
    InternalKey end_key;

    while(iter->Valid()) {
        end_key = iter->GetKey();
        std::string value = iter->GetValue();
        builder.Add(iter->GetKey(), value);
        iter->Next();
    }

    auto sstable = builder.Builder();

    FileMetaData file_meta_data(file_number, start_key, end_key);
    table_meta_data_.AddFileMetaData(0, file_meta_data);
}




}; // namespace ljdb