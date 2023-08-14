#pragma once

#include <atomic>
#include <utility>
#include <algorithm>
#include <mutex>
#include <condition_variable>

#include "common/config.h"
#include "format.h"
#include "common/iterator.h"
#include "mem_table/mem_table.h"
#include "db_options.h"
#include "compaction.h"

namespace ljdb {

// 返回每层的最大文件大小
static auto MaxBytesForLevel(int level) -> double {
    double result = 10. * 1048576.0;
    while (level > 1) {
        result *= 10;
        level--;
    }
    return result;
}

enum CompactionType { SizeCompaction, SeekCompaction };

struct CompactionTask {
    // 压缩的目标层数
    int level_;

    // 压缩类型
    CompactionType type_;

    // 输出文件的最大大小
    uint64_t max_output_file_size_;

    // 压缩的输入文件
    std::vector<std::shared_ptr<FileMetaData>> input_files_[2];

    // 压缩的输出文件
    std::vector<std::shared_ptr<FileMetaData>> output_files_;
};

class TableMetaData {
public:
    auto GetFileMetaData(size_t level) const -> const std::vector<FileMetaDataPtr>& {
        return files_[level];
    }

    // 添加 SSTable
    void AddFileMetaData(int32_t level, const std::vector<FileMetaDataPtr> &file);

    // 删除 SSTable
    void RemoveFileMetaData(int32_t level, const std::vector<FileMetaDataPtr> &file);

    // 返回 level 层与 [begin, end] 存在重叠的文件
    void GetOverlappingInputs(int level, const InternalKey* begin, const InternalKey* end, std::vector<FileMetaDataPtr>* inputs);

    // 返回 level 层的文件总大小
    auto TotalFileSize(int level) -> uint64_t;

    // 是否存在压缩计划
    auto ExistCompactionTask() -> bool {
        return size_compaction_score_ >= 1 || seek_compaction_level_ != -1;
    }

    // 确定下一次 Size Compaction 计划
    // 需要获取 TableMetaData 的锁
    void Finalize();

    // Seek Compaction 计划
    // 需要获取 TableMetaData 的锁
    void SeekCompaction(int level, FileMetaDataPtr meta_data);

    void ClearCompaction();

    // 生成压缩计划
    auto GenerateCompactionTask() -> CompactionTask*;

    // 从文件中读取元数据
    auto ReadMetaData(std::ifstream &file) const -> void;

    // 写入元数据
    auto WriteMetaData(std::ofstream &file) const -> void;

private:
    std::string table_name_;
    // int32_t table_number_;

    // sstable
    std::vector<FileMetaDataPtr> files_[K_NUM_LEVELS];

    // Size Compaction 的压缩分数
    int size_compaction_level_{-1};
    double size_compaction_score_{-1};

    // Size Compaction 上次压缩结束的位置
    InternalKey size_compaction_pointer_[K_NUM_LEVELS];

    // Seek Compaction 的压缩信息
    int seek_compaction_level_{-1};
    FileMetaDataPtr seek_compaction_file_{nullptr};
};


} // namespace ljdb