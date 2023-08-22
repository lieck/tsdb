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

namespace LindormContest {

// 返回每层的最大文件大小
static auto MaxBytesForLevel(int level) -> double {
#ifdef DEBUG_MODE
    double result = 4. * MAX_FILE_SIZE;
    while (level > 1) {
        result *= 4;
        level--;
    }
    return result;
#else
    double result = 10. * 1048576.0;
    while (level > 1) {
        result *= 10;
        level--;
    }
    return result;
#endif
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
    std::vector<FileMetaDataPtr> input_files_[2];

    // 压缩的输出文件
    std::vector<FileMetaDataPtr> output_files_;

    // 是否需要删除压缩文件
    bool need_delete_{true};
};

class TableMetaData {
public:
    auto GetFileMetaData(int level) const -> std::vector<FileMetaDataPtr> {
        ASSERT(level < K_NUM_LEVELS, "Invalid level");
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

    // 是否存在 sstable file 的压缩计划
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

    auto GetEraseFileQueue() -> std::queue<file_number_t>& {
        return erase_file_queue_;
    }

    // sstable
    std::vector<FileMetaDataPtr> files_[K_NUM_LEVELS];

private:
    // 需要删除的文件
    // TODO 使用引用计数将过期文件及时删除
    std::queue<file_number_t> erase_file_queue_;

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