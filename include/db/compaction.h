#include "table_meta_data.h"
#include "cache/table_cache.h"
#include "mem_table/mem_table.h"

namespace ljdb {

struct CompactionTask {
    // 压缩的目标层数
    int level_;

    // 输出文件的最大大小
    uint64_t max_output_file_size_;

    // 压缩的输入文件
    std::vector<FileMetaData*> input_files_[2];

    // 压缩的输出文件
    std::vector<FileMetaData*> output_files_;
};





}; // namespace ljdb