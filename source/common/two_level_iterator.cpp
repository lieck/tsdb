#include "common/two_level_iterator.h"

namespace ljdb {

class TwoLevelIterator : public Iterator {
public:
    explicit TwoLevelIterator(std::unique_ptr<Iterator> index_iter, BlockFunction block_function, void *arg) : index_iter_(std::move(index_iter)),
    block_function_(block_function), arg_(arg) {
        index_iter_->SeekToFirst();
        InitDataBlock();
        if(data_iter_ != nullptr) {
            data_iter_->SeekToFirst();
        }
    }

    auto SeekToFirst() -> void override;

    void Seek(const InternalKey &key) override;

    auto GetKey() -> InternalKey override;

    auto GetValue() -> std::string override;

    auto Valid() -> bool override;

    auto Next() -> void override;
private:
    void InitDataBlock();

    std::unique_ptr<Iterator> index_iter_;
    std::unique_ptr<Iterator> data_iter_{};

    BlockFunction block_function_;
    void *arg_;
};

void TwoLevelIterator::SeekToFirst() {
    index_iter_->SeekToFirst();
    InitDataBlock();
    if(data_iter_ != nullptr) {
        data_iter_->SeekToFirst();
    }
}

void TwoLevelIterator::Seek(const InternalKey &key) {
    index_iter_->Seek(key);
    InitDataBlock();
    if(data_iter_ != nullptr) {
        data_iter_->Seek(key);
    }
}

auto TwoLevelIterator::GetKey() -> InternalKey {
    ASSERT(data_iter_ != nullptr && data_iter_->Valid(), "data_iter_ is nullptr or invalid");
    return data_iter_->GetKey();
}

auto TwoLevelIterator::GetValue() -> std::string {
    ASSERT(data_iter_ != nullptr && data_iter_->Valid(), "data_iter_ is nullptr or invalid");
    return data_iter_->GetValue();
}

auto TwoLevelIterator::Valid() -> bool {
    return data_iter_ != nullptr && data_iter_->Valid();
}

void TwoLevelIterator::Next() {
    ASSERT(data_iter_ != nullptr, "data_iter_ is nullptr");
    data_iter_->Next();
    if(!data_iter_->Valid()) {
        data_iter_ = nullptr;

        index_iter_->Next();
        InitDataBlock();
        if(data_iter_ != nullptr) {
            data_iter_->SeekToFirst();
        }
    }
}

void TwoLevelIterator::InitDataBlock() {
    if(index_iter_->Valid()) {
        data_iter_ = block_function_(arg_, index_iter_->GetValue());
    }
}

auto NewTwoLevelIterator(std::unique_ptr<Iterator> index_iter, BlockFunction block_function, void *arg) -> std::unique_ptr<Iterator> {
    return std::make_unique<TwoLevelIterator>(std::move(index_iter), block_function, arg);
}

}; // namespace ljdb