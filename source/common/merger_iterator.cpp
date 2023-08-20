#include "queue"
#include "common/merger_iterator.h"

#include <utility>

namespace LindormContest {

class MergingIterator : public Iterator {
private:
    struct IteratorCompare {
        auto operator()(Iterator *lhs, Iterator *rhs) const -> bool {
            return lhs->GetKey() < rhs->GetKey();
        }
    };

public:
    explicit MergingIterator(std::vector<std::unique_ptr<Iterator>> children) : children_(std::move(children)) {}

    ~MergingIterator() override = default;

    auto SeekToFirst() -> void override;

    void Seek(const InternalKey &key) override;

    auto GetKey() -> InternalKey override {
        return children_[idx_]->GetKey();
    }

    auto GetValue() -> std::string override {
        return children_[idx_]->GetValue();
    }

    auto Valid() -> bool override;

    auto Next() -> void override;

private:
    void InitHeap();

    size_t idx_{0};
    std::vector<std::unique_ptr<Iterator>> children_;
};

auto MergingIterator::SeekToFirst() -> void {
    for (auto &child : children_) {
        child->SeekToFirst();
    }
    InitHeap();
}

void MergingIterator::Seek(const InternalKey &key) {
    for (auto &child : children_) {
        child->Seek(key);
    }
    InitHeap();
}

auto MergingIterator::Valid() -> bool {
    return children_[idx_]->Valid();
}

auto MergingIterator::Next() -> void {
    children_[idx_]->Next();
    InitHeap();
}

void MergingIterator::InitHeap() {
    bool vis = false;
    for(size_t i = 0; i < children_.size(); i++) {
        if(children_[i]->Valid()) {
            if(!vis) {
                idx_ = i;
                vis = true;
            } else {
                if(children_[i]->GetKey() < children_[idx_]->GetKey()) {
                    idx_ = i;
                }
            }
        }
    }
}

auto NewMergingIterator(std::vector<std::unique_ptr<Iterator>> children) -> std::unique_ptr<Iterator> {
    return std::make_unique<MergingIterator>(std::move(children));
}


}  // namespace LindormContest
