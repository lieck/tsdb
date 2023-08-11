#include "queue"
#include "common/merger_iterator.h"

#include <utility>

namespace ljdb {

class MergingIterator : public Iterator {
private:
    struct IteratorCompare {
        auto operator()(Iterator *lhs, Iterator *rhs) const -> bool {
            return lhs->GetKey() < rhs->GetKey();
        }
    };

public:
    explicit MergingIterator(std::vector<std::unique_ptr<Iterator>> children) : children_(std::move(children)) {
        for (auto &child : children_) {
            heap_.push(child.get());
        }
    }

    ~MergingIterator() override = default;

    auto SeekToFirst() -> void override;

    void Seek(const InternalKey &key) override;

    auto GetKey() -> InternalKey override {
        return heap_.top()->GetKey();
    }

    auto GetValue() -> std::string override {
        return heap_.top()->GetValue();
    }

    auto Valid() -> bool override;

    auto Next() -> void override;

private:
    void InitHeap();

    std::vector<std::unique_ptr<Iterator>> children_;
    std::priority_queue<Iterator *, std::vector<Iterator*>, IteratorCompare> heap_;
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
    return !heap_.empty();
}

auto MergingIterator::Next() -> void {
    auto *top = heap_.top();
    heap_.pop();
    top->Next();
    if (top->Valid()) {
        heap_.push(top);
    }
}

void MergingIterator::InitHeap() {
    while (!heap_.empty()) {
        heap_.pop();
    }

    for (auto &child : children_) {
        if (child->Valid()) {
            heap_.push(child.get());
        }
    }
}


auto NewMergingIterator(std::vector<std::unique_ptr<Iterator>> children) -> std::unique_ptr<Iterator> {
    return std::make_unique<MergingIterator>(std::move(children));
}


} // namespace ljdb
