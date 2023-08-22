#include <thread>
#include <atomic>
#include "db/background.h"
#include "common/macros.h"


namespace LindormContest {

void BackgroundTask::Schedule(void (*function)(void *), void *arg) {
    std::lock_guard<std::mutex> lock(mutex_);

    if(!started_background_thread_) {
        started_background_thread_ = true;
        std::thread(&BackgroundTask::BackgroundThread, this).detach();
    }

    if(background_work_queue_.empty()) {
        cv_.notify_one();
    }

    background_work_queue_.emplace(function, arg);
}

void BackgroundTask::BackgroundThreadMain() {
    std::unique_lock<std::mutex> lock(mutex_);
    while(!is_shutting_down_.load(std::memory_order_acquire)) {
        while (background_work_queue_.empty()) {
            cv_.notify_all();
            cv_.wait(lock);

            if(is_shutting_down_.load(std::memory_order_acquire)) {
                return;
            }
        }

        ASSERT(!background_work_queue_.empty(), "background_work_queue_ is empty");
        auto background_work_function = background_work_queue_.front().function_;
        void* background_work_arg = background_work_queue_.front().arg_;
        lock.unlock();

        background_work_function(background_work_arg);

        lock.lock();
        background_work_queue_.pop();
    };
}


}; // namespace ljdb