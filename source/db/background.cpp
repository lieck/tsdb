#include <thread>
#include "db/background.h"
#include "common/macros.h"


namespace ljdb {

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
    while(true) {
        std::unique_lock<std::mutex> lock(mutex_);

        while (background_work_queue_.empty()) {
            cv_.wait(lock);
        }

        ASSERT(background_work_queue_.empty(), "background_work_queue_ is empty");
        auto background_work_function = background_work_queue_.front().function;
        void* background_work_arg = background_work_queue_.front().arg;
        background_work_queue_.pop();
        lock.unlock();

        background_work_function(background_work_arg);
    };
}


}; // namespace ljdb