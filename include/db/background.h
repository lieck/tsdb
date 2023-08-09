#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>

namespace ljdb {

class BackgroundTask {
private:
    struct BackgroundTaskItem {
        void (*const function_)(void*);
        void* const arg_;

        explicit BackgroundTaskItem(void (*function)(void* arg), void* arg)
                : function_(function), arg_(arg) {}
    };

public:

    BackgroundTask() = default;

    void Schedule(void (*function)(void* arg), void* arg);

private:
    static void BackgroundThread(BackgroundTask *background_task) {
        background_task->BackgroundThreadMain();
    }

    void BackgroundThreadMain();

    std::mutex mutex_;
    std::condition_variable cv_;

    std::queue<BackgroundTaskItem> background_work_queue_;
    bool started_background_thread_{false};

};


}; // namespace ljdb