#include <cmath>
#include <cstdio>
#include <vector>
#include <iostream>
#include <algorithm>
#include <thread>
#include <unistd.h>
#include <mutex>
#include <queue>
#include <functional>
#include <condition_variable>
#include <signal.h>
#include <future>

using namespace std;

struct FunctionInfo {
    FunctionInfo(function<void()> fn, int time_to_run)
    : fn(fn),
      time_to_run(time_to_run) {}

    function<void()> fn;
    int time_to_run;
};

class ThreadPool {
    public:
    ThreadPool(size_t num_threads = thread::hardware_concurrency()) {
        stop_ = false;
        for (size_t i = 0; i < num_threads; i++) {
            threads_.emplace_back([this]() {
                while(true) {
                    function<void()> task;
                    {
                        unique_lock<mutex> lock(mt_);
                        cv_.wait(lock, [this] {
                            return (que.size() > 0 || stop_);
                        });

                        if (stop_ && que.size() == 0) {
                            break;
                        }

                        task = que.front();
                        que.pop();
                    }
                    task();
                }
            });
        }
    }

    ~ThreadPool() {
        {
            unique_lock<mutex> lock(mt_);
            stop_ = true;
        }
        cv_.notify_all();

        for (size_t i = 0; i < threads_.size(); i++) {
            threads_[i].join();
        }
    }

    void AddTask(function<void()> fn) {
        mt_.lock();
        //cout << " Adding task to queue ";
        que.push(fn);
        mt_.unlock();
        cv_.notify_one();
    }

private:
    vector<thread> threads_;
    mutex mt_;
    condition_variable cv_;
    queue<function<void()>> que;
    bool stop_;
};

struct RunTimeOrder {
    bool operator()(FunctionInfo* a, FunctionInfo* b) {
        return (a->time_to_run > b->time_to_run);
    }
};

class Scheduler {
public:
    Scheduler(int pool_size)
     : thread_pool_(pool_size),
       stop_(false) {
        thread_ = thread([this]() {
            while (true) {
                function<void()> fn;
                {
                    unique_lock<mutex> lock(mt_);
                    cv_.wait(lock, [this] {
                        return (que.size() > 0 || stop_);
                    });

                    if (stop_ && que.size() == 0) {
                        break;
                    }

                    // busy loop.
                    const auto timeNow= chrono::system_clock::now();
                    const auto duration= timeNow.time_since_epoch();
                    uint64_t current_time = chrono::duration_cast<chrono::seconds>(duration).count();

                    if (que.top()->time_to_run > current_time) {
                        continue;
                    }

                    auto top = que.top();
                    que.pop();
                    fn = top->fn;
                    delete top;
                }
                thread_pool_.AddTask(fn);
            }
        });
    }

    ~Scheduler() {
        {
            unique_lock<mutex> lock(mt_);
            stop_ = true;
            cv_.notify_all();
        }
        thread_.join();
    }

    void ScheduleOnce(function<void()> fn, int start_time) {
        FunctionInfo* fn_info = new FunctionInfo(fn, start_time);
        {
            unique_lock<mutex>lock (mt_);
            que.push(fn_info);
        }
        cv_.notify_all();
    }

private:
    ThreadPool thread_pool_;
    thread thread_;
    mutex mt_;
    condition_variable cv_;
    bool stop_;
    priority_queue<FunctionInfo*, vector<FunctionInfo*>, RunTimeOrder> que;
};

int main() {

    Scheduler sch(3);

    const auto timeNow= chrono::system_clock::now();
    const auto duration= timeNow.time_since_epoch();
    uint64_t current_time = chrono::duration_cast<chrono::seconds>(duration).count();

    cout << "Current time : " << current_time << "\n";

    sch.ScheduleOnce([]() {
        cout << "Executing Task 1 by  " << this_thread::get_id() << "\n";
        }, current_time + 30);

    sch.ScheduleOnce([]() {
        cout << "Executing Task 2 by " << this_thread::get_id() << "\n";
        }, current_time + 10);

    return 0;
}
