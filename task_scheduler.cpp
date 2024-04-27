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

enum TASK_TYPE {
    TASK_ONCE,
    TASK_ATFIXEDRATE,
    TASK_ATDELAY,
};

class Scheduler;

struct FunctionInfo {
    FunctionInfo(function<void()> fn, int time_to_run, TASK_TYPE type,
        int fixed_rate, function<void(function<void()>, int, int)> callback)
    : fn(fn),
      time_to_run(time_to_run),
      type(type),
      fixed_rate(fixed_rate),
      callback(callback) {}

    void Run() {
        fn();
    }

    function<void()> fn;
    function<void(function<void()>, int, int)> callback;
    int time_to_run;
    TASK_TYPE type;
    int fixed_rate;
};

class ThreadPool {
    public:
    ThreadPool(size_t num_threads = thread::hardware_concurrency()) {
        stop_ = false;
        for (size_t i = 0; i < num_threads; i++) {
            threads_.emplace_back([this]() {
                while(true) {
                    FunctionInfo* task;
                    {
                        unique_lock<mutex> lock(mt_);
                        cv_.wait(lock, [this] {
                            return (que.size() > 0 || stop_);
                        });

                        if (stop_) {
                            break;
                        }

                        task = que.front();
                        que.pop();
                    }


                    if (task->type == TASK_ATFIXEDRATE) {
                         const auto timeNow= chrono::system_clock::now();
                        const auto duration= timeNow.time_since_epoch();
                        uint64_t current_time = chrono::duration_cast<chrono::seconds>(duration).count();
                        task->callback(task->fn, current_time + task->fixed_rate, task->fixed_rate);
                    }

                    task->Run();

                    if (task->type == TASK_ATDELAY) {
                        const auto timeNow= chrono::system_clock::now();
                        const auto duration= timeNow.time_since_epoch();
                        uint64_t current_time = chrono::duration_cast<chrono::seconds>(duration).count();
                        task->callback(task->fn, current_time + task->fixed_rate, task->fixed_rate);
                    }
                    delete task;
                }
            });
        }
    }

    ~ThreadPool() {
        cout << "Thread Deletion\n";
        {
            unique_lock<mutex> lock(mt_);
            stop_ = true;
        }
        cv_.notify_all();

        for (size_t i = 0; i < threads_.size(); i++) {
            threads_[i].join();
        }
    }

    void AddTask(FunctionInfo* fn) {
        mt_.lock();
        que.push(fn);
        mt_.unlock();
        cv_.notify_one();
    }

private:
    vector<thread> threads_;
    mutex mt_;
    condition_variable cv_;
    queue<FunctionInfo*> que;
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
                FunctionInfo* fn;
                {
                    unique_lock<mutex> lock(mt_);
                    cv_.wait(lock, [this] {
                        return (que.size() > 0 || stop_);
                    });

                    if (stop_) {
                        break;
                    }

                    // busy loop.
                    const auto timeNow= chrono::system_clock::now();
                    const auto duration= timeNow.time_since_epoch();
                    uint64_t current_time = chrono::duration_cast<chrono::seconds>(duration).count();

                    if (que.top()->time_to_run > current_time) {
                        continue;
                    }

                    fn = que.top();
                    que.pop();
                }
                thread_pool_.AddTask(fn);
            }
        });
    }

    void cancel () {
        unique_lock<mutex> lock(mt_);
        stop_ = true;
        cv_.notify_all();
    }

    ~Scheduler() {
        {
            cout << "Deletion\n";
            unique_lock<mutex> lock(mt_);
            stop_ = true;
            cv_.notify_all();
        }
        thread_.join();
    }

    void ScheduleOnce(function<void()> fn, int start_time) {
        FunctionInfo* fn_info = new FunctionInfo(fn, start_time, TASK_TYPE::TASK_ONCE, 0, nullptr);
        {
            unique_lock<mutex>lock (mt_);
            que.push(fn_info);
        }
        cv_.notify_all();
    }

    void ScheduleAtFixedRate(function<void()> fn, int start_time, int fixed_rate) {
        auto callback = std::bind(&Scheduler::ScheduleAtFixedRate, this,
            std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
        FunctionInfo* fn_info = new FunctionInfo(fn, start_time, TASK_TYPE::TASK_ATFIXEDRATE, fixed_rate, callback);
        {
            unique_lock<mutex>lock (mt_);
            que.push(fn_info);
        }
        cv_.notify_all();
    }

    void ScheduleAtFixedRateWithDelay(function<void()> fn, int start_time, int fixed_rate) {
        auto callback = std::bind(&Scheduler::ScheduleAtFixedRateWithDelay, this,
            std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
        FunctionInfo* fn_info = new FunctionInfo(fn, start_time, TASK_TYPE::TASK_ATDELAY, fixed_rate, callback);
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

    Scheduler* sch = new Scheduler(2);


    const auto timeNow= chrono::system_clock::now();
    const auto duration= timeNow.time_since_epoch();
    uint64_t current_time = chrono::duration_cast<chrono::seconds>(duration).count();

    cout << "Current time : " << current_time << "\n";

    sch->ScheduleOnce([]() {
        cout << "Executing Task 1 by  " << this_thread::get_id() << "\n";
        }, current_time + 10);

    sch->ScheduleAtFixedRate([]() {
        cout << "Executing Task 2 by  " << this_thread::get_id() << "\n";
        }, current_time + 10, 5);

    sch->ScheduleAtFixedRate([]() {
        cout << "Executing Task 3 by " << this_thread::get_id() << "\n";
        }, current_time + 5, 10);

     sch->ScheduleAtFixedRateWithDelay([]() {
        cout << "Executing Task 4 by " << this_thread::get_id() << "\n";
        sleep(10);
        }, current_time + 5, 10);

    sleep (100);
    delete sch;
    return 0;
}
