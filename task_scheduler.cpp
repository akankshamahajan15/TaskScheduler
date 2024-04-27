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

using namespace std;

enum TASK_TYPE {
    TASK_ONCE,
    TASK_ATFIXEDRATE
};

class Scheduler;

struct FunctionInfo {
    FunctionInfo(function<void()> fn, int time_to_run, TASK_TYPE type,
        int fixed_rate, function<void(function<void()>, int)> callback)
    : fn(fn),
      time_to_run(time_to_run),
      type(type),
      fixed_rate(fixed_rate),
      callback(callback) {}

    void Run() {
        fn();
    }

    function<void()> fn;
    function<void(function<void()>, int)> callback;
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
                    task->Run();
                    const auto timeNow= chrono::system_clock::now();
                    const auto duration= timeNow.time_since_epoch();
                    uint64_t current_time = chrono::duration_cast<chrono::seconds>(duration).count();
                    task->callback(task->fn, current_time + 20);
                    delete task;
                }
            });
        }
    }

   void cancel () {
        {
            unique_lock<mutex> lock(mt_);
            stop_ = true;
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
        auto callback = std::bind(&Scheduler::ScheduleOnce, this, std::placeholders::_1, std::placeholders::_2);
        FunctionInfo* fn_info = new FunctionInfo(fn, start_time, TASK_TYPE::TASK_ONCE, 0, callback);
        {
            unique_lock<mutex>lock (mt_);
            que.push(fn_info);
        }
        cv_.notify_all();
    }

    void ScheduleAtFixedRate(function<void()> fn, int start_time, int fixed_rate) {
        auto callback = std::bind(&Scheduler::ScheduleOnce, this, std::placeholders::_1, std::placeholders::_2);
        FunctionInfo* fn_info = new FunctionInfo(fn, start_time, TASK_TYPE::TASK_ATFIXEDRATE, fixed_rate, callback);
        {
            unique_lock<mutex>lock (mt_);
            que.push(fn_info);
        }
        cv_.notify_all();
    }

    void Callback() {

    }

    /*
    static Scheduler* GetInstance();

    static Scheduler* sc_;
    */

private:
    ThreadPool thread_pool_;
    thread thread_;
    mutex mt_;
    condition_variable cv_;
    bool stop_;
    priority_queue<FunctionInfo*, vector<FunctionInfo*>, RunTimeOrder> que;
};

/*
Scheduler* Scheduler::sc_ = nullptr;;


Scheduler* Scheduler::GetInstance() {

    if (sc_ == nullptr) {
        sc_ = new Scheduler(3);
    }
    return sc_;
}
*/

int main() {

    Scheduler* sch = new Scheduler(2);


    const auto timeNow= chrono::system_clock::now();
    const auto duration= timeNow.time_since_epoch();
    uint64_t current_time = chrono::duration_cast<chrono::seconds>(duration).count();

    cout << "Current time : " << current_time << "\n";

    sch->ScheduleOnce([]() {
        cout << "Executing Task 1 by  " << this_thread::get_id() << "\n";
        }, current_time + 10);
    sleep(2);

    sch->ScheduleOnce([]() {
        cout << "Executing Task 2 by  " << this_thread::get_id() << "\n";
        }, current_time + 10);

    sch->ScheduleOnce([]() {
        cout << "Executing Task 3 by " << this_thread::get_id() << "\n";
        }, current_time + 5);

    sleep (30);
    delete sch;
    return 0;
}
