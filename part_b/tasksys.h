#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <thread>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
struct WaitingTask {
    IRunnable* runner;
    TaskID id;
    TaskID depTaskMaxId;
    int totalTask;
    WaitingTask(IRunnable* _runner, TaskID _id, TaskID _depTaskMaxId, int _totalTask): runner(_runner), id(_id), depTaskMaxId(_depTaskMaxId), totalTask(_totalTask) {}
    bool operator<(const WaitingTask& other) const {
        return depTaskMaxId > other.depTaskMaxId;
    }
};

struct ReadyTask {
    IRunnable* runner;
    TaskID id;          // batch id
    int currentTask;    // this task id
    int totalTask;
    ReadyTask(IRunnable* _runner, TaskID _id, int _totalTask): runner(_runner), id(_id), currentTask{0}, totalTask(_totalTask) {}
    ReadyTask() {}
};

class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        void workerFunc();
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        int maxThread;
        std::vector<std::thread> workers;
        
        std::map<TaskID, std::pair<int, int>> isTaskDone;   // task id -> {finished tasks, total}
        TaskID finishedTask;    // if deptaskid <= finishedTask, blocked tasks are runnable
        std::mutex mutexMap;
        std::condition_variable finished;   // wait/notify sync thread

        std::priority_queue<WaitingTask, std::vector<WaitingTask>> waitingQueue;
        std::mutex mutexWaitingQueue;

        std::queue<ReadyTask> readyQueue;
        std::mutex mutexReadyQueue;
        
        TaskID nextTaskId;

        bool stop;  // stop worker threads
};

#endif
