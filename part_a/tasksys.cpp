#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    maxThread = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::spawnThreadRunFunc(IRunnable* runnable, int num_total_tasks, std::atomic<int>& taskIndex) {
    while (true) {
        int curIndex = taskIndex.fetch_add(1);
        if (curIndex > num_total_tasks) {
            break;
        }
        runnable->runTask(curIndex, num_total_tasks);
    }
    runnable->runTask(taskIndex, num_total_tasks);
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::atomic<int> taskIndex(0);
    std::vector<std::thread> allThread;
    for (int i = 0; i < maxThread; i++) {
        allThread.emplace_back(std::thread(&TaskSystemParallelSpawn::spawnThreadRunFunc, this, runnable, num_total_tasks, std::ref(taskIndex)));
    }
    for (auto& t : allThread) {
        t.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    maxThread = num_threads;
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::spinThreadRunFunc(IRunnable* runnable, int num_total_tasks, TasLock& lock, int& nextTask) {
    int curTask = -1;
    while (curTask < num_total_tasks) {
        lock.lock();
        curTask = nextTask;
        nextTask++;
        lock.unlock();
        runnable->runTask(curTask, num_total_tasks);
    }
    
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    TasLock spinlock;
    int nextTask = 0;
    for (int i = 0; i < maxThread; i++) {
        threadPool.emplace_back(std::thread(&TaskSystemParallelThreadPoolSpinning::spinThreadRunFunc, this, runnable, num_total_tasks, std::ref(spinlock), std::ref(nextTask)));
    }
    for (auto& t : threadPool) {
        t.join();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    maxThread = num_threads;
    threadPool.reserve(maxThread);
    producerFinish = false;
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {}

void TaskSystemParallelThreadPoolSleeping::sleepThreadRunFunc(IRunnable* runnable, int num_total_tasks) {
    while (true) {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [this]() {
            return !queue.empty() || producerFinish;
        });
        int taskIndex;
        if (queue.empty()) {
            // std::cerr << "worker quit" << std::endl;
            break;
        } else {
            taskIndex = queue.front();
            queue.pop();
        }
        // std::cerr << "work run: " << taskIndex << std::endl;
        runnable->runTask(taskIndex, num_total_tasks);
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    {
        std::lock_guard<std::mutex> lock(mutex);
        for (int i = 0; i < num_total_tasks; i++) {
            queue.emplace(i);
        }
    }
    // std::cerr << "complete produce" << std::endl;

    for (int i = 0; i < maxThread; i++) {
        threadPool.emplace_back(std::thread(&TaskSystemParallelThreadPoolSleeping::sleepThreadRunFunc, this, runnable, num_total_tasks));
    }
    // std::cerr << "begin notify" << std::endl;
    producerFinish = true;
    cv.notify_all();

    for(auto& t : threadPool) {
        t.join();
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
