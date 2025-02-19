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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
    workers.reserve(maxThread);
    stop = false;
    finishedTask = -1;
    nextTaskId = 0;
    for (int i = 0; i < maxThread; i++) {
        workers.emplace_back(std::thread(&TaskSystemParallelThreadPoolSleeping::workerFunc, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    stop = true;
    for (auto& t : workers) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::workerFunc() {
    while (!stop) {
        ReadyTask task;
        bool runTask = false;
        mutexReadyQueue.lock();
        if (!readyQueue.empty()) {
            task = readyQueue.front();
            if (task.currentTask < task.totalTask) {
                runTask = true;
                readyQueue.front().currentTask++;
            } else {
                readyQueue.pop();
            }
        } else {
            mutexWaitingQueue.lock();
            while (!waitingQueue.empty()) {
                auto& nextWaitingTask = waitingQueue.top();
                if (nextWaitingTask.depTaskMaxId > finishedTask) break;
                readyQueue.push(ReadyTask(nextWaitingTask.runner, nextWaitingTask.id, nextWaitingTask.totalTask));
                isTaskDone.insert({nextWaitingTask.id, {0, nextWaitingTask.totalTask}});
                waitingQueue.pop();
            }
            mutexWaitingQueue.unlock();
        }
        mutexReadyQueue.unlock();

        if (runTask) {
            task.runner->runTask(task.currentTask, task.totalTask);
            // std::cerr << "worker run, cur task:" << task.currentTask << ", total:" << task.totalTask << std::endl;
            mutexMap.lock();
            auto& [finished, total] = isTaskDone[task.id];
            finished++;
            if (finished == total) {
                isTaskDone.erase(task.id);
                finishedTask = std::max(task.id, finishedTask);
            }
            mutexMap.unlock();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
    TaskID dependancy = -1;
    if (!deps.empty()) {
        dependancy = *std::max(deps.begin(), deps.end());
    }
    WaitingTask task(runnable, nextTaskId, dependancy, num_total_tasks);
    mutexWaitingQueue.lock();
    waitingQueue.push(std::move(task));
    mutexWaitingQueue.unlock();
    return nextTaskId++;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    while (true) {
        std::lock_guard<std::mutex> lock(mutexMap);
        if (finishedTask + 1 == nextTaskId) {
            break;
        }
    }
}
