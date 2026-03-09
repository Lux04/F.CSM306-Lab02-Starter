#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <unordered_map>
#include <memory>
#include <utility>

class TaskSystemSerial : public ITaskSystem
{
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

class TaskSystemParallelSpawn : public ITaskSystem
{
private:
    int num_threads_;

public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

class TaskSystemParallelThreadPoolSpinning : public ITaskSystem
{
private:
    int num_threads_;
    std::vector<std::thread> workers;
    std::mutex mtx;

    IRunnable *current_runnable;
    int total_tasks;
    int next_task;
    int completed_tasks;
    bool has_work;
    bool shutdown;

    void workerLoop();

public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

class TaskSystemParallelThreadPoolSleeping : public ITaskSystem
{
private:
    struct TaskGroup
    {
        TaskID id;
        IRunnable *runnable;
        int num_total_tasks;
        int remaining_tasks;
        int unresolved_deps;
        std::vector<TaskID> dependents;

        TaskGroup(TaskID gid, IRunnable *r, int total)
            : id(gid), runnable(r), num_total_tasks(total),
              remaining_tasks(total), unresolved_deps(0) {}
    };

    int num_threads_;
    std::vector<std::thread> workers;

    std::mutex mtx;
    std::condition_variable cv_work;
    std::condition_variable cv_done;

    std::queue<std::pair<TaskID, int>> ready_tasks;
    std::unordered_map<TaskID, std::shared_ptr<TaskGroup>> groups;

    bool shutdown;
    int unfinished_groups;
    TaskID next_group_id;

    void workerLoop();

public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

#endif