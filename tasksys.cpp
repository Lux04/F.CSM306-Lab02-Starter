#include "tasksys.h"
#include <algorithm>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    return 0;
}

void TaskSystemSerial::sync()
{
    return;
}

/*
 * ================================================================
 * Parallel Spawn
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads)
{
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
        return;

    int workers = std::min(num_threads_, num_total_tasks);
    std::vector<std::thread> threads;

    for (int t = 0; t < workers; t++)
    {
        threads.emplace_back([=]() {
            int start = (t * num_total_tasks) / workers;
            int end = ((t + 1) * num_total_tasks) / workers;

            for (int i = start; i < end; i++)
            {
                runnable->runTask(i, num_total_tasks);
            }
        });
    }

    for (auto &th : threads)
    {
        th.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(num_threads),
      current_runnable(nullptr),
      total_tasks(0),
      next_task(0),
      completed_tasks(0),
      has_work(false),
      shutdown(false)
{
    for (int i = 0; i < num_threads_; i++)
    {
        workers.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerLoop, this);
    }
}

void TaskSystemParallelThreadPoolSpinning::workerLoop()
{
    while (true)
    {
        IRunnable *runnable = nullptr;
        int task_id = -1;
        int total = 0;

        {
            std::lock_guard<std::mutex> lock(mtx);

            if (shutdown)
                return;

            if (has_work && next_task < total_tasks)
            {
                task_id = next_task++;
                runnable = current_runnable;
                total = total_tasks;
            }
        }

        if (task_id != -1)
        {
            runnable->runTask(task_id, total);

            std::lock_guard<std::mutex> lock(mtx);
            completed_tasks++;
            if (completed_tasks == total_tasks)
            {
                has_work = false;
            }
        }
        else
        {
            std::this_thread::yield();
        }
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{
    {
        std::lock_guard<std::mutex> lock(mtx);
        shutdown = true;
    }

    for (auto &th : workers)
    {
        th.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
        return;

    {
        std::lock_guard<std::mutex> lock(mtx);
        current_runnable = runnable;
        total_tasks = num_total_tasks;
        next_task = 0;
        completed_tasks = 0;
        has_work = true;
    }

    while (true)
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (!has_work)
            break;
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(num_threads),
      shutdown(false),
      unfinished_groups(0),
      next_group_id(0)
{
    for (int i = 0; i < num_threads_; i++)
    {
        workers.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    {
        std::lock_guard<std::mutex> lock(mtx);
        shutdown = true;
    }

    cv_work.notify_all();

    for (auto &th : workers)
    {
        th.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::workerLoop()
{
    while (true)
    {
        std::pair<TaskID, int> work_item;

        {
            std::unique_lock<std::mutex> lock(mtx);
            cv_work.wait(lock, [this]() {
                return shutdown || !ready_tasks.empty();
            });

            if (shutdown && ready_tasks.empty())
                return;

            work_item = ready_tasks.front();
            ready_tasks.pop();
        }

        TaskID gid = work_item.first;
        int task_id = work_item.second;

        std::shared_ptr<TaskGroup> group;
        {
            std::lock_guard<std::mutex> lock(mtx);
            group = groups[gid];
        }

        group->runnable->runTask(task_id, group->num_total_tasks);

        std::vector<TaskID> newly_ready_groups;
        bool all_done = false;

        {
            std::lock_guard<std::mutex> lock(mtx);

            group->remaining_tasks--;

            if (group->remaining_tasks == 0)
            {
                for (TaskID dep_id : group->dependents)
                {
                    auto dep_group = groups[dep_id];
                    dep_group->unresolved_deps--;

                    if (dep_group->unresolved_deps == 0)
                    {
                        newly_ready_groups.push_back(dep_id);
                    }
                }

                unfinished_groups--;
                if (unfinished_groups == 0)
                {
                    all_done = true;
                }
            }

            for (TaskID ready_gid : newly_ready_groups)
            {
                auto ready_group = groups[ready_gid];
                for (int i = 0; i < ready_group->num_total_tasks; i++)
                {
                    ready_tasks.push({ready_gid, i});
                }
            }
        }

        if (!newly_ready_groups.empty())
        {
            cv_work.notify_all();
        }

        if (all_done)
        {
            cv_done.notify_all();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    std::lock_guard<std::mutex> lock(mtx);

    TaskID id = next_group_id++;
    auto group = std::make_shared<TaskGroup>(id, runnable, num_total_tasks);
    group->unresolved_deps = (int)deps.size();

    groups[id] = group;
    unfinished_groups++;

    for (TaskID dep : deps)
    {
        if (groups.find(dep) != groups.end())
        {
            groups[dep]->dependents.push_back(id);
        }
    }

    if (group->unresolved_deps == 0)
    {
        for (int i = 0; i < num_total_tasks; i++)
        {
            ready_tasks.push({id, i});
        }
        cv_work.notify_all();
    }

    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{
    std::unique_lock<std::mutex> lock(mtx);
    cv_done.wait(lock, [this]() {
        return unfinished_groups == 0;
    });

    while (!ready_tasks.empty())
        ready_tasks.pop();

    groups.clear();
}