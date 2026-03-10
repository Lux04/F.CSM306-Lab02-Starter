#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep + Deps";
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