#include "tasksys.h"

// IRunnable destructor
IRunnable::~IRunnable() {}

// ITaskSystem constructor
ITaskSystem::ITaskSystem(int num_threads) {}

// ITaskSystem destructor
ITaskSystem::~ITaskSystem() {}

// System-ийн нэр
const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep + Deps";
}

// Constructor
TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(num_threads), // Worker thread-ийн тоо
      shutdown(false),           // Эхэндээ shutdown биш
      unfinished_groups(0),      // Дуусаагүй group байхгүй
      next_group_id(0)           // Эхний group ID = 0
{
    // Thread pool-ийг constructor дээр нэг удаа үүсгэнэ
    for (int i = 0; i < num_threads_; i++)
    {
        workers.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

// Destructor
TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    {
        // Worker-үүдийг зогсоохын тулд shutdown=true болгоно
        std::lock_guard<std::mutex> lock(mtx);
        shutdown = true;
    }

    // Унтаж байгаа бүх worker-үүдийг сэрээнэ
    cv_work.notify_all();

    // Бүх worker thread-ийг join хийнэ
    for (auto &th : workers)
    {
        th.join();
    }
}

// Worker thread-ийн гол loop
void TaskSystemParallelThreadPoolSleeping::workerLoop()
{
    while (true)
    {
        // Queue-оос авах work item
        // first = group id
        // second = task id
        std::pair<TaskID, int> work_item;

        {
            // ready_tasks queue дээр ажил хүлээж унтана
            std::unique_lock<std::mutex> lock(mtx);
            cv_work.wait(lock, [this]() {
                return shutdown || !ready_tasks.empty();
            });

            // Хэрэв shutdown=true бөгөөд queue хоосон бол thread-ээ дуусгана
            if (shutdown && ready_tasks.empty())
                return;

            // Queue-оос хамгийн эхний ready task-ийг авна
            work_item = ready_tasks.front();
            ready_tasks.pop();
        }

        // pair-ийг салгаж авна
        TaskID gid = work_item.first;
        int task_id = work_item.second;

        // Тухайн group-ийг map-аас олно
        std::shared_ptr<TaskGroup> group;
        {
            std::lock_guard<std::mutex> lock(mtx);
            group = groups[gid];
        }

        // Task-ийг ажиллуулна
        group->runnable->runTask(task_id, group->num_total_tasks);

        // Шинээр ready болох group-үүдийг энд түр хадгална
        std::vector<TaskID> newly_ready_groups;

        // Бүх ажил дууссан эсэхийг тэмдэглэх bool
        bool all_done = false;

        {
            std::lock_guard<std::mutex> lock(mtx);

            // Энэ group-ийн нэг subtask дууссан тул remaining_tasks-ийг бууруулна
            group->remaining_tasks--;

            // Хэрэв энэ group-ийн бүх subtask дууссан бол
            if (group->remaining_tasks == 0)
            {
                // Энэ group-оос хамаарч байсан dependent group-үүдийн dependency тоог бууруулна
                for (TaskID dep_id : group->dependents)
                {
                    auto dep_group = groups[dep_id];
                    dep_group->unresolved_deps--;

                    // Хэрэв dependency бүгд шийдэгдсэн бол энэ group ready боллоо
                    if (dep_group->unresolved_deps == 0)
                    {
                        newly_ready_groups.push_back(dep_id);
                    }
                }

                // Нэг group бүрэн дууссан тул unfinished_groups-ийг бууруулна
                unfinished_groups--;

                // Хэрэв дуусаагүй group үлдээгүй бол бүх ажил дууссан
                if (unfinished_groups == 0)
                {
                    all_done = true;
                }
            }

            // Шинээр ready болсон group-үүдийн бүх task-ийг ready queue-д оруулна
            for (TaskID ready_gid : newly_ready_groups)
            {
                auto ready_group = groups[ready_gid];
                for (int i = 0; i < ready_group->num_total_tasks; i++)
                {
                    ready_tasks.push({ready_gid, i});
                }
            }
        }

        // Хэрэв шинэ ready group гарсан бол worker-үүдийг сэрээнэ
        if (!newly_ready_groups.empty())
        {
            cv_work.notify_all();
        }

        // Хэрэв бүх ажил дууссан бол sync() хүлээж байгаа thread-ийг сэрээнэ
        if (all_done)
        {
            cv_done.notify_all();
        }
    }
}

// Dependencyгүй run
// Үүнийг dependencyгүйгээр runAsyncWithDeps() + sync() гэж ойлгож болно
void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

// Dependency-тэй шинэ task group үүсгэнэ
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // Shared state-ээ хамгаалж lock авна
    std::lock_guard<std::mutex> lock(mtx);

    // Шинэ group ID үүсгэнэ
    TaskID id = next_group_id++;

    // Шинэ task group үүсгэнэ
    auto group = std::make_shared<TaskGroup>(id, runnable, num_total_tasks);

    // Энэ group хэдэн dependency-тэй вэ гэдгийг хадгална
    group->unresolved_deps = (int)deps.size();

    // Map-д хадгална
    groups[id] = group;

    // Нийт дуусаагүй group-ийн тоог нэмэгдүүлнэ
    unfinished_groups++;

    // Dependency graph үүсгэнэ
    // dep -> id гэсэн хамааралтай бол dep-ийн dependents дотор id орно
    for (TaskID dep : deps)
    {
        if (groups.find(dep) != groups.end())
        {
            groups[dep]->dependents.push_back(id);
        }
    }

    // Хэрэв dependency байхгүй бол энэ group шууд ready
    if (group->unresolved_deps == 0)
    {
        // Бүх subtask-ийг ready queue-д оруулна
        for (int i = 0; i < num_total_tasks; i++)
        {
            ready_tasks.push({id, i});
        }

        // Worker thread-үүдийг сэрээнэ
        cv_work.notify_all();
    }

    // Шинэ group-ийн ID-г буцаана
    return id;
}

// Бүх task group дуусахыг хүлээнэ
void TaskSystemParallelThreadPoolSleeping::sync()
{
    // Main thread wait хийхэд unique_lock хэрэгтэй
    std::unique_lock<std::mutex> lock(mtx);

    // unfinished_groups == 0 болох хүртэл хүлээнэ
    cv_done.wait(lock, [this]() {
        return unfinished_groups == 0;
    });

    // Ready queue хоослох
    while (!ready_tasks.empty())
        ready_tasks.pop();

    // Group map-ийг цэвэрлэх
    groups.clear();
}