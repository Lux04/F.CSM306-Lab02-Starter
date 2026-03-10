const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
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
        IRunnable *runnable = nullptr;
        int task_id = -1;
        int total = 0;

        {
            std::unique_lock<std::mutex> lock(mtx);
            cv_work.wait(lock, [this]() {
                return shutdown || (has_work && next_task < total_tasks);
            });

            if (shutdown)
                return;

            if (next_task < total_tasks)
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
                cv_done.notify_one();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
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

    cv_work.notify_all();

    std::unique_lock<std::mutex> lock(mtx);
    cv_done.wait(lock, [this]() {
        return completed_tasks == total_tasks;
    });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{
    return;
}