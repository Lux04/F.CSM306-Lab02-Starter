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