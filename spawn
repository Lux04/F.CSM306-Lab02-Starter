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