#include <iostream>
#include <vector>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <thread>
#include <mutex>
#include "tasksys.h"

class ComputeTask : public IRunnable
{
public:
    std::vector<double> results;
    int workload_intensity;

    ComputeTask(int num_tasks, int intensity)
        : results(num_tasks, 0.0), workload_intensity(intensity) {}

    void runTask(int taskID, int num_total_tasks) override
    {
        double val = 0.0;
        for (int i = 0; i < workload_intensity; ++i)
        {
            val += std::sin(i * 0.01 + taskID) * std::cos(i * 0.02 + taskID);
        }
        results[taskID] = val;
    }
};

void runBenchmark(ITaskSystem *system, IRunnable *task, int num_tasks, const std::string &name)
{
    std::cout << "Testing [" << name << "]..." << std::flush;
    auto start = std::chrono::high_resolution_clock::now();
    system->run(task, num_tasks);
    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> elapsed = end - start;
    std::cout << " Done. Time: " << std::fixed << std::setprecision(4)
              << elapsed.count() << "s" << std::endl;
}

/*
 * II хэсгийн dependency шалгах жижиг task
 */
class PrintTask : public IRunnable
{
private:
    std::string label;
    std::mutex *print_mutex;

public:
    PrintTask(const std::string &name, std::mutex *m)
        : label(name), print_mutex(m) {}

    void runTask(int taskID, int num_total_tasks) override
    {
        std::lock_guard<std::mutex> lock(*print_mutex);
        std::cout << "  Task Group [" << label << "] -> subtask " 
                  << taskID << "/" << num_total_tasks - 1 << std::endl;
    }
};

void runDependencyDemo()
{
    std::cout << "\n========================================" << std::endl;
    std::cout << "Dependency Demo (Part II)" << std::endl;
    std::cout << "Expected order: A -> (B and C) -> D" << std::endl;
    std::cout << "========================================" << std::endl;

    int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0)
        num_threads = 4;

    TaskSystemParallelThreadPoolSleeping system(num_threads);

    std::mutex print_mutex;

    PrintTask taskA("A", &print_mutex);
    PrintTask taskB("B", &print_mutex);
    PrintTask taskC("C", &print_mutex);
    PrintTask taskD("D", &print_mutex);

    TaskID A = system.runAsyncWithDeps(&taskA, 3, {});
    TaskID B = system.runAsyncWithDeps(&taskB, 2, {A});
    TaskID C = system.runAsyncWithDeps(&taskC, 2, {A});
    TaskID D = system.runAsyncWithDeps(&taskD, 2, {B, C});

    system.sync();

    std::cout << "Dependency demo finished." << std::endl;
}

int main()
{
    int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0)
        num_threads = 4;

    int num_tasks = 5000;
    int workload_intensity = 200;

    std::cout << "========================================" << std::endl;
    std::cout << "Task System Benchmark" << std::endl;
    std::cout << "Threads: " << num_threads << ", Tasks: " << num_tasks << std::endl;
    std::cout << "========================================" << std::endl;

    ComputeTask task(num_tasks, workload_intensity);

    ITaskSystem *serialSystem = new TaskSystemSerial(num_threads);
    runBenchmark(serialSystem, &task, num_tasks, "Serial System");
    delete serialSystem;

    ITaskSystem *spawnSystem = new TaskSystemParallelSpawn(num_threads);
    runBenchmark(spawnSystem, &task, num_tasks, "Parallel Spawn");
    delete spawnSystem;

    ITaskSystem *spinningSystem = new TaskSystemParallelThreadPoolSpinning(num_threads);
    runBenchmark(spinningSystem, &task, num_tasks, "Parallel Spinning Pool");
    delete spinningSystem;

    ITaskSystem *sleepingSystem = new TaskSystemParallelThreadPoolSleeping(num_threads);
    runBenchmark(sleepingSystem, &task, num_tasks, "Parallel Sleeping Pool");
    delete sleepingSystem;

    // II хэсгийн dependency test
    runDependencyDemo();

    return 0;
}