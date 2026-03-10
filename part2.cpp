#ifndef _ITASKSYS_H
#define _ITASKSYS_H

#include <vector>

// Task group-ийн ID төрөл
typedef int TaskID;

// Task interface
class IRunnable
{
public:
    virtual ~IRunnable();

    // Нэг task-ийг ажиллуулах функц
    virtual void runTask(int task_id, int num_total_tasks) = 0;
};

// Task system interface
class ITaskSystem
{
public:
    ITaskSystem(int num_threads);
    virtual ~ITaskSystem();
    virtual const char *name() = 0;

    // Dependencyгүй synchronous run
    virtual void run(IRunnable *runnable, int num_total_tasks) = 0;

    // Dependency-тэй async run
    virtual TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                    const std::vector<TaskID> &deps) = 0;

    // Бүх task group дуусахыг хүлээнэ
    virtual void sync() = 0;
};

#endif