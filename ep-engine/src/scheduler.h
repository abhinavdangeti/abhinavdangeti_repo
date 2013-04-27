/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef SRC_DISPATCHER_SCHEDULER_H_
#define SRC_DISPATCHER_SCHEDULER_H_ 1

#include "config.h"

#include <list>
#include <map>
#include <string>

#include "atomic.hh"
#include "common.hh"
#include "mutex.hh"
#include "objectregistry.hh"
#include "ringbuffer.hh"
#include "tasks.h"

#define TASK_LOG_SIZE 20

class ExecutorPool;

typedef enum {
    EXECUTOR_CREATING,
    EXECUTOR_RUNNING,
    EXECUTOR_SHUTDOWN,
    EXECUTOR_DEAD
} executor_state_t;

/**
 * Log entry for previous job runs.
 */
class TaskLogEntry {
public:

    // This is useful for the ringbuffer to initialize
    TaskLogEntry() : name("invalid"), duration(0) {}
    TaskLogEntry(const std::string &n, const hrtime_t d, rel_time_t t = 0)
        : name(n), ts(t), duration(d) {}

    /**
     * Get the name of the job.
     */
    std::string getName() const { return name; }

    /**
     * Get the amount of time (in microseconds) this job ran.
     */
    hrtime_t getDuration() const { return duration; }

    /**
     * Get a timestamp indicating when this thing started.
     */
    rel_time_t getTimestamp() const { return ts; }

private:
    std::string name;
    rel_time_t ts;
    hrtime_t duration;
};

class ExecutorThread {
public:

    ExecutorThread(ExecutorPool *m, const std::string nm)
        : name(nm), state(EXECUTOR_CREATING),
          manager(m), tasklog(TASK_LOG_SIZE), slowjobs(TASK_LOG_SIZE) {}

    ~ExecutorThread() {
        std::cout << "Executor killing" << std::endl;
        stop();
    }

    void start();

    void run();

    void stop();

    void shutdown() {
        state = EXECUTOR_SHUTDOWN;
    }

    void schedule(ExTask &task);

    void reschedule(ExTask &task);

    void wake(ExTask &task);

    void notify() {
        mutex.notify();
    }

    const std::string& getName() const {
        return name;
    }

private:

    ExTask nextTask();

    bool empty() {
        return readyQueue.empty() && futureQueue.empty();
    }

    void moveReadyTasks(const struct timeval &tv);

    void popNext();

    SyncObject mutex;
    pthread_t thread;
    const std::string name;
    executor_state_t state;
    ExecutorPool *manager;
    std::priority_queue<ExTask, std::deque<ExTask >,
                        CompareByPriority> readyQueue;
    std::priority_queue<ExTask, std::deque<ExTask >,
                        CompareByDueDate> futureQueue;
    RingBuffer<TaskLogEntry> tasklog;
    RingBuffer<TaskLogEntry> slowjobs;
};

typedef std::pair<ExTask, ExecutorThread*> lookupId;

class ExecutorPool {
public:

    bool cancel(size_t taskId);

    bool wake(size_t taskId);

    bool snooze(size_t taskId, double tosleep);

    void registerBucket(EventuallyPersistentEngine *engine);

    void unregisterBucket(EventuallyPersistentEngine *engine);

protected:

    ExecutorPool(int workers, const std::string prefix) {
        threads.reserve(workers);
        for (int tidx = 0; tidx < workers; tidx++) {
            std::stringstream ss;
            ss << prefix << tidx;
            threads[tidx] = new ExecutorThread(this, ss.str());
            threads[tidx]->start();
        }
    }

    size_t schedule(ExTask task, int tidx);

    SyncObject mutex;
    //! A list of all of this pools worker threads
    std::vector<ExecutorThread*> threads;
    //! A registry of buckets using this pool and a count of their tasks
    std::map<EventuallyPersistentEngine*, Atomic<size_t> > bucketRegistry;
    //! A mapping of task ids to workers in the thread pool
    std::map<size_t, lookupId> taskLocator;
};

#endif /* SRC_DISPATCHER_SCHEDULER_H_ */
