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

#ifndef SRC_DISPATCHER_TASKS_H_
#define SRC_DISPATCHER_TASKS_H_ 1

#include "config.h"

#include "atomic.hh"
#include "priority.hh"

typedef enum {
    TASK_RUNNING,
    TASK_DEAD,
} task_state_t;

class BgFetcher;
class CompareTasksByDueDate;
class CompareTasksByPriority;
class EventuallyPersistentEngine;
class Flusher;
class Warmup;

class GlobalTask : public RCValue {
friend class CompareByDueDate;
friend class CompareByPriority;
friend class ExecutorThread;
public:
    GlobalTask(EventuallyPersistentEngine *e, const Priority &p,
               double sleeptime = 0, size_t sttime = 0, bool isDaemon = true,
               bool completeBeforeShutdown = true) :
          RCValue(), engine(e), priority(p), starttime(sttime),
          isDaemonTask(isDaemon), blockShutdown(completeBeforeShutdown),
          state(TASK_RUNNING), taskId(nextTaskId()) {
        snooze(sleeptime, true);
    }

    /**
     * The invoked function when the task is executed.
     *
     * @return Whether or not this task should be rescheduled
     */
    virtual bool run() = 0;

    /**
     * Gives a description of this task.
     *
     * @return A description of this task
     */
    virtual std::string getDescription() = 0;

    virtual int maxExpectedDuration() {
        return 3600;
    }

    /**
     * Cancels this task by marking it dead.
     */
    void cancel() {
        state = TASK_DEAD;
    }

    /**
     * Puts the task to sleep for a given duration.
     */
    void snooze(const double secs, bool first);

    /**
     * Returns the id of this task.
     *
     * @return A unique task id number.
     */
    size_t getId() { return taskId; }

    /**
     * Gets the engine that this task was scheduled from
     *
     * @returns A handle to the engine
     */
    EventuallyPersistentEngine* getEngine() { return engine; }

protected:
    const Priority &priority;
    size_t starttime;
    bool isDaemonTask;
    bool blockShutdown;
    task_state_t state;
    const size_t taskId;
    struct timeval waketime;
    EventuallyPersistentEngine *engine;
    Mutex mutex;

    static Atomic<size_t> task_id_counter;
    static size_t nextTaskId() { return task_id_counter.incr(1); }
};

typedef RCPtr<GlobalTask> ExTask;

/**
 * A task for persisting items to disk.
 */
class FlusherTask : public GlobalTask {
public:
    FlusherTask(EventuallyPersistentEngine *e, Flusher* f, const Priority &p,
                bool isDaemon = true, bool completeBeforeShutdown = true) :
                GlobalTask(e, p, 0, 0, isDaemon, completeBeforeShutdown),
                           flusher(f) {}

    bool run();

    std::string getDescription() {
        return "Running a flusher loop";
    }

private:
    Flusher* flusher;
};

/**
 * A task for persisting VBucket state changes to disk and creating a new
 * VBucket database files.
 */
class VBSnapshotTask : public GlobalTask {
public:
    VBSnapshotTask(EventuallyPersistentEngine *e, const Priority &p,
                bool isDaemon = false, bool completeBeforeShutdown = true) :
                GlobalTask(e, p, 0, 0, isDaemon, completeBeforeShutdown) {}

    bool run();

    std::string getDescription() {
        return "Snapshotting a VBucket";
    }
};

/**
 * A task for deleting VBucket files from disk and cleaning up any outstanding
 * writes for that VBucket file.
 */
class VBDeleteTask : public GlobalTask {
public:
    VBDeleteTask(EventuallyPersistentEngine *e, uint16_t vb, const void* c,
                 const Priority &p, bool rc = false, bool isDaemon = false,
                 bool completeBeforeShutdown = true) :
        GlobalTask(e, p, 0, 0, isDaemon, completeBeforeShutdown), vbucket(vb),
        recreate(rc), cookie(c) {}

    bool run();

    std::string getDescription() {
        return "Deleting a VBucket";
    }

private:
    uint16_t vbucket;
    bool recreate;
    const void* cookie;
};

/**
 * A task that periodically takes a snapshot of the stats and persists then to
 * disk.
 */
class StatSnap : public GlobalTask {
public:
    StatSnap(EventuallyPersistentEngine *e, const Priority &p,
             bool runOneTimeOnly = false, bool sleeptime = 0,
             bool isDaemon = false, bool blockShutdown = false) :
        GlobalTask(e, p, sleeptime, 0, isDaemon, blockShutdown),
        runOnce(runOneTimeOnly) {}

    bool run();

    std::string getDescription() {
        std::string rv("Updating stat snapshot on disk");
        return rv;
    }

private:
    bool runOnce;
};

/**
 * A task that compacts a mutation log file.
 */
class MutationLogCompactor : public GlobalTask {
public:
    MutationLogCompactor(EventuallyPersistentEngine *e, const Priority &p,
                         bool sleeptime = 0, bool isDaemon = false,
                         bool blockShutdown = false) :
        GlobalTask(e, p, sleeptime, 0, isDaemon, blockShutdown) { }

    bool run();

    std::string getDescription() {
        return "MutationLogCompactor: Writing hash table items to new log file";
    }
};

/**
 * A task for fetching items from disk.
 */
class BgFetcherTask : public GlobalTask {
public:
    BgFetcherTask(EventuallyPersistentEngine *e, BgFetcher *b,
                  const Priority &p,bool sleeptime = 0, bool isDaemon = false,
                  bool blockShutdown = false) :
        GlobalTask(e, p, sleeptime, 0, isDaemon, blockShutdown),
        bgfetcher(b) { }

    bool run();

    std::string getDescription() {
        return std::string("Batching background fetch.");
    }

private:
    BgFetcher *bgfetcher;
};

/**
 * A task for performing disk fetches for "stats vkey".
 */
class VKeyStatBGFetchTask : public GlobalTask {
public:
    VKeyStatBGFetchTask(EventuallyPersistentEngine *e, const std::string &k,
                        uint16_t vbid, uint64_t s, const void *c,
                        const Priority &p, int sleeptime = 0, size_t delay = 0,
                        bool isDaemon = false, bool blockShutdown = false) :
        GlobalTask(e, p, sleeptime, delay, isDaemon, blockShutdown), key(k),
        vbucket(vbid), bySeqNum(s), cookie(c) { }

    bool run();

    std::string getDescription() {
        std::stringstream ss;
        ss << "Fetching item from disk for vkey stat:  " << key;
        return ss.str();
    }

private:
    std::string                      key;
    uint16_t                         vbucket;
    uint64_t                         bySeqNum;
    const void                      *cookie;
};

/**
 * A task that performs disk fetches for non-resident get requests.
 */
class BGFetchTask : public GlobalTask {
public:
    BGFetchTask(EventuallyPersistentEngine *e, const std::string &k,
            uint16_t vbid, uint64_t s, const void *c, bool isMeta,
            const Priority &p, int sleeptime = 0, size_t delay = 0,
            bool isDaemon = false, bool blockShutdown = false) :
        GlobalTask(e, p, sleeptime, delay, isDaemon, blockShutdown),
        key(k), vbucket(vbid), seqNum(s), cookie(c), metaFetch(isMeta),
        init(gethrtime()) { }

    bool run();

    std::string getDescription() {
        std::stringstream ss;
        ss << "Fetching item from disk:  " << key;
        return ss.str();
    }

private:
    const std::string          key;
    uint16_t                   vbucket;
    uint64_t                   seqNum;
    const void                *cookie;
    bool                       metaFetch;
    hrtime_t                   init;
};

/**
 * Order tasks by their priority.
 */
class CompareByPriority {
public:
    bool operator()(ExTask &t1, ExTask &t2) {
        return t1->priority < t2->priority;
    }
};

/**
 * Order tasks by their ready date.
 */
class CompareByDueDate {
public:
    bool operator()(ExTask &t1, ExTask &t2) {
        return less_tv(t2->waketime, t1->waketime);
    }
};

#endif /* SRC_DISPATCHER_IOMANAGER_H_ */
