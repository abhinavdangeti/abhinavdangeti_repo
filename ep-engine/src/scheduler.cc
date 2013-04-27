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

#include "config.h"

#include "common.hh"
#include "ep_engine.h"
#include "scheduler.h"
#include "locks.hh"

Atomic<size_t> GlobalTask::task_id_counter = 1;

extern "C" {
    static void* launch_executor_thread(void* arg);
}

static void* launch_executor_thread(void *arg) {
    ExecutorThread *executor = (ExecutorThread*) arg;
    try {
        executor->run();
    } catch (std::exception& e) {
        LOG(EXTENSION_LOG_WARNING, "%s: Caught an exception: %s\n",
            executor->getName().c_str(), e.what());
    } catch(...) {
        LOG(EXTENSION_LOG_WARNING, "%s: Caught a fatal exception\n",
            executor->getName().c_str());
    }
    return NULL;
}

void ExecutorThread::moveReadyTasks(const struct timeval &tv) {
    if (!readyQueue.empty()) {
        return;
    }
    while (!futureQueue.empty()) {
        ExTask tid = futureQueue.top();
        if (less_tv(tid->waketime, tv) || tid->state == TASK_DEAD) {
            readyQueue.push(tid);
            futureQueue.pop();
        } else {
            return;
        }
    }
}

ExTask ExecutorThread::nextTask() {
    assert (!empty());
    return readyQueue.empty() ? futureQueue.top() : readyQueue.top();
}

void ExecutorThread::popNext() {
    assert (!empty());
    readyQueue.empty() ? futureQueue.pop() : readyQueue.pop();
}

void ExecutorThread::start() {
    assert(state == EXECUTOR_CREATING);
    if(pthread_create(&thread, NULL, launch_executor_thread, this) != 0) {
        std::stringstream ss;
        ss << name.c_str() << ": Initialization error!!!";
        throw std::runtime_error(ss.str().c_str());
    }
}

void ExecutorThread::stop() {
    LockHolder lh(mutex);
    if (state == EXECUTOR_SHUTDOWN || state == EXECUTOR_DEAD) {
        return;
    }
    LOG(EXTENSION_LOG_INFO, "%s: Stopping", name.c_str());
    state = EXECUTOR_SHUTDOWN;
    notify();
    lh.unlock();
    pthread_join(thread, NULL);
    LOG(EXTENSION_LOG_INFO, "%s: Stopped", name.c_str());
}

void ExecutorThread::run() {
    state = EXECUTOR_RUNNING;
    for (;;) {
        //std::cout << "Ready: " << readyQueue.size() << " Future: " << futureQueue.size() << std::endl;
        LockHolder lh(mutex);
        if (state != EXECUTOR_RUNNING) {
            break;
        }
        if (empty()) {
            if (state == EXECUTOR_RUNNING) {
                mutex.wait();
            }
        } else {
            struct timeval tv;
            gettimeofday(&tv, NULL);

            // Get any ready tasks out of the due queue.
            moveReadyTasks(tv);

            ExTask task = nextTask();
            assert(task);
            LockHolder tlh(task->mutex);
            if (task->state == TASK_DEAD) {
                popNext();
                continue;
            }

            if (less_tv(tv, task->waketime)) {
                tlh.unlock();
                mutex.wait(task->waketime);
                lh.unlock();
                continue;
            } else {
                popNext();
            }
            tlh.unlock();

            hrtime_t taskStart = gethrtime();
            lh.unlock();
            rel_time_t startReltime = ep_current_time();
            try {
                //running_task = true;
                //std::cout << task->getDescription() << " " << task->taskId << std::endl;
                EventuallyPersistentEngine *oldEngine =
                    ObjectRegistry::onSwitchThread(task->engine, true);
                bool again = task->run();
                ObjectRegistry::onSwitchThread(oldEngine);
                if(again) {
                    reschedule(task);
                } else if (!task->isDaemonTask) {
                    manager->cancel(task->taskId);
                    //std::cout << "--> Killed task " << task->taskId << std::endl;
                } else {
                    //std::cout << "--> Skipped task " << task->taskId << std::endl;
                }
            } catch (std::exception& e) {
                LOG(EXTENSION_LOG_WARNING,
                    "%s: Exception caught in task \"%s\": %s", name.c_str(),
                    task->getDescription().c_str(), e.what());
            } catch(...) {
                LOG(EXTENSION_LOG_WARNING,
                    "%s: Fatal exception caught in task \"%s\"\n", name.c_str(),
                    task->getDescription().c_str());
            }
            //running_task = false;

            hrtime_t runtime((gethrtime() - taskStart) / 1000);
            TaskLogEntry tle(task->getDescription(), runtime, startReltime);
            tasklog.add(tle);
            if (runtime > task->maxExpectedDuration()) {
                slowjobs.add(tle);
            }
        }
    }
    state = EXECUTOR_DEAD;
}

void ExecutorThread::schedule(ExTask &task) {
    if (state == EXECUTOR_SHUTDOWN || state == EXECUTOR_DEAD) {
        return;
    }

    LockHolder lh(mutex);
    readyQueue.push(task);
    notify();
    LOG(EXTENSION_LOG_DEBUG, "%s: Schedule a task \"%s\"", name.c_str(),
        task->getDescription().c_str());
}

void ExecutorThread::reschedule(ExTask &task) {
    LockHolder lh(mutex);
    LOG(EXTENSION_LOG_DEBUG, "%s: Reschedule a task \"%s\"", name.c_str(),
        task->getDescription().c_str());
    futureQueue.push(task);
    notify();
}

void ExecutorThread::wake(ExTask &task) {
    LockHolder lh(mutex);
    LOG(EXTENSION_LOG_DEBUG, "%s: Wake a task \"%s\"", name.c_str(),
        task->getDescription().c_str());
    task->snooze(0, false);
    notify();
}

bool ExecutorPool::cancel(size_t taskId) {
    LockHolder lh(mutex);
    std::map<size_t, lookupId>::iterator itr = taskLocator.find(taskId);
    if (itr == taskLocator.end()) {
        //std::cout << "Cancel called, but task " << taskId << " not found." << std::endl;
        return false;
    }

    //std::cout << "Cancelled " << taskId << std::endl;
    bucketRegistry[itr->second.first->getEngine()].decr(1);
    assert(bucketRegistry[itr->second.first->getEngine()] < GIGANTOR);
    itr->second.first->cancel();
    taskLocator.erase(itr);
    lh.unlock();
    mutex.notify();
    return true;
}

bool ExecutorPool::wake(size_t taskId) {
    LockHolder lh(mutex);
    std::map<size_t, lookupId>::iterator itr = taskLocator.find(taskId);
    if (itr != taskLocator.end()) {
        //std::cout << "Woke up " << taskId << std::endl;
        itr->second.second->wake(itr->second.first);
        return true;
    }
    std::cout << "Wake called, but task " << taskId << " not found." << std::endl;
    return false;
}

bool ExecutorPool::snooze(size_t taskId, double tosleep) {
    LockHolder lh(mutex);
    std::map<size_t, lookupId>::iterator itr = taskLocator.find(taskId);
    if (itr != taskLocator.end()) {
        //std::cout << "Snoozed " << taskId << std::endl;
        itr->second.first->snooze(tosleep, false);
        return true;
    }
    //std::cout << "Snooze called, but task " << taskId << " not found." << std::endl;
    return false;
}

size_t ExecutorPool::schedule(ExTask task, int tidx) {
    LockHolder lh(mutex);
    if (bucketRegistry.find(task->getEngine()) == bucketRegistry.end()) {
        std::cout << "Trying to schedule task for unregistered bucket" << std::endl;
        LOG(EXTENSION_LOG_WARNING, "Trying to schedule task for unregistered "
            "bucket %s", task->getEngine()->getName());
    } else {
        bucketRegistry[task->getEngine()].incr(1);
    }
    lookupId loc(task, threads[tidx]);
    taskLocator[task->getId()] = loc;
    threads[tidx]->schedule(task);
    return task->getId();
}

void ExecutorPool::registerBucket(EventuallyPersistentEngine *engine) {
    LockHolder lh(mutex);
    if(bucketRegistry.find(engine) == bucketRegistry.end()) {
        bucketRegistry[engine] = 0;
    } else {
        LOG(EXTENSION_LOG_WARNING, "Bucket %s is trying to re-register itself",
            engine->getName());
        std::cout << "Bucket is trying to re-register itself" << std::endl;
    }
}

void ExecutorPool::unregisterBucket(EventuallyPersistentEngine *engine) {
    while (1) {
        LockHolder lh(mutex);
        std::map<EventuallyPersistentEngine*, Atomic<size_t> >::iterator itr =
            bucketRegistry.find(engine);
        if (itr == bucketRegistry.end()) {
            std::cout << "trying to deregister a non-existent bucket" << std::endl;
            return;
        }

        if (itr->second.get() == 0) {
            //std::cout << "Deregistering bucket " << itr->first->getName() << std::endl;
            bucketRegistry.erase(itr);
            return;
        }

        LOG(EXTENSION_LOG_INFO,
            "Waiting for %d tasks in bucket: %s", itr->second.get(),
            itr->first->getName());
        //std::cout << "Task left: " << itr->second.get() << std::endl;
        mutex.wait();
    }
}
