/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef BGFETCHER_HH
#define BGFETCHER_HH 1

#include <map>
#include <vector>
#include <list>

#include "common.hh"
#include "dispatcher.hh"
#include "item.hh"
#include "stats.hh"

const uint16_t MAX_BGFETCH_RETRY=5;

class VBucketBGFetchItem {
public:
    VBucketBGFetchItem(const std::string k, uint64_t s, const void *c) :
                       key(k), cookie(c), retryCount(0), initTime(gethrtime()) {
        value.setId(s);
    }
    ~VBucketBGFetchItem() {}

    void delValue() {
        delete value.getValue();
        value.setValue(NULL);
    }
    bool canRetry() {
        return retryCount < MAX_BGFETCH_RETRY;
    }
    void incrRetryCount() {
        ++retryCount;
    }
    uint16_t getRetryCount() {
        return retryCount;
    }

    const std::string key;
    const void * cookie;
    GetValue value;
    uint16_t retryCount;
    hrtime_t initTime;
};

typedef unordered_map<uint64_t, std::list<VBucketBGFetchItem *> > vb_bgfetch_queue_t;

// Forward declaration.
class EventuallyPersistentStore;

/**
 * Dispatcher job responsible for batching data reads and push to
 * underlying storage
 */

class KVShard;

class BgFetcher {
public:
    static const double sleepInterval;

    BgFetcher(EventuallyPersistentStore *s, KVShard *k, EPStats &st) :
        store(s), shard(k), taskId(0), stats(st) {}

    void start(void);
    void stop(void);
    bool run(size_t tid);
    bool pendingJob(void);
    void notifyBGEvent(void);

private:
    void doFetch(uint16_t vbId);
    void clearItems(uint16_t vbId);

    EventuallyPersistentStore *store;
    KVShard *shard;
    vb_bgfetch_queue_t items2fetch;
    size_t taskId;
    Mutex taskMutex;
    EPStats &stats;

    Atomic<bool> pendingFetch;
};

#endif /* BGFETCHER_HH */
