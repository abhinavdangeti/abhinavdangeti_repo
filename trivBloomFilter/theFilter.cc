#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <math.h>

#include <bitset>
#include <functional>
#include <string>
#include <sstream>

#include "MurmurHash3.h"

struct _CTX_ {
    _CTX_(int size, int count):
        el_size(size), seed(count) {}

    const int el_size;
    const int seed;
    std::bitset<1000000> bitArray;
};

/*
 * The following non-cryptographic hash function is the MURMUR hash
 * function, obtained the following algorithm from wikipedia.
 *
uint32_t murmur3_32(const char *key, uint32_t len, uint32_t seed) {
    static const uint32_t c1 = 0xcc9e2d51;
    static const uint32_t c2 = 0x1b873593;
    static const uint32_t r1 = 15;
    static const uint32_t r2 = 13;
    static const uint32_t m = 5;
    static const uint32_t n = 0xe6546b64;

    uint32_t hash = seed;

    const int nblocks = len / 4;
    const uint32_t *blocks = (const uint32_t *) key;
    int i;
    for (i = 0; i < nblocks; i++) {
        uint32_t k = blocks[i];
        k *= c1;
        k = (k << r1) | (k >> (32 - r1));
        k *= c2;

        hash ^= k;
        hash = ((hash << r2) | (hash >> (32 - r2))) * m + n;
    }

    const uint8_t *tail = (const uint8_t *) (key + nblocks * 4);
    uint32_t k1 = 0;

    switch (len & 3) {
        case 3:
            k1 ^= tail[2] << 16;
        case 2:
            k1 ^= tail[1] << 8;
        case 1:
            k1 ^= tail[0];

            k1 *= c1;
            k1 = (k1 << r1) | (k1 >> (32 - r1));
            k1 *= c2;
            hash ^= k1;
    }

    hash ^= len;
    hash ^= (hash >> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >> 16);

    return hash;
}
 */

void add(const char *key, uint32_t keylen, void* ctx) {
    uint32_t i;
    uint64_t result;
    _CTX_ *cb = static_cast<_CTX_ *>(ctx);
    for (i = 0; i < cb->seed; i++) {
        //result = murmur3_32(key, keylen, i);
        if (sizeof(void*) == 8) { // 64 bit
            MurmurHash3_x64_128(key, keylen, i, &result);
        } else { // 32 bit
            MurmurHash3_x86_128(key, keylen, i, &result);
        }
        cb->bitArray[result % cb->el_size] = 1;
    }
}

int lookup(const char *key, uint32_t keylen, void* ctx) {
    uint32_t i;
    uint64_t result;
    _CTX_ *cb = static_cast<_CTX_ *>(ctx);
    for (i = 0; i < cb->seed; i++) {
        //result = murmur3_32(key, keylen, i);
        if (sizeof(void*) == 8) { // 64 bit
            MurmurHash3_x64_128(key, keylen, i, &result);
        } else { // 32 bit
            MurmurHash3_x86_128(key, keylen, i, &result);
        }
        if (cb->bitArray[result % cb->el_size] == 0) {
            // The key does not exist
            return 0;
        }
    }
    // In this case, the key probably exists
    return 1;
}

void isKeyPresent(const char *key, uint32_t keylen, void* ctx) {
    if (lookup(key, keylen, ctx) == 0) {
        printf("%s: DOESN'T EXIST\n", key);
    } else {
        printf("%s: MAYBE PRESENT\n", key);
    }
}

int main(int argc, char *argv[]) {

    if (argc != 3) {
        printf("ERROR: Incorrect number of arguments\n");
        printf("Usage: make estimate TESTARGS=\"<bit_array_size> <hashes_count>\"\n");
        return 0;
    }

    std::stringstream ss1(argv[1]);
    std::stringstream ss2(argv[2]);
    int m, k;

    if (!(ss1 >> m) || !(ss2 >> k)) {
        printf("ERROR: Invalid numbers\n");
        printf("Usage: make estimate TESTARGS=\"<bit_array_size> <hashes_count>\"\n");
        return 0;
    }

    if (m < k) {
        printf("ERROR: No. of hashes cannot be greater than bit_array_size\n");
        printf("Usage: make estimate TESTARGS=\"<bit_array_size> <hashes_count>\"\n");
        return 0;
    }

    _CTX_ ctx(m, k);

    int i;

    ctx.bitArray.reset();

    for (i = 0; i < 100000; i++) {
        std::stringstream ss;
        ss << "key+" << i;
        add(ss.str().c_str(), ss.str().size(), &ctx);
    }

    isKeyPresent("key+10", 6, &ctx);
    isKeyPresent("key+10000000", 12, &ctx);
    isKeyPresent("key+9999999", 11, &ctx);
    isKeyPresent("key+9873648", 11, &ctx);
    isKeyPresent("key+12481", 9, &ctx);
    isKeyPresent("key+5496", 8, &ctx);
    isKeyPresent("key54324", 8, &ctx);
    isKeyPresent("key+343422", 10, &ctx);
    isKeyPresent("key+132", 7, &ctx);
    isKeyPresent("key+1", 5, &ctx);
    isKeyPresent("key+85938", 9, &ctx);
    isKeyPresent("shitshitshit", 12, &ctx);
    isKeyPresent("key+0", 5, &ctx);

    return 0;
}
