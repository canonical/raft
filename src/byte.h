/**
 * Byte-level utilities:
 *
 * - Convert between big-endian and little-endian representations.
 * - Calculate CRC32 checksum.
 */

#ifndef RAFT_BYTE_H_
#define RAFT_BYTE_H_

#include <stdint.h>
#include <unistd.h>

#if RAFT_COVERAGE
#define RAFT_INLINE static inline
#elif defined(__cplusplus) || \
    (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L)
#define RAFT_INLINE inline
#else
#define RAFT_INLINE static
#endif

/* Flip a 32-bit number to network byte order (little endian) */
RAFT_INLINE uint32_t byte__flip32(uint32_t v)
{
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
    return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) && \
    defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
    return __builtin_bswap32(v);
#else
    union {
        uint32_t u;
        uint8_t v[4];
    } s;

    s.v[0] = (uint8_t)v;
    s.v[1] = (uint8_t)(v >> 8);
    s.v[2] = (uint8_t)(v >> 16);
    s.v[3] = (uint8_t)(v >> 24);

    return s.u;
#endif
}

/* Flip a 64-bit number to network byte order (little endian) */
RAFT_INLINE uint64_t byte__flip64(uint64_t v)
{
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
    return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) && \
    defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
    return __builtin_bswap64(v);
#else
    union {
        uint64_t u;
        uint8_t v[8];
    } s;

    s.v[0] = (uint8_t)v;
    s.v[1] = (uint8_t)(v >> 8);
    s.v[2] = (uint8_t)(v >> 16);
    s.v[3] = (uint8_t)(v >> 24);
    s.v[4] = (uint8_t)(v >> 32);
    s.v[5] = (uint8_t)(v >> 40);
    s.v[6] = (uint8_t)(v >> 48);
    s.v[7] = (uint8_t)(v >> 56);

    return s.u;
#endif
}

RAFT_INLINE void byte__put8(void **cursor, uint8_t value)
{
    *(uint8_t *)(*cursor) = value;
    *cursor += sizeof(uint8_t);
}

RAFT_INLINE void byte__put32(void **cursor, uint32_t value)
{
    *(uint32_t *)(*cursor) = value;
    *cursor += sizeof(uint32_t);
}

RAFT_INLINE void byte__put64(void **cursor, uint64_t value)
{
    *(uint64_t *)(*cursor) = byte__flip64(value);
    *cursor += sizeof(uint64_t);
}

RAFT_INLINE uint8_t byte__get8(const void **cursor)
{
    uint8_t value = *(uint8_t *)(*cursor);
    *cursor += sizeof(uint8_t);
    return value;
}

RAFT_INLINE uint32_t byte__get32(const void **cursor)
{
    uint32_t value = byte__flip32(*(uint32_t *)(*cursor));
    *cursor += sizeof(uint32_t);
    return value;
}

RAFT_INLINE uint64_t byte__get64(const void **cursor)
{
    uint64_t value = byte__flip64(*(uint64_t *)(*cursor));
    *cursor += sizeof(uint64_t);
    return value;
}

/**
 * Add padding to size if it's not a multiple of 8.
 */
RAFT_INLINE size_t byte__pad64(size_t size)
{
    size_t rest = size % sizeof(uint64_t);

    if (rest != 0) {
        size += sizeof(uint64_t) - rest;
    }

    return size;
}

/**
 * Calculate the CRC32 checksum of the given data buffer.
 */
unsigned byte__crc32(const void *buf, size_t size, unsigned init);

#endif /* RAFT_BYTE_H_ */
