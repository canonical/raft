/* Byte-level utilities. */

#ifndef BYTE_H_
#define BYTE_H_

#include <stdint.h>
#include <unistd.h>

#if defined(__cplusplus)
#    define RAFT__INLINE__ inline
#else
#    if defined(__clang__)
#        define RAFT__INLINE__ static inline __attribute__((unused))
#    else
#        define RAFT__INLINE__ static inline
#    endif
#endif

/* Compile-time endianess detection (best effort). */
#if (defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)) || \
    (defined(__ARMEL__) && (__ARMEL__ == 1))
#    define RAFT__LITTLE_ENDIAN
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) && \
    defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
#    define RAFT__BIG_ENDIAN
#endif

/* Flip a 32-bit number to network byte order (little endian) */
RAFT__INLINE__ uint32_t byteFlip32(uint32_t v)
{
#if defined(RAFT__LITTLE_ENDIAN)
    return v;
#elif defined(RAFT__BIG_ENDIAN)
    return __builtin_bswap32(v);
#else /* Unknown endianess */
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
RAFT__INLINE__ uint64_t byteFlip64(uint64_t v)
{
#if defined(RAFT__LITTLE_ENDIAN)
    return v;
#elif defined(RAFT__BIG_ENDIAN)
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

RAFT__INLINE__ void bytePut8(void **cursor, uint8_t value)
{
    *(uint8_t *)(*cursor) = value;
    *cursor += sizeof(uint8_t);
}

RAFT__INLINE__ void bytePut32(void **cursor, uint32_t value)
{
    *(uint32_t *)(*cursor) = byteFlip32(value);
    *cursor += sizeof(uint32_t);
}

RAFT__INLINE__ void bytePut64(void **cursor, uint64_t value)
{
    *(uint64_t *)(*cursor) = byteFlip64(value);
    *cursor += sizeof(uint64_t);
}

RAFT__INLINE__ uint8_t byteGet8(const void **cursor)
{
    uint8_t value = *(uint8_t *)(*cursor);
    *cursor += sizeof(uint8_t);
    return value;
}

RAFT__INLINE__ uint32_t byteGet32(const void **cursor)
{
    uint32_t value = byteFlip32(*(uint32_t *)(*cursor));
    *cursor += sizeof(uint32_t);
    return value;
}

RAFT__INLINE__ uint64_t byteGet64(const void **cursor)
{
    uint64_t value = byteFlip64(*(uint64_t *)(*cursor));
    *cursor += sizeof(uint64_t);
    return value;
}

/* Add padding to size if it's not a multiple of 8. */
RAFT__INLINE__ size_t bytePad64(size_t size)
{
    size_t rest = size % sizeof(uint64_t);

    if (rest != 0) {
        size += sizeof(uint64_t) - rest;
    }

    return size;
}

/* Calculate the CRC32 checksum of the given data buffer. */
unsigned byteCrc32(const void *buf, size_t size, unsigned init);

#undef RAFT__INLINE__

#endif /* BYTE_H_ */
