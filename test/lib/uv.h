/* Helpers around the libuv-based implementation of the raft_io interface. */

#ifndef TEST_UV_H
#define TEST_UV_H

#include "../../include/raft.h"
#include "../../include/raft/uv.h"
#include "../../src/byte.h"
#include "../../src/uv.h"
#include "../../src/uv_encoding.h"
#include "configuration.h"
#include "dir.h"
#include "heap.h"
#include "logger.h"
#include "loop.h"
#include "munit.h"
#include "tcp.h"

#define FIXTURE_UV                      \
    FIXTURE_HEAP;                       \
    FIXTURE_TCP;                        \
    FIXTURE_LOOP;                       \
    FIXTURE_DIR;                        \
    FIXTURE_LOGGER;                     \
    struct raft_uv_transport transport; \
    struct raft_io io;                  \
    struct uv *uv;                      \
    bool initialized;                   \
    bool closed

#define SETUP_UV_NO_INIT                                                 \
    {                                                                    \
        int rv_;                                                         \
        SETUP_DIR;                                                       \
        if (f->dir == NULL) { /* Desired fs not available, skip test. */ \
            free(f);                                                     \
            return NULL;                                                 \
        }                                                                \
        SETUP_HEAP;                                                      \
        SETUP_TCP;                                                       \
        SETUP_LOOP;                                                      \
        SETUP_LOGGER;                                                    \
        rv_ = raft_uv_tcp_init(&f->transport, &f->loop);                 \
        munit_assert_int(rv_, ==, 0);                                    \
        rv_ = raft_uv_init(&f->io, &f->loop, f->dir, &f->transport);     \
        munit_assert_int(rv_, ==, 0);                                    \
        f->io.data = f;                                                  \
        f->uv = f->io.impl;                                              \
        f->initialized = false;                                          \
        f->closed = false;                                               \
    }

#define SETUP_UV      \
    SETUP_UV_NO_INIT; \
    UV_INIT

#define TEAR_DOWN_UV                  \
    if (f == NULL) {                  \
        return;                       \
    }                                 \
    if (f->initialized) {             \
        UV_CLOSE;                     \
    }                                 \
    LOOP_STOP;                        \
    raft_uv_close(&f->io);            \
    raft_uv_tcp_close(&f->transport); \
    TEAR_DOWN_DIR;                    \
    TEAR_DOWN_LOOP;                   \
    TEAR_DOWN_TCP;                    \
    TEAR_DOWN_HEAP;

/* Run the raft_io->init() method. */
#define UV_INIT_RV f->io.init(&f->io, &f->logger, 1, "127.0.0.1:9000")
#define UV_INIT                              \
    {                                        \
        munit_assert_int(UV_INIT_RV, ==, 0); \
        f->initialized = true;               \
    }
#define UV_INIT_ERROR(RV) munit_assert_int(UV_INIT_RV, ==, RV)

/* Run the raft_io->close() method, if not ran already. */
#define UV_CLOSE                         \
    if (!f->closed) {                    \
        int rv_;                         \
        rv_ = f->io.close(&f->io, NULL); \
        munit_assert_int(rv_, ==, 0);    \
        f->closed = true;                \
    }

/* Create a valid closed segment file with FIRST_INDEX and N batches, each
 * containing one entry. DATA should be an integer that will be used as base
 * value for the data of the first entry, and will be incremented by one at each
 * new entry. */
#define UV_WRITE_CLOSED_SEGMENT(FIRST_INDEX, N, DATA)                  \
    {                                                                  \
        char filename_[256];                                           \
        raft_index end_index_ = FIRST_INDEX + N - 1;                   \
        sprintf(filename_, "%016llu-%016llu", (raft_index)FIRST_INDEX, \
                end_index_);                                           \
        UV__WRITE_SEGMENT(filename_, N, DATA, true);                   \
    }

/* Create a valid open segment file with the given COUNTER and N batches, each
 * containing one entry. DATA should be an integer that will be used as base
 * value for the data of the firt entry, and will be incremented. */
#define UV_WRITE_OPEN_SEGMENT(COUNTER, N, DATA)              \
    {                                                        \
        char filename_[256];                                 \
        sprintf(filename_, "open-%llu", (uvCounter)COUNTER); \
        UV__WRITE_SEGMENT(filename_, N, DATA, false);        \
    }

#define UV__WRITE_SEGMENT(FILENAME, N, DATA, TRUNCATE)              \
    {                                                               \
        struct uvSegmentBuffer buf_;                                \
        uv_buf_t out_;                                              \
        struct raft_entry *entries_;                                \
        unsigned i_;                                                \
        int rv_;                                                    \
                                                                    \
        uvSegmentBufferInit(&buf_, 4096);                           \
        rv_ = uvSegmentBufferFormat(&buf_);                         \
        munit_assert_int(rv_, ==, 0);                               \
                                                                    \
        entries_ = munit_malloc(N * sizeof *entries_);              \
        for (i_ = 0; i_ < (unsigned)N; i_++) {                      \
            struct raft_entry *entry_ = &entries_[i_];              \
            void *cursor_;                                          \
            entry_->term = 1;                                       \
            entry_->type = RAFT_COMMAND;                            \
            entry_->buf.len = sizeof(uint64_t);                     \
            entry_->buf.base = munit_malloc(entry_->buf.len);       \
            cursor_ = entry_->buf.base;                             \
            bytePut64(&cursor_, DATA + i_); /* Entry data */        \
        }                                                           \
                                                                    \
        rv_ = uvSegmentBufferAppend(&buf_, entries_, N);            \
        munit_assert_int(rv_, ==, 0);                               \
                                                                    \
        uvSegmentBufferFinalize(&buf_, &out_);                      \
        if (TRUNCATE) {                                             \
            out_.len = buf_.n;                                      \
        }                                                           \
        test_dir_write_file(f->dir, FILENAME, out_.base, out_.len); \
                                                                    \
        for (i_ = 0; i_ < (unsigned)N; i_++) {                      \
            struct raft_entry *entry_ = &entries_[i_];              \
            free(entry_->buf.base);                                 \
        }                                                           \
        free(entries_);                                             \
        uvSegmentBufferClose(&buf_);                                \
    }

/* Create a valid snapshot metadata file with the given TERM, INDEX and
 * TIMESTAMP. The snapshot configuration will contain CONF_N
 * servers. */
#define UV_WRITE_SNAPSHOT_META(DIR, TERM, INDEX, TIMESTAMP, CONF_N,        \
                               CONF_INDEX)                                 \
    {                                                                      \
        char filename_[256];                                               \
        struct raft_configuration conf_;                                   \
        struct raft_buffer buf_;                                           \
        int rv_;                                                           \
                                                                           \
        CONFIGURATION_CREATE(&conf_, CONF_N);                              \
                                                                           \
        rv_ = uvEncodeSnapshotMeta(&conf_, (raft_index)CONF_INDEX, &buf_); \
        munit_assert_int(rv_, ==, 0);                                      \
                                                                           \
        sprintf(filename_, UV__SNAPSHOT_META_TEMPLATE, (raft_term)TERM,    \
                (raft_index)INDEX, (raft_time)TIMESTAMP);                  \
                                                                           \
        test_dir_write_file(DIR, filename_, buf_.base, buf_.len);          \
        raft_free(buf_.base);                                              \
        CONFIGURATION_CLOSE(&conf_);                                       \
    }

/* Create a valid snapshot file with the given TERM, INDEX and TIMESTAMP. The
 * content of the snapshot file will be the one from BUF. */
#define UV_WRITE_SNAPSHOT_DATA(DIR, TERM, INDEX, TIMESTAMP, BUF, SIZE) \
    {                                                                  \
        char filename_[256];                                           \
        sprintf(filename_, UV__SNAPSHOT_TEMPLATE, (raft_term)TERM,     \
                (raft_index)INDEX, (raft_time)TIMESTAMP);              \
        test_dir_write_file(DIR, filename_, BUF, SIZE);                \
    }

#define UV_WRITE_SNAPSHOT(DIR, TERM, INDEX, TIMESTAMP, CONF_N, CONF_INDEX, \
                          BUF, SIZE)                                       \
    {                                                                      \
        UV_WRITE_SNAPSHOT_META(DIR, TERM, INDEX, TIMESTAMP, CONF_N,        \
                               CONF_INDEX);                                \
        UV_WRITE_SNAPSHOT_DATA(DIR, TERM, INDEX, TIMESTAMP, BUF, SIZE);    \
    }

#endif /* TEST_IO_UV_H */
