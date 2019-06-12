/* Helpers around the libuv-based implementation of the raft_io interface. */

#ifndef TEST_UV_H
#define TEST_UV_H

#include "../../include/raft.h"
#include "../../include/raft/uv.h"

#include "../../src/byte.h"
#include "../../src/uv.h"

#include "fs.h"
#include "heap.h"
#include "loop.h"
#include "munit.h"
#include "tcp.h"

#define FIXTURE_UV                      \
    struct raft_heap heap;              \
    struct test_tcp tcp;                \
    struct uv_loop_s loop;              \
    char *dir;                          \
    struct raft_logger logger;          \
    struct raft_uv_transport transport; \
    struct raft_io io;                  \
    struct uv *uv;                      \
    bool closed;

#define SETUP_UV                                                      \
    (void)user_data;                                                  \
    {                                                                 \
        int rv__;                                                     \
        test_heap_setup(params, &f->heap);                            \
        test_tcp_setup(params, &f->tcp);                              \
        SETUP_LOOP;                                                   \
        f->dir = test_dir_setup(params);                              \
        rv__ = raft_uv_tcp_init(&f->transport, &f->loop);             \
        munit_assert_int(rv__, ==, 0);                                \
        rv__ = raft_uv_init(&f->io, &f->loop, f->dir, &f->transport); \
        munit_assert_int(rv__, ==, 0);                                \
        f->io.data = f;                                               \
        rv__ = raft_default_logger_init(&f->logger);                  \
        munit_assert_int(rv__, ==, 0);                                \
        rv__ = f->io.init(&f->io, &f->logger, 1, "127.0.0.1:9000");   \
        munit_assert_int(rv__, ==, 0);                                \
        f->uv = f->io.impl;                                           \
        f->closed = false;                                            \
    }

#define TEAR_DOWN_UV                  \
    if (!f->closed) {                 \
        UV_CLOSE;                     \
    }                                 \
    LOOP_STOP;                        \
    raft_uv_close(&f->io);            \
    raft_uv_tcp_close(&f->transport); \
    test_dir_tear_down(f->dir);       \
    TEAR_DOWN_LOOP;                   \
    test_tcp_tear_down(&f->tcp);      \
    test_heap_tear_down(&f->heap);

#define UV_CLOSE                         \
    {                                    \
        int rv_;                         \
        rv_ = f->io.close(&f->io, NULL); \
        munit_assert_int(rv_, ==, 0);    \
        f->closed = true;                \
    }

/* Create a valid closed segment file with FIRST_INDEX and N batches each
 * containing one entry. DATA should be an integer that will be used as base
 * value for the data of the firt entry, and will be incremented.. */
#define UV_WRITE_CLOSED_SEGMENT(FIRST_INDEX, N, DATA)                         \
    {                                                                         \
        char filename_[strlen("N-N") + 20 * 2 + 1];                           \
        raft_index end_index_ = FIRST_INDEX + N - 1;                          \
        sprintf(filename_, "%llu-%llu", (raft_index)FIRST_INDEX, end_index_); \
        UV__WRITE_SEGMENT(filename_, N, DATA, true);                          \
    }

/* Create a valid open segment file with the given COUNTER and N batches, each
 * containing one entry. DATA should be an integer that will be used as base
 * value for the data of the firt entry, and will be incremented. */
#define UV_WRITE_OPEN_SEGMENT(COUNTER, N, DATA)              \
    {                                                        \
        char filename_[strlen("open-N") + 1];                \
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

/**
 * Create a valid snapshot metadata file with the given @term, @index and
 * @timestamp. The snapshot configuration will contain @nconfiguration_n
 * servers. Return the size of the created file.
 */
size_t test_io_uv_write_snapshot_meta_file(const char *dir,
                                           raft_term term,
                                           raft_index index,
                                           unsigned long long timestamp,
                                           unsigned configuration_n,
                                           raft_index configuration_index);

/**
 * Create a valid snapshot file with the given @term, @index and @timestamp. The
 * content of the snapshot file will be @buf.
 */
size_t test_io_uv_write_snapshot_data_file(const char *dir,
                                           raft_term term,
                                           raft_index index,
                                           unsigned long long timestamp,
                                           void *buf,
                                           size_t size);

#endif /* TEST_IO_UV_H */
