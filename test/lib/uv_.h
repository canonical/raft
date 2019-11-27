/* Helpers around the libuv-based implementation of the raft_io interface. */

#ifndef TEST_UV_H
#define TEST_UV_H

#include "../../include/raft.h"
#include "../../include/raft/uv.h"
#include "dir.h"
#include "heap.h"
#include "loop.h"
#include "tracer.h"

#define FIXTURE_UV_TRANSPORT struct raft_uv_transport transport
#define SETUP_UV_TRANSPORT                               \
    do {                                                 \
        int rv_;                                         \
        rv_ = raft_uv_tcp_init(&f->transport, &f->loop); \
        munit_assert_int(rv_, ==, 0);                    \
    } while (0)
#define TEAR_DOWN_UV_TRANSPORT raft_uv_tcp_close(&f->transport)

#define FIXTURE_UV_DEPS \
    FIXTURE_DIR;        \
    FIXTURE_HEAP;       \
    FIXTURE_LOOP;       \
    FIXTURE_TRACER;     \
    FIXTURE_UV_TRANSPORT
#define SETUP_UV_DEPS  \
    SETUP_DIR_OR_SKIP; \
    SETUP_HEAP;        \
    SETUP_LOOP;        \
    SETUP_TRACER;      \
    SETUP_UV_TRANSPORT
#define TEAR_DOWN_UV_DEPS   \
    TEAR_DOWN_UV_TRANSPORT; \
    TEAR_DOWN_TRACER;       \
    TEAR_DOWN_LOOP;         \
    TEAR_DOWN_HEAP;         \
    TEAR_DOWN_DIR

#define FIXTURE_UV struct raft_io io

#define SETUP_UV                                                     \
    do {                                                             \
        int rv_;                                                     \
        rv_ = raft_uv_init(&f->io, &f->loop, f->dir, &f->transport); \
        munit_assert_int(rv_, ==, 0);                                \
        raft_uv_set_tracer(&f->io, &f->tracer);                      \
        rv_ = f->io.init(&f->io, 1, "127.0.0.1:9001");               \
        munit_assert_int(rv_, ==, 0);                                \
    } while (0)

MUNIT_UNUSED static void uvCloseCb(struct raft_io *io)
{
    bool *closed = io->data;
    *closed = true;
}

#define TEAR_DOWN_UV                    \
    do {                                \
        bool _closed = false;           \
        f->io.data = &_closed;          \
        f->io.close(&f->io, uvCloseCb); \
        LOOP_RUN_UNTIL(&_closed);       \
        raft_uv_close(&f->io);          \
    } while (0)

#endif /* TEST_UV_H */
