#include "../../include/raft.h"
#include "../../include/raft/uv.h"
#include "../../src/byte.h"
#include "../lib/heap.h"
#include "../lib/loop.h"
#include "../lib/runner.h"
#include "../lib/tcp.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    struct raft_heap heap;
    struct test_tcp tcp;
    FIXTURE_LOOP;
    struct raft_uv_transport transport;
    bool closed;
    struct raft_uv_connect req;
    int invoked;
    int status;
    struct uv_stream_s *stream;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    test_heap_setup(params, &f->heap);
    test_tcp_setup(params, &f->tcp);
    SETUP_LOOP;
    raft_uv_tcp_init(&f->transport, &f->loop);
    f->transport.config(&f->transport, 1, "127.0.0.1:9000");
    f->closed = false;
    f->req.data = f;
    f->invoked = 0;
    f->status = -1;
    f->stream = NULL;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    if (!f->closed) {
        f->transport.stop(&f->transport);
        raft_uv_tcp_close(&f->transport, NULL);
    }
    LOOP_STOP;
    TEAR_DOWN_LOOP;
    test_tcp_tear_down(&f->tcp);
    test_heap_tear_down(&f->heap);
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

static void connect_cb(struct raft_uv_connect *req,
                       struct uv_stream_s *stream,
                       int status)
{
    struct fixture *f = req->data;
    f->invoked++;
    f->status = status;
    f->stream = stream;
}

#define CONNECT(RV)                                                   \
    {                                                                 \
        int rv;                                                       \
        rv = f->transport.connect(&f->transport, &f->req, 2,          \
                                  f->tcp.server.address, connect_cb); \
        munit_assert_int(rv, ==, RV);                                 \
    }

#define WAIT_CONNECT_CB(STATUS)                  \
    {                                            \
        int i;                                   \
        for (i = 0; i < 2; i++) {                \
            if (f->invoked == 1)                 \
                break;                           \
            uv_run(&f->loop, UV_RUN_NOWAIT);     \
        }                                        \
        munit_assert_int(f->invoked, ==, 1);     \
        munit_assert_int(f->status, ==, STATUS); \
    }

#define PEER_SHUTDOWN test_tcp_stop(&f->tcp);

#define CLOSE                               \
    f->transport.stop(&f->transport);       \
    raft_uv_tcp_close(&f->transport, NULL); \
    f->closed = true;

/******************************************************************************
 *
 * Success scenarios
 *
 *****************************************************************************/

SUITE(tcp_connect)

/* Successfully connect to the peer. */
TEST(tcp_connect, first, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    CONNECT(0);
    WAIT_CONNECT_CB(0);
    munit_assert_ptr_not_null(f->stream);
    uv_close((struct uv_handle_s *)f->stream, (uv_close_cb)raft_free);
    return MUNIT_OK;
}

/* The peer has shutdown */
TEST(tcp_connect, refused, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    PEER_SHUTDOWN;
    CONNECT(0);
    WAIT_CONNECT_CB(RAFT_NOCONNECTION);
    return MUNIT_OK;
}

static char *connect_error_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *connect_error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum connect_error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, connect_error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, connect_error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST(tcp_connect, oom, setup, tear_down, 0, connect_error_oom_params)
{
    struct fixture *f = data;
    (void)params;
    test_heap_fault_enable(&f->heap);
    CONNECT(RAFT_NOMEM);
    return MUNIT_OK;
}

static char *connect_error_oom_async_heap_fault_delay[] = {"0", NULL};
static char *connect_error_oom_async_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum connect_error_oom_async_params[] = {
    {TEST_HEAP_FAULT_DELAY, connect_error_oom_async_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, connect_error_oom_async_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory condition after the attempt has started. */
TEST(tcp_connect, oom_assync, setup, tear_down, 0, connect_error_oom_async_params)
{
    struct fixture *f = data;
    (void)params;
    CONNECT(0);
    test_heap_fault_enable(&f->heap);
    WAIT_CONNECT_CB(RAFT_NOMEM);
    return MUNIT_OK;
}

/* The transport is closed immediately after a connect request as been
 * submitted. The request's callback is invoked with RAFT_CANCELED. */
TEST(tcp_connect, closeImmediately, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    CONNECT(0);
    CLOSE;
    WAIT_CONNECT_CB(RAFT_CANCELED);
    return MUNIT_OK;
}

/* The transport gets closed during the handshake. */
TEST(tcp_connect, closeDuringHandshake, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    CONNECT(0);
    uv_run(&f->loop, UV_RUN_NOWAIT);
    CLOSE;
    WAIT_CONNECT_CB(RAFT_CANCELED);
    return MUNIT_OK;
}
