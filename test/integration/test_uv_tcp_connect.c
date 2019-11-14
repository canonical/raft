#include "../../include/raft.h"
#include "../../include/raft/uv.h"
#include "../../src/byte.h"
#include "../lib/heap.h"
#include "../lib/loop.h"
#include "../lib/runner.h"
#include "../lib/tcp.h"

/******************************************************************************
 *
 * Fixture with a pristine TCP-based raft_uv_transport.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_HEAP;
    FIXTURE_LOOP;
    struct test_tcp tcp;
    struct raft_uv_transport transport;
    bool closed;
};

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_HEAP;
    SETUP_LOOP;
    test_tcp_setup(params, &f->tcp);
    raft_uv_tcp_init(&f->transport, &f->loop);
    f->transport.config(&f->transport, 1, "127.0.0.1:9000");
    f->closed = false;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    if (!f->closed) {
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

struct connectResult
{
    int status;
    const char *errmsg;
    bool done;
};

static void connectCbAssertOk(struct raft_uv_connect *req,
                              struct uv_stream_s *stream,
                              int status)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    munit_assert_int(UV_TCP, ==, stream->type);
    uv_close((struct uv_handle_s *)stream, (uv_close_cb)raft_free);
    *done = true;
}

static void connectCbAssertFail(struct raft_uv_connect *req,
                                struct uv_stream_s *stream,
                                int status)
{
    struct connectResult *result = req->data;
    (void)stream;
    munit_assert_int(status, !=, 0);
    munit_assert_int(status, ==, result->status);
    /*munit_assert_string_equal(req->errmsg, result->errmsg);*/
    result->done = true;
}

/* Submit a connect request with the given parameters and wait for the operation
 * to successfully complete. */
#define CONNECT(ID, ADDRESS)                                          \
    {                                                                 \
        struct raft_uv_connect _req;                                  \
        bool _done = false;                                           \
        int _i;                                                       \
        int _rv;                                                      \
        _req.data = &_done;                                           \
        _rv = f->transport.connect(&f->transport, &_req, ID, ADDRESS, \
                                   connectCbAssertOk);                \
        munit_assert_int(_rv, ==, 0);                                 \
        for (_i = 0; _i < 2; _i++) {                                  \
            LOOP_RUN(1);                                              \
            if (_done) {                                              \
                break;                                                \
            }                                                         \
        }                                                             \
        munit_assert_true(_done);                                     \
    }

/* Submit a connect request and assert that it fails synchronously with the
 * given error code and message. */
#define CONNECT_ERROR(ID, ADDRESS, RV, ERRMSG)                               \
    {                                                                        \
        struct raft_uv_connect _req;                                         \
        int _rv;                                                             \
        _rv = f->transport.connect(&f->transport, &_req, ID, ADDRESS, NULL); \
        munit_assert_int(_rv, ==, RV);                                       \
    }

/* Submit a connect request with the given parameters and wait for the operation
 * to fail with the given code and message. */
#define CONNECT_FAILURE(ID, ADDRESS, STATUS, ERRMSG)                  \
    {                                                                 \
        struct raft_uv_connect _req;                                  \
        struct connectResult _result = {STATUS, ERRMSG, false};       \
        int _i;                                                       \
        int _rv;                                                      \
        _req.data = &_result;                                         \
        _rv = f->transport.connect(&f->transport, &_req, ID, ADDRESS, \
                                   connectCbAssertFail);              \
        munit_assert_int(_rv, ==, 0);                                 \
        for (_i = 0; _i < 2; _i++) {                                  \
            LOOP_RUN(1);                                              \
            if (_result.done) {                                       \
                break;                                                \
            }                                                         \
        }                                                             \
        munit_assert_true(_result.done);                              \
    }

/* Submit a connect request with the given parameters, close the transport after
 * N loop iterations and assert that the request got canceled.. */
#define CONNECT_CANCEL(ID, ADDRESS, N)                                \
    {                                                                 \
        struct raft_uv_connect _req;                                  \
        struct connectResult _result = {RAFT_CANCELED, "", false};    \
        int _i;                                                       \
        int _rv;                                                      \
        _req.data = &_result;                                         \
        _rv = f->transport.connect(&f->transport, &_req, ID, ADDRESS, \
                                   connectCbAssertFail);              \
        munit_assert_int(_rv, ==, 0);                                 \
        for (_i = 0; _i < N; _i++) {                                  \
            LOOP_RUN(1);                                              \
            munit_assert_false(_result.done);                         \
        }                                                             \
        raft_uv_tcp_close(&f->transport, NULL);                       \
        f->closed = true;                                             \
        for (_i = 0; _i < 2; _i++) {                                  \
            LOOP_RUN(1);                                              \
            if (_result.done) {                                       \
                break;                                                \
            }                                                         \
        }                                                             \
        munit_assert_true(_result.done);                              \
    }

/******************************************************************************
 *
 * Success scenarios
 *
 *****************************************************************************/

SUITE(tcp_connect)

/* Successfully connect to the peer. */
TEST(tcp_connect, first, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CONNECT(2, f->tcp.server.address);
    return MUNIT_OK;
}

/* The peer has shutdown */
TEST(tcp_connect, refused, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    CONNECT_FAILURE(2, "127.0.0.1:6666", RAFT_NOCONNECTION, "");
    return MUNIT_OK;
}

static char *oomHeapFaultDelay[] = {"0", "1", NULL};
static char *oomHeapFaultRepeat[] = {"1", NULL};

static MunitParameterEnum oomParams[] = {
    {TEST_HEAP_FAULT_DELAY, oomHeapFaultDelay},
    {TEST_HEAP_FAULT_REPEAT, oomHeapFaultRepeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST(tcp_connect, oom, setUp, tearDown, 0, oomParams)
{
    struct fixture *f = data;
    test_heap_fault_enable(&f->heap);
    CONNECT_ERROR(2, f->tcp.server.address, RAFT_NOMEM, "");
    return MUNIT_OK;
}

static char *oomAsyncHeapFaultDelay[] = {"3", NULL};
static char *oomAsyncHeapFaultRepeat[] = {"1", NULL};

static MunitParameterEnum oomAsyncParams[] = {
    {TEST_HEAP_FAULT_DELAY, oomAsyncHeapFaultDelay},
    {TEST_HEAP_FAULT_REPEAT, oomAsyncHeapFaultRepeat},
    {NULL, NULL},
};

/* Out of memory condition after the attempt has started. */
TEST(tcp_connect, oomAsync, setUp, tearDown, 0, oomAsyncParams)
{
    struct fixture *f = data;
    test_heap_fault_enable(&f->heap);
    CONNECT_FAILURE(2, f->tcp.server.address, RAFT_NOMEM, "");
    return MUNIT_OK;
}

/* The transport is closed immediately after a connect request as been
 * submitted. The request's callback is invoked with RAFT_CANCELED. */
TEST(tcp_connect, closeImmediately, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CONNECT_CANCEL(2, f->tcp.server.address, 0);
    return MUNIT_OK;
}

/* The transport gets closed during the handshake. */
TEST(tcp_connect, closeDuringHandshake, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CONNECT_CANCEL(2, f->tcp.server.address, 1);
    return MUNIT_OK;
}
