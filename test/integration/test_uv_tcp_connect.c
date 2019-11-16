#include "../../include/raft.h"
#include "../../include/raft/uv.h"
#include "../lib/heap.h"
#include "../lib/loop.h"
#include "../lib/runner.h"
#include "../lib/tcp.h"

/******************************************************************************
 *
 * Fixture with a TCP-based raft_uv_transport.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_HEAP;
    FIXTURE_LOOP;
    FIXTURE_TCP;
    struct raft_uv_transport transport;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

struct result
{
    int status;
    bool done;
};

static void connectCbAssertResult(struct raft_uv_connect *req,
                                  struct uv_stream_s *stream,
                                  int status)
{
    struct result *result = req->data;
    (void)stream;
    munit_assert_int(status, ==, result->status);
    if (status == 0) {
        uv_close((struct uv_handle_s *)stream, (uv_close_cb)raft_free);
    }
    result->done = true;
}

#define INIT                                                         \
    do {                                                             \
        int _rv;                                                     \
        _rv = raft_uv_tcp_init(&f->transport, &f->loop);             \
        munit_assert_int(_rv, ==, 0);                                \
        _rv = f->transport.init(&f->transport, 1, "127.0.0.1:9000"); \
        munit_assert_int(_rv, ==, 0);                                \
    } while (0)

#define CLOSE raft_uv_tcp_close(&f->transport, NULL)

#define CONNECT_REQ(ID, ADDRESS, RV, STATUS)                      \
    struct raft_uv_connect _req;                                  \
    struct result _result = {STATUS, false};                      \
    int _rv;                                                      \
    _req.data = &_result;                                         \
    _rv = f->transport.connect(&f->transport, &_req, ID, ADDRESS, \
                               connectCbAssertResult);            \
    munit_assert_int(_rv, ==, RV)

/* Submit a connect request and assert that it fails synchronously with the
 * given error code and message. */
#define CONNECT_ERROR(ID, ADDRESS, RV, ERRMSG)                  \
    {                                                           \
        CONNECT_REQ(ID, ADDRESS, RV /* rv */, 0 /* status */);  \
        munit_assert_string_equal(f->transport.errmsg, ERRMSG); \
    }

/* Submit a connect request with the given parameters and wait for the operation
 * to successfully complete. */
#define CONNECT(ID, ADDRESS)                                  \
    {                                                         \
        CONNECT_REQ(ID, ADDRESS, 0 /* rv */, 0 /* status */); \
        LOOP_RUN_UNTIL(&_result.done);                        \
    }

/* Submit a connect request with the given parameters and wait for the operation
 * to fail with the given code and message. */
#define CONNECT_FAILURE(ID, ADDRESS, STATUS, ERRMSG)            \
    {                                                           \
        CONNECT_REQ(ID, ADDRESS, 0 /* rv */, STATUS);           \
        LOOP_RUN_UNTIL(&_result.done);                          \
        munit_assert_string_equal(f->transport.errmsg, ERRMSG); \
    }

/* Submit a connect request with the given parameters, close the transport after
 * N loop iterations and assert that the request got canceled. */
#define CONNECT_CANCEL(ID, ADDRESS, N)                       \
    {                                                        \
        CONNECT_REQ(ID, ADDRESS, 0 /* rv */, RAFT_CANCELED); \
        LOOP_RUN(N);                                         \
        munit_assert_false(_result.done);                    \
        CLOSE;                                               \
        LOOP_RUN_UNTIL(&_result.done);                       \
    }

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setUpDeps(const MunitParameter params[],
                       MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_HEAP;
    SETUP_LOOP;
    SETUP_TCP;
    return f;
}

static void tearDownDeps(void *data)
{
    struct fixture *f = data;
    LOOP_STOP;
    TEAR_DOWN_TCP;
    TEAR_DOWN_LOOP;
    TEAR_DOWN_HEAP;
    free(f);
}

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = setUpDeps(params, user_data);
    INIT;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    CLOSE;
    tearDownDeps(f);
}

/******************************************************************************
 *
 * Success scenarios
 *
 *****************************************************************************/

#define BOGUS_ADDRESS "127.0.0.1:6666"

SUITE(tcp_connect)

/* Successfully connect to the peer. */
TEST(tcp_connect, first, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    TCP_LISTEN;
    CONNECT(2, TCP_ADDRESS);
    return MUNIT_OK;
}

/* The peer has shutdown */
TEST(tcp_connect, refused, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    CONNECT_FAILURE(2, BOGUS_ADDRESS, RAFT_NOCONNECTION,
                    "uv_tcp_connect(): connection refused");
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
    HEAP_FAULT_ENABLE;
    CONNECT_ERROR(2, BOGUS_ADDRESS, RAFT_NOMEM, "out of memory");
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
    TCP_LISTEN;
    HEAP_FAULT_ENABLE;
    CONNECT_FAILURE(2, TCP_ADDRESS, RAFT_NOMEM, "out of memory");
    return MUNIT_OK;
}

/* The transport is closed immediately after a connect request as been
 * submitted. The request's callback is invoked with RAFT_CANCELED. */
TEST(tcp_connect, closeImmediately, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    CONNECT_CANCEL(2, BOGUS_ADDRESS, 0);
    return MUNIT_OK;
}

/* The transport gets closed during the handshake. */
TEST(tcp_connect, closeDuringHandshake, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    TCP_LISTEN;
    CONNECT_CANCEL(2, TCP_ADDRESS, 1);
    return MUNIT_OK;
}
