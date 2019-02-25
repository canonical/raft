#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/munit.h"
#include "../lib/tcp.h"
#include "../lib/uv.h"

#include "../../include/raft.h"

#include "../../src/binary.h"

/**
 * Helpers
 */

#define __FIXTURE              \
    struct raft_heap heap;     \
    struct test_tcp tcp;       \
    struct raft_logger logger; \
    struct uv_loop_s loop;     \
    struct raft_io_uv_transport transport;

#define __SETUP                                                 \
    int rv;                                                     \
    (void)user_data;                                            \
    test_heap_setup(params, &f->heap);                          \
    test_tcp_setup(params, &f->tcp);                            \
    test_logger_setup(params, &f->logger, 1);                   \
    test_uv_setup(params, &f->loop);                            \
    raft_io_uv_tcp_init(&f->transport, &f->logger, &f->loop);   \
    rv = f->transport.init(&f->transport, 1, "127.0.0.1:9000"); \
    munit_assert_int(rv, ==, 0);

#define __FIXTURE_TEAR_DOWN                  \
    f->transport.close(&f->transport, NULL); \
    test_uv_stop(&f->loop);                  \
    raft_io_uv_tcp_close(&f->transport);     \
    test_uv_tear_down(&f->loop);             \
    test_logger_tear_down(&f->logger);       \
    test_tcp_tear_down(&f->tcp);             \
    test_heap_tear_down(&f->heap);

#define __test(NAME, FUNC, SETUP, TEAR_DOWN, PARAMS)       \
    {                                                      \
        "/" NAME, test_##FUNC, SETUP, TEAR_DOWN, 0, PARAMS \
    }

/**
 * raft__io_uv_tcp_listen
 */

struct listen_fixture
{
    __FIXTURE;
    int invoked;
    unsigned id;
    char address[64];
    struct uv_stream_s *stream;
    struct
    {
        uint8_t buf[sizeof(uint64_t) + /* Protocol version */
                    sizeof(uint64_t) + /* Server ID */
                    sizeof(uint64_t) + /* Length of address */
                    sizeof(uint64_t) * 2 /* Address */];
        size_t offset;
    } handshake;
};

static void __accept_cb(struct raft_io_uv_transport *t,
                        unsigned id,
                        const char *address,
                        struct uv_stream_s *stream)
{
    struct listen_fixture *f = t->data;
    f->invoked++;
    f->id = id;
    strcpy(f->address, address);
    f->stream = stream;
}

static void *listen_setup(const MunitParameter params[], void *user_data)
{
    struct listen_fixture *f = munit_malloc(sizeof *f);
    void *cursor;
    __SETUP;

    f->invoked = 0;
    f->handshake.offset = 0;

    cursor = f->handshake.buf;
    raft__put64(&cursor, 1);
    raft__put64(&cursor, 2);
    raft__put64(&cursor, 16);
    strcpy(cursor, "127.0.0.1:666");

    f->transport.data = f;
    rv = f->transport.listen(&f->transport, __accept_cb);
    munit_assert_int(rv, ==, 0);

    return f;
}

static void listen_tear_down(void *data)
{
    struct listen_fixture *f = data;
    f->transport.stop(&f->transport);
    __FIXTURE_TEAR_DOWN;
}

/* Connect to the listening socket of the transport, creating a new connection
 * that is waiting to be accepted. */
#define __listen_peer_connect(F) test_tcp_connect(&f->tcp, 9000);

/* Make the connected client send handshake data. If N is greater than zero,
 * only N bytes will be sent (starting from the offset of the last call). */
#define __listen_peer_handshake(F, N)                                      \
    {                                                                      \
        size_t n = sizeof F->handshake.buf;                                \
        if (N > 0) {                                                       \
            n = N;                                                         \
        }                                                                  \
        test_tcp_send(&F->tcp, F->handshake.buf + F->handshake.offset, n); \
    }

/* After a __listen_peer_connect() call, spin the event loop until the connected
 * callback of the listening TCP handle gets called. */
#define __listen_wait_connected_cb(F) test_uv_run(&F->loop, 1);

/* After a __listen_peer_handshake() call, spin the event loop until the read
 * callback gets called. */
#define __listen_wait_read_cb(F) test_uv_run(&F->loop, 1);

/* Spin the event loop until the accept callback gets eventually invoked. */
#define __listen_wait_cb(F)                  \
    {                                        \
        int i;                               \
                                             \
        for (i = 0; i < 5; i++) {            \
            if (F->invoked > 0) {            \
                break;                       \
            }                                \
                                             \
            test_uv_run(&F->loop, 1);        \
        }                                    \
                                             \
        munit_assert_int(F->invoked, ==, 1); \
    }

#define __test_listen(NAME, FUNC, PARAMS) \
    __test(NAME, listen_##FUNC, listen_setup, listen_tear_down, PARAMS)

/* If the handshake is successful, the accept callback is invoked. */
static MunitResult test_listen_success(const MunitParameter params[],
                                       void *data)
{
    struct listen_fixture *f = data;

    (void)params;

    __listen_peer_connect(f);
    __listen_peer_handshake(f, 0);

    __listen_wait_cb(f);

    munit_assert_int(f->id, ==, 2);
    munit_assert_string_equal(f->address, "127.0.0.1:666");
    munit_assert_ptr_not_null(f->stream);

    uv_close((struct uv_handle_s *)f->stream, (uv_close_cb)raft_free);

    return MUNIT_OK;
}

static MunitTest listen_success_tests[] = {
    __test_listen("", success, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/* The client sends us a bad protocol version */
static MunitResult test_listen_error_bad_protocol(const MunitParameter params[],
                                                  void *data)
{
    struct listen_fixture *f = data;

    (void)params;

    memset(f->handshake.buf, 999, sizeof(uint64_t));

    __listen_peer_connect(f);
    __listen_peer_handshake(f, 0);

    __listen_wait_connected_cb(f);
    __listen_wait_read_cb(f);

    return MUNIT_OK;
}

/* The peer closes the connection after having sent a partial handshake. */
static MunitResult test_listen_error_abort(const MunitParameter params[],
                                           void *data)
{
    struct listen_fixture *f = data;
    const char *n_param = munit_parameters_get(params, "n");

    __listen_peer_connect(f);
    __listen_peer_handshake(f, atoi(n_param));

    __listen_wait_connected_cb(f);
    __listen_wait_read_cb(f);

    test_tcp_close(&f->tcp);

    __listen_wait_read_cb(f);

    return MUNIT_OK;
}

/* Parameters for sending a partial handshake */
static char *partial_handshake_n[] = {"8", "16", "24", "32", NULL};

static MunitParameterEnum listen_error_abort_params[] = {
    {"n", partial_handshake_n},
    {NULL, NULL},
};

/* Out of memory conditions */
static MunitResult test_listen_error_oom(const MunitParameter params[],
                                         void *data)
{
    struct listen_fixture *f = data;

    (void)params;

    __listen_peer_connect(f);
    __listen_peer_handshake(f, 0);

    test_heap_fault_enable(&f->heap);

    /* Run as much as possible. */
    uv_run(&f->loop, UV_RUN_NOWAIT);
    uv_run(&f->loop, UV_RUN_NOWAIT);
    uv_run(&f->loop, UV_RUN_NOWAIT);

    return MUNIT_OK;
}

/* TODO: skip "2" because it makes libuv crash, as it calls abort(). See also
 * https://github.com/libuv/libuv/issues/1948 */
static char *listen_error_oom_heap_fault_delay[] = {"0", "1", "2", NULL};
static char *listen_error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum listen_error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, listen_error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, listen_error_oom_heap_fault_repeat},
    {NULL, NULL},
};

#define __test_listen_error(NAME, FUNC, PARAMS) \
    __test_listen(NAME, error_##FUNC, PARAMS)

static MunitTest listen_error_tests[] = {
    __test_listen_error("bad-protocol", bad_protocol, NULL),
    __test_listen_error("abort", abort, listen_error_abort_params),
    __test_listen_error("oom", oom, listen_error_oom_params),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/* Close the transport right after an incoming connection becomes pending, but
 * it hasn't been accepted yet. */
static MunitResult test_listen_close_pending(const MunitParameter params[],
                                             void *data)
{
    struct listen_fixture *f = data;

    (void)params;

    __listen_peer_connect(f);

    return MUNIT_OK;
}

/* Close the transport right after an incoming connection gets accepted, and the
 * peer hasn't sent handshake data yet. */
static MunitResult test_listen_close_connected(const MunitParameter params[],
                                               void *data)
{
    struct listen_fixture *f = data;

    (void)params;

    __listen_peer_connect(f);
    __listen_wait_connected_cb(f);

    return MUNIT_OK;
}

/* Close the transport right after the peer has started to send handshake data,
 * but isn't done with it yet. */
static MunitResult test_listen_close_handshake(const MunitParameter params[],
                                               void *data)
{
    struct listen_fixture *f = data;
    const char *n_param = munit_parameters_get(params, "n");

    __listen_peer_connect(f);
    __listen_peer_handshake(f, atoi(n_param));

    __listen_wait_connected_cb(f);
    __listen_wait_read_cb(f);

    return MUNIT_OK;
}

static MunitParameterEnum listen_close_handshake_params[] = {
    {"n", partial_handshake_n},
    {NULL, NULL},
};

#define __test_listen_close(NAME, FUNC, PARAMS) \
    __test_listen(NAME, close_##FUNC, PARAMS)

static MunitTest listen_close_tests[] = {
    __test_listen_close("pending", pending, NULL),
    __test_listen_close("connected", connected, NULL),
    __test_listen_close("handshake", handshake, listen_close_handshake_params),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

MunitSuite listen_suites[] = {
    {"/success", listen_success_tests, NULL, 1, 0},
    {"/error", listen_error_tests, NULL, 1, 0},
    {"/close", listen_close_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};

/**
 * raft__io_uv_tcp_connect
 */

struct connect_fixture
{
    __FIXTURE;
    struct raft_io_uv_connect req;
    int invoked;
    int status;
    struct uv_stream_s *stream;
};

static void *connect_setup(const MunitParameter params[], void *user_data)
{
    struct connect_fixture *f = munit_malloc(sizeof *f);
    __SETUP;
    f->req.data = f;
    f->invoked = 0;
    f->status = -1;
    f->stream = NULL;
    return f;
}

static void connect_tear_down(void *data)
{
    struct listen_fixture *f = data;
    __FIXTURE_TEAR_DOWN;
}

static void __connect_cb(struct raft_io_uv_connect *req,
                         struct uv_stream_s *stream,
                         int status)
{
    struct connect_fixture *f = req->data;
    f->invoked++;
    f->status = status;
    f->stream = stream;
}

#define __connect_trigger(F, RV)                                        \
    {                                                                   \
        int rv;                                                         \
                                                                        \
        rv = F->transport.connect(&F->transport, &F->req, 2,            \
                                  F->tcp.server.address, __connect_cb); \
        munit_assert_int(rv, ==, RV);                                   \
    }

#define __connect_cancel(F) F->req.cancel(&F->req);
#define __connect_wait_connect_cb(F) uv_run(&f->loop, UV_RUN_NOWAIT);
#define __connect_wait_read_cb(F) uv_run(&f->loop, UV_RUN_NOWAIT);

#define __connect_wait_cb(F, STATUS)             \
    {                                            \
        __connect_wait_connect_cb(F);            \
        __connect_wait_read_cb(F);               \
        munit_assert_int(F->invoked, ==, 1);     \
        munit_assert_int(F->status, ==, STATUS); \
    }

#define __connect_peer_shutdown(F) test_tcp_stop(&f->tcp);

#define __test_connect(NAME, FUNC, PARAMS) \
    __test(NAME, connect_##FUNC, connect_setup, connect_tear_down, PARAMS)

/* The request is canceled immediately after submission. */
static MunitResult test_connect_cancel_immediately(
    const MunitParameter params[],
    void *data)
{
    struct connect_fixture *f = data;

    (void)params;

    __connect_trigger(f, 0);
    __connect_cancel(f);
    __connect_wait_cb(f, RAFT_ERR_IO_CANCELED);

    return MUNIT_OK;
}

/* The request is canceled during the handshake. */
static MunitResult test_connect_cancel_handshake(const MunitParameter params[],
                                                 void *data)
{
    struct connect_fixture *f = data;

    (void)params;

    __connect_trigger(f, 0);
    __connect_wait_connect_cb(f);
    __connect_cancel(f);
    __connect_wait_cb(f, RAFT_ERR_IO_CANCELED);

    return MUNIT_OK;
}

#define __test_connect_cancel(NAME, FUNC, PARAMS) \
    __test_connect(NAME, cancel_##FUNC, PARAMS)

static MunitTest connect_cancel_tests[] = {
    __test_connect_cancel("immediately", immediately, NULL),
    __test_connect_cancel("handshake", handshake, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/* The peer has shutdown */
static MunitResult test_connect_error_refused(const MunitParameter params[],
                                              void *data)
{
    struct connect_fixture *f = data;

    (void)params;

    __connect_peer_shutdown(f);

    __connect_trigger(f, 0);
    __connect_wait_cb(f, RAFT_ERR_IO_CONNECT);

    return MUNIT_OK;
}

/* Out of memory conditions. */
static MunitResult test_connect_error_oom(const MunitParameter params[],
                                          void *data)
{
    struct connect_fixture *f = data;

    (void)params;

    test_heap_fault_enable(&f->heap);

    __connect_trigger(f, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

static char *connect_error_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *connect_error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum connect_error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, connect_error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, connect_error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory condition after the attempt has started. */
static MunitResult test_connect_error_oom_async(const MunitParameter params[],
                                                void *data)
{
    struct connect_fixture *f = data;

    (void)params;

    __connect_trigger(f, 0);

    test_heap_fault_enable(&f->heap);

    __connect_wait_cb(f, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

static char *connect_error_oom_async_heap_fault_delay[] = {"0", NULL};
static char *connect_error_oom_async_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum connect_error_oom_async_params[] = {
    {TEST_HEAP_FAULT_DELAY, connect_error_oom_async_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, connect_error_oom_async_heap_fault_repeat},
    {NULL, NULL},
};

#define __test_connect_error(NAME, FUNC, PARAMS) \
    __test_connect(NAME, error_##FUNC, PARAMS)

static MunitTest connect_error_tests[] = {
    __test_connect_error("refused", refused, NULL),
    __test_connect_error("oom", oom, connect_error_oom_params),
    __test_connect_error("oom-async",
                         oom_async,
                         connect_error_oom_async_params),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/* Successfully connect to the peer. */
static MunitResult test_connect_success(const MunitParameter params[],
                                        void *data)
{
    struct connect_fixture *f = data;

    (void)params;

    __connect_trigger(f, 0);
    __connect_wait_cb(f, 0);

    munit_assert_ptr_not_null(f->stream);
    uv_close((struct uv_handle_s *)f->stream, (uv_close_cb)raft_free);

    return MUNIT_OK;
}

static MunitTest connect_success_tests[] = {
    __test_connect("", success, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

MunitSuite connect_suites[] = {
    {"/cancel", connect_cancel_tests, NULL, 1, 0},
    {"/error", connect_error_tests, NULL, 1, 0},
    {"/success", connect_success_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};

/**
 * Test suite
 */

MunitSuite raft_io_uv_tcp_suites[] = {
    {"/listen", NULL, listen_suites, 1, 0},
    {"/connect", NULL, connect_suites, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
