#include "../lib/heap.h"
#include "../lib/loop.h"
#include "../lib/runner.h"
#include "../lib/tcp.h"

#include "../../include/raft.h"
#include "../../include/raft/uv.h"

#include "../../src/byte.h"

TEST_MODULE(uv_tcp);

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

#define FIXTURE                         \
    struct raft_heap heap;              \
    struct test_tcp tcp;                \
    FIXTURE_LOOP;                       \
    struct raft_uv_transport transport; \
    bool closed;

#define SETUP                                                   \
    int rv;                                                     \
    (void)user_data;                                            \
    test_heap_setup(params, &f->heap);                          \
    test_tcp_setup(params, &f->tcp);                            \
    SETUP_LOOP;                                                 \
    raft_uv_tcp_init(&f->transport, &f->loop);                  \
    rv = f->transport.init(&f->transport, 1, "127.0.0.1:9000"); \
    munit_assert_int(rv, ==, 0);                                \
    f->closed = false;

#define TEAR_DOWN                                \
    if (!f->closed) {                            \
        f->transport.close(&f->transport, NULL); \
    }                                            \
    LOOP_STOP;                                   \
    raft_uv_tcp_close(&f->transport);            \
    TEAR_DOWN_LOOP;                              \
    test_tcp_tear_down(&f->tcp);                 \
    test_heap_tear_down(&f->heap);

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Connect to the listening socket of the transport, creating a new connection
 * that is waiting to be accepted. */
#define PEER_CONNECT test_tcp_connect(&f->tcp, 9000);

/* Make the connected client send handshake data. If N is greater than zero,
 * only N bytes will be sent (starting from the offset of the last call). */
#define PEER_HANDSHAKE(N)                                          \
    {                                                                      \
        size_t n = sizeof f->handshake.buf;                                \
        if (N > 0) {                                                       \
            n = N;                                                         \
        }                                                                  \
        test_tcp_send(&f->tcp, f->handshake.buf + f->handshake.offset, n); \
    }

/******************************************************************************
 *
 * raft_uv_transport->listen
 *
 *****************************************************************************/

TEST_SUITE(listen);

TEST_GROUP(listen, error)
TEST_GROUP(listen, close)

struct listen_fixture
{
    FIXTURE;
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

static void listen__accept_cb(struct raft_uv_transport *t,
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

static bool listen__accept_cb_invoked(void *data)
{
    struct listen_fixture *f = data;
    return f->invoked > 0;
}

TEST_SETUP(listen)
{
    struct listen_fixture *f = munit_malloc(sizeof *f);
    void *cursor;
    SETUP;

    f->invoked = 0;
    f->handshake.offset = 0;

    cursor = f->handshake.buf;
    byte__put64(&cursor, 1);
    byte__put64(&cursor, 2);
    byte__put64(&cursor, 16);
    strcpy(cursor, "127.0.0.1:666");

    f->transport.data = f;
    rv = f->transport.listen(&f->transport, listen__accept_cb);
    munit_assert_int(rv, ==, 0);

    return f;
}

TEST_TEAR_DOWN(listen)
{
    struct listen_fixture *f = data;
    TEAR_DOWN;
}

/* After a PEER_CONNECT() call, spin the event loop until the connected
 * callbloathack of the listening TCP handle gets called. */
#define WAIT_CONNECTED_CB LOOP_RUN(1);

/* After a PEER_HANDSHAKE() call, spin the event loop until the read
 * callback gets called. */
#define listen__wait_read_cb LOOP_RUN(1);

/* Spin the event loop until the accept callback gets eventually invoked. */
#define WAIT_ACCEPTED_CB                           \
    LOOP_RUN_UNTIL(listen__accept_cb_invoked, f); \
    f->invoked = 0;

/* If the handshake is successful, the accept callback is invoked. */
TEST_CASE(listen, success, NULL)
{
    struct listen_fixture *f = data;

    (void)params;

    PEER_CONNECT;
    PEER_HANDSHAKE(0);

    WAIT_ACCEPTED_CB;

    munit_assert_int(f->id, ==, 2);
    munit_assert_string_equal(f->address, "127.0.0.1:666");
    munit_assert_ptr_not_null(f->stream);

    uv_close((struct uv_handle_s *)f->stream, (uv_close_cb)raft_free);

    return MUNIT_OK;
}

/* The client sends us a bad protocol version */
TEST_CASE(listen, error, bad_protocol, NULL)
{
    struct listen_fixture *f = data;

    (void)params;

    memset(f->handshake.buf, 999, sizeof(uint64_t));

    PEER_CONNECT;
    PEER_HANDSHAKE(0);

    WAIT_CONNECTED_CB;
    listen__wait_read_cb;

    return MUNIT_OK;
}

/* Parameters for sending a partial handshake */
static char *partial_handshake_n[] = {"8", "16", "24", "32", NULL};

static MunitParameterEnum listen_error_abort_params[] = {
    {"n", partial_handshake_n},
    {NULL, NULL},
};

/* The peer closes the connection after having sent a partial handshake. */
TEST_CASE(listen, error, abort, listen_error_abort_params)
{
    struct listen_fixture *f = data;
    const char *n_param = munit_parameters_get(params, "n");

    PEER_CONNECT;
    PEER_HANDSHAKE(atoi(n_param));

    WAIT_CONNECTED_CB;
    listen__wait_read_cb;

    test_tcp_close(&f->tcp);

    listen__wait_read_cb;

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

/* Out of memory conditions */
TEST_CASE(listen, error, oom, listen_error_oom_params)
{
    struct listen_fixture *f = data;

    (void)params;

    PEER_CONNECT;
    PEER_HANDSHAKE(0);

    test_heap_fault_enable(&f->heap);

    /* Run as much as possible. */
    uv_run(&f->loop, UV_RUN_NOWAIT);
    uv_run(&f->loop, UV_RUN_NOWAIT);
    uv_run(&f->loop, UV_RUN_NOWAIT);

    return MUNIT_OK;
}

/* Close the transport right after an incoming connection becomes pending, but
 * it hasn't been accepted yet. */
TEST_CASE(listen, close, pending, NULL)
{
    struct listen_fixture *f = data;

    (void)params;

    PEER_CONNECT;

    return MUNIT_OK;
}

/* Close the transport right after an incoming connection gets accepted, and the
 * peer hasn't sent handshake data yet. */
TEST_CASE(listen, close, connected, NULL)
{
    struct listen_fixture *f = data;

    (void)params;

    PEER_CONNECT;
    WAIT_CONNECTED_CB;

    return MUNIT_OK;
}

static MunitParameterEnum listen_close_handshake_params[] = {
    {"n", partial_handshake_n},
    {NULL, NULL},
};

/* Close the transport right after the peer has started to send handshake data,
 * but isn't done with it yet. */
TEST_CASE(listen, close, handshake, listen_close_handshake_params)
{
    struct listen_fixture *f = data;
    const char *n_param = munit_parameters_get(params, "n");

    PEER_CONNECT;
    PEER_HANDSHAKE(atoi(n_param));

    WAIT_CONNECTED_CB;
    listen__wait_read_cb;

    return MUNIT_OK;
}

/******************************************************************************
 *
 * raft_uv_transport->connect
 *
 *****************************************************************************/

TEST_SUITE(connect);

TEST_GROUP(connect, close)
TEST_GROUP(connect, error)

struct connect_fixture
{
    FIXTURE;
    struct raft_uv_connect req;
    int invoked;
    int status;
    struct uv_stream_s *stream;
};

TEST_SETUP(connect)
{
    struct connect_fixture *f = munit_malloc(sizeof *f);
    SETUP;
    f->req.data = f;
    f->invoked = 0;
    f->status = -1;
    f->stream = NULL;
    return f;
}

TEST_TEAR_DOWN(connect)
{
    struct listen_fixture *f = data;
    TEAR_DOWN;
}

static void connect__connect_cb(struct raft_uv_connect *req,
                                struct uv_stream_s *stream,
                                int status)
{
    struct connect_fixture *f = req->data;
    f->invoked++;
    f->status = status;
    f->stream = stream;
}

#define connect__invoke(RV)                                                    \
    {                                                                          \
        int rv;                                                                \
        rv = f->transport.connect(&f->transport, &f->req, 2,                   \
                                  f->tcp.server.address, connect__connect_cb); \
        munit_assert_int(rv, ==, RV);                                          \
    }

#define connect__wait_connect_cb uv_run(&f->loop, UV_RUN_NOWAIT);
#define connect__wait_read_cb uv_run(&f->loop, UV_RUN_NOWAIT);

#define WAIT_CONNECT_CB(STATUS)                 \
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

#define connect__peer_shutdown(F) test_tcp_stop(&f->tcp);

#define connect__close                       \
    f->transport.close(&f->transport, NULL); \
    f->closed = true;

/* Successfully connect to the peer. */
TEST_CASE(connect, success, NULL)
{
    struct connect_fixture *f = data;

    (void)params;

    connect__invoke(0);
    WAIT_CONNECT_CB(0);

    munit_assert_ptr_not_null(f->stream);
    uv_close((struct uv_handle_s *)f->stream, (uv_close_cb)raft_free);

    return MUNIT_OK;
}

/* The transport is closed immediately after a connect request as been
 * submitted. The request's callback is invoked with RAFT_CANCELED. */
TEST_CASE(connect, close, immediately, NULL)
{
    struct connect_fixture *f = data;

    (void)params;

    connect__invoke(0);
    connect__close;
    WAIT_CONNECT_CB(RAFT_CANCELED);

    return MUNIT_OK;
}

/* The transport gets closed during the handshake. */
TEST_CASE(connect, close, handshake, NULL)
{
    struct connect_fixture *f = data;

    (void)params;

    connect__invoke(0);
    connect__wait_connect_cb;
    connect__close;
    WAIT_CONNECT_CB(RAFT_CANCELED);

    return MUNIT_OK;
}

/* The peer has shutdown */
TEST_CASE(connect, error, refused, NULL)
{
    struct connect_fixture *f = data;

    (void)params;

    connect__peer_shutdown(f);

    connect__invoke(0);
    WAIT_CONNECT_CB(RAFT_CANTCONNECT);

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
TEST_CASE(connect, error, oom, connect_error_oom_params)
{
    struct connect_fixture *f = data;

    (void)params;

    test_heap_fault_enable(&f->heap);

    connect__invoke(RAFT_NOMEM);

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
TEST_CASE(connect, error, oom_async, connect_error_oom_async_params)
{
    struct connect_fixture *f = data;

    (void)params;

    connect__invoke(0);

    test_heap_fault_enable(&f->heap);

    WAIT_CONNECT_CB(RAFT_NOMEM);

    return MUNIT_OK;
}
