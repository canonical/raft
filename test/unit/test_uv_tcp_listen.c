#include "../lib/heap.h"
#include "../lib/loop.h"
#include "../lib/runner.h"
#include "../lib/tcp.h"

#include "../../include/raft.h"
#include "../../include/raft/uv.h"

#include "../../src/byte.h"

TEST_MODULE(uv_tcp_listen);

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

static void acceptCb(struct raft_uv_transport *t,
                     unsigned id,
                     const char *address,
                     struct uv_stream_s *stream)
{
    struct fixture *f = t->data;
    f->invoked++;
    f->id = id;
    strcpy(f->address, address);
    f->stream = stream;
}

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    void *cursor;
    int rv;
    (void)user_data;
    test_heap_setup(params, &f->heap);
    test_tcp_setup(params, &f->tcp);
    SETUP_LOOP;
    raft_uv_tcp_init(&f->transport, &f->loop);
    rv = f->transport.init(&f->transport, 1, "127.0.0.1:9000");
    munit_assert_int(rv, ==, 0);
    f->closed = false;
    f->invoked = 0;
    f->handshake.offset = 0;

    cursor = f->handshake.buf;
    bytePut64(&cursor, 1);
    bytePut64(&cursor, 2);
    bytePut64(&cursor, 16);
    strcpy(cursor, "127.0.0.1:666");

    f->transport.data = f;
    rv = f->transport.listen(&f->transport, acceptCb);
    munit_assert_int(rv, ==, 0);

    return f;
};

static void tear_down(void *data)
{
    struct fixture *f = data;
    if (!f->closed) {
        f->transport.close(&f->transport, NULL);
    }
    LOOP_STOP;
    raft_uv_tcp_close(&f->transport);
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

static bool acceptCbInvoked(void *data)
{
    struct fixture *f = data;
    return f->invoked > 0;
}

/* Connect to the listening socket of the transport, creating a new connection
 * that is waiting to be accepted. */
#define PEER_CONNECT test_tcp_connect(&f->tcp, 9000);

/* Make the connected client send handshake data. If N is greater than zero,
 * only N bytes will be sent (starting from the offset of the last call). */
#define PEER_HANDSHAKE(N)                                                  \
    {                                                                      \
        size_t n = sizeof f->handshake.buf;                                \
        if (N > 0) {                                                       \
            n = N;                                                         \
        }                                                                  \
        test_tcp_send(&f->tcp, f->handshake.buf + f->handshake.offset, n); \
    }

/* After a PEER_CONNECT() call, spin the event loop until the connected
 * callbloathack of the listening TCP handle gets called. */
#define WAIT_CONNECTED_CB LOOP_RUN(1);

/* After a PEER_HANDSHAKE() call, spin the event loop until the read
 * callback gets called. */
#define WAIT_READ_CB LOOP_RUN(1);

/* Spin the event loop until the accept callback gets eventually invoked. */
#define WAIT_ACCEPTED_CB                \
    LOOP_RUN_UNTIL(acceptCbInvoked, f); \
    f->invoked = 0;

/******************************************************************************
 *
 * Success scenarios.
 *
 *****************************************************************************/

TEST_SUITE(success);

TEST_SETUP(success, setup);
TEST_TEAR_DOWN(success, tear_down)

/* If the handshake is successful, the accept callback is invoked. */
TEST_CASE(success, first, NULL)
{
    struct fixture *f = data;
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

/******************************************************************************
 *
 * Failure scenarios.
 *
 *****************************************************************************/

TEST_SUITE(error);

TEST_SETUP(error, setup);
TEST_TEAR_DOWN(error, tear_down)

/* The client sends us a bad protocol version */
TEST_CASE(error, bad_protocol, NULL)
{
    struct fixture *f = data;
    (void)params;
    memset(f->handshake.buf, 999, sizeof(uint64_t));

    PEER_CONNECT;
    PEER_HANDSHAKE(0);

    WAIT_CONNECTED_CB;
    WAIT_READ_CB;

    return MUNIT_OK;
}

/* Parameters for sending a partial handshake */
static char *partial_handshake_n[] = {"8", "16", "24", "32", NULL};

static MunitParameterEnum listen_error_abort_params[] = {
    {"n", partial_handshake_n},
    {NULL, NULL},
};

/* The peer closes the connection after having sent a partial handshake. */
TEST_CASE(error, abort, listen_error_abort_params)
{
    struct fixture *f = data;
    const char *n_param = munit_parameters_get(params, "n");
    PEER_CONNECT;
    PEER_HANDSHAKE(atoi(n_param));
    WAIT_CONNECTED_CB;
    WAIT_READ_CB;
    test_tcp_close(&f->tcp);
    WAIT_READ_CB;

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
TEST_CASE(error, oom, listen_error_oom_params)
{
    struct fixture *f = data;
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

/******************************************************************************
 *
 * Close scenarios.
 *
 *****************************************************************************/

TEST_SUITE(close);

TEST_SETUP(close, setup);
TEST_TEAR_DOWN(close, tear_down)

/* Close the transport right after an incoming connection becomes pending, but
 * it hasn't been accepted yet. */
TEST_CASE(close, pending, NULL)
{
    struct fixture *f = data;
    (void)params;
    PEER_CONNECT;
    return MUNIT_OK;
}

/* Close the transport right after an incoming connection gets accepted, and the
 * peer hasn't sent handshake data yet. */
TEST_CASE(close, connected, NULL)
{
    struct fixture *f = data;
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
TEST_CASE(close, handshake, listen_close_handshake_params)
{
    struct fixture *f = data;
    const char *n_param = munit_parameters_get(params, "n");
    PEER_CONNECT;
    PEER_HANDSHAKE(atoi(n_param));
    WAIT_CONNECTED_CB;
    WAIT_READ_CB;
    return MUNIT_OK;
}
