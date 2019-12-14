#include "../../include/raft.h"
#include "../../include/raft/uv.h"
#include "../../src/byte.h"
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
    bool accepted;
    bool closed;
    struct
    {
        uint8_t buf[sizeof(uint64_t) + /* Protocol version */
                    sizeof(uint64_t) + /* Server ID */
                    sizeof(uint64_t) + /* Length of address */
                    sizeof(uint64_t) * 2 /* Address */];
        size_t offset;
    } handshake;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

#define PEER_ID 2
#define PEER_ADDRESS "127.0.0.1:666"

static void closeCb(struct raft_uv_transport *transport)
{
    struct fixture *f = transport->data;
    f->closed = true;
}

static void acceptCb(struct raft_uv_transport *t,
                     raft_id id,
                     const char *address,
                     struct uv_stream_s *stream)
{
    struct fixture *f = t->data;
    munit_assert_int(id, ==, PEER_ID);
    munit_assert_string_equal(address, PEER_ADDRESS);
    f->accepted = true;
    uv_close((struct uv_handle_s *)stream, (uv_close_cb)raft_free);
}

#define INIT                                                         \
    do {                                                             \
        int _rv;                                                     \
        _rv = raft_uv_tcp_init(&f->transport, &f->loop);             \
        munit_assert_int(_rv, ==, 0);                                \
        _rv = f->transport.init(&f->transport, 1, "127.0.0.1:9000"); \
        munit_assert_int(_rv, ==, 0);                                \
        f->transport.data = f;                                       \
        f->closed = false;                                           \
    } while (0)

#define CLOSE                                       \
    do {                                            \
        f->transport.close(&f->transport, closeCb); \
        LOOP_RUN_UNTIL(&f->closed);                 \
        raft_uv_tcp_close(&f->transport);           \
    } while (0)

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
    TEAR_DOWN_TCP;
    TEAR_DOWN_LOOP;
    TEAR_DOWN_HEAP;
    free(f);
}

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = setUpDeps(params, user_data);
    void *cursor;
    int rv;
    /* test_tcp_listen(&f->tcp); */
    INIT;
    f->accepted = false;
    f->handshake.offset = 0;

    cursor = f->handshake.buf;
    bytePut64(&cursor, 1);
    bytePut64(&cursor, PEER_ID);
    bytePut64(&cursor, 16);
    strcpy(cursor, PEER_ADDRESS);

    rv = f->transport.listen(&f->transport, acceptCb);
    munit_assert_int(rv, ==, 0);

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
 * Helper macros
 *
 *****************************************************************************/

/* Connect to the listening socket of the transport, creating a new connection
 * that is waiting to be accepted. */
#define PEER_CONNECT TCP_CLIENT_CONNECT(9000)

/* Make the peer close the connection. */
#define PEER_CLOSE TCP_CLIENT_CLOSE

/* Make the connected client send handshake data. */
#define PEER_HANDSHAKE                        \
    do {                                      \
        size_t n = sizeof f->handshake.buf;   \
        TCP_CLIENT_SEND(f->handshake.buf, n); \
    } while (0)

/* Make the connected client send partial handshake data: only N bytes will be
 * sent, starting from the offset of the last call. */
#define PEER_HANDSHAKE_PARTIAL(N)                                   \
    do {                                                            \
        TCP_CLIENT_SEND(f->handshake.buf + f->handshake.offset, N); \
    } while (0)

/* After a PEER_CONNECT() call, spin the event loop until the connected
 * callback of the listening TCP handle gets called. */
#define LOOP_RUN_UNTIL_CONNECTED LOOP_RUN(1);

/* After a PEER_HANDSHAKE_PARTIAL() call, spin the event loop until the read
 * callback gets called. */
#define LOOP_RUN_UNTIL_READ LOOP_RUN(1);

/* Spin the event loop until the accept callback gets eventually invoked. */
#define ACCEPT LOOP_RUN_UNTIL(&f->accepted);

/******************************************************************************
 *
 * Success scenarios.
 *
 *****************************************************************************/

SUITE(tcp_listen)

/* If the handshake is successful, the accept callback is invoked. */
TEST(tcp_listen, first, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PEER_CONNECT;
    PEER_HANDSHAKE;
    ACCEPT;
    return MUNIT_OK;
}

/* The client sends us a bad protocol version */
TEST(tcp_listen, badProtocol, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    memset(f->handshake.buf, 999, sizeof(uint64_t));
    PEER_CONNECT;
    PEER_HANDSHAKE;
    LOOP_RUN_UNTIL_CONNECTED;
    LOOP_RUN_UNTIL_READ;
    return MUNIT_OK;
}

/* Parameters for sending a partial handshake */
static char *partialHandshakeN[] = {"8", "16", "24", "32", NULL};

static MunitParameterEnum peerAbortParams[] = {
    {"n", partialHandshakeN},
    {NULL, NULL},
};

/* The peer closes the connection after having sent a partial handshake. */
TEST(tcp_listen, peerAbort, setUp, tearDown, 0, peerAbortParams)
{
    struct fixture *f = data;
    const char *n = munit_parameters_get(params, "n");
    PEER_CONNECT;
    PEER_HANDSHAKE_PARTIAL(atoi(n));
    LOOP_RUN_UNTIL_CONNECTED;
    LOOP_RUN_UNTIL_READ;
    PEER_CLOSE;
    return MUNIT_OK;
}

/* TODO: skip "2" because it makes libuv crash, as it calls abort(). See also
 * https://github.com/libuv/libuv/issues/1948 */
static char *oomHeapFaultDelay[] = {"0", "1", "3", NULL};
static char *oomHeapFaultRepeat[] = {"1", NULL};

static MunitParameterEnum oomParams[] = {
    {TEST_HEAP_FAULT_DELAY, oomHeapFaultDelay},
    {TEST_HEAP_FAULT_REPEAT, oomHeapFaultRepeat},
    {NULL, NULL},
};

/* Out of memory conditions */
TEST(tcp_listen, oom, setUp, tearDown, 0, oomParams)
{
    struct fixture *f = data;
    PEER_CONNECT;
    PEER_HANDSHAKE;
    HEAP_FAULT_ENABLE;

    /* Run as much as possible. */
    uv_run(&f->loop, UV_RUN_NOWAIT);
    uv_run(&f->loop, UV_RUN_NOWAIT);
    uv_run(&f->loop, UV_RUN_NOWAIT);

    return MUNIT_OK;
}

/* Close the transport right after an incoming connection becomes pending, but
 * it hasn't been accepted yet. */
TEST(tcp_listen, pending, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PEER_CONNECT;
    return MUNIT_OK;
}

/* Close the transport right after an incoming connection gets accepted, and the
 * peer hasn't sent handshake data yet. */
TEST(tcp_listen, closeBeforeHandshake, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PEER_CONNECT;
    LOOP_RUN_UNTIL_CONNECTED;
    return MUNIT_OK;
}

static MunitParameterEnum closeDuringHandshake[] = {
    {"n", partialHandshakeN},
    {NULL, NULL},
};

/* Close the transport right after the peer has started to send handshake data,
 * but isn't done with it yet. */
TEST(tcp_listen, handshake, setUp, tearDown, 0, closeDuringHandshake)
{
    struct fixture *f = data;
    const char *n_param = munit_parameters_get(params, "n");
    PEER_CONNECT;
    PEER_HANDSHAKE_PARTIAL(atoi(n_param));
    LOOP_RUN_UNTIL_CONNECTED;
    LOOP_RUN_UNTIL_READ;
    return MUNIT_OK;
}
