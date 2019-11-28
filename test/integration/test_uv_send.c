#include <unistd.h>

#include "../lib/runner.h"
#include "../lib/tcp.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture with a libuv-based raft_io instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_TCP;
    FIXTURE_UV;
    struct raft_message message;
    bool closed;
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

static void closeCb(struct raft_io *io)
{
    struct fixture *f = io->data;
    f->closed = true;
}

static void sendCbAssertResult(struct raft_io_send *req, int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

/* Set the type of the fixture's message. */
#define SET_MESSAGE_TYPE(TYPE) f->message.type = TYPE;

/* Change the default connect retry delay. */
#define SET_CONNECT_RETRY_DELAY(MSECS) \
    raft_uv_set_connect_retry_delay(&f->io, MSECS)

#define SEND_REQ(RV, STATUS)                                          \
    struct raft_io_send _req;                                         \
    struct result _result = {STATUS, false};                          \
    int _rv;                                                          \
    _req.data = &_result;                                             \
    _rv = f->io.send(&f->io, &_req, &f->message, sendCbAssertResult); \
    munit_assert_int(_rv, ==, RV)

/* Submit a send request for the fixture's message and wait for the operation to
 * successfully complete. */
#define SEND                                  \
    {                                         \
        SEND_REQ(0 /* rv */, 0 /* status */); \
        LOOP_RUN_UNTIL(&_result.done);        \
    }

/* Submit a send request and assert that it fails synchronously with the
 * given error code and message. */
#define SEND_ERROR(RV, ERRMSG)                                       \
    {                                                                \
        SEND_REQ(RV, 0 /* status */);                                \
        /* munit_assert_string_equal(f->transport.errmsg, ERRMSG);*/ \
    }

/* Submit a send request and wait for the operation to fail with the given code
 * and message. */
#define SEND_FAILURE(STATUS, ERRMSG)                                \
    {                                                               \
        SEND_REQ(0 /* rv */, STATUS);                               \
        LOOP_RUN_UNTIL(&_result.done);                              \
        /*munit_assert_string_equal(f->transport.errmsg, ERRMSG);*/ \
    }

/* Submit a send request, close the backend after N loop iterations and assert
 * that the request got canceled. */
#define SEND_CLOSE(N)                        \
    {                                        \
        SEND_REQ(0 /* rv */, RAFT_CANCELED); \
        LOOP_RUN(N);                         \
        munit_assert_false(_result.done);    \
        f->io.close(&f->io, closeCb);        \
        LOOP_RUN_UNTIL(&_result.done);       \
        LOOP_RUN_UNTIL(&f->closed);          \
        raft_uv_close(&f->io);               \
    }

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setUpDeps(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    SETUP_TCP;
    TCP_SERVER_LISTEN;
    f->io.data = f;
    f->closed = false;
    return f;
}

static void tearDownDeps(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_TCP;
    TEAR_DOWN_UV_DEPS;
    free(f);
}

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = setUpDeps(params, user_data);
    SETUP_UV;
    f->message.type = RAFT_IO_REQUEST_VOTE;
    f->message.server_id = 1;
    f->message.server_address = f->tcp.server.address;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
    tearDownDeps(f);
}

/******************************************************************************
 *
 * raft_io->send()
 *
 *****************************************************************************/

SUITE(send)

/* The first time a request is sent to a server a connection attempt is
 * triggered. If the connection succeeds the request gets written out. */
TEST(send, first, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SEND;
    return MUNIT_OK;
}

/* The second time a request is sent it re-uses the connection that was already
 * established */
TEST(send, second, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SEND;
    SEND;
    return MUNIT_OK;
}

/* Send a request vote result message. */
TEST(send, voteResult, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SET_MESSAGE_TYPE(RAFT_IO_REQUEST_VOTE_RESULT);
    SEND;
    return MUNIT_OK;
}

/* Send an append entries message. */
TEST(send, appendEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entries[2];
    entries[0].buf.base = raft_malloc(16);
    entries[0].buf.len = 16;
    entries[1].buf.base = raft_malloc(8);
    entries[1].buf.len = 8;

    SET_MESSAGE_TYPE(RAFT_IO_APPEND_ENTRIES);
    f->message.append_entries.entries = entries;
    f->message.append_entries.n_entries = 2;

    SEND;

    raft_free(entries[0].buf.base);
    raft_free(entries[1].buf.base);

    return MUNIT_OK;
}

/* Send an append entries message with zero entries (i.e. a heartbeat). */
TEST(send, heartbeat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SET_MESSAGE_TYPE(RAFT_IO_APPEND_ENTRIES);
    f->message.append_entries.entries = NULL;
    f->message.append_entries.n_entries = 0;
    SEND;
    return MUNIT_OK;
}

/* Send an append entries result message. */
TEST(send, appendEntriesResult, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SET_MESSAGE_TYPE(RAFT_IO_APPEND_ENTRIES_RESULT);
    SEND;
    return MUNIT_OK;
}

/* Send an install snapshot message. */
TEST(send, installSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_install_snapshot *p = &f->message.install_snapshot;
    int rv;

    SET_MESSAGE_TYPE(RAFT_IO_INSTALL_SNAPSHOT);

    raft_configuration_init(&p->conf);
    rv = raft_configuration_add(&p->conf, 1, "1", true);
    munit_assert_int(rv, ==, 0);

    p->data.len = 8;
    p->data.base = raft_malloc(p->data.len);

    SEND;

    raft_configuration_close(&p->conf);
    raft_free(p->data.base);

    return MUNIT_OK;
}

/* A connection attempt fails asynchronously after the connect function
 * returns. */
TEST(send, noConnection, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    f->message.server_address = "127.0.0.1:123456";
    SET_CONNECT_RETRY_DELAY(1);
    SEND_CLOSE(2);
    return MUNIT_OK;
}

/* The message has an invalid IPv4 address. */
TEST(send, badAddress, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    f->message.server_address = "1";
    SEND_CLOSE(1);
    return MUNIT_OK;
}

/* The message has an invalid type. */
TEST(send, badMessage, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SET_MESSAGE_TYPE(666);
    SEND_ERROR(RAFT_MALFORMED, "");
    return MUNIT_OK;
}

/* After the connection is established the peer dies and then comes back a
 * little bit later. */
TEST(send, reconnect, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int socket;
    SEND;
    socket = test_tcp_accept(&f->tcp);
    close(socket);
    SEND_FAILURE(RAFT_IOERR, "");
    SEND;
    return MUNIT_OK;
}

/* If there's no more space in the queue of pending requests, the oldest request
 * gets evicted and its callback fired with RAFT_NOCONNECTION. */
TEST(send, queue, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    struct raft_io_send reqs[4];
    struct result result = {RAFT_NOCONNECTION, false};
    unsigned i;
    int rv;
    test_tcp_stop(&f->tcp);

    reqs[0].data = &result;
    rv = f->io.send(&f->io, &reqs[0], &f->message, sendCbAssertResult);
    munit_assert_int(rv, ==, 0);

    for (i = 1; i < 4; i++) {
        rv = f->io.send(&f->io, &reqs[i], &f->message, NULL);
        munit_assert_int(rv, ==, 0);
    }

    LOOP_RUN_UNTIL(&result.done);
    TEAR_DOWN_UV;

    return MUNIT_OK;
}

static char *oomHeapFaultDelay[] = {"0", "1", "2", "3", "4", NULL};
static char *oomHeapFaultRepeat[] = {"1", NULL};

static MunitParameterEnum oomParams[] = {
    {TEST_HEAP_FAULT_DELAY, oomHeapFaultDelay},
    {TEST_HEAP_FAULT_REPEAT, oomHeapFaultRepeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST(send, oom, setUp, tearDown, 0, oomParams)
{
    struct fixture *f = data;
    HEAP_FAULT_ENABLE;
    SEND_ERROR(RAFT_NOMEM, "");
    return MUNIT_OK;
}

static char *oomAsyncHeapFaultDelay[] = {"2", NULL};
static char *oomAsyncHeapFaultRepeat[] = {"1", NULL};

static MunitParameterEnum oomAsyncParams[] = {
    {TEST_HEAP_FAULT_DELAY, oomAsyncHeapFaultDelay},
    {TEST_HEAP_FAULT_REPEAT, oomAsyncHeapFaultRepeat},
    {NULL, NULL},
};

/* Transient out of memory error happening after send() has returned. */
TEST(send, oomAsync, setUp, tearDown, 0, oomAsyncParams)
{
    struct fixture *f = data;
    SET_CONNECT_RETRY_DELAY(1);
    SEND;
    return MUNIT_OK;
}

/* The backend gets closed while there is a pending write. */
TEST(send, closeDuringWrite, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;

    /* Set a very large message that is likely to fill the socket buffer.
     * TODO: figure a more deterministic way to choose the value. */
    entry.buf.len = 1024 * 1024 * 8;
    entry.buf.base = raft_malloc(entry.buf.len);

    SET_MESSAGE_TYPE(RAFT_IO_APPEND_ENTRIES);
    f->message.append_entries.entries = &entry;
    f->message.append_entries.n_entries = 1;

    SEND_CLOSE(2);

    raft_free(entry.buf.base);

    return MUNIT_OK;
}

/* The backend gets closed while there is a pending connect request. */
TEST(send, closeDuringConnection, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    SEND_CLOSE(0);
    return MUNIT_OK;
}
