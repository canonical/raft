#include "../../src/uv.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture with a libuv-based raft_io instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV;
    struct raft_io_send req;
    struct raft_message message;
    int invoked;
    int status;
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

static void sendCbAssertResult(struct raft_io_send *req, int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

/* Set the type of the fixture's message. */
#define SET_MESSAGE_TYPE(TYPE) f->message.type = TYPE;

#define SEND_REQ(MESSAGE, RV, STATUS)                             \
    struct raft_io_send _req;                                     \
    struct result _result = {STATUS, false};                      \
    int _rv;                                                      \
    _req.data = &_result;                                         \
    _rv = f->io.send(&f->io, &_req, MESSAGE, sendCbAssertResult); \
    munit_assert_int(_rv, ==, RV)

/* Submit a send request for the fixture's message and wait for the operation to
 * successfully complete. */
#define SEND                                               \
    {                                                      \
        SEND_REQ(&f->message, 0 /* rv */, 0 /* status */); \
        LOOP_RUN_UNTIL(&_result.done);                     \
    }

/* Submit a send request and assert that it fails synchronously with the
 * given error code and message. */
#define SEND_ERROR(RV, ERRMSG)                                       \
    {                                                                \
        SEND_REQ(&f->message, RV /* rv */, 0 /* status */);          \
        /* munit_assert_string_equal(f->transport.errmsg, ERRMSG);*/ \
    }

/* Submit a send request and wait for the operation to fail with the given code
 * and message. */
#define SEND_FAILURE(STATUS, ERRMSG)                                \
    {                                                               \
        SEND_REQ(&f->message, 0 /* rv */, STATUS);                  \
        LOOP_RUN_UNTIL(&_result.done);                              \
        /*munit_assert_string_equal(f->transport.errmsg, ERRMSG);*/ \
    }

#define send__invoke(RV)                                               \
    {                                                                  \
        int rv2;                                                       \
        rv2 = f->io.send(&f->io, &f->req, &f->message, send__send_cb); \
        munit_assert_int(rv2, ==, RV);                                 \
    }

#define send__wait_cb(STATUS)                    \
    {                                            \
        int i;                                   \
        for (i = 0; i < 5; i++) {                \
            if (f->invoked > 0) {                \
                break;                           \
            }                                    \
            LOOP_RUN(1);                         \
        }                                        \
        munit_assert_int(f->invoked, ==, 1);     \
        munit_assert_int(f->status, ==, STATUS); \
        f->invoked = 0;                          \
    }

#define send__set_message_type(TYPE) f->message.type = TYPE;

#define send__set_connect_retry_delay(MSECS) \
    {                                        \
        struct uv *uv = f->io.impl;          \
        uv->connect_retry_delay = 1;         \
    }

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV;
    f->message.type = RAFT_IO_REQUEST_VOTE;
    f->message.server_id = 1;
    f->message.server_address = f->tcp.server.address;
    f->req.data = f;
    f->invoked = 0;
    f->status = -1;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
    free(f);
}

static void send__send_cb(struct raft_io_send *req, int status)
{
    struct fixture *f = req->data;
    f->invoked++;
    f->status = status;
}

/******************************************************************************
 *
 * raft_io->send()
 *
 *****************************************************************************/

SUITE(send)

/* The first time a request is sent to a server a connection attempt is
 * triggered. If the connection succeeds the request gets written out. */
TEST(send, first, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    SEND;
    return MUNIT_OK;
}

/* The second time a request is sent it re-uses the connection that was already
 * established */
TEST(send, second, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    SEND;
    SEND;
    return MUNIT_OK;
}

/* Send a request vote result message. */
TEST(send, voteResult, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    SET_MESSAGE_TYPE(RAFT_IO_REQUEST_VOTE_RESULT);
    SEND;
    return MUNIT_OK;
}

/* Send an append entries message. */
TEST(send, appendEntries, setup, tear_down, 0, NULL)
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
TEST(send, heartbeat, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    SET_MESSAGE_TYPE(RAFT_IO_APPEND_ENTRIES);
    f->message.append_entries.entries = NULL;
    f->message.append_entries.n_entries = 0;
    SEND;
    return MUNIT_OK;
}

/* Send an append entries result message. */
TEST(send, appendEntriesResult, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    SET_MESSAGE_TYPE(RAFT_IO_APPEND_ENTRIES_RESULT);
    SEND;
    return MUNIT_OK;
}

/* Send an install snapshot message. */
TEST(send, installSnapshot, setup, tear_down, 0, NULL)
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
TEST(send, noConnection, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;

    f->message.server_address = "127.0.0.1:123456";

    send__set_connect_retry_delay(1);

    send__invoke(0);

    /* We keep retrying indefinitely */
    LOOP_RUN(2);

    munit_assert_int(f->invoked, ==, 0);

    return MUNIT_OK;
}

/* The message has an invalid IPv4 address. */
TEST(send, badAddress, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;

    (void)params;

    f->message.server_address = "1";

    send__invoke(0);

    /* The only active handle is the timer one, to retry the connection. */
    LOOP_RUN(1);

    /* The message hasn't been sent */
    munit_assert_int(f->invoked, ==, 0);

    return MUNIT_OK;
}

/* The message has an invalid type. */
TEST(send, badMessage, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    SET_MESSAGE_TYPE(666);
    SEND_ERROR(RAFT_MALFORMED, "");
    return MUNIT_OK;
}

/* After the connection is established the peer dies and then comes back a
 * little bit later. */
TEST(send, reconnect, setup, tear_down, 0, NULL)
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
TEST(send, queue, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_tcp_stop(&f->tcp);

    send__invoke(0);
    send__invoke(0);
    send__invoke(0);
    send__invoke(0);

    send__wait_cb(RAFT_NOCONNECTION);

    return MUNIT_OK;
}

static char *error_oom_heap_fault_delay[] = {"0", "1", "2", "3",
                                             "4", "5", NULL};
static char *error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST(send, oom, setup, tear_down, 0, error_oom_params)
{
    struct fixture *f = data;
    HEAP_FAULT_ENABLE;
    SEND_ERROR(RAFT_NOMEM, "");
    return MUNIT_OK;
}

static char *error_oom_async_heap_fault_delay[] = {"0", NULL};
static char *error_oom_async_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum error_oom_async_params[] = {
    {TEST_HEAP_FAULT_DELAY, error_oom_async_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, error_oom_async_heap_fault_repeat},
    {NULL, NULL},
};

/* Transient out of memory error happening after @raft__io_uv_rpc_send has
 * returned. */
TEST(send, oom_async, setup, tear_down, 0, error_oom_async_params)
{
    struct fixture *f = data;
    send__set_connect_retry_delay(1);
    send__invoke(0);
    HEAP_FAULT_ENABLE;
    send__wait_cb(0);
    return MUNIT_OK;
}

/* The backend gets closed while there is a pending write. */
TEST(send, closeDuringWrite, setup, tear_down, 0, NULL)
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

    send__invoke(0);

    /* Spin once so the connection attempt succeeds and we flush the pending
     * request, triggering the write. */
    LOOP_RUN(1);

    UV_CLOSE;

    send__wait_cb(RAFT_NOCONNECTION);

    raft_free(entry.buf.base);

    return MUNIT_OK;
}

/* The backend gets closed while there is a pending connect request. */
TEST(send, closeDuringConnection, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;

    send__invoke(0);
    UV_CLOSE;

    send__wait_cb(RAFT_NOCONNECTION);

    return MUNIT_OK;
}
