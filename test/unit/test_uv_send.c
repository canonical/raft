#include <unistd.h>

#include "../lib/uv.h"
#include "../lib/runner.h"

#include "../../src/uv.h"

TEST_MODULE(io_uv_send);

/**
 * Helpers.
 */

struct fixture
{
    FIXTURE_UV
    struct raft_io_send req;
    struct raft_message message;
    int invoked;
    int status;
};

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
}

static void send__send_cb(struct raft_io_send *req, int status)
{
    struct fixture *f = req->data;
    f->invoked++;
    f->status = status;
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
            LOOP_RUN(1);            \
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

/**
 * Success scenarios.
 */

TEST_SUITE(success);

TEST_SETUP(success, setup);
TEST_TEAR_DOWN(success, tear_down);

/* The first time a request is sent to a server a connection attempt is
 * triggered. If the connection succeeds the request gets written out. */
TEST_CASE(success, first, NULL)
{
    struct fixture *f = data;

    (void)params;

    send__invoke(0);
    send__wait_cb(0);

    return MUNIT_OK;
}

/* The second time a request is sent it re-uses the connection that was already
 * established */
TEST_CASE(success, second, NULL)
{
    struct fixture *f = data;

    (void)params;

    send__invoke(0);
    send__wait_cb(0);

    send__invoke(0);
    send__wait_cb(0);

    return MUNIT_OK;
}

/* Send a request vote result message. */
TEST_CASE(success, vote_result, NULL)
{
    struct fixture *f = data;

    (void)params;

    send__set_message_type(RAFT_IO_REQUEST_VOTE_RESULT);
    send__invoke(0);
    send__wait_cb(0);

    return MUNIT_OK;
}

/* Send an append entries message. */
TEST_CASE(success, append_entries, NULL)
{
    struct fixture *f = data;
    struct raft_entry entries[2];

    (void)params;

    entries[0].buf.base = raft_malloc(16);
    entries[0].buf.len = 16;

    entries[1].buf.base = raft_malloc(8);
    entries[1].buf.len = 8;

    send__set_message_type(RAFT_IO_APPEND_ENTRIES);

    f->message.append_entries.entries = entries;
    f->message.append_entries.n_entries = 2;

    send__invoke(0);
    send__wait_cb(0);

    raft_free(entries[0].buf.base);
    raft_free(entries[1].buf.base);

    return MUNIT_OK;
}

/* Send an append entries message with zero entries (i.e. a heartbeat). */
TEST_CASE(success, heartbeat, NULL)
{
    struct fixture *f = data;

    (void)params;

    send__set_message_type(RAFT_IO_APPEND_ENTRIES);

    f->message.append_entries.entries = NULL;
    f->message.append_entries.n_entries = 0;

    send__invoke(0);
    send__wait_cb(0);

    return MUNIT_OK;
}

/* Send an append entries result message. */
TEST_CASE(success, append_entries_result, NULL)
{
    struct fixture *f = data;

    (void)params;

    send__set_message_type(RAFT_IO_APPEND_ENTRIES_RESULT);
    send__invoke(0);
    send__wait_cb(0);

    return MUNIT_OK;
}

/* Send an install snapshot message. */
TEST_CASE(success, install_snapshot, NULL)
{
    struct fixture *f = data;
    struct raft_install_snapshot *p = &f->message.install_snapshot;
    int rv;

    (void)params;

    send__set_message_type(RAFT_IO_INSTALL_SNAPSHOT);

    raft_configuration_init(&p->conf);
    rv = raft_configuration_add(&p->conf, 1, "1", true);
    munit_assert_int(rv, ==, 0);

    p->data.len = 8;
    p->data.base = raft_malloc(p->data.len);

    send__invoke(0);
    send__wait_cb(0);

    raft_configuration_close(&p->conf);
    raft_free(p->data.base);

    return MUNIT_OK;
}

/**
 * Error scenarios.
 */

TEST_SUITE(error);

TEST_SETUP(error, setup);
TEST_TEAR_DOWN(error, tear_down);

/* A connection attempt fails asynchronously after the connect function
 * returns. */
TEST_CASE(error, connect, NULL)
{
    struct fixture *f = data;

    (void)params;

    f->message.server_address = "127.0.0.1:123456";

    send__set_connect_retry_delay(1);

    send__invoke(0);

    /* We keep retrying indefinitely */
    LOOP_RUN(2);

    munit_assert_int(f->invoked, ==, 0);

    return MUNIT_OK;
}

/* The message has an invalid IPv4 address. */
TEST_CASE(error, bad_address, NULL)
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
TEST_CASE(error, bad_message, NULL)
{
    struct fixture *f = data;

    (void)params;

    send__set_message_type(666);

    send__invoke(RAFT_MALFORMED);

    return MUNIT_OK;
}

/* After the connection is established the peer dies and then comes back a
 * little bit later. */
TEST_CASE(error, reconnect, NULL)
{
    struct fixture *f = data;
    int socket;

    (void)params;

    send__invoke(0);
    send__wait_cb(0);

    socket = test_tcp_accept(&f->tcp);
    close(socket);

    send__invoke(0);
    send__wait_cb(RAFT_IOERR);

    send__invoke(0);
    send__wait_cb(0);

    return MUNIT_OK;
}

/* If there's no more space in the queue of pending requests, the oldest request
 * gets evicted and its callback fired with RAFT_NOCONNECTION. */
TEST_CASE(error, queue, NULL)
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
TEST_CASE(error, oom, error_oom_params)
{
    struct fixture *f = data;

    (void)params;

    test_heap_fault_enable(&f->heap);

    send__invoke(RAFT_NOMEM);

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
TEST_CASE(error, oom_async, error_oom_async_params)
{
    struct fixture *f = data;

    (void)params;

    send__set_connect_retry_delay(1);

    send__invoke(0);

    test_heap_fault_enable(&f->heap);

    send__wait_cb(0);

    return MUNIT_OK;
}

/**
 * Close back scenarios.
 */

TEST_SUITE(close);

TEST_SETUP(close, setup);
TEST_TEAR_DOWN(close, tear_down);

/* The backend gets closed while there is a pending write. */
TEST_CASE(close, writing, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;

    (void)params;

    /* Set a very large message that is likely to fill the socket buffer.
     * TODO: figure a more deterministic way to choose the value. */
    entry.buf.len = 1024 * 1024 * 8;
    entry.buf.base = raft_malloc(entry.buf.len);

    send__set_message_type(RAFT_IO_APPEND_ENTRIES);

    f->message.append_entries.entries = &entry;
    f->message.append_entries.n_entries = 1;

    send__invoke(0);

    /* Spin once so the connection attempt succeeds and we flush the pending
     * request, triggering the write. */
    LOOP_RUN(1);

    UV_CLOSE;

    send__wait_cb(RAFT_CANCELED);

    raft_free(entry.buf.base);

    return MUNIT_OK;
}

/* The backend gets closed while there is a pending connect request. */
TEST_CASE(close, connecting, NULL)
{
    struct fixture *f = data;

    (void)params;

    send__invoke(0);
    UV_CLOSE;

    send__wait_cb(RAFT_CANCELED);

    return MUNIT_OK;
}
