#include "../../src/binary.h"
#include "../../src/io_uv_encoding.h"
#include "../../src/io_uv_rpc.h"

#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/munit.h"
#include "../lib/tcp.h"
#include "../lib/uv.h"

/**
 * Fixture
 */

struct fixture
{
    struct raft_heap heap;
    struct raft_logger logger;
    struct test_tcp tcp;
    struct uv_loop_s loop;
    struct raft_io_uv_transport transport;
    struct raft_io_uv_rpc rpc;
    struct
    {
        bool invoked;
        int status;
    } send_cb;
    struct
    {
        bool invoked;
        struct raft_message *message;
    } recv_cb;
    struct
    {
        bool invoked;
    } stop_cb;
};

static void __send_cb(void *data, int status)
{
    struct fixture *f = data;

    f->send_cb.status = status;
}

static void __recv_cb(void *data, struct raft_message *message)
{
    struct fixture *f = data;

    f->recv_cb.message = message;
    f->recv_cb.invoked = true;
}

static void __stop_cb(void *data)
{
    struct fixture *f = data;

    f->stop_cb.invoked = true;
}

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int rv;

    (void)user_data;

    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, 1);
    test_tcp_setup(params, &f->tcp);
    test_uv_setup(params, &f->loop);

    rv = raft_io_uv_tcp_init(&f->transport, &f->logger, &f->loop);
    munit_assert_int(rv, ==, 0);

    rv = raft_io_uv_rpc__init(&f->rpc, &f->logger, &f->loop, &f->transport);
    munit_assert_int(rv, ==, 0);

    rv = raft_io_uv_rpc__start(&f->rpc, 1, "127.0.0.1:9000", __recv_cb);
    munit_assert_int(rv, ==, 0);

    f->rpc.data = f;

    f->send_cb.invoked = false;
    f->send_cb.status = -1;

    f->recv_cb.invoked = false;
    f->recv_cb.message = NULL;

    f->stop_cb.invoked = false;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    raft_io_uv_rpc__stop(&f->rpc, f, __stop_cb);

    test_uv_stop(&f->loop);

    /* The stop callback was invoked. */
    munit_assert_true(f->stop_cb.invoked);

    /* Release any entries that we received */
    if (f->recv_cb.message != NULL) {
        if (f->recv_cb.message->type == RAFT_IO_APPEND_ENTRIES) {
            if (f->recv_cb.message->append_entries.entries != NULL) {
                raft_free(f->recv_cb.message->append_entries.entries[0].batch);
                raft_free(f->recv_cb.message->append_entries.entries);
            }
        }
    }

    raft_io_uv_rpc__close(&f->rpc);
    raft_io_uv_tcp_close(&f->transport);

    test_uv_tear_down(&f->loop);
    test_tcp_tear_down(&f->tcp);
    test_logger_tear_down(&f->logger);
    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Initialize the given message struct, setting its type and recipient.
 */
#define __message(F, MESSAGE, TYPE)                     \
    {                                                   \
        MESSAGE.type = TYPE;                            \
        MESSAGE.server_id = 1;                          \
        MESSAGE.server_address = F->tcp.server.address; \
    }

/**
 * Send a new request and check that no error occurs;
 */
#define __send(F, MESSAGE)                                          \
    {                                                               \
        int rv;                                                     \
                                                                    \
        f->send_cb.status = -1;                                     \
                                                                    \
        rv = raft_io_uv_rpc__send(&F->rpc, &MESSAGE, F, __send_cb); \
        munit_assert_int(rv, ==, 0);                                \
    }

/**
 * Connect to the fixture's RPC endpoint and send the handshake data.
 */
#define __conn(F)                                                      \
    {                                                                  \
        size_t size;                                                   \
        void *buf;                                                     \
        void *cursor;                                                  \
                                                                       \
        test_tcp_connect(&F->tcp, 9000);                               \
                                                                       \
        size = sizeof(uint64_t) * 3 /* Preamble */ + 16 /* Address */; \
                                                                       \
        buf = munit_malloc(size);                                      \
        cursor = buf;                                                  \
                                                                       \
        raft__put64(&cursor, 1);  /* Protocol */                       \
        raft__put64(&cursor, 2);  /* Server ID */                      \
        raft__put64(&cursor, 16); /* Address size */                   \
        strcpy(cursor, "127.0.0.1:66");                                \
                                                                       \
        test_tcp_send(&F->tcp, buf, size);                             \
                                                                       \
        free(buf);                                                     \
    }

/**
 * Receive the given message sent over the given socket and check that no error
 * occurs.
 */
#define __recv(F, MESSAGE, SOCKET)                                 \
    {                                                              \
        uv_buf_t *bufs;                                            \
        unsigned n_bufs;                                           \
        unsigned i;                                                \
        int rv;                                                    \
                                                                   \
        rv = raft_io_uv_encode__message(&MESSAGE, &bufs, &n_bufs); \
        munit_assert_int(rv, ==, 0);                               \
                                                                   \
        for (i = 0; i < n_bufs; i++) {                             \
            test_tcp_send(&F->tcp, bufs[i].base, bufs[i].len);     \
            raft_free(bufs[i].base);                               \
        }                                                          \
                                                                   \
        raft_free(bufs);                                           \
    }

/**
 * Assertions
 */

#define __assert_send_error(F, MESSAGE, RV)                       \
    {                                                             \
        int rv;                                                   \
                                                                  \
        rv = raft_io_uv_rpc__send(&F->rpc, &MESSAGE, NULL, NULL); \
        munit_assert_int(rv, ==, RV);                             \
    }

/**
 * raft_io_uv_rpc__send
 */

/**
 * The first time a request is sent to a server a connection attempt is
 * triggered. If the connection succeeds the request gets written out.
 */
static MunitResult test_send_first(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_REQUEST_VOTE);

    __send(f, message);

    /* The connection succeeds in the first iteration, then second one writes
     * the handshake, and the request gets completed in the third one. */
    n_handles = test_uv_run(&f->loop, 3);
    munit_assert_int(n_handles, ==, 1); /* Only the listener handles is left */

    /* The request succeeded. */
    munit_assert_int(f->send_cb.status, ==, 0);

    return MUNIT_OK;
}

/**
 * The second time a request is sent it re-uses the connection that was already
 * established
 */
static MunitResult test_send_second(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_REQUEST_VOTE);

    __send(f, message);

    /* The connection succeeds in the first iteration, then second one writes
     * the handshake, and the request gets completed in the third one. */
    n_handles = test_uv_run(&f->loop, 3);
    munit_assert_int(n_handles, ==, 1); /* Only the listener handles is left */

    /* The request succeeded. */
    munit_assert_int(f->send_cb.status, ==, 0);

    /* Issue a second request */
    __send(f, message);

    /* This time only one iteration is needed, as the connection was already
     * established. */
    n_handles = test_uv_run(&f->loop, 1);
    munit_assert_int(n_handles, ==, 1); /* Only the listener handles is left */

    /* The second request succeeded as well. */
    munit_assert_int(f->send_cb.status, ==, 0);

    return MUNIT_OK;
}

/**
 * Send a request vote result message.
 */
static MunitResult test_send_vote_result(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_REQUEST_VOTE);

    __send(f, message);

    /* The connection succeeds in the first iteration, then second one writes
     * the handshake, and the request gets completed in the third one. */
    n_handles = test_uv_run(&f->loop, 3);
    munit_assert_int(n_handles, ==, 1); /* Only the listener handles is left */

    /* The request succeeded. */
    munit_assert_int(f->send_cb.status, ==, 0);

    return MUNIT_OK;
}

/**
 * Send an append entries message.
 */
static MunitResult test_send_append_entries(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    struct raft_entry entries[2];
    int n_handles;

    (void)params;

    entries[0].buf.base = raft_malloc(16);
    entries[0].buf.len = 16;

    entries[1].buf.base = raft_malloc(8);
    entries[1].buf.len = 8;

    __message(f, message, RAFT_IO_APPEND_ENTRIES);

    message.append_entries.entries = entries;
    message.append_entries.n_entries = 2;

    __send(f, message);

    n_handles = test_uv_run(&f->loop, 3);
    munit_assert_int(n_handles, ==, 1); /* Only the listener handles is left */

    /* The request succeeded. */
    munit_assert_int(f->send_cb.status, ==, 0);

    raft_free(entries[0].buf.base);
    raft_free(entries[1].buf.base);

    return MUNIT_OK;
}
/**
 * Send an append entries message with zero entries (i.e. a heartbeat).
 */
static MunitResult test_send_heartbeat(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_APPEND_ENTRIES);

    message.append_entries.entries = NULL;
    message.append_entries.n_entries = 0;

    __send(f, message);

    n_handles = test_uv_run(&f->loop, 3);
    munit_assert_int(n_handles, ==, 1); /* Only the listener handles is left */

    /* The request succeeded. */
    munit_assert_int(f->send_cb.status, ==, 0);

    return MUNIT_OK;
}

/**
 * Send an append entries result message.
 */
static MunitResult test_send_append_result(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_APPEND_ENTRIES_RESULT);

    __send(f, message);

    n_handles = test_uv_run(&f->loop, 3);
    munit_assert_int(n_handles, ==, 1); /* Only the listener handles is left */

    /* The request succeeded. */
    munit_assert_int(f->send_cb.status, ==, 0);

    return MUNIT_OK;
}

/**
 * A connection attempt fails asynchronously after the connect function returns.
 */
static MunitResult test_send_connect_error(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_REQUEST_VOTE);

    message.server_address = "127.0.0.1:123456";

    f->rpc.connect_retry_delay = 1;

    __send(f, message);

    /* We keep retrying indefinitely */
    n_handles = test_uv_run(&f->loop, 2);
    munit_assert_int(n_handles, ==, 1);

    return MUNIT_OK;
}

/**
 * The message has an invalid IPv4 address.
 */

static MunitResult test_send_bad_address(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_REQUEST_VOTE);

    message.server_address = "1";

    __send(f, message);

    /* The only active handle is the timer one, to retry the connection. */
    n_handles = test_uv_run(&f->loop, 1);
    munit_assert_int(n_handles, ==, 1);

    /* The message hasn't been sent */
    munit_assert_false(f->send_cb.invoked);

    return MUNIT_OK;
}

/**
 * The message has an invalid type.
 */

static MunitResult test_send_bad_message(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    struct raft_message message;

    (void)params;

    __message(f, message, 666);

    __assert_send_error(f, message, RAFT_ERR_IO_MALFORMED);

    return MUNIT_OK;
}


/**
 * After the connection is established the peer dies and then comes back a
 * little bit later.
 */

static MunitResult test_send_reconnect(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int socket;

    (void)params;

    __message(f, message, RAFT_IO_REQUEST_VOTE);

    __send(f, message);

    test_uv_run(&f->loop, 3);
    munit_assert_int(f->send_cb.status, ==, 0);

    f->send_cb.status = -1;

    socket = test_tcp_accept(&f->tcp);
    close(socket);

    __send(f, message);

    test_uv_run(&f->loop, 1);
    munit_assert_int(f->send_cb.status, ==, UV_ECONNRESET);

    __send(f, message);
    test_uv_run(&f->loop, 3);
    munit_assert_int(f->send_cb.status, ==, 0);

    return MUNIT_OK;
}

static char *send_oom_heap_fault_delay[] = {"0", "1", "2", NULL};
static char *send_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum send_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, send_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, send_oom_heap_fault_repeat},
    {NULL, NULL},
};

/**
 * Out of memory conditions.
 */
static MunitResult test_send_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_message message;

    (void)params;

    __message(f, message, RAFT_IO_REQUEST_VOTE);

    test_heap_fault_enable(&f->heap);

    __assert_send_error(f, message, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

static char *send_oom_async_heap_fault_delay[] = {"0", "1", NULL};
static char *send_oom_async_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum send_oom_async_params[] = {
    {TEST_HEAP_FAULT_DELAY, send_oom_async_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, send_oom_async_heap_fault_repeat},
    {NULL, NULL},
};

/**
 * Out of memory happening after @raft_io_uv_rpc__send has returned.
 */
static MunitResult test_send_oom_async(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_REQUEST_VOTE);

    __send(f, message);

    test_heap_fault_enable(&f->heap);

    n_handles = test_uv_run(&f->loop, 3);
    munit_assert_int(n_handles, ==, 1);

    return MUNIT_OK;
}

static MunitTest send_tests[] = {
    {"/first", test_send_first, setup, tear_down, 0, NULL},
    {"/second", test_send_second, setup, tear_down, 0, NULL},
    {"/vote-result", test_send_vote_result, setup, tear_down, 0, NULL},
    {"/append-entries", test_send_append_entries, setup, tear_down, 0, NULL},
    {"/heartbeat", test_send_heartbeat, setup, tear_down, 0, NULL},
    {"/append-result", test_send_append_result, setup, tear_down, 0, NULL},
    {"/connect-error", test_send_connect_error, setup, tear_down, 0, NULL},
    {"/bad-address", test_send_bad_address, setup, tear_down, 0, NULL},
    {"/bad-message", test_send_bad_message, setup, tear_down, 0, NULL},
    {"/reconnect", test_send_reconnect, setup, tear_down, 0, NULL},
    {"/oom", test_send_oom, setup, tear_down, 0, send_oom_params},
    {"/oom-async", test_send_oom_async, setup, tear_down, 0,
     send_oom_async_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv_rpc recv callback
 */

/**
 * The handshake fails because of an unexpected protocon version.
 */
static MunitResult test_recv_bad_proto(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    uint64_t buf[3];
    void *cursor = buf;
    int n_handles;

    (void)params;

    test_tcp_connect(&f->tcp, 9000);

    raft__put64(&cursor, 666); /* Protocol */
    raft__put64(&cursor, 1);   /* Server id */
    raft__put64(&cursor, 2);   /* Address length */

    test_tcp_send(&f->tcp, buf, sizeof buf);

    n_handles = test_uv_run(&f->loop, 2);
    munit_assert_int(n_handles, ==, 1);

    munit_assert_false(f->recv_cb.invoked);

    return MUNIT_OK;
}

/**
 * A message can't have zero length.
 */
static MunitResult test_recv_bad_size(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    uint64_t buf[2];
    void *cursor = buf;
    int n_handles;

    (void)params;

    __conn(f);

    raft__put64(&cursor, RAFT_IO_REQUEST_VOTE); /* Message type */
    raft__put64(&cursor, 0);                    /* Message size */

    test_tcp_send(&f->tcp, buf, sizeof buf);

    n_handles = test_uv_run(&f->loop, 2);
    munit_assert_int(n_handles, ==, 1);

    munit_assert_false(f->recv_cb.invoked);

    return MUNIT_OK;
}

/**
 * A message with a bad type causes the connection to be aborted.
 */
static MunitResult test_recv_bad_type(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    uint64_t buf[3];
    void *cursor = buf;
    int n_handles;

    (void)params;

    __conn(f);

    raft__put64(&cursor, 666); /* Message type */
    raft__put64(&cursor, 1);   /* Message size */

    test_tcp_send(&f->tcp, buf, sizeof buf);

    n_handles = test_uv_run(&f->loop, 2);
    munit_assert_int(n_handles, ==, 1);

    munit_assert_false(f->recv_cb.invoked);

    return MUNIT_OK;
}

/**
 * Receive the very first message over the connection.
 */
static MunitResult test_recv_first(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_REQUEST_VOTE);

    message.request_vote.term = 3;
    message.request_vote.candidate_id = 2;
    message.request_vote.last_log_index = 123;
    message.request_vote.last_log_term = 2;

    __conn(f);
    __recv(f, message, socket);

    n_handles = test_uv_run(&f->loop, 2);
    munit_assert_int(n_handles, ==, 1);

    munit_assert_true(f->recv_cb.invoked);
    munit_assert_ptr_not_null(f->recv_cb.message);

    munit_assert_int(f->recv_cb.message->type, ==, RAFT_IO_REQUEST_VOTE);
    munit_assert_int(f->recv_cb.message->request_vote.term, ==, 3);
    munit_assert_int(f->recv_cb.message->request_vote.candidate_id, ==, 2);
    munit_assert_int(f->recv_cb.message->request_vote.last_log_index, ==, 123);
    munit_assert_int(f->recv_cb.message->request_vote.last_log_term, ==, 2);

    return MUNIT_OK;
}

/**
 * Receive the a first message then another one.
 */
static MunitResult test_recv_second(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_REQUEST_VOTE);

    __conn(f);
    __recv(f, message, socket);
    __recv(f, message, socket);

    n_handles = test_uv_run(&f->loop, 2);
    munit_assert_int(n_handles, ==, 1);

    return MUNIT_OK;
}

/**
 * Receive an RequestVote result message.
 */
static MunitResult test_recv_vote_result(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_REQUEST_VOTE_RESULT);

    message.request_vote_result.term = 3;
    message.request_vote_result.vote_granted = true;

    __conn(f);
    __recv(f, message, socket);

    n_handles = test_uv_run(&f->loop, 2);
    munit_assert_int(n_handles, ==, 1);

    munit_assert_int(f->recv_cb.message->type, ==, RAFT_IO_REQUEST_VOTE_RESULT);

    munit_assert_int(f->recv_cb.message->request_vote_result.term, ==, 3);
    munit_assert_true(f->recv_cb.message->request_vote_result.vote_granted);

    return MUNIT_OK;
}

/**
 * Receive an AppendEntries message with two entries.
 */
static MunitResult test_recv_append_entries(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    struct raft_entry entries[2];
    int n_handles;

    (void)params;

    entries[0].type = RAFT_LOG_COMMAND;
    entries[0].buf.base = raft_malloc(16);
    entries[0].buf.len = 16;
    strcpy(entries[0].buf.base, "hello");

    entries[1].type = RAFT_LOG_COMMAND;
    entries[1].buf.base = raft_malloc(8);
    entries[1].buf.len = 8;
    strcpy(entries[1].buf.base, "world");

    __message(f, message, RAFT_IO_APPEND_ENTRIES);

    message.append_entries.entries = entries;
    message.append_entries.n_entries = 2;

    __conn(f);
    __recv(f, message, socket);

    n_handles = test_uv_run(&f->loop, 2);
    munit_assert_int(n_handles, ==, 1);

    munit_assert_true(f->recv_cb.invoked);
    munit_assert_ptr_not_null(f->recv_cb.message);

    munit_assert_int(f->recv_cb.message->type, ==, RAFT_IO_APPEND_ENTRIES);
    munit_assert_int(f->recv_cb.message->append_entries.n_entries, ==, 2);

    munit_assert_string_equal(
        f->recv_cb.message->append_entries.entries[0].buf.base, "hello");
    munit_assert_string_equal(
        f->recv_cb.message->append_entries.entries[1].buf.base, "world");

    return MUNIT_OK;
}

/**
 * Receive an AppendEntries message with no entries (i.e. an heartbeat).
 */
static MunitResult test_recv_heartbeat(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_APPEND_ENTRIES);

    message.append_entries.entries = NULL;
    message.append_entries.n_entries = 0;

    __conn(f);
    __recv(f, message, socket);

    n_handles = test_uv_run(&f->loop, 2);
    munit_assert_int(n_handles, ==, 1);

    return MUNIT_OK;
}

/**
 * Receive an AppendEntries result message.
 */
static MunitResult test_recv_append_result(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_APPEND_ENTRIES_RESULT);

    message.append_entries_result.term = 3;
    message.append_entries_result.success = true;
    message.append_entries_result.last_log_index = 123;

    __conn(f);
    __recv(f, message, socket);

    n_handles = test_uv_run(&f->loop, 2);
    munit_assert_int(n_handles, ==, 1);

    munit_assert_int(f->recv_cb.message->type, ==,
                     RAFT_IO_APPEND_ENTRIES_RESULT);

    munit_assert_int(f->recv_cb.message->append_entries_result.term, ==, 3);
    munit_assert_true(f->recv_cb.message->append_entries_result.success);
    munit_assert_int(f->recv_cb.message->append_entries_result.last_log_index,
                     ==, 123);

    return MUNIT_OK;
}

static char *recv_oom_heap_fault_delay[] = {"2", "3", "4", "5", "6", "7", NULL};
static char *recv_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum recv_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, recv_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, recv_oom_heap_fault_repeat},
    {NULL, NULL},
};

/**
 * Out of memory conditions.
 */
static MunitResult test_recv_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int n_handles;

    (void)params;

    __message(f, message, RAFT_IO_REQUEST_VOTE);

    __conn(f);

    test_heap_fault_enable(&f->heap);

    __recv(f, message, socket);

    n_handles = test_uv_run(&f->loop, 1);
    munit_assert_int(n_handles, ==, 1);

    munit_assert_false(f->recv_cb.invoked);

    return MUNIT_OK;
}

static MunitTest recv_tests[] = {
    {"/bad-proto", test_recv_bad_proto, setup, tear_down, 0, NULL},
    {"/bad-size", test_recv_bad_size, setup, tear_down, 0, NULL},
    {"/bad-type", test_recv_bad_type, setup, tear_down, 0, NULL},
    {"/first", test_recv_first, setup, tear_down, 0, NULL},
    {"/second", test_recv_second, setup, tear_down, 0, NULL},
    {"/vote-result", test_recv_vote_result, setup, tear_down, 0, NULL},
    {"/append-entries", test_recv_append_entries, setup, tear_down, 0, NULL},
    {"/heartbeat", test_recv_heartbeat, setup, tear_down, 0, NULL},
    {"/append-result", test_recv_append_result, setup, tear_down, 0, NULL},
    {"/oom", test_recv_oom, setup, tear_down, 0, recv_oom_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_io_uv_rpc_suites[] = {
    {"/send", send_tests, NULL, 1, 0},
    {"/recv", recv_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
