#include "../../src/binary.h"
#include "../../src/io_uv_encoding.h"
#include "../../src/io_uv_rpc.h"

#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/munit.h"
#include "../lib/tcp.h"
#include "../lib/uv.h"

/**
 * Helpers
 */

#define __FIXTURE                          \
    struct raft_heap heap;                 \
    struct test_tcp tcp;                   \
    struct raft_logger logger;             \
    struct uv_loop_s loop;                 \
    struct raft_io_uv_transport transport; \
    struct raft__io_uv_rpc rpc;

#define __SETUP(RECV_CB)                                                     \
    int rv;                                                                  \
    (void)user_data;                                                         \
    test_heap_setup(params, &f->heap);                                       \
    test_tcp_setup(params, &f->tcp);                                         \
    test_logger_setup(params, &f->logger, 1);                                \
    test_uv_setup(params, &f->loop);                                         \
    raft_io_uv_tcp_init(&f->transport, &f->logger, &f->loop);                \
    rv = f->transport.init(&f->transport, 1, "127.0.0.1:9000");              \
    munit_assert_int(rv, ==, 0);                                             \
    rv = raft__io_uv_rpc_init(&f->rpc, &f->logger, &f->loop, &f->transport); \
    munit_assert_int(rv, ==, 0);                                             \
    rv = raft__io_uv_rpc_start(&f->rpc, RECV_CB);                            \
    munit_assert_int(rv, ==, 0);

#define __FIXTURE_TEAR_DOWN                  \
    raft__io_uv_rpc_close(&f->rpc, NULL);    \
    uv_run(&f->loop, UV_RUN_NOWAIT);         \
    uv_run(&f->loop, UV_RUN_NOWAIT);         \
    uv_run(&f->loop, UV_RUN_NOWAIT);         \
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
 * raft__io_uv_rpc_send
 */

struct send_fixture
{
    __FIXTURE;
    struct raft_io_send req;
    struct raft_message message;
    int invoked;
    int status;
};

static void *send_setup(const MunitParameter params[], void *user_data)
{
    struct send_fixture *f = munit_malloc(sizeof *f);
    __SETUP(NULL);
    f->message.type = RAFT_IO_REQUEST_VOTE;
    f->message.server_id = 1;
    f->message.server_address = f->tcp.server.address;
    f->req.data = f;
    f->invoked = 0;
    f->status = -1;
    return f;
}

static void send_tear_down(void *data)
{
    struct send_fixture *f = data;
    __FIXTURE_TEAR_DOWN;
}

static void __send_cb(struct raft_io_send *req, int status)
{
    struct send_fixture *f = req->data;
    f->invoked++;
    f->status = status;
}

#define __send_set_message_type(F, TYPE) F->message.type = TYPE;

#define __send_trigger(F, RV)                                                \
    {                                                                        \
        int rv;                                                              \
        rv = raft__io_uv_rpc_send(&F->rpc, &F->req, &F->message, __send_cb); \
        munit_assert_int(rv, ==, RV);                                        \
    }
#define __send_wait_cb(F, STATUS)            \
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
        F->invoked = 0;                      \
    }

#define __test_send(NAME, FUNC, PARAMS) \
    __test(NAME, send_##FUNC, send_setup, send_tear_down, PARAMS)

/* The first time a request is sent to a server a connection attempt is
 * triggered. If the connection succeeds the request gets written out. */
static MunitResult test_send_success_first(const MunitParameter params[],
                                           void *data)
{
    struct send_fixture *f = data;

    (void)params;

    __send_trigger(f, 0);
    return 0;
    __send_wait_cb(f, 0);

    return MUNIT_OK;
}

/* The second time a request is sent it re-uses the connection that was already
 * established */
static MunitResult test_send_success_second(const MunitParameter params[],
                                            void *data)
{
    struct send_fixture *f = data;

    (void)params;

    __send_trigger(f, 0);
    __send_wait_cb(f, 0);

    __send_trigger(f, 0);
    __send_wait_cb(f, 0);

    return MUNIT_OK;
}

/* Send a request vote result message. */
static MunitResult test_send_success_vote_result(const MunitParameter params[],
                                                 void *data)
{
    struct send_fixture *f = data;

    (void)params;

    __send_set_message_type(f, RAFT_IO_REQUEST_VOTE_RESULT);
    __send_trigger(f, 0);
    __send_wait_cb(f, 0);

    return MUNIT_OK;
}

/* Send an append entries message. */
static MunitResult test_send_success_append_entries(
    const MunitParameter params[],
    void *data)
{
    struct send_fixture *f = data;
    struct raft_entry entries[2];

    (void)params;

    entries[0].buf.base = raft_malloc(16);
    entries[0].buf.len = 16;

    entries[1].buf.base = raft_malloc(8);
    entries[1].buf.len = 8;

    __send_set_message_type(f, RAFT_IO_APPEND_ENTRIES);

    f->message.append_entries.entries = entries;
    f->message.append_entries.n_entries = 2;

    __send_trigger(f, 0);
    __send_wait_cb(f, 0);

    raft_free(entries[0].buf.base);
    raft_free(entries[1].buf.base);

    return MUNIT_OK;
}

/* Send an append entries message with zero entries (i.e. a heartbeat). */
static MunitResult test_send_success_heartbeat(const MunitParameter params[],
                                               void *data)
{
    struct send_fixture *f = data;

    (void)params;

    __send_set_message_type(f, RAFT_IO_APPEND_ENTRIES);

    f->message.append_entries.entries = NULL;
    f->message.append_entries.n_entries = 0;

    __send_trigger(f, 0);
    __send_wait_cb(f, 0);

    return MUNIT_OK;
}

/* Send an append entries result message. */
static MunitResult test_send_success_append_entries_result(
    const MunitParameter params[],
    void *data)
{
    struct send_fixture *f = data;

    (void)params;

    __send_set_message_type(f, RAFT_IO_APPEND_ENTRIES_RESULT);
    __send_trigger(f, 0);
    __send_wait_cb(f, 0);

    return MUNIT_OK;
}

#define __test_send_success(NAME, FUNC, PARAMS) \
    __test_send(NAME, success_##FUNC, PARAMS)

static MunitTest send_success_tests[] = {
    __test_send_success("first", first, NULL),
    __test_send_success("second", second, NULL),
    __test_send_success("vote-result", vote_result, NULL),
    __test_send_success("append-entries", append_entries, NULL),
    __test_send_success("heartbeat", heartbeat, NULL),
    __test_send_success("append-entries-result", append_entries_result, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/* A connection attempt fails asynchronously after the connect function
 * returns. */
static MunitResult test_send_error_connect(const MunitParameter params[],
                                           void *data)
{
    struct send_fixture *f = data;

    (void)params;

    f->message.server_address = "127.0.0.1:123456";
    f->rpc.connect_retry_delay = 1;

    __send_trigger(f, 0);

    /* We keep retrying indefinitely */
    test_uv_run(&f->loop, 2);

    munit_assert_int(f->invoked, ==, 0);

    return MUNIT_OK;
}

/* The message has an invalid IPv4 address. */
static MunitResult test_send_error_bad_address(const MunitParameter params[],
                                               void *data)
{
    struct send_fixture *f = data;

    (void)params;

    f->message.server_address = "1";

    __send_trigger(f, 0);

    /* The only active handle is the timer one, to retry the connection. */
    test_uv_run(&f->loop, 1);

    /* The message hasn't been sent */
    munit_assert_int(f->invoked, ==, 0);

    return MUNIT_OK;
}

/* The message has an invalid type. */
static MunitResult test_send_error_bad_message(const MunitParameter params[],
                                               void *data)
{
    struct send_fixture *f = data;

    (void)params;

    __send_set_message_type(f, 666);

    __send_trigger(f, RAFT_ERR_IO_MALFORMED);

    return MUNIT_OK;
}

/* After the connection is established the peer dies and then comes back a
 * little bit later. */
static MunitResult test_send_error_reconnect(const MunitParameter params[],
                                             void *data)
{
    struct send_fixture *f = data;
    int socket;

    (void)params;

    __send_trigger(f, 0);
    __send_wait_cb(f, 0);

    socket = test_tcp_accept(&f->tcp);
    close(socket);

    __send_trigger(f, 0);
    __send_wait_cb(f, RAFT_ERR_IO);

    __send_trigger(f, 0);
    __send_wait_cb(f, 0);

    return MUNIT_OK;
}

/* Out of memory conditions. */
static MunitResult test_send_error_oom(const MunitParameter params[],
                                       void *data)
{
    struct send_fixture *f = data;

    (void)params;

    test_heap_fault_enable(&f->heap);

    __send_trigger(f, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

static char *send_error_oom_heap_fault_delay[] = {"0", "1", "2", "3",
                                                  "4", "5", NULL};
static char *send_error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum send_error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, send_error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, send_error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory happening after @raft__io_uv_rpc_send has returned. */
static MunitResult test_send_error_oom_async(const MunitParameter params[],
                                             void *data)
{
    struct send_fixture *f = data;

    (void)params;

    f->rpc.connect_retry_delay = 1;

    __send_trigger(f, 0);

    test_heap_fault_enable(&f->heap);

    __send_wait_cb(f, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

static char *send_error_oom_async_heap_fault_delay[] = {"0", "1", NULL};
static char *send_error_oom_async_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum send_error_oom_async_params[] = {
    {TEST_HEAP_FAULT_DELAY, send_error_oom_async_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, send_error_oom_async_heap_fault_repeat},
    {NULL, NULL},
};

#define __test_send_error(NAME, FUNC, PARAMS) \
    __test_send(NAME, error_##FUNC, PARAMS)

static MunitTest send_error_tests[] = {
    __test_send_error("connect", connect, NULL),
    __test_send_error("bad-address", bad_address, NULL),
    __test_send_error("bad-message", bad_message, NULL),
    __test_send_error("reconnect", reconnect, NULL),
    __test_send_error("oom", oom, send_error_oom_params),
    __test_send_error("oom-async", oom_async, send_error_oom_async_params),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

MunitSuite send_suites[] = {
    {"/success", send_success_tests, NULL, 1, 0},
    {"/error", send_error_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};

/**
 * raft__io_uv_rpc recv callback
 */

struct recv_fixture
{
    __FIXTURE;
    struct
    {
        char handshake[sizeof(uint64_t) * 3 /* Preamble */ + 16 /* Address */];
        struct raft_message message;
    } peer;
    int invoked;
    struct raft_message *message;
};

static void __recv_cb(struct raft__io_uv_rpc *rpc, struct raft_message *message)
{
    struct recv_fixture *f = rpc->data;
    f->invoked++;
    f->message = message;
}

static void *recv_setup(const MunitParameter params[], void *user_data)
{
    struct recv_fixture *f = munit_malloc(sizeof *f);
    __SETUP(__recv_cb);
    f->rpc.data = f;
    f->peer.message.type = RAFT_IO_REQUEST_VOTE;
    f->peer.message.server_id = 1;
    f->peer.message.server_address = f->tcp.server.address;
    f->invoked = 0;
    f->message = NULL;
    return f;
}

static void recv_tear_down(void *data)
{
    struct recv_fixture *f = data;
    __FIXTURE_TEAR_DOWN;
}

#define __recv_peer_connect(F) test_tcp_connect(&F->tcp, 9000);
#define __recv_peer_handshake(F)                                             \
    {                                                                        \
        void *cursor;                                                        \
        cursor = F->peer.handshake;                                          \
        raft__put64(&cursor, 1);  /* Protocol */                             \
        raft__put64(&cursor, 2);  /* Server ID */                            \
        raft__put64(&cursor, 16); /* Address size */                         \
        strcpy(cursor, "127.0.0.1:66");                                      \
        test_tcp_send(&F->tcp, F->peer.handshake, sizeof F->peer.handshake); \
    }
#define __recv_set_message_type(F, TYPE) F->peer.message.type = TYPE;
#define __recv_peer_send(F)                                                \
    {                                                                      \
        uv_buf_t *bufs;                                                    \
        unsigned n_bufs;                                                   \
        unsigned i;                                                        \
        int rv;                                                            \
                                                                           \
        rv = raft_io_uv_encode__message(&F->peer.message, &bufs, &n_bufs); \
        munit_assert_int(rv, ==, 0);                                       \
                                                                           \
        for (i = 0; i < n_bufs; i++) {                                     \
            test_tcp_send(&F->tcp, bufs[i].base, bufs[i].len);             \
            raft_free(bufs[i].base);                                       \
        }                                                                  \
                                                                           \
        raft_free(bufs);                                                   \
    }

#define __test_recv(NAME, FUNC, PARAMS) \
    __test(NAME, recv_##FUNC, recv_setup, recv_tear_down, PARAMS)

/* The handshake fails because of an unexpected protocon version. */
static MunitResult test_recv_error_bad_protocol(const MunitParameter params[],
                                                void *data)
{
    struct recv_fixture *f = data;
    void *cursor = f->peer.handshake;

    (void)params;

    __recv_peer_connect(f);

    raft__put64(&cursor, 666); /* Protocol */
    raft__put64(&cursor, 1);   /* Server id */
    raft__put64(&cursor, 2);   /* Address length */

    test_tcp_send(&f->tcp, f->peer.handshake, sizeof f->peer.handshake);

    test_uv_run(&f->loop, 2);

    munit_assert_int(f->invoked, ==, 0);

    return MUNIT_OK;
}

/* A message can't have zero length. */
static MunitResult test_recv_error_bad_size(const MunitParameter params[],
                                            void *data)
{
    struct recv_fixture *f = data;
    uint64_t buf[2];
    void *cursor = buf;

    (void)params;

    __recv_peer_connect(f);
    __recv_peer_handshake(f);

    raft__put64(&cursor, RAFT_IO_REQUEST_VOTE); /* Message type */
    raft__put64(&cursor, 0);                    /* Message size */

    test_tcp_send(&f->tcp, buf, sizeof buf);

    test_uv_run(&f->loop, 2);

    munit_assert_int(f->invoked, ==, 0);

    return MUNIT_OK;
}

/* A message with a bad type causes the connection to be aborted. */
static MunitResult test_recv_error_bad_type(const MunitParameter params[],
                                            void *data)
{
    struct recv_fixture *f = data;
    uint64_t buf[3];
    void *cursor = buf;

    (void)params;

    __recv_peer_connect(f);
    __recv_peer_handshake(f);

    raft__put64(&cursor, 666); /* Message type */
    raft__put64(&cursor, 1);   /* Message size */

    test_tcp_send(&f->tcp, buf, sizeof buf);

    test_uv_run(&f->loop, 2);

    munit_assert_int(f->invoked, ==, 0);

    return MUNIT_OK;
}

/* Out of memory conditions. */
static MunitResult test_recv_error_oom(const MunitParameter params[],
                                       void *data)
{
    struct recv_fixture *f = data;

    (void)params;

    __recv_peer_connect(f);
    __recv_peer_handshake(f);
    __recv_peer_send(f);

    test_heap_fault_enable(&f->heap);

    test_uv_run(&f->loop, 1);

    munit_assert_false(f->invoked);

    return MUNIT_OK;
}

static char *recv_error_oom_heap_fault_delay[] = {"2", "3", "4", "5",
                                                  "6", "7", NULL};
static char *recv_error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum recv_error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, recv_error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, recv_error_oom_heap_fault_repeat},
    {NULL, NULL},
};

#define __test_recv_error(NAME, FUNC, PARAMS) \
    __test_recv(NAME, error_##FUNC, PARAMS)

static MunitTest recv_error_tests[] = {
    __test_recv_error("bad-protocol", bad_protocol, NULL),
    __test_recv_error("bad-size", bad_size, NULL),
    __test_recv_error("bad-type", bad_type, NULL),
    __test_recv_error("oom", oom, recv_error_oom_params),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/* Receive the very first message over the connection. */
static MunitResult test_recv_success_first(const MunitParameter params[],
                                           void *data)
{
    struct recv_fixture *f = data;

    (void)params;

    f->peer.message.request_vote.term = 3;
    f->peer.message.request_vote.candidate_id = 2;
    f->peer.message.request_vote.last_log_index = 123;
    f->peer.message.request_vote.last_log_term = 2;

    __recv_peer_connect(f);
    __recv_peer_handshake(f);
    __recv_peer_send(f);

    test_uv_run(&f->loop, 2);

    munit_assert_int(f->invoked, ==, 1);
    munit_assert_ptr_not_null(f->message);

    munit_assert_int(f->message->type, ==, RAFT_IO_REQUEST_VOTE);
    munit_assert_int(f->message->request_vote.term, ==, 3);
    munit_assert_int(f->message->request_vote.candidate_id, ==, 2);
    munit_assert_int(f->message->request_vote.last_log_index, ==, 123);
    munit_assert_int(f->message->request_vote.last_log_term, ==, 2);

    return MUNIT_OK;
}

/* Receive the a first message then another one. */
static MunitResult test_recv_success_second(const MunitParameter params[],
                                            void *data)
{
    struct recv_fixture *f = data;

    (void)params;

    __recv_peer_connect(f);
    __recv_peer_handshake(f);
    __recv_peer_send(f);
    __recv_peer_send(f);

    test_uv_run(&f->loop, 2);

    return MUNIT_OK;
}

/* Receive a RequestVote result message. */
static MunitResult test_recv_success_request_vote_result(
    const MunitParameter params[],
    void *data)
{
    struct recv_fixture *f = data;

    (void)params;

    f->peer.message.type = RAFT_IO_REQUEST_VOTE_RESULT;
    f->peer.message.request_vote_result.term = 3;
    f->peer.message.request_vote_result.vote_granted = true;

    __recv_peer_connect(f);
    __recv_peer_handshake(f);
    __recv_peer_send(f);

    test_uv_run(&f->loop, 2);

    munit_assert_int(f->message->type, ==, RAFT_IO_REQUEST_VOTE_RESULT);
    munit_assert_int(f->message->request_vote_result.term, ==, 3);
    munit_assert_true(f->message->request_vote_result.vote_granted);

    return MUNIT_OK;
}

/* Receive an AppendEntries message with two entries. */
static MunitResult test_recv_success_append_entries(
    const MunitParameter params[],
    void *data)
{
    struct recv_fixture *f = data;
    struct raft_entry entries[2];

    (void)params;

    entries[0].type = RAFT_LOG_COMMAND;
    entries[0].buf.base = raft_malloc(16);
    entries[0].buf.len = 16;
    strcpy(entries[0].buf.base, "hello");

    entries[1].type = RAFT_LOG_COMMAND;
    entries[1].buf.base = raft_malloc(8);
    entries[1].buf.len = 8;
    strcpy(entries[1].buf.base, "world");

    f->peer.message.type = RAFT_IO_APPEND_ENTRIES;
    f->peer.message.append_entries.entries = entries;
    f->peer.message.append_entries.n_entries = 2;

    __recv_peer_connect(f);
    __recv_peer_handshake(f);
    __recv_peer_send(f);

    test_uv_run(&f->loop, 2);

    munit_assert_int(f->invoked, ==, 1);
    munit_assert_ptr_not_null(f->message);

    munit_assert_int(f->message->type, ==, RAFT_IO_APPEND_ENTRIES);
    munit_assert_int(f->message->append_entries.n_entries, ==, 2);

    munit_assert_string_equal(f->message->append_entries.entries[0].buf.base,
                              "hello");
    munit_assert_string_equal(f->message->append_entries.entries[1].buf.base,
                              "world");

    raft_free(f->message->append_entries.entries[0].batch);
    raft_free(f->message->append_entries.entries);

    return MUNIT_OK;
}

/* Receive an AppendEntries message with no entries (i.e. an heartbeat). */
static MunitResult test_recv_success_heartbeat(const MunitParameter params[],
                                               void *data)
{
    struct recv_fixture *f = data;

    (void)params;

    f->peer.message.type = RAFT_IO_APPEND_ENTRIES;
    f->peer.message.append_entries.entries = NULL;
    f->peer.message.append_entries.n_entries = 0;

    __recv_peer_connect(f);
    __recv_peer_handshake(f);
    __recv_peer_send(f);

    test_uv_run(&f->loop, 2);

    return MUNIT_OK;
}

/**
 * Receive an AppendEntries result f->peer.message.
 */
static MunitResult test_recv_success_append_entries_result(
    const MunitParameter params[],
    void *data)
{
    struct recv_fixture *f = data;

    (void)params;

    f->peer.message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    f->peer.message.append_entries_result.term = 3;
    f->peer.message.append_entries_result.success = true;
    f->peer.message.append_entries_result.last_log_index = 123;

    __recv_peer_connect(f);
    __recv_peer_handshake(f);
    __recv_peer_send(f);

    test_uv_run(&f->loop, 2);

    munit_assert_int(f->message->type, ==, RAFT_IO_APPEND_ENTRIES_RESULT);

    munit_assert_int(f->message->append_entries_result.term, ==, 3);
    munit_assert_true(f->message->append_entries_result.success);
    munit_assert_int(f->message->append_entries_result.last_log_index, ==, 123);

    return MUNIT_OK;
}

#define __test_recv_success(NAME, FUNC, PARAMS) \
    __test_recv(NAME, success_##FUNC, PARAMS)

static MunitTest recv_success_tests[] = {
    __test_recv_success("first", first, NULL),
    __test_recv_success("second", second, NULL),
    __test_recv_success("request-vote-result", request_vote_result, NULL),
    __test_recv_success("append-entries", append_entries, NULL),
    __test_recv_success("heartbeat", heartbeat, NULL),
    __test_recv_success("append-entries-result", append_entries_result, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

MunitSuite recv_suites[] = {
    {"/error", recv_error_tests, NULL, 1, 0},
    {"/success", recv_success_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};

/**
 * Test suite
 */

MunitSuite raft_io_uv_rpc_suites[] = {
    {"/send", NULL, send_suites, 1, 0},
    {"/recv", NULL, recv_suites, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
