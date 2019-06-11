#include "../lib/uv.h"
#include "../lib/runner.h"

#include "../../src/byte.h"
#include "../../src/uv_encoding.h"

TEST_MODULE(io_uv_recv);

/**
 * Helpers.
 */

struct fixture
{
    FIXTURE_UV;
    struct
    {
        char handshake[sizeof(uint64_t) * 3 /* Preamble */ + 16 /* Address */];
        struct raft_message message;
    } peer;
    int invoked;
    struct raft_message *message;
};

static void recv_cb(struct raft_io *io, struct raft_message *message)
{
    struct fixture *f = io->data;
    f->invoked++;
    f->message = message;
}

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int rv;
    SETUP_UV;
    rv = f->io.start(&f->io, 10000, NULL, recv_cb);
    munit_assert_int(rv, ==, 0);
    f->peer.message.type = RAFT_IO_REQUEST_VOTE;
    f->peer.message.server_id = 1;
    f->peer.message.server_address = f->tcp.server.address;
    f->invoked = 0;
    f->message = NULL;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
}

#define recv__peer_connect test_tcp_connect(&f->tcp, 9000);
#define recv__peer_handshake                                                 \
    {                                                                        \
        void *cursor2;                                                       \
        cursor2 = f->peer.handshake;                                         \
        bytePut64(&cursor2, 1);  /* Protocol */                            \
        bytePut64(&cursor2, 2);  /* Server ID */                           \
        bytePut64(&cursor2, 16); /* Address size */                        \
        strcpy(cursor2, "127.0.0.1:66");                                     \
        test_tcp_send(&f->tcp, f->peer.handshake, sizeof f->peer.handshake); \
    }
#define recv__set_message_type(TYPE) f->peer.message.type = TYPE;

/* Send the first N buffers of f->peer.message. If N is 0, send the whole
 * message */
#define recv__peer_send_bufs(N)                                        \
    {                                                                  \
        uv_buf_t *bufs;                                                \
        unsigned n;                                                    \
        unsigned n_bufs;                                               \
        unsigned i;                                                    \
        int rv2;                                                       \
        rv2 = uvEncodeMessage(&f->peer.message, &bufs, &n_bufs); \
        munit_assert_int(rv2, ==, 0);                                  \
        if (N == 0) {                                                  \
            n = n_bufs;                                                \
        } else {                                                       \
            n = N;                                                     \
        }                                                              \
        for (i = 0; i < n_bufs; i++) {                                 \
            if (i < n) {                                               \
                test_tcp_send(&f->tcp, bufs[i].base, bufs[i].len);     \
            }                                                          \
            raft_free(bufs[i].base);                                   \
        }                                                              \
        raft_free(bufs);                                               \
    }
#define recv__peer_send recv__peer_send_bufs(0)

/**
 * Success scenarios.
 */

TEST_SUITE(success);

TEST_SETUP(success, setup);
TEST_TEAR_DOWN(success, tear_down);

/* Receive the very first message over the connection. */
TEST_CASE(success, first, NULL)
{
    struct fixture *f = data;

    (void)params;

    f->peer.message.request_vote.term = 3;
    f->peer.message.request_vote.candidate_id = 2;
    f->peer.message.request_vote.last_log_index = 123;
    f->peer.message.request_vote.last_log_term = 2;

    recv__peer_connect;
    recv__peer_handshake;
    recv__peer_send;

    LOOP_RUN(2);

    munit_assert_int(f->invoked, ==, 1);
    munit_assert_ptr_not_null(f->message);

    munit_assert_int(f->message->request_vote.term, ==, 3);
    munit_assert_int(f->message->request_vote.candidate_id, ==, 2);
    munit_assert_int(f->message->request_vote.last_log_index, ==, 123);
    munit_assert_int(f->message->request_vote.last_log_term, ==, 2);

    return MUNIT_OK;
}

/* Receive the a first message then another one. */
TEST_CASE(success, second, NULL)
{
    struct fixture *f = data;

    (void)params;

    recv__peer_connect;
    recv__peer_handshake;
    recv__peer_send;
    recv__peer_send;

    LOOP_RUN(2);

    return MUNIT_OK;
}

/* Receive a RequestVote result message. */
TEST_CASE(success, request_vote_result, NULL)
{
    struct fixture *f = data;

    (void)params;

    f->peer.message.type = RAFT_IO_REQUEST_VOTE_RESULT;
    f->peer.message.request_vote_result.term = 3;
    f->peer.message.request_vote_result.vote_granted = true;

    recv__peer_connect;
    recv__peer_handshake;
    recv__peer_send;

    LOOP_RUN(2);

    munit_assert_int(f->message->request_vote_result.term, ==, 3);
    munit_assert_true(f->message->request_vote_result.vote_granted);

    return MUNIT_OK;
}

/* Receive an AppendEntries message with two entries. */
TEST_CASE(success, append_entries, NULL)
{
    struct fixture *f = data;
    struct raft_entry entries[2];

    (void)params;

    entries[0].type = RAFT_COMMAND;
    entries[0].buf.base = raft_malloc(16);
    entries[0].buf.len = 16;
    strcpy(entries[0].buf.base, "hello");

    entries[1].type = RAFT_COMMAND;
    entries[1].buf.base = raft_malloc(8);
    entries[1].buf.len = 8;
    strcpy(entries[1].buf.base, "world");

    f->peer.message.type = RAFT_IO_APPEND_ENTRIES;
    f->peer.message.append_entries.entries = entries;
    f->peer.message.append_entries.n_entries = 2;

    recv__peer_connect;
    recv__peer_handshake;
    recv__peer_send;

    LOOP_RUN(2);

    munit_assert_int(f->invoked, ==, 1);
    munit_assert_ptr_not_null(f->message);

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
TEST_CASE(success, heartbeat, NULL)
{
    struct fixture *f = data;

    (void)params;

    f->peer.message.type = RAFT_IO_APPEND_ENTRIES;
    f->peer.message.append_entries.entries = NULL;
    f->peer.message.append_entries.n_entries = 0;

    recv__peer_connect;
    recv__peer_handshake;
    recv__peer_send;

    LOOP_RUN(2);

    return MUNIT_OK;
}

/* Receive an AppendEntries result f->peer.message. */
TEST_CASE(success, append_entries_result, NULL)
{
    struct fixture *f = data;

    (void)params;

    f->peer.message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    f->peer.message.append_entries_result.term = 3;
    f->peer.message.append_entries_result.rejected = 0;
    f->peer.message.append_entries_result.last_log_index = 123;

    recv__peer_connect;
    recv__peer_handshake;
    recv__peer_send;

    LOOP_RUN(2);

    munit_assert_int(f->message->append_entries_result.term, ==, 3);
    munit_assert_int(f->message->append_entries_result.rejected, ==, 0);
    munit_assert_int(f->message->append_entries_result.last_log_index, ==, 123);

    return MUNIT_OK;
}

/* Receive an InstallSnapshot message. */
TEST_CASE(success, install_snapshot, NULL)
{
    struct fixture *f = data;
    struct raft_install_snapshot *p = &f->peer.message.install_snapshot;
    int rv;

    (void)params;

    f->peer.message.type = RAFT_IO_INSTALL_SNAPSHOT;
    raft_configuration_init(&p->conf);
    rv = raft_configuration_add(&p->conf, 1, "1", true);
    munit_assert_int(rv, ==, 0);
    p->data.len = 8;
    p->data.base = raft_malloc(p->data.len);
    *(uint64_t *)p->data.base = byteFlip64(666);

    recv__peer_connect;
    recv__peer_handshake;
    recv__peer_send;
    raft_configuration_close(&p->conf);

    LOOP_RUN(2);

    munit_assert_int(f->invoked, ==, 1);
    munit_assert_ptr_not_null(f->message);

    munit_assert_int(f->message->install_snapshot.conf.n, ==, 1);
    munit_assert_int(f->message->install_snapshot.conf.servers[0].id, ==, 1);
    munit_assert_string_equal(
        f->message->install_snapshot.conf.servers[0].address, "1");

    raft_configuration_close(&f->message->install_snapshot.conf);

    munit_assert_int(
        byteFlip64(*(uint64_t *)f->message->install_snapshot.data.base), ==,
        666);
    raft_free(f->message->install_snapshot.data.base);

    return MUNIT_OK;
}

/**
 * Failure scenarios.
 */

TEST_SUITE(error);

TEST_SETUP(error, setup);
TEST_TEAR_DOWN(error, tear_down);

/* The handshake fails because of an unexpected protocon version. */
TEST_CASE(error, bad_protocol, NULL)
{
    struct fixture *f = data;
    void *cursor = f->peer.handshake;

    (void)params;

    recv__peer_connect;

    bytePut64(&cursor, 666); /* Protocol */
    bytePut64(&cursor, 1);   /* Server id */
    bytePut64(&cursor, 2);   /* Address length */

    test_tcp_send(&f->tcp, f->peer.handshake, sizeof f->peer.handshake);

    LOOP_RUN(2);

    munit_assert_int(f->invoked, ==, 0);

    return MUNIT_OK;
}

/* A message can't have zero length. */
TEST_CASE(error, bad_size, NULL)
{
    struct fixture *f = data;
    uint64_t buf[2];
    void *cursor = buf;

    (void)params;

    recv__peer_connect;
    recv__peer_handshake;

    bytePut64(&cursor, RAFT_IO_REQUEST_VOTE); /* Message type */
    bytePut64(&cursor, 0);                    /* Message size */

    test_tcp_send(&f->tcp, buf, sizeof buf);

    LOOP_RUN(2);

    munit_assert_int(f->invoked, ==, 0);

    return MUNIT_OK;
}

/* A message with a bad type causes the connection to be aborted. */
TEST_CASE(error, bad_type, NULL)
{
    struct fixture *f = data;
    uint64_t buf[3];
    void *cursor = buf;

    (void)params;

    recv__peer_connect;
    recv__peer_handshake;

    bytePut64(&cursor, 666); /* Message type */
    bytePut64(&cursor, 1);   /* Message size */

    test_tcp_send(&f->tcp, buf, sizeof buf);

    LOOP_RUN(2);

    munit_assert_int(f->invoked, ==, 0);

    return MUNIT_OK;
}

static char *error_oom_heap_fault_delay[] = {"3", "4", "5", "6", NULL};
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

    recv__peer_connect;
    recv__peer_handshake;
    recv__peer_send;

    test_heap_fault_enable(&f->heap);

    uv_run(&f->loop, UV_RUN_NOWAIT);
    uv_run(&f->loop, UV_RUN_NOWAIT);
    uv_run(&f->loop, UV_RUN_NOWAIT);

    munit_assert_false(f->invoked);

    return MUNIT_OK;
}

/**
 * Close backend scenarios.
 */

TEST_SUITE(close);

TEST_SETUP(close, setup);
TEST_TEAR_DOWN(close, tear_down);

/* The backend is closed just before accepting a new connection. */
TEST_CASE(close, accept, NULL)
{
    struct fixture *f = data;

    (void)params;

    recv__peer_connect;
    recv__peer_handshake;

    LOOP_RUN(1);

    UV_CLOSE;

    return MUNIT_OK;
}

/* The backend is closed after receiving the header of an AppendEntries
 * message. */
TEST_CASE(close, append_entries, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;

    (void)params;

    entry.type = RAFT_COMMAND;
    entry.buf.base = raft_malloc(16);
    entry.buf.len = 16;

    f->peer.message.type = RAFT_IO_APPEND_ENTRIES;
    f->peer.message.append_entries.entries = &entry;
    f->peer.message.append_entries.n_entries = 1;

    recv__peer_connect;
    recv__peer_handshake;
    recv__peer_send_bufs(1); /* Send only the message header */

    LOOP_RUN(2);

    munit_assert_int(f->invoked, ==, 0);
    munit_assert_ptr_null(f->message);

    UV_CLOSE;

    return MUNIT_OK;
}
