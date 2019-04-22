#include "../../include/raft.h"
#include "../../include/raft/uv.h"

#include "../../src/byte.h"
#include "../../src/uv_encoding.h"

#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/tcp.h"
#include "../lib/loop.h"

TEST_MODULE(uv);

/**
 * Helpers
 */

struct fixture
{
    struct raft_heap heap;
    struct test_tcp tcp;
    struct uv_loop_s loop;
    char *dir;
    struct raft_uv_transport transport;
    struct raft_io io;
    struct raft_io_send req;
    struct
    {
        bool invoked;
    } tick_cb;
    struct
    {
        bool invoked;
        int status;
    } append_cb;
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

static void __tick_cb(struct raft_io *io)
{
    struct fixture *f = io->data;

    f->tick_cb.invoked = true;
}

static void __append_cb(struct raft_io_append *req, const int status)
{
    struct fixture *f = req->data;

    f->append_cb.invoked = true;
    f->append_cb.status = status;
}

static void __send_cb(struct raft_io_send *req, int status)
{
    struct fixture *f = req->data;

    f->send_cb.invoked = true;
    f->send_cb.status = status;
}

static void __recv_cb(struct raft_io *io, struct raft_message *message)
{
    struct fixture *f = io->data;

    f->recv_cb.invoked = true;
    f->recv_cb.message = message;
}

static void __stop_cb(struct raft_io *io)
{
    struct fixture *f = io->data;

    f->stop_cb.invoked = true;
}

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int rv;

    (void)user_data;

    test_heap_setup(params, &f->heap);
    test_tcp_setup(params, &f->tcp);
    SETUP_LOOP;

    f->dir = test_dir_setup(params);

    rv = raft_uv_tcp_init(&f->transport, &f->loop);
    munit_assert_int(rv, ==, 0);

    rv = raft_uv_init(&f->io, &f->loop, f->dir, &f->transport);
    munit_assert_int(rv, ==, 0);

    rv = f->io.init(&f->io, 1, "127.0.0.1:9000");
    munit_assert_int(rv, ==, 0);

    rv = f->io.start(&f->io, 50, __tick_cb, __recv_cb);
    munit_assert_int(rv, ==, 0);

    f->io.data = f;
    f->req.data = f;

    f->tick_cb.invoked = false;

    f->append_cb.invoked = false;
    f->append_cb.status = -1;

    f->send_cb.invoked = false;
    f->send_cb.status = -1;

    f->stop_cb.invoked = false;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    int rv;

    rv = f->io.close(&f->io, __stop_cb);
    munit_assert_int(rv, ==, 0);

    LOOP_STOP;

    munit_assert_true(f->stop_cb.invoked);

    raft_uv_close(&f->io);
    raft_uv_tcp_close(&f->transport);

    test_dir_tear_down(f->dir);

    TEAR_DOWN_LOOP;
    test_tcp_tear_down(&f->tcp);
    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Load the initial state from the store and check that no error occurs.
 */
#define __load(F)                                                            \
    {                                                                        \
        raft_term term;                                                      \
        unsigned voted_for;                                                  \
        struct raft_snapshot *snapshot;                                      \
        raft_index start_index;                                              \
        struct raft_entry *entries;                                          \
        size_t n_entries;                                                    \
        int rv2;                                                             \
        rv2 = F->io.load(&F->io, &term, &voted_for, &snapshot, &start_index, \
                         &entries, &n_entries);                              \
        munit_assert_int(rv2, ==, 0);                                        \
    }

/**
 * raft_uv_init
 */

TEST_SUITE(init);
TEST_SETUP(init, setup);
TEST_TEAR_DOWN(init, tear_down);

/* The given path is not a directory. */
TEST_CASE(init, not_a_dir, NULL)
{
    struct fixture *f = data;
    struct raft_io io;
    int rv;
    return MUNIT_OK;

    (void)params;

    rv = raft_uv_init(&io, &f->loop, "/dev/null", &f->transport);
    munit_assert_int(rv, ==, RAFT_IOERR);

    return MUNIT_OK;
}

/* Data directory path is too long */
TEST_CASE(init, dir_too_long, NULL)
{
    struct fixture *f = data;
    struct raft_uv_transport transport;
    struct raft_io io;
    int rv;

    (void)params;

    char dir[1024];

    memset(dir, 'a', sizeof dir - 1);
    dir[sizeof dir - 1] = 0;

    rv = raft_uv_init(&io, &f->loop, dir, &transport);
    munit_assert_int(rv, ==, RAFT_NAMETOOLONG);

    return MUNIT_OK;
}

/* Can't create data directory */
TEST_CASE(init, cant_create_dir, NULL)
{
    struct fixture *f = data;
    struct raft_uv_transport transport;
    struct raft_io io;
    int rv;

    (void)params;

    const char *dir = "/non/existing/path";

    rv = raft_uv_init(&io, &f->loop, dir, &transport);
    munit_assert_int(rv, ==, 0);

    rv = io.init(&io, 1, "1");
    munit_assert_int(rv, ==, RAFT_IOERR);

    raft_uv_close(&io);

    return MUNIT_OK;
}

/* Data directory not accessible */
TEST_CASE(init, access_error, NULL)
{
    struct fixture *f = data;
    struct raft_io io;
    struct raft_uv_transport transport;
    int rv;

    (void)params;

    const char *dir = "/root/foo";

    rv = raft_uv_init(&io, &f->loop, dir, &transport);
    munit_assert_int(rv, ==, 0);

    rv = io.init(&io, 1, "1");
    munit_assert_int(rv, ==, RAFT_IOERR);

    raft_uv_close(&io);

    return MUNIT_OK;
}

/**
 * raft_io_uv__start
 */

TEST_SUITE(start);
TEST_SETUP(start, setup);
TEST_TEAR_DOWN(start, tear_down);

/* Once the raft_io_uv instance is started, the tick function gets called
 * periodically. */
TEST_CASE(start, tick, NULL)
{
    struct fixture *f = data;

    (void)params;

    /* Run the loop and check that the tick callback was called. */
    LOOP_RUN(1);

    munit_assert_true(f->tick_cb.invoked);

    return MUNIT_OK;
}

/* Once the raft_io_uv instance is started, the recv callback is invoked when a
 * message is received.. */
TEST_CASE(start, recv, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    uint8_t handshake[sizeof(uint64_t) * 3 /* Preamble */ + 16 /* Address */];
    void *cursor = handshake;
    uv_buf_t *bufs;
    unsigned n_bufs;
    int rv;

    (void)params;

    /* Connect to our test raft_io instance and send the handshake */
    test_tcp_connect(&f->tcp, 9000);

    bytePut64(&cursor, 1);  /* Protocol */
    bytePut64(&cursor, 2);  /* Server ID */
    bytePut64(&cursor, 16); /* Address size */
    strcpy(cursor, "127.0.0.1:66");
    test_tcp_send(&f->tcp, handshake, sizeof handshake);

    /* Encode a message that we'll send to our test raft_io instance */
    message.type = RAFT_IO_REQUEST_VOTE;
    message.server_id = 1;
    message.server_address = "127.0.0.1:9000";

    rv = uvEncodeMessage(&message, &bufs, &n_bufs);
    munit_assert_int(rv, ==, 0);
    munit_assert_int(n_bufs, ==, 1);

    test_tcp_send(&f->tcp, bufs[0].base, bufs[0].len);
    raft_free(bufs[0].base);
    raft_free(bufs);

    /* Run the loop and check that the tick callback was called. */
    LOOP_RUN(2);

    munit_assert_true(f->recv_cb.invoked);
    munit_assert_int(f->recv_cb.message->type, ==, RAFT_IO_REQUEST_VOTE);
    munit_assert_int(f->recv_cb.message->server_id, ==, 2);
    munit_assert_string_equal(f->recv_cb.message->server_address,
                              "127.0.0.1:66");

    return MUNIT_OK;
}

/**
 * raft_io_uv__load
 */

TEST_SUITE(load);
TEST_SETUP(load, setup);
TEST_TEAR_DOWN(load, tear_down);

/* Load the initial state of a pristine server. */
TEST_CASE(load, pristine, NULL)
{
    struct fixture *f = data;
    raft_term term;
    unsigned voted_for;
    struct raft_snapshot *snapshot;
    raft_index start_index;
    struct raft_entry *entries;
    size_t n_entries;
    int rv;

    (void)params;

    rv = f->io.load(&f->io, &term, &voted_for, &snapshot, &start_index,
                    &entries, &n_entries);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(term, ==, 0);
    munit_assert_int(voted_for, ==, 0);
    munit_assert_ptr_null(snapshot);
    munit_assert_ptr_null(entries);
    munit_assert_int(n_entries, ==, 0);

    return MUNIT_OK;
}

/**
 * raft_io_uv__bootstrap
 */

TEST_SUITE(bootstrap);
TEST_SETUP(bootstrap, setup);
TEST_TEAR_DOWN(bootstrap, tear_down);

/* Bootstrap a pristine server. */
TEST_CASE(bootstrap, pristine, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    int rv;

    (void)params;

    __load(f);

    /* Create a configuration */
    raft_configuration_init(&configuration);

    rv = raft_configuration_add(&configuration, 1, "1", true);
    munit_assert_int(rv, ==, 0);

    rv = f->io.bootstrap(&f->io, &configuration);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/**
 * raft_io_uv__set_term
 */

TEST_SUITE(set_term);
TEST_SETUP(set_term, setup);
TEST_TEAR_DOWN(set_term, tear_down);

/* Set the term on a pristine store. */
TEST_CASE(set_term, term, NULL)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    __load(f);

    rv = f->io.set_term(&f->io, 1);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

/**
 * raft_io_uv__set_vote
 */

TEST_SUITE(set_vote);
TEST_SETUP(set_vote, setup);
TEST_TEAR_DOWN(set_vote, tear_down);

/* Set the vote on a pristine store. */
TEST_CASE(set_vote, pristine, NULL)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    __load(f);

    rv = f->io.set_term(&f->io, 1);
    munit_assert_int(rv, ==, 0);

    rv = f->io.set_vote(&f->io, 2);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

/**
 * raft_io_uv__append
 */

TEST_SUITE(append);
TEST_SETUP(append, setup);
TEST_TEAR_DOWN(append, tear_down);

/* Append entries on a pristine store. */
TEST_CASE(append, pristine, NULL)
{
    struct fixture *f = data;
    struct raft_io_append req;
    struct raft_entry entry;
    int rv;

    (void)params;

    __load(f);

    entry.term = 1;
    entry.type = RAFT_COMMAND;
    entry.buf.base = munit_malloc(8);
    entry.buf.len = 8;

    ((char *)entry.buf.base)[0] = 'x';

    req.data = f;
    rv = f->io.append(&f->io, &req, &entry, 1, __append_cb);
    munit_assert_int(rv, ==, 0);

    LOOP_RUN(10);

    munit_assert_true(f->append_cb.invoked);

    free(entry.buf.base);

    return MUNIT_OK;
}

/**
 * raft_io_uv__send
 */

TEST_SUITE(send);
TEST_SETUP(send, setup);
TEST_TEAR_DOWN(send, tear_down);

/* Send the very first message. */
TEST_CASE(send, first, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    int rv;

    (void)params;

    __load(f);

    message.type = RAFT_IO_REQUEST_VOTE;
    message.server_id = 1;
    message.server_address = f->tcp.server.address;

    rv = f->io.send(&f->io, &f->req, &message, __send_cb);
    munit_assert_int(rv, ==, 0);

    LOOP_RUN(3);

    munit_assert_true(f->send_cb.invoked);

    return MUNIT_OK;
}
