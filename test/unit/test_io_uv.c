#include "../../include/raft.h"

#include "../../src/binary.h"
#include "../../src/checksum.h"
#include "../../src/io_uv_encoding.h"

#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/munit.h"
#include "../lib/tcp.h"
#include "../lib/uv.h"

/**
 * Helpers
 */

struct fixture
{
    struct raft_heap heap;
    struct raft_logger logger;
    struct test_tcp tcp;
    struct uv_loop_s loop;
    char *dir;
    struct raft_io_uv_transport transport;
    struct raft_io io;
    struct
    {
        bool invoked;
        unsigned elapsed; /* Milliseconds since last call to __tick */
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

static void __tick_cb(void *data, const unsigned elapsed)
{
    struct fixture *f = data;

    f->tick_cb.invoked = true;
    f->tick_cb.elapsed = elapsed;
}

static void __append_cb(void *data, const int status)
{
    struct fixture *f = data;

    f->append_cb.invoked = true;
    f->append_cb.status = status;
}

static void __send_cb(void *data, const int status)
{
    struct fixture *f = data;

    f->send_cb.invoked = true;
    f->send_cb.status = status;
}

static void __recv_cb(void *data, struct raft_message *message)
{
    struct fixture *f = data;

    f->recv_cb.invoked = true;
    f->recv_cb.message = message;
}

static void __stop_cb(void *data)
{
    struct fixture *f = data;

    f->stop_cb.invoked = true;
}

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    const uint64_t id = 1;
    int rv;

    (void)user_data;

    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, id);
    test_tcp_setup(params, &f->tcp);
    test_uv_setup(params, &f->loop);

    f->dir = test_dir_setup(params);

    rv = raft_io_uv_tcp_init(&f->transport, &f->logger, &f->loop);
    munit_assert_int(rv, ==, 0);

    rv = raft_io_uv_init(&f->io, &f->logger, &f->loop, f->dir, &f->transport);
    munit_assert_int(rv, ==, 0);

    rv = f->io.start(&f->io, 1, "127.0.0.1:9000", 50, f, __tick_cb, __recv_cb);
    munit_assert_int(rv, ==, 0);

    f->tick_cb.invoked = false;
    f->tick_cb.elapsed = 0;

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

    rv = f->io.stop(&f->io, f, __stop_cb);
    munit_assert_int(rv, ==, 0);

    test_uv_stop(&f->loop);

    munit_assert_true(f->stop_cb.invoked);

    raft_io_uv_close(&f->io);
    raft_io_uv_tcp_close(&f->transport);

    test_dir_tear_down(f->dir);

    test_uv_tear_down(&f->loop);
    test_tcp_tear_down(&f->tcp);
    test_logger_tear_down(&f->logger);
    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Load the initial state from the store and check that no error occurs.
 */
#define __load(F)                                                          \
    {                                                                      \
        raft_term term;                                                    \
        unsigned voted_for;                                                \
        raft_index start_index;                                            \
        struct raft_entry *entries;                                        \
        size_t n_entries;                                                  \
        int rv;                                                            \
                                                                           \
        rv = F->io.load(&F->io, &term, &voted_for, &start_index, &entries, \
                        &n_entries);                                       \
        munit_assert_int(rv, ==, 0);                                       \
    }

/**
 * raft_io_uv_init
 */

static char *init_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *init_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum init_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, init_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, init_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
static MunitResult test_init_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_io io;
    int rv;

    (void)params;

    test_heap_fault_enable(&f->heap);

    rv = raft_io_uv_init(&io, &f->logger, &f->loop, f->dir, &f->transport);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

/* The given path is not a directory. */
static MunitResult test_init_not_a_dir(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    struct raft_io io;
    int rv;

    (void)params;

    rv = raft_io_uv_init(&io, &f->logger, &f->loop, "/dev/null", &f->transport);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* Create data directory if it does not exist */
static MunitTest init_tests[] = {
    {"/oom", test_init_oom, setup, tear_down, 0, init_oom_params},
    {"/not-a-dir", test_init_not_a_dir, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv__start
 */

/* Once the raft_io_uv instance is started, the tick function gets called
 * periodically. */
static MunitResult test_start_tick(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    /* Run the loop and check that the tick callback was called. */
    test_uv_run(&f->loop, 1);

    munit_assert_true(f->tick_cb.invoked);
    munit_assert_int(f->tick_cb.elapsed, >=, 25);

    return MUNIT_OK;
}

/* Once the raft_io_uv instance is started, the recv callback is invoked when a
 * message is received.. */
static MunitResult test_start_recv(const MunitParameter params[], void *data)
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

    raft__put64(&cursor, 1);  /* Protocol */
    raft__put64(&cursor, 2);  /* Server ID */
    raft__put64(&cursor, 16); /* Address size */
    strcpy(cursor, "127.0.0.1:66");
    test_tcp_send(&f->tcp, handshake, sizeof handshake);

    /* Encode a message that we'll send to our test raft_io instance */
    message.type = RAFT_IO_REQUEST_VOTE;
    message.server_id = 1;
    message.server_address = "127.0.0.1:9000";

    rv = raft_io_uv_encode__message(&message, &bufs, &n_bufs);
    munit_assert_int(rv, ==, 0);
    munit_assert_int(n_bufs, ==, 1);

    test_tcp_send(&f->tcp, bufs[0].base, bufs[0].len);
    raft_free(bufs[0].base);
    raft_free(bufs);

    /* Run the loop and check that the tick callback was called. */
    test_uv_run(&f->loop, 2);

    munit_assert_true(f->recv_cb.invoked);
    munit_assert_int(f->recv_cb.message->type, ==, RAFT_IO_REQUEST_VOTE);
    munit_assert_int(f->recv_cb.message->server_id, ==, 2);
    munit_assert_string_equal(f->recv_cb.message->server_address, "127.0.0.1:66");

    return MUNIT_OK;
}

static MunitTest start_tests[] = {
    {"/tick", test_start_tick, setup, tear_down, 0, NULL},
    {"/recv", test_start_recv, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv__load
 */

/**
 * Load the initial state of a pristine server.
 */
static MunitResult test_load_pristine(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    raft_term term;
    unsigned voted_for;
    raft_index start_index;
    struct raft_entry *entries;
    size_t n_entries;
    int rv;

    (void)params;

    rv = f->io.load(&f->io, &term, &voted_for, &start_index, &entries,
                    &n_entries);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(term, ==, 0);
    munit_assert_int(voted_for, ==, 0);
    munit_assert_int(start_index, ==, 1);
    munit_assert_ptr_null(entries);
    munit_assert_int(n_entries, ==, 0);

    return MUNIT_OK;
}

static MunitTest load_tests[] = {
    {"/pristine", test_load_pristine, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv__bootstrap
 */

/**
 * Bootstrap a pristine server.
 */
static MunitResult test_bootstrap_pristine(const MunitParameter params[],
                                           void *data)
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

static MunitTest bootstrap_tests[] = {
    {"/pristine", test_bootstrap_pristine, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv__set_term
 */

/**
 * Set the term on a pristine store.
 */
static MunitResult test_set_term_pristine(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    __load(f);

    rv = f->io.set_term(&f->io, 1);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

static MunitTest set_term_tests[] = {
    {"/pristine", test_set_term_pristine, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv__set_vote
 */

/**
 * Set the vote on a pristine store.
 */

static MunitResult test_set_vote_pristine(const MunitParameter params[],
                                          void *data)
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

static MunitTest set_vote_tests[] = {
    {"/pristine", test_set_vote_pristine, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv__append
 */

/**
 * Append entries on a pristine store.
 */

static MunitResult test_append_pristine(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    struct raft_entry entry;
    int rv;

    (void)params;

    __load(f);

    entry.term = 1;
    entry.type = RAFT_LOG_COMMAND;
    entry.buf.base = munit_malloc(1);
    entry.buf.len = 1;

    ((char *)entry.buf.base)[0] = 'x';

    rv = f->io.append(&f->io, &entry, 1, f, __append_cb);
    munit_assert_int(rv, ==, 0);

    test_uv_run(&f->loop, 10);

    munit_assert_true(f->append_cb.invoked);

    free(entry.buf.base);

    return MUNIT_OK;
}

static MunitTest append_tests[] = {
    {"/pristine", test_append_pristine, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv__send
 */

/**
 * Send the very first message.
 */

static MunitResult test_send_first(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_message message;
    int rv;

    (void)params;

    __load(f);

    message.type = RAFT_IO_REQUEST_VOTE;
    message.server_id = 1;
    message.server_address = f->tcp.server.address;

    rv = f->io.send(&f->io, &message, f, __send_cb);
    munit_assert_int(rv, ==, 0);

    test_uv_run(&f->loop, 3);

    munit_assert_true(f->send_cb.invoked);

    return MUNIT_OK;
}

static MunitTest send_tests[] = {
    {"/first", test_send_first, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_io_uv_suites[] = {
    {"/init", init_tests, NULL, 1, 0},
    {"/start", start_tests, NULL, 1, 0},
    {"/load", load_tests, NULL, 1, 0},
    {"/bootstrap", bootstrap_tests, NULL, 1, 0},
    {"/set-term", set_term_tests, NULL, 1, 0},
    {"/set-vote", set_vote_tests, NULL, 1, 0},
    {"/append", append_tests, NULL, 1, 0},
    {"/send", send_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
