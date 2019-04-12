#include "../../include/raft.h"
#include "../../include/raft/io_stub.h"

#include "../../src/snapshot.h"

#include "../lib/fsm.h"
#include "../lib/heap.h"
#include "../lib/io.h"
#include "../lib/raft.h"
#include "../lib/runner.h"

TEST_MODULE(raft);

/**
 * Helpers
 */

struct fixture
{
    RAFT_FIXTURE;
    struct
    {
        bool invoked;
    } stop_cb;
};

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)user_data;

    RAFT_SETUP(f);

    f->stop_cb.invoked = false;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    RAFT_TEAR_DOWN(f);

    free(f);
}

/**
 * Start the fixture's instance and check that no error occurs.
 */
#define __start(F)                    \
    {                                 \
        int rv2;                      \
        rv2 = raft_start(&F->raft);   \
        munit_assert_int(rv2, ==, 0); \
    }

/**
 * Start the fixture's instance and that the given error occurs.
 */
#define __assert_start_error(F, RV)    \
    {                                  \
        int rv2;                       \
        rv2 = raft_start(&F->raft);    \
        munit_assert_int(rv2, ==, RV); \
    }

/**
 * Assert the current state of the raft instance of the given fixture.
 */
#define __assert_state(F, STATE)            \
    {                                       \
        int state = raft_state(&F->raft);   \
        munit_assert_int(state, ==, STATE); \
    }

/**
 * raft_init
 */

TEST_SUITE(init);

TEST_SETUP(init, setup);
TEST_TEAR_DOWN(init, tear_down);

TEST_GROUP(init, error);
TEST_GROUP(init, success);

static char *init_oom_heap_fault_delay[] = {"0", NULL};
static char *init_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum init_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, init_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, init_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST_CASE(init, error, oom, init_oom_params)
{
    struct fixture *f = data;
    struct raft_io io;
    struct raft raft;
    int rv;

    (void)params;

    raft_io_stub_init(&io);
    test_heap_fault_enable(&f->heap);

    rv = raft_init(&raft, &io, &f->fsm, 1, "1");
    munit_assert_int(rv, ==, RAFT_NOMEM);

    raft_io_stub_close(&io);

    return MUNIT_OK;
}

/* The raft state is properly initialized. */
TEST_CASE(init, success, state, NULL)
{
    struct fixture *f = data;
    (void)params;

    munit_assert_ptr_not_null(f->raft.io);

    munit_assert_int(f->raft.id, ==, 1);

    munit_assert_int(f->raft.state, ==, RAFT_UNAVAILABLE);

    munit_assert_int(f->raft.current_term, ==, 0);
    munit_assert_int(f->raft.voted_for, ==, 0);
    munit_assert_ptr_null(f->raft.log.entries);
    munit_assert_int(f->raft.log.offset, ==, 0);

    munit_assert_int(f->raft.commit_index, ==, 0);
    munit_assert_int(f->raft.last_applied, ==, 0);

    munit_assert_ptr_null(f->raft.configuration.servers);
    munit_assert_int(f->raft.configuration.n, ==, 0);

    munit_assert_int(f->raft.configuration_index, ==, 0);
    munit_assert_int(f->raft.configuration_uncommitted_index, ==, 0);

    munit_assert_int(f->raft.election_timeout, ==, 1000);
    munit_assert_int(f->raft.heartbeat_timeout, ==, 100);

    munit_assert_int(f->raft.election_elapsed, ==, 0);

    return MUNIT_OK;
}

/**
 * raft_start
 */

TEST_SUITE(start);

TEST_SETUP(start, setup);
TEST_TEAR_DOWN(start, tear_down);

TEST_GROUP(start, error);
TEST_GROUP(start, success);

/* An error occurs when starting the I/O backend. */
TEST_CASE(start, error, io, NULL)
{
    struct fixture *f = data;

    (void)params;

    raft_io_stub_fault(&f->io, 0, 1);

    __assert_start_error(f, RAFT_IOERR);

    return MUNIT_OK;
}

/* There's only a single voting server and that's us, but we fail to convert to
   candidate due to an OOM error. */
TEST_CASE(start, error, self_elect_candidate_oom, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_io_bootstrap(&f->io, 1, 1, 1);

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    __assert_start_error(f, RAFT_NOMEM);

    return MUNIT_OK;
}

/* There's only a single voting server and that's us, but we fail to convert to
   candidate due the disk being full. */
TEST_CASE(start, error, self_elect_candidate_io_err, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_io_bootstrap(&f->io, 1, 1, 1);

    raft_io_stub_fault(&f->io, 0, 1);
    raft_io_stub_set_time(&f->io, 100);

    __assert_start_error(f, RAFT_IOERR);

    return MUNIT_OK;
}

/* The state after a successful start of a pristine server is
 * RAFT_FOLLOWER. */
TEST_CASE(start, success, pristine, NULL)
{
    struct fixture *f = data;

    (void)params;

    __start(f);

    __assert_state(f, RAFT_FOLLOWER);

    munit_assert_int(f->raft.randomized_election_timeout, >=,
                     f->raft.election_timeout);
    munit_assert_int(f->raft.randomized_election_timeout, <,
                     2 * f->raft.election_timeout);

    return MUNIT_OK;
}

/* Start an instance that has been bootstrapped. */
TEST_CASE(start, success, bootstrapped, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_io_bootstrap(&f->io, 2, 1, 2);

    __start(f);

    __assert_state(f, RAFT_FOLLOWER);

    return MUNIT_OK;
}

/* Start an instance with a state that contains a snapshot. */
TEST_CASE(start, success, snapshot, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_set_initial_snapshot(&f->raft, 3, 8, 7, 3);

    __start(f);

    munit_assert_int(test_fsm_get_x(&f->fsm), ==, 7);
    munit_assert_int(test_fsm_get_y(&f->fsm), ==, 3);

    munit_assert_int(f->raft.log.snapshot.last_term, ==, 3);
    munit_assert_int(f->raft.log.snapshot.last_index, ==, 8);

    munit_assert_int(f->raft.configuration.n, ==, 1);
    munit_assert_int(f->raft.configuration.servers[0].id, ==, 1);
    munit_assert_string_equal(f->raft.configuration.servers[0].address, "1");

    return MUNIT_OK;
}

static char *start_oom_heap_fault_delay[] = {"0", "1,", "2", "3", NULL};
static char *start_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum start_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, start_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, start_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory condditions. */
TEST_CASE(start, error, oom, start_oom_params)
{
    struct fixture *f = data;

    (void)params;

    test_io_bootstrap(&f->io, 2, 1, 2);

    test_heap_fault_enable(&f->heap);

    __assert_start_error(f, RAFT_NOMEM);

    return MUNIT_OK;
}

/**
 * raft__recv_cb
 */

TEST_SUITE(recv_cb);

TEST_SETUP(recv_cb, setup);
TEST_TEAR_DOWN(recv_cb, tear_down);

TEST_GROUP(recv_cb, success);

/* Receive an AppendEntries message. */
TEST_CASE(recv_cb, success, append_entries, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    struct raft_append_entries *args = &message.append_entries;
    struct raft_entry *entry = raft_malloc(sizeof *entry);

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    message.type = RAFT_IO_APPEND_ENTRIES;
    message.server_id = 2;
    message.server_address = "2";

    /* Include a log entry in the message */
    entry->type = RAFT_COMMAND;
    entry->term = 1;

    test_fsm_encode_set_x(123, &entry->buf);

    args->term = 1;
    args->leader_id = 2;
    args->prev_log_index = 1;
    args->prev_log_term = 1;
    args->entries = entry;
    args->n_entries = 1;
    args->leader_commit = 2;

    raft_io_stub_deliver(&f->io, &message);

    /* Notify the raft instance about the completed write. */
    raft_io_stub_flush_all(f->raft.io);

    /* The commit index has been bumped. */
    munit_assert_int(f->raft.commit_index, ==, 2);

    return MUNIT_OK;
}

/* Receive an RequestVote message. */
TEST_CASE(recv_cb, success, request_vote, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    struct raft_request_vote *args = &message.request_vote;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    message.type = RAFT_IO_REQUEST_VOTE;
    message.server_id = 2;
    message.server_address = "2";

    args->term = 1;
    args->candidate_id = 2;
    args->last_log_index = 2;

    raft_io_stub_deliver(&f->io, &message);

    /* The voted for field has been updated. */
    munit_assert_int(f->raft.voted_for, ==, 2);

    raft_io_stub_flush_all(&f->io);

    return MUNIT_OK;
}

/* Receive a message with an unknown type. */
TEST_CASE(recv_cb, success, unknown_type, NULL)
{
    struct fixture *f = data;
    struct raft_message message;

    (void)params;

    message.type = 666;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    raft_io_stub_deliver(&f->io, &message);

    return MUNIT_OK;
}

/**
 * raft_bootstrap
 */

TEST_SUITE(bootstrap);

TEST_SETUP(bootstrap, setup);
TEST_TEAR_DOWN(bootstrap, tear_down);

TEST_GROUP(bootstrap, error);
TEST_GROUP(bootstrap, success);

/* An error occurs when bootstrapping the state on disk. */
TEST_CASE(bootstrap, error, io, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    int rv;

    (void)params;

    raft_configuration_init(&configuration);

    raft_io_stub_fault(&f->io, 0, 1);

    rv = raft_bootstrap(&f->raft, &configuration);
    munit_assert_int(rv, ==, RAFT_IOERR);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/* Starting an instance after it's bootstrapped initializes the
 * configuration. */
TEST_CASE(bootstrap, success, state, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    int rv;

    (void)params;

    raft_configuration_init(&configuration);

    rv = raft_configuration_add(&configuration, 1, "1", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_bootstrap(&f->raft, &configuration);
    munit_assert_int(rv, ==, 0);

    __start(f);

    munit_assert_int(f->raft.configuration.n, ==, 1);
    munit_assert_int(f->raft.configuration.servers[0].id, ==, 1);
    munit_assert_string_equal(f->raft.configuration.servers[0].address, "1");

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}
