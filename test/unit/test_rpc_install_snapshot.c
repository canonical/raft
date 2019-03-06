#include <stdio.h>

#include "../lib/raft.h"
#include "../lib/runner.h"

#include "../../src/rpc_append_entries.h"
#include "../../src/rpc_install_snapshot.h"

TEST_MODULE(rpc_install_snapshot);

TEST_SUITE(success);

struct fixture
{
    RAFT_FIXTURE;
};

TEST_SETUP(success)
{
    struct fixture *f = munit_malloc(sizeof *f);
    RAFT_SETUP(f);
    return f;
}

TEST_TEAR_DOWN(success)
{
    struct fixture *f = data;
    RAFT_TEAR_DOWN(f);
    free(f);
}

static struct raft_entry *__create_entries_batch()
{
    void *batch;
    struct raft_entry *entries;

    batch = raft_malloc(8 +  /*Number of entries in the batch, little endian */
                        16 + /* Header data of the first entry */
                        8 /* Payload data of the first entry */);
    munit_assert_ptr_not_null(batch);

    entries = raft_malloc(sizeof *entries);
    entries[0].term = 1;
    entries[0].type = RAFT_LOG_COMMAND;
    entries[0].buf.base = batch + 8 + 16;
    entries[0].buf.len = 8;
    entries[0].batch = batch;

    return entries;
}

/**
 * Call raft_rpc__recv_append_entries with the given parameters and check that
 * no error occurs.
 */
#define __recv_append_entries(F, TERM, LEADER_ID, PREV_LOG_INDEX,        \
                              PREV_LOG_TERM, ENTRIES, N, COMMIT)         \
    {                                                                    \
        struct raft_append_entries args;                                 \
        char address[4];                                                 \
        int rv;                                                          \
                                                                         \
        sprintf(address, "%d", LEADER_ID);                               \
                                                                         \
        args.term = TERM;                                                \
        args.leader_id = LEADER_ID;                                      \
        args.prev_log_index = PREV_LOG_INDEX;                            \
        args.prev_log_term = PREV_LOG_TERM;                              \
        args.entries = ENTRIES;                                          \
        args.n_entries = N;                                              \
        args.leader_commit = COMMIT;                                     \
                                                                         \
        rv = raft_rpc__recv_append_entries(&F->raft, LEADER_ID, address, \
                                           &args);                       \
        munit_assert_int(rv, ==, 0);                                     \
    }

#define __recv_install_snapshot(F, TERM, LEADER_ID, LAST_INDEX, LAST_TERM) \
    {                                                                      \
        struct raft_install_snapshot args;                                 \
        struct raft_configuration *configuration = &args.conf;             \
        struct raft_buffer buf;                                            \
        struct raft_buffer *bufs;                                          \
        unsigned n_bufs;                                                   \
        struct raft_fsm fsm;                                               \
        char address[4];                                                   \
        int rv;                                                            \
                                                                           \
        sprintf(address, "%d", LEADER_ID);                                 \
                                                                           \
        args.term = TERM;                                                  \
        args.leader_id = LEADER_ID;                                        \
        args.last_index = LAST_INDEX;                                      \
        args.last_term = LAST_TERM;                                        \
        args.conf_index = 1;                                               \
                                                                           \
        raft_configuration_init(configuration);                            \
        rv = raft_configuration_add(configuration, 1, "1", true);          \
        munit_assert_int(rv, ==, 0);                                       \
        rv = raft_configuration_add(configuration, 2, "2", true);          \
        munit_assert_int(rv, ==, 0);                                       \
        rv = raft_configuration_add(configuration, 3, "3", true);          \
        munit_assert_int(rv, ==, 0);                                       \
                                                                           \
        test_fsm_setup(NULL, &fsm);                                        \
        test_fsm_encode_set_x(333, &buf);                                  \
        rv = fsm.apply(&fsm, &buf);                                        \
        munit_assert_int(0, ==, rv);                                       \
        raft_free(buf.base);                                               \
        test_fsm_encode_set_y(666, &buf);                                  \
        rv = fsm.apply(&fsm, &buf);                                        \
        munit_assert_int(0, ==, rv);                                       \
        raft_free(buf.base);                                               \
                                                                           \
        rv = fsm.snapshot(&fsm, &bufs, &n_bufs);                           \
        munit_assert_int(rv, ==, 0);                                       \
        test_fsm_tear_down(&fsm);                                          \
                                                                           \
        args.data = bufs[0];                                               \
        raft_free(bufs);                                                   \
                                                                           \
        rv = raft_rpc__recv_install_snapshot(&F->raft, LEADER_ID, address, \
                                             &args);                       \
        munit_assert_int(rv, ==, 0);                                       \
    }

/**
 * raft_rpc__recv_install_snapshot
 */

TEST_CASE(success, replace_log, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries1 = __create_entries_batch();
    struct raft_entry *entries2 = __create_entries_batch();

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 3);

    __recv_append_entries(f, 1, 2, 1, 1, entries1, 1, 2);
    raft_io_stub_flush(&f->io);

    __recv_append_entries(f, 1, 2, 2, 1, entries2, 1, 3);
    raft_io_stub_flush(&f->io);

    __recv_install_snapshot(f, 2, 3, 2, 2);
    raft_io_stub_flush(&f->io);

    return MUNIT_OK;
}
