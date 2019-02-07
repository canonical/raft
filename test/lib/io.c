#include "io.h"

void test_io_setup(const MunitParameter params[],
                   struct raft_io *io,
                   struct raft_logger *logger)
{
    int rv;

    (void)params;

    rv = raft_io_stub_init(io, logger);
    munit_assert_int(rv, ==, 0);
}

void test_io_tear_down(struct raft_io *io)
{
    raft_io_stub_close(io);
}

void test_io_bootstrap(struct raft_io *io,
                       const int n_servers,
                       const int voting_a,
                       const int voting_b)
{
    struct raft_configuration configuration;
    int i;
    int rv;

    munit_assert_int(n_servers, >=, 1);
    munit_assert_int(voting_a, >=, 1);
    munit_assert_int(voting_a, <=, voting_b);
    munit_assert_int(voting_b, >=, 1);
    munit_assert_int(voting_b, <=, n_servers);

    /* Populate the configuration. */
    raft_configuration_init(&configuration);

    for (i = 0; i < n_servers; i++) {
        unsigned id = i + 1;
        char address[4];
        bool voting = (int)id >= voting_a && (int)id <= voting_b;

        sprintf(address, "%d", id);
        rv = raft_configuration_add(&configuration, id, address, voting);
        munit_assert_int(rv, ==, 0);
    }

    /* Bootstrap the instance */
    rv = io->bootstrap(io, &configuration);
    munit_assert_int(rv, ==, 0);

    /* Cleanup */
    raft_configuration_close(&configuration);
}

void test_io_set_term_and_vote(struct raft_io *io,
                               const uint64_t term,
                               const uint64_t vote)
{
    int rv;

    rv = io->set_term(io, term);
    munit_assert_int(rv, ==, 0);

    rv = io->set_vote(io, vote);
    munit_assert_int(rv, ==, 0);
}

void test_io_append_entry(struct raft_io *io, const struct raft_entry *entry)
{
    int rv;

    rv = io->append(io, entry, 1, NULL, NULL);
    munit_assert_int(rv, ==, 0);
    raft_io_stub_flush(io);
}
