/**
 * Raft servers configuration test helpers.
 */

#ifndef TEST_CONFIGURATION_H
#define TEST_CONFIGURATION_H

#include "../../src/configuration.h"

#define FIXTURE_CONFIGURATION struct raft_configuration configuration;
#define SETUP_CONFIGURATION raft_configuration_init(&f->configuration)
#define TEAR_DOWN_CONFIGURATION raft_configuration_close(&f->configuration)

/**
 * Add a server to the fixture's configuration and check that no error occurs.
 */
#define CONFIGURATION__ADD(ID, ADDRESS, VOTING)                              \
    {                                                                        \
        int rv;                                                              \
                                                                             \
        rv = raft_configuration_add(&f->configuration, ID, ADDRESS, VOTING); \
        munit_assert_int(rv, ==, 0);                                         \
    }

/**
 * Remove a server from the fixture's configuration and check that no error
 * occurs.
 */
#define CONFIGURATION__REMOVE(ID)                              \
    {                                                          \
        int rv;                                                \
                                                               \
        rv = configuration__remove(&f->configuration, ID); \
        munit_assert_int(rv, ==, 0);                           \
    }

#define CONFIGURATION__INDEX_OF(ID) configuration__index_of(&f->configuration, ID)

/**
 * Assert that the fixture's configuration has n servers.
 */
#define CONFIGURATION__ASSERT_N(N)                               \
    {                                                            \
        munit_assert_int(f->configuration.n, ==, N);             \
                                                                 \
        if (N == 0) {                                            \
            munit_assert_ptr_null(f->configuration.servers);     \
        } else {                                                 \
            munit_assert_ptr_not_null(f->configuration.servers); \
        }                                                        \
    }

/**
 * Assert that the I'th server in the fixture's configuration equals the given
 * value.
 */
#define CONFIGURATION__ASSERT_SERVER(I, ID, ADDRESS, VOTING) \
    {                                                        \
        struct raft_server *server;                          \
        munit_assert_int(I, <, f->configuration.n);          \
        server = &f->configuration.servers[I];               \
        munit_assert_int(server->id, ==, ID);                \
        munit_assert_string_equal(server->address, ADDRESS); \
        munit_assert_int(server->voting, ==, VOTING);        \
    }

#endif /* TEST_CONFIGURATION_H */
