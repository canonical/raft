#include "../../src/binary.h"
#include "../../src/configuration.h"

#include "../lib/heap.h"
#include "../lib/munit.h"

/**
 * Helpers
 */

struct fixture
{
    struct raft_heap heap;
    struct raft_configuration configuration;
};

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)user_data;
    (void)params;

    test_heap_setup(params, &f->heap);

    raft_configuration_init(&f->configuration);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    raft_configuration_close(&f->configuration);
    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * raft_configuration_add
 */

/* Add a server to the configuration. */
static MunitResult test_add_one(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_server *server;
    int rv;

    (void)params;

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->configuration.n, ==, 1);
    munit_assert_ptr_not_null(f->configuration.servers);

    server = &f->configuration.servers[0];

    munit_assert_int(server->id, ==, 1);
    munit_assert_string_equal(server->address, "127.0.0.1:666");
    munit_assert_true(server->voting);

    return MUNIT_OK;
}

/* Add two servers to the configuration. */
static MunitResult test_add_two(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_server *server1;
    struct raft_server *server2;
    int rv;

    (void)params;

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_configuration_add(&f->configuration, 2, "192.168.1.1:666", false);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->configuration.n, ==, 2);
    munit_assert_ptr_not_null(f->configuration.servers);

    server1 = &f->configuration.servers[0];
    server2 = &f->configuration.servers[1];

    munit_assert_int(server1->id, ==, 1);
    munit_assert_string_equal(server1->address, "127.0.0.1:666");
    munit_assert_true(server1->voting);

    munit_assert_int(server2->id, ==, 2);
    munit_assert_string_equal(server2->address, "192.168.1.1:666");
    munit_assert_false(server2->voting);

    return MUNIT_OK;
}

/* Add a server with invalid ID. */
static MunitResult test_add_invalid_id(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = raft_configuration_add(&f->configuration, 0, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, RAFT_ERR_BAD_SERVER_ID);

    munit_assert_string_equal(raft_strerror(rv), "server ID is not valid");

    return MUNIT_OK;
}

/* Add a server with no address. */
static MunitResult test_add_no_address(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = raft_configuration_add(&f->configuration, 1, NULL, true);
    munit_assert_int(rv, ==, RAFT_ERR_NO_SERVER_ADDRESS);

    munit_assert_string_equal(raft_strerror(rv), "server has no address");

    return MUNIT_OK;
}

/* Add a server with an ID which is already in use. */
static MunitResult test_add_dup_server(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_configuration_add(&f->configuration, 1, "192.168.1.1:666", false);
    munit_assert_int(rv, ==, RAFT_ERR_DUP_SERVER_ID);

    munit_assert_string_equal(raft_strerror(rv),
                              "a server with the same ID already exists");

    return MUNIT_OK;
}

/* Out of memory. */
static MunitResult test_add_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    munit_assert_string_equal(raft_strerror(rv), "out of memory");

    return MUNIT_OK;
}

static MunitTest add_tests[] = {
    {"/one", test_add_one, setup, tear_down, 0, NULL},
    {"/two", test_add_two, setup, tear_down, 0, NULL},
    {"/invalid-id", test_add_invalid_id, setup, tear_down, 0, NULL},
    {"/no-address", test_add_no_address, setup, tear_down, 0, NULL},
    {"/dup-server", test_add_dup_server, setup, tear_down, 0, NULL},
    {"/oom", test_add_oom, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_configuration__n_voting
 */

/* Return only voting nodes. */
static MunitResult test_n_voting(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    size_t n;
    int rv;

    (void)params;

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_configuration_add(&f->configuration, 2, "192.168.1.1:666", false);
    munit_assert_int(rv, ==, 0);

    n = raft_configuration__n_voting(&f->configuration);
    munit_assert_int(n, ==, 1);

    return MUNIT_OK;
}

static MunitTest n_voting_tests[] = {
    {"/", test_n_voting, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_configuration__index
 */

/* If no matching server is found, the length of the configuration is
   returned. */
static MunitResult test_index_no_match(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    size_t i;
    int rv;

    (void)params;

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, 0);

    i = raft_configuration__index(&f->configuration, 3);
    munit_assert_int(i, ==, f->configuration.n);

    return MUNIT_OK;
}

static MunitTest index_tests[] = {
    {"/no-match", test_index_no_match, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_configuration__voting_index
 */

/* The index of the matching voting server (relative to the number of voting
   servers) is returned. */
static MunitResult test_voting_index_match(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    size_t i;
    int rv;

    (void)params;

    rv = raft_configuration_add(&f->configuration, 1, "192.168.1.1:666", false);
    munit_assert_int(rv, ==, 0);

    rv = raft_configuration_add(&f->configuration, 2, "192.168.1.2:666", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_configuration_add(&f->configuration, 3, "192.168.1.3:666", true);
    munit_assert_int(rv, ==, 0);

    i = raft_configuration__voting_index(&f->configuration, 3);
    munit_assert_int(i, ==, 1);

    return MUNIT_OK;
}

/* If no matching server is found, the length of the configuration is
 * returned. */
static MunitResult test_voting_index_no_match(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    size_t i;
    int rv;

    (void)params;

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, 0);

    i = raft_configuration__voting_index(&f->configuration, 3);
    munit_assert_int(i, ==, f->configuration.n);

    return MUNIT_OK;
}

/* If the server exists but is non-voting, the length of the configuration is
   returned. . */
static MunitResult test_voting_index_non_voting(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    size_t i;
    int rv;

    (void)params;

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", false);
    munit_assert_int(rv, ==, 0);

    i = raft_configuration__voting_index(&f->configuration, 1);
    munit_assert_int(i, ==, f->configuration.n);

    return MUNIT_OK;
}

static MunitTest voting_index_tests[] = {
    {"/match", test_voting_index_match, setup, tear_down, 0, NULL},
    {"/no-match", test_voting_index_no_match, setup, tear_down, 0, NULL},
    {"/non-voting", test_voting_index_non_voting, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_configuration_suites[] = {
    {"/add", add_tests, NULL, 1, 0},
    {"/n_voting", n_voting_tests, NULL, 1, 0},
    {"/index", index_tests, NULL, 1, 0},
    {"/voting-index", voting_index_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
