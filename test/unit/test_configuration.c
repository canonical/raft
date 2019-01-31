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
 * Add a server to the fixture's configuration and check that no error occurs.
 */
#define __add(F, ID, ADDRESS, VOTING)                                        \
    {                                                                        \
        int rv;                                                              \
                                                                             \
        rv = raft_configuration_add(&F->configuration, ID, ADDRESS, VOTING); \
        munit_assert_int(rv, ==, 0);                                         \
    }

/**
 * Remove a server from the fixture's configuration and check that no error
 * occurs.
 */
#define __remove(F, ID)                                        \
    {                                                          \
        int rv;                                                \
                                                               \
        rv = raft_configuration_remove(&F->configuration, ID); \
        munit_assert_int(rv, ==, 0);                           \
    }

/**
 * Assert that the fixture's configuration has n servers.
 */
#define __assert_n_servers(F, N)                                 \
    {                                                            \
        munit_assert_int(F->configuration.n, ==, N);             \
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
#define __assert_server_equal(F, I, ID, ADDRESS, VOTING)     \
    {                                                        \
        struct raft_server *server;                          \
                                                             \
        munit_assert_int(I, <, F->configuration.n);          \
                                                             \
        server = &f->configuration.servers[I];               \
                                                             \
        munit_assert_int(server->id, ==, ID);                \
        munit_assert_string_equal(server->address, ADDRESS); \
        munit_assert_int(server->voting, ==, VOTING);        \
    }

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
 * raft_configuration__index
 */

/* If a matching server is found, it's index is returned. */
static MunitResult test_index_match(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    size_t i;

    (void)params;

    __add(f, 1, "192.168.1.1:666", true);
    __add(f, 2, "192.168.1.2:666", false);

    i = raft_configuration__index(&f->configuration, 2);
    munit_assert_int(i, ==, 1);

    return MUNIT_OK;
}

/* If no matching server is found, the length of the configuration is
 * returned. */
static MunitResult test_index_no_match(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    size_t i;

    (void)params;

    __add(f, 1, "127.0.0.1:666", true);

    i = raft_configuration__index(&f->configuration, 3);
    munit_assert_int(i, ==, f->configuration.n);

    return MUNIT_OK;
}

static MunitTest index_tests[] = {
    {"/match", test_index_match, setup, tear_down, 0, NULL},
    {"/no-match", test_index_no_match, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_configuration__get
 */

/* If a matching server is found, it's returned. */
static MunitResult test_get_match(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;

    (void)params;

    __add(f, 1, "192.168.1.1:666", true);
    __add(f, 2, "192.168.1.2:666", false);

    server = raft_configuration__get(&f->configuration, 2);

    munit_assert_ptr_not_null(server);
    munit_assert_int(server->id, ==, 2);
    munit_assert_string_equal(server->address, "192.168.1.2:666");

    return MUNIT_OK;
}

/**
 * raft_configuration__voting_index
 */

/* The index of the matching voting server (relative to the number of voting
   servers) is returned. */
static MunitResult test_voting_index_match(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    size_t i;

    (void)params;

    __add(f, 1, "192.168.1.1:666", false);
    __add(f, 2, "192.168.1.2:666", true);
    __add(f, 3, "192.168.1.3:666", true);

    i = raft_configuration__voting_index(&f->configuration, 3);
    munit_assert_int(i, ==, 1);

    return MUNIT_OK;
}

/* If no matching server is found, the length of the configuration is
 * returned. */
static MunitResult test_voting_index_no_match(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    size_t i;

    (void)params;

    __add(f, 1, "192.168.1.1:666", true);

    i = raft_configuration__voting_index(&f->configuration, 3);
    munit_assert_int(i, ==, f->configuration.n);

    return MUNIT_OK;
}

/* If the server exists but is non-voting, the length of the configuration is
   returned. . */
static MunitResult test_voting_index_non_voting(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;
    size_t i;

    (void)params;

    __add(f, 1, "192.168.1.1:666", false);

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

/* If no matching server is found, NULL is returned. */
static MunitResult test_get_no_match(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;

    (void)params;

    __add(f, 1, "127.0.0.1:666", true);

    server = raft_configuration__get(&f->configuration, 3);
    munit_assert_ptr_null(server);

    return MUNIT_OK;
}

static MunitTest get_tests[] = {
    {"/match", test_get_match, setup, tear_down, 0, NULL},
    {"/no-match", test_get_no_match, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_configuration__n_voting
 */

/* Return only voting nodes. */
static MunitResult test_n_voting_filter(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    size_t n;

    (void)params;

    __add(f, 1, "192.168.1.1:666", true);
    __add(f, 2, "192.168.1.2:666", false);

    n = raft_configuration__n_voting(&f->configuration);
    munit_assert_int(n, ==, 1);

    return MUNIT_OK;
}

static MunitTest n_voting_tests[] = {
    {"/filter", test_n_voting_filter, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_configuration__copy
 */

/* Copy a configuration containing two servers */
static MunitResult test_copy_two(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    int rv;

    (void)params;

    __add(f, 1, "192.168.1.1:666", false);
    __add(f, 2, "192.168.1.2:666", true);

    raft_configuration_init(&configuration);

    rv = raft_configuration__copy(&f->configuration, &configuration);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(configuration.n, ==, 2);
    munit_assert_int(configuration.servers[0].id, ==, 1);
    munit_assert_int(configuration.servers[1].id, ==, 2);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/* Out of memory */
static MunitResult test_copy_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    int rv;

    (void)params;

    __add(f, 1, "192.168.1.1:666", false);

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    raft_configuration_init(&configuration);

    rv = raft_configuration__copy(&f->configuration, &configuration);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

static MunitTest copy_tests[] = {
    {"/two", test_copy_two, setup, tear_down, 0, NULL},
    {"/oom", test_copy_oom, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_configuration_add
 */

/* Add a server to the configuration. */
static MunitResult test_add_one(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __add(f, 1, "127.0.0.1:666", true);

    __assert_n_servers(f, 1);
    __assert_server_equal(f, 0, 1, "127.0.0.1:666", true);

    return MUNIT_OK;
}

/* Add two servers to the configuration. */
static MunitResult test_add_two(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __add(f, 1, "127.0.0.1:666", true);
    __add(f, 2, "192.168.1.1:666", false);

    __assert_n_servers(f, 2);
    __assert_server_equal(f, 0, 1, "127.0.0.1:666", true);
    __assert_server_equal(f, 1, 2, "192.168.1.1:666", false);

    return MUNIT_OK;
}

/* Add a server with an ID which is already in use. */
static MunitResult test_add_dup_id(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    __add(f, 1, "127.0.0.1:666", true);

    rv = raft_configuration_add(&f->configuration, 1, "192.168.1.1:666", false);
    munit_assert_int(rv, ==, RAFT_ERR_DUP_SERVER_ID);

    munit_assert_string_equal(raft_strerror(rv), "server ID already in use");

    return MUNIT_OK;
}

/* Add a server with an address which is already in use. */
static MunitResult test_add_dup_address(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    __add(f, 1, "127.0.0.1:666", true);

    rv = raft_configuration_add(&f->configuration, 2, "127.0.0.1:666", false);
    munit_assert_int(rv, ==, RAFT_ERR_DUP_SERVER_ADDRESS);

    munit_assert_string_equal(raft_strerror(rv),
                              "server address already in use");

    return MUNIT_OK;
}

static char *add_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *add_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum add_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, add_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, add_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory. */
static MunitResult test_add_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_heap_fault_enable(&f->heap);

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    munit_assert_string_equal(raft_strerror(rv), "out of memory");

    return MUNIT_OK;
}

static MunitTest add_tests[] = {
    {"/one", test_add_one, setup, tear_down, 0, NULL},
    {"/two", test_add_two, setup, tear_down, 0, NULL},
    {"/dup-id", test_add_dup_id, setup, tear_down, 0, NULL},
    {"/dup-address", test_add_dup_address, setup, tear_down, 0, NULL},
    {"/oom", test_add_oom, setup, tear_down, 0, add_oom_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_configuration_remove
 */

/* Attempts to remove a server with an unknown ID result in an error. */
static MunitResult test_remove_unknown(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = raft_configuration_remove(&f->configuration, 1);
    munit_assert_int(rv, ==, RAFT_ERR_UNKNOWN_SERVER_ID);

    munit_assert_string_equal(raft_strerror(rv), "server ID is unknown");

    return MUNIT_OK;
}

/* Remove the last and only server. */
static MunitResult test_remove_last(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __add(f, 1, "127.0.0.1:666", true);

    __remove(f, 1);

    __assert_n_servers(f, 0);

    return MUNIT_OK;
}

/* Remove the first server. */
static MunitResult test_remove_first(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __add(f, 1, "127.0.0.1:666", true);
    __add(f, 2, "192.168.1.1:666", false);

    __remove(f, 1);

    __assert_n_servers(f, 1);
    __assert_server_equal(f, 0, 2, "192.168.1.1:666", false);
    ;

    return MUNIT_OK;
}

/* Remove a server in the middle. */
static MunitResult test_remove_middle(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __add(f, 1, "127.0.0.1:666", true);
    __add(f, 2, "192.168.1.1:666", false);
    __add(f, 3, "10.0.1.1:666", true);

    __remove(f, 2);

    __assert_n_servers(f, 2);
    __assert_server_equal(f, 0, 1, "127.0.0.1:666", true);
    ;
    __assert_server_equal(f, 1, 3, "10.0.1.1:666", true);
    ;

    return MUNIT_OK;
}

/* Out of memory. */
static MunitResult test_remove_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    __add(f, 1, "127.0.0.1:666", true);
    __add(f, 2, "192.168.1.1:666", false);

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    rv = raft_configuration_remove(&f->configuration, 2);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

static MunitTest remove_tests[] = {
    {"/unknown", test_remove_unknown, setup, tear_down, 0, NULL},
    {"/last", test_remove_last, setup, tear_down, 0, NULL},
    {"/first", test_remove_first, setup, tear_down, 0, NULL},
    {"/middle", test_remove_middle, setup, tear_down, 0, NULL},
    {"/oom", test_remove_oom, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_configuration_encode
 */

/* Trying to encode an empty configuration results in an error. */
static MunitResult test_encode_empty(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int rv;

    (void)data;
    (void)params;

    rv = raft_configuration_encode(&f->configuration, &buf);
    munit_assert_int(rv, ==, RAFT_ERR_EMPTY_CONFIGURATION);

    munit_assert_string_equal(raft_strerror(rv),
                              "configuration has no servers");

    return MUNIT_OK;
}

/* Out of memory. */
static MunitResult test_encode_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_heap_fault_config(&f->heap, 2, 1);
    test_heap_fault_enable(&f->heap);

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_configuration_encode(&f->configuration, &buf);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

/* Encode a configuration with one server. */
static MunitResult test_encode_one_server(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    uint8_t *bytes;
    int rv;

    (void)data;
    (void)params;

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_configuration_encode(&f->configuration, &buf);
    munit_assert_int(rv, ==, 0);

    bytes = buf.base;

    munit_assert_int(buf.len, ==,
                     1 + 8 + /* Version and n of servers */
                         8 + strlen("127.0.0.1:666") + 1 + 1); /* Server */

    munit_assert_int(bytes[0], ==, 1);
    munit_assert_int(raft__flip64(*(uint64_t *)(bytes + 1)), ==, 1);

    munit_assert_int(raft__flip64(*(uint64_t *)(bytes + 9)), ==, 1);
    munit_assert_string_equal((char *)(bytes + 17), "127.0.0.1:666");
    munit_assert_true(bytes[17 + strlen("127.0.0.1:666") + 1]);

    raft_free(buf.base);

    return MUNIT_OK;
}

/* Encode a configuration with two servers. */
static MunitResult test_encode_two_servers(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    uint8_t *bytes;
    int rv;

    (void)data;
    (void)params;

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", false);
    munit_assert_int(rv, ==, 0);

    rv = raft_configuration_add(&f->configuration, 2, "192.168.1.1:666", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_configuration_encode(&f->configuration, &buf);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(buf.len, ==,
                     1 + 8 + /* Version and n of servers */
                         8 + strlen("127.0.0.1:666") + 1 + 1 +   /* Server 1 */
                         8 + strlen("192.168.1.1:666") + 1 + 1); /* Server 2 */

    bytes = buf.base;

    munit_assert_int(bytes[0], ==, 1);
    munit_assert_int(raft__flip64(*(uint64_t *)(bytes + 1)), ==, 2);

    bytes = buf.base + 9;
    munit_assert_int(raft__flip64(*(uint64_t *)bytes), ==, 1);
    munit_assert_string_equal((char *)(bytes + 8), "127.0.0.1:666");
    munit_assert_false(bytes[8 + strlen("127.0.0.1:666") + 1]);

    bytes = buf.base + 9 + 8 + strlen("127.0.0.1:666") + 1 + 1;

    munit_assert_int(raft__flip64(*(uint64_t *)bytes), ==, 2);
    munit_assert_string_equal((char *)(bytes + 8), "192.168.1.1:666");
    munit_assert_true(bytes[8 + strlen("192.168.1.1:666") + 1]);

    raft_free(buf.base);

    return MUNIT_OK;
}

static MunitTest encode_tests[] = {
    {"/oom", test_encode_oom, setup, tear_down, 0, NULL},
    {"/empty", test_encode_empty, setup, tear_down, 0, NULL},
    {"/one", test_encode_one_server, setup, tear_down, 0, NULL},
    {"/two", test_encode_two_servers, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_configuration_decode
 */

/* If the destination configuration is not empty, an error is returned. */
static MunitResult test_decode_empty(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    uint8_t bytes = 127;
    struct raft_buffer buf;
    int rv;

    (void)data;
    (void)params;

    buf.base = &bytes;
    buf.len = 1;

    rv = raft_configuration_add(&f->configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_configuration_decode(&buf, &f->configuration);
    munit_assert_int(rv, ==, RAFT_ERR_CONFIGURATION_NOT_EMPTY);

    return MUNIT_OK;
}

/* Not enough memory of the servers array. */
static MunitResult test_decode_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    uint8_t bytes[] = {1,                            /* Version */
                       1,   0,   0,   0, 0, 0, 0, 0, /* Number of servers */
                       5,   0,   0,   0, 0, 0, 0, 0, /* Server ID */
                       'x', '.', 'y', 0,             /* Server address */
                       1 /* Voting flag */};
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    buf.base = bytes;
    buf.len = sizeof bytes;

    rv = raft_configuration_decode(&buf, &f->configuration);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

/* If the encoding version is wrong, an error is returned. */
static MunitResult test_decode_bad_version(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    uint8_t bytes = 127;
    struct raft_buffer buf;
    int rv;

    (void)data;
    (void)params;

    buf.base = &bytes;
    buf.len = 1;

    rv = raft_configuration_decode(&buf, &f->configuration);
    munit_assert_int(rv, ==, RAFT_ERR_MALFORMED);

    return MUNIT_OK;
}

/* The address of a server is not a nul-terminated string. */
static MunitResult test_decode_bad_address(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    uint8_t bytes[] = {1,                            /* Version */
                       1,   0,   0,   0, 0, 0, 0, 0, /* Number of servers */
                       5,   0,   0,   0, 0, 0, 0, 0, /* Server ID */
                       'x', '.', 'y',                /* Server address */
                       1 /* Voting flag */};
    struct raft_buffer buf;
    int rv;

    (void)data;
    (void)params;

    buf.base = bytes;
    buf.len = sizeof bytes;

    rv = raft_configuration_decode(&buf, &f->configuration);
    munit_assert_int(rv, ==, RAFT_ERR_MALFORMED);

    return MUNIT_OK;
}

/* The decode a payload encoding a configuration with one server */
static MunitResult test_decode_one_server(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    uint8_t bytes[] = {1,                            /* Version */
                       1,   0,   0,   0, 0, 0, 0, 0, /* Number of servers */
                       5,   0,   0,   0, 0, 0, 0, 0, /* Server ID */
                       'x', '.', 'y', 0,             /* Server address */
                       1 /* Voting flag */};
    struct raft_buffer buf;
    int rv;

    (void)data;
    (void)params;

    buf.base = bytes;
    buf.len = sizeof bytes;

    rv = raft_configuration_decode(&buf, &f->configuration);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->configuration.n, ==, 1);

    munit_assert_int(f->configuration.servers[0].id, ==, 5);
    munit_assert_string_equal(f->configuration.servers[0].address, "x.y");
    munit_assert_int(f->configuration.servers[0].voting, ==, 1);

    return MUNIT_OK;
}

/* The decode size is the size of a raft_server array plus the length of the
 * addresses. */
static MunitResult test_decode_two_servers(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    uint8_t bytes[] = {1,                                /* Version */
                       2,   0,   0,   0,   0,   0, 0, 0, /* Number of servers */
                       5,   0,   0,   0,   0,   0, 0, 0, /* Server ID */
                       'x', '.', 'y', 0,                 /* Server address */
                       1,                                /* Voting flag */
                       3,   0,   0,   0,   0,   0, 0, 0, /* Server ID */
                       '1', '9', '2', '.', '2', 0,       /* Server address */
                       0 /* Voting flag */};
    struct raft_buffer buf;
    int rv;

    buf.base = bytes;
    buf.len = sizeof bytes;

    (void)data;
    (void)params;

    rv = raft_configuration_decode(&buf, &f->configuration);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->configuration.n, ==, 2);

    munit_assert_int(f->configuration.servers[0].id, ==, 5);
    munit_assert_string_equal(f->configuration.servers[0].address, "x.y");
    munit_assert_int(f->configuration.servers[0].voting, ==, 1);

    munit_assert_int(f->configuration.servers[1].id, ==, 3);
    munit_assert_string_equal(f->configuration.servers[1].address, "192.2");
    munit_assert_int(f->configuration.servers[1].voting, ==, 0);

    return MUNIT_OK;
}

static MunitTest decode_tests[] = {
    {"/empty", test_decode_empty, setup, tear_down, 0, NULL},
    {"/oom", test_decode_oom, setup, tear_down, 0, NULL},
    {"/bad-version", test_decode_bad_version, setup, tear_down, 0, NULL},
    {"/bad-address", test_decode_bad_address, setup, tear_down, 0, NULL},
    {"/one", test_decode_one_server, setup, tear_down, 0, NULL},
    {"/two", test_decode_two_servers, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_configuration_suites[] = {
    {"/index", index_tests, NULL, 1, 0},
    {"/voting-index", voting_index_tests, NULL, 1, 0},
    {"/get", get_tests, NULL, 1, 0},
    {"/n_voting", n_voting_tests, NULL, 1, 0},
    {"/copy", copy_tests, NULL, 1, 0},
    {"/add", add_tests, NULL, 1, 0},
    {"/remove", remove_tests, NULL, 1, 0},
    {"/encode", encode_tests, NULL, 1, 0},
    {"/decode", decode_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
