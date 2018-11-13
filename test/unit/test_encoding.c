#include "../../include/raft.h"

#include "../../src/binary.h"

#include "../lib/heap.h"
#include "../lib/munit.h"

/**
 * Helpers
 */

struct fixture
{
    struct raft_heap heap;
};

/**
 * Fill the fields of the given append entries message with some samle data.
 */
static void __fill_append_entries_args(struct raft_append_entries_args *args)
{
    args->term = 3;
    args->leader_id = 123;
    args->prev_log_index = 1;
    args->prev_log_term = 2;
    args->leader_commit = 0;
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

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * raft_encode_configuration
 */

/* Trying to encode an empty configuration results in an error. */
static MunitResult test_encode_configuration_empty(
    const MunitParameter params[],
    void *data)
{
    struct raft_configuration configuration;
    struct raft_buffer buf;
    int rv;

    (void)data;
    (void)params;

    raft_configuration_init(&configuration);

    rv = raft_encode_configuration(&configuration, &buf);
    munit_assert_int(rv, ==, RAFT_ERR_EMPTY_CONFIGURATION);

    munit_assert_string_equal(raft_strerror(rv),
                              "configuration has no servers");

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/* Out of memory. */
static MunitResult test_encode_configuration_oom(const MunitParameter params[],
                                                 void *data)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_buffer buf;
    int rv;

    (void)params;

    raft_configuration_init(&configuration);

    test_heap_fault_config(&f->heap, 2, 1);
    test_heap_fault_enable(&f->heap);

    rv = raft_configuration_add(&configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_encode_configuration(&configuration, &buf);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/* Encode a configuration with one server. */
static MunitResult test_encode_configuration_one_server(
    const MunitParameter params[],
    void *data)
{
    struct raft_configuration configuration;
    struct raft_buffer buf;
    uint8_t *bytes;
    int rv;

    (void)data;
    (void)params;

    raft_configuration_init(&configuration);

    rv = raft_configuration_add(&configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_encode_configuration(&configuration, &buf);
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

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/* Encode a configuration with two servers. */
static MunitResult test_encode_configuration_two_servers(
    const MunitParameter params[],
    void *data)
{
    struct raft_configuration configuration;
    struct raft_buffer buf;
    uint8_t *bytes;
    int rv;

    (void)data;
    (void)params;

    raft_configuration_init(&configuration);

    rv = raft_configuration_add(&configuration, 1, "127.0.0.1:666", false);
    munit_assert_int(rv, ==, 0);

    rv = raft_configuration_add(&configuration, 2, "192.168.1.1:666", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_encode_configuration(&configuration, &buf);
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

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

static MunitTest encode_configuration_tests[] = {
    {"/oom", test_encode_configuration_oom, setup, tear_down, 0, NULL},
    {"/empty", test_encode_configuration_empty, setup, tear_down, 0, NULL},
    {"/one", test_encode_configuration_one_server, setup, tear_down, 0, NULL},
    {"/two", test_encode_configuration_two_servers, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_decode_configuration
 */

/* If the destination configuration is not empty, an error is returned. */
static MunitResult test_decode_configuration_empty(const MunitParameter params[],
                                             void *data)
{
    struct raft_configuration configuration;
    uint8_t bytes = 127;
    struct raft_buffer buf;
    int rv;

    (void)data;
    (void)params;

    raft_configuration_init(&configuration);

    buf.base = &bytes;
    buf.len = 1;

    rv = raft_configuration_add(&configuration, 1, "127.0.0.1:666", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_decode_configuration(&buf, &configuration);
    munit_assert_int(rv, ==, RAFT_ERR_CONFIGURATION_NOT_EMPTY);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/* Not enough memory of the servers array. */
static MunitResult test_decode_configuration_oom(const MunitParameter params[],
                                                 void *data)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    uint8_t bytes[] = {1,                            /* Version */
                       1,   0,   0,   0, 0, 0, 0, 0, /* Number of servers */
                       5,   0,   0,   0, 0, 0, 0, 0, /* Server ID */
                       'x', '.', 'y', 0,             /* Server address */
                       1 /* Voting flag */};
    struct raft_buffer buf;
    int rv;

    (void)params;

    raft_configuration_init(&configuration);

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    buf.base = bytes;
    buf.len = sizeof bytes;

    rv = raft_decode_configuration(&buf, &configuration);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/* If the encoding version is wrong, an error is returned. */
static MunitResult test_decode_configuration_bad_version(
    const MunitParameter params[],
    void *data)
{
    struct raft_configuration configuration;
    uint8_t bytes = 127;
    struct raft_buffer buf;
    int rv;

    (void)data;
    (void)params;

    raft_configuration_init(&configuration);

    buf.base = &bytes;
    buf.len = 1;

    rv = raft_decode_configuration(&buf, &configuration);
    munit_assert_int(rv, ==, RAFT_ERR_MALFORMED);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/* The address of a server is not a nul-terminated string. */
static MunitResult test_decode_configuration_bad_address(
    const MunitParameter params[],
    void *data)
{
    struct raft_configuration configuration;
    uint8_t bytes[] = {1,                            /* Version */
                       1,   0,   0,   0, 0, 0, 0, 0, /* Number of servers */
                       5,   0,   0,   0, 0, 0, 0, 0, /* Server ID */
                       'x', '.', 'y',                /* Server address */
                       1 /* Voting flag */};
    struct raft_buffer buf;
    int rv;

    (void)data;
    (void)params;

    raft_configuration_init(&configuration);

    buf.base = bytes;
    buf.len = sizeof bytes;

    rv = raft_decode_configuration(&buf, &configuration);
    munit_assert_int(rv, ==, RAFT_ERR_MALFORMED);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/* The decode a payload encoding a configuration with one server */
static MunitResult test_decode_configuration_one_server(
    const MunitParameter params[],
    void *data)
{
    struct raft_configuration configuration;
    uint8_t bytes[] = {1,                            /* Version */
                       1,   0,   0,   0, 0, 0, 0, 0, /* Number of servers */
                       5,   0,   0,   0, 0, 0, 0, 0, /* Server ID */
                       'x', '.', 'y', 0,             /* Server address */
                       1 /* Voting flag */};
    struct raft_buffer buf;
    int rv;

    (void)data;
    (void)params;

    raft_configuration_init(&configuration);

    buf.base = bytes;
    buf.len = sizeof bytes;

    rv = raft_decode_configuration(&buf, &configuration);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(configuration.n, ==, 1);

    munit_assert_int(configuration.servers[0].id, ==, 5);
    munit_assert_string_equal(configuration.servers[0].address, "x.y");
    munit_assert_int(configuration.servers[0].voting, ==, 1);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/* The decode size is the size of a raft_server array plus the length of the
 * addresses. */
static MunitResult test_decode_configuration_two_servers(
    const MunitParameter params[],
    void *data)
{
    struct raft_configuration configuration;
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

    raft_configuration_init(&configuration);

    rv = raft_decode_configuration(&buf, &configuration);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(configuration.n, ==, 2);

    munit_assert_int(configuration.servers[0].id, ==, 5);
    munit_assert_string_equal(configuration.servers[0].address, "x.y");
    munit_assert_int(configuration.servers[0].voting, ==, 1);

    munit_assert_int(configuration.servers[1].id, ==, 3);
    munit_assert_string_equal(configuration.servers[1].address, "192.2");
    munit_assert_int(configuration.servers[1].voting, ==, 0);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

static MunitTest decode_configuration_tests[] = {
    {"/empty", test_decode_configuration_empty, setup, tear_down, 0, NULL},
    {"/oom", test_decode_configuration_oom, setup, tear_down, 0, NULL},
    {"/bad-version", test_decode_configuration_bad_version, setup, tear_down, 0,
     NULL},
    {"/bad-address", test_decode_configuration_bad_address, setup, tear_down, 0,
     NULL},
    {"/one", test_decode_configuration_one_server, setup, tear_down, 0, NULL},
    {"/two", test_decode_configuration_two_servers, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_encode_append_entries
 */

/* Encode the header of an append entries request with no entries. */
static MunitResult test_encode_append_entries_0(const MunitParameter params[],
                                                void *data)
{
    struct raft_append_entries_args args;
    struct raft_buffer buf;
    int rv;

    (void)data;
    (void)params;

    __fill_append_entries_args(&args);
    args.entries = NULL;
    args.n = 0;

    rv = raft_encode_append_entries(&args, &buf);
    munit_assert_int(rv, ==, 0);

    /* Encoding version */
    munit_assert_int(raft__flip32(*(uint32_t *)buf.base), ==, 1);

    /* Message type */
    munit_assert_int(raft__flip32(*(uint32_t *)(buf.base + 4)), ==,
                     RAFT_IO_APPEND_ENTRIES);

    /* Message size */
    munit_assert_int(raft__flip64(*(uint32_t *)(buf.base + 8)), ==, 48);

    raft_free(buf.base);

    return MUNIT_OK;
}

/* Encode the header of an append entries request with one entry. */
static MunitResult test_encode_append_entries_1(const MunitParameter params[],
                                                void *data)
{
    struct raft_append_entries_args args;
    struct raft_buffer buf;
    struct raft_entry entry;
    int rv;

    (void)data;
    (void)params;

    entry.type = RAFT_LOG_COMMAND;
    entry.term = 2;
    entry.buf.base = munit_malloc(8);
    entry.buf.len = 8;

    __fill_append_entries_args(&args);
    args.entries = &entry;
    args.n = 1;

    rv = raft_encode_append_entries(&args, &buf);
    munit_assert_int(rv, ==, 0);

    /* Encoding version */
    munit_assert_int(raft__flip32(*(uint32_t *)buf.base), ==, 1);

    /* Message type */
    munit_assert_int(raft__flip32(*(uint32_t *)(buf.base + 4)), ==,
                     RAFT_IO_APPEND_ENTRIES);

    /* Message size */
    munit_assert_int(raft__flip64(*(uint32_t *)(buf.base + 8)), ==, 64);

    free(entry.buf.base);
    raft_free(buf.base);

    return MUNIT_OK;
}

/* Encode the header of an append entries request with one entry whose data size
 * is not a multiple of 8. */
static MunitResult test_encode_append_entries_pad(const MunitParameter params[],
                                                  void *data)
{
    struct raft_append_entries_args args;
    struct raft_buffer buf;
    struct raft_entry entry;
    int rv;

    (void)data;
    (void)params;

    entry.type = RAFT_LOG_COMMAND;
    entry.term = 2;
    entry.buf.base = munit_malloc(7);
    entry.buf.len = 7;

    __fill_append_entries_args(&args);
    args.entries = &entry;
    args.n = 1;

    rv = raft_encode_append_entries(&args, &buf);
    munit_assert_int(rv, ==, 0);

    /* Encoding version */
    munit_assert_int(raft__flip32(*(uint32_t *)buf.base), ==, 1);

    /* Message type */
    munit_assert_int(raft__flip32(*(uint32_t *)(buf.base + 4)), ==,
                     RAFT_IO_APPEND_ENTRIES);

    /* Message size */
    munit_assert_int(raft__flip64(*(uint32_t *)(buf.base + 8)), ==, 64);

    free(entry.buf.base);
    raft_free(buf.base);

    return MUNIT_OK;
}

static MunitTest encode_append_entries_tests[] = {
    {"/0", test_encode_append_entries_0, setup, tear_down, 0, NULL},
    {"/1", test_encode_append_entries_1, setup, tear_down, 0, NULL},
    {"/pad", test_encode_append_entries_pad, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_decode_append_entries
 */

/* Decode the body of an append entries message with no entries. */
static MunitResult test_decode_append_entries_0(const MunitParameter params[],
                                                void *data)
{
    struct raft_append_entries_args args;
    struct raft_buffer buf1;
    struct raft_buffer buf2;
    int rv;

    (void)data;
    (void)params;

    __fill_append_entries_args(&args);
    args.entries = NULL;
    args.n = 0;

    rv = raft_encode_append_entries(&args, &buf1);
    munit_assert_int(rv, ==, 0);

    memset(&args, 0, sizeof args);

    /* Skip the message header. */
    buf2.len = buf1.len - 16;
    buf2.base = munit_malloc(buf2.len);
    memcpy(buf2.base, buf1.base + 16, buf2.len);

    rv = raft_decode_append_entries(&buf2, &args);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(args.term, ==, 3);
    munit_assert_int(args.leader_id, ==, 123);
    munit_assert_int(args.prev_log_index, ==, 1);
    munit_assert_int(args.prev_log_term, ==, 2);
    munit_assert_int(args.leader_commit, ==, 0);

    munit_assert_ptr_null(args.entries);
    munit_assert_int(args.n, ==, 0);

    raft_free(buf1.base);
    free(buf2.base);

    return MUNIT_OK;
}

/* Decode the body of an append entries message with one entry. */
static MunitResult test_decode_append_entries_1(const MunitParameter params[],
                                                void *data)
{
    struct raft_append_entries_args args;
    struct raft_buffer buf1;
    struct raft_buffer buf2;
    struct raft_buffer buf3;
    struct raft_entry entry;
    int rv;

    (void)data;
    (void)params;

    entry.type = RAFT_LOG_COMMAND;
    entry.term = 2;
    entry.buf.base = munit_malloc(8);
    entry.buf.len = 8;

    *(uint64_t *)entry.buf.base = 123456789;

    __fill_append_entries_args(&args);
    args.entries = &entry;
    args.n = 1;

    rv = raft_encode_append_entries(&args, &buf1);
    munit_assert_int(rv, ==, 0);

    /* Skip the message header and append the entry data. */
    buf2.len = buf1.len - 16;
    buf2.base = munit_malloc(buf2.len);
    memcpy(buf2.base, buf1.base + 16, buf2.len);

    /* Copy the entry data */
    buf3.len = 8;
    buf3.base = munit_malloc(buf3.len);
    memcpy(buf3.base, entry.buf.base, entry.buf.len);

    memset(&args, 0, sizeof args);

    rv = raft_decode_append_entries(&buf2, &args);
    munit_assert_int(rv, ==, 0);

    munit_assert_ptr_not_null(args.entries);
    munit_assert_int(args.n, ==, 1);

    rv = raft_decode_entries_batch(&buf3, args.entries, args.n);
    munit_assert_int(rv, ==, 0);

    munit_assert_ptr_not_null(args.entries[0].buf.base);
    munit_assert_int(args.entries[0].buf.len, ==, 8);

    munit_assert_ptr_equal(args.entries[0].buf.base, buf3.base);
    munit_assert_int(*(uint64_t *)args.entries[0].buf.base, ==, 123456789);

    raft_free(args.entries);
    free(entry.buf.base);
    raft_free(buf1.base);
    free(buf2.base);
    free(buf3.base);

    return MUNIT_OK;
}

/* Decode the body of an append entries message with one entry whose data size
 * is not a multiple of 8. */
static MunitResult test_decode_append_entries_pad(const MunitParameter params[],
                                                  void *data)
{
    struct raft_append_entries_args args;
    struct raft_buffer buf1;
    struct raft_buffer buf2;
    struct raft_buffer buf3;
    struct raft_entry entry;
    int rv;

    (void)data;
    (void)params;

    entry.type = RAFT_LOG_COMMAND;
    entry.term = 2;
    entry.buf.base = munit_malloc(6);
    entry.buf.len = 6;

    strcpy(entry.buf.base, "hello");

    __fill_append_entries_args(&args);
    args.entries = &entry;
    args.n = 1;

    rv = raft_encode_append_entries(&args, &buf1);
    munit_assert_int(rv, ==, 0);

    /* Skip the message header. */
    buf2.len = buf1.len - 16;
    buf2.base = munit_malloc(buf2.len);
    memcpy(buf2.base, buf1.base + 16, buf2.len);

    /* Copy the entry data */
    buf3.len = 8;
    buf3.base = munit_malloc(buf3.len);
    memcpy(buf3.base, entry.buf.base, entry.buf.len);

    memset(&args, 0, sizeof args);

    rv = raft_decode_append_entries(&buf2, &args);
    munit_assert_int(rv, ==, 0);

    rv = raft_decode_entries_batch(&buf3, args.entries, args.n);
    munit_assert_int(rv, ==, 0);

    munit_assert_ptr_not_null(args.entries);
    munit_assert_int(args.n, ==, 1);

    munit_assert_ptr_not_null(args.entries[0].buf.base);
    munit_assert_int(args.entries[0].buf.len, ==, 6);

    munit_assert_ptr_equal(args.entries[0].buf.base, buf3.base);
    munit_assert_string_equal(args.entries[0].buf.base, "hello");

    raft_free(args.entries);
    free(entry.buf.base);
    raft_free(buf1.base);
    free(buf2.base);
    free(buf3.base);

    return MUNIT_OK;
}

/* Decode the body of an append entries message with two entries. */
static MunitResult test_decode_append_entries_2(const MunitParameter params[],
                                                void *data)
{
    struct raft_append_entries_args args;
    struct raft_buffer buf1;
    struct raft_buffer buf2;
    struct raft_entry entries[2];
    int rv;

    (void)data;
    (void)params;

    entries[0].type = RAFT_LOG_COMMAND;
    entries[0].term = 2;
    entries[0].buf.base = munit_malloc(6);
    entries[0].buf.len = 6;

    strcpy(entries[0].buf.base, "hello");

    entries[1].type = RAFT_LOG_COMMAND;
    entries[1].term = 2;
    entries[1].buf.base = munit_malloc(8);
    entries[1].buf.len = 8;
    *(uint64_t *)entries[1].buf.base = 123456789;

    __fill_append_entries_args(&args);
    args.entries = entries;
    args.n = 2;

    rv = raft_encode_append_entries(&args, &buf1);
    munit_assert_int(rv, ==, 0);

    /* Skip the message header and append the entries data. */
    buf2.len = buf1.len - 16 + 8 + 8;
    buf2.base = munit_malloc(buf2.len);
    memcpy(buf2.base, buf1.base + 16, buf2.len - 16);
    memcpy(buf2.base + buf2.len - 16, entries[0].buf.base, entries[0].buf.len);
    memcpy(buf2.base + buf2.len - 8, entries[1].buf.base, entries[1].buf.len);

    memset(&args, 0, sizeof args);

    rv = raft_decode_append_entries(&buf2, &args);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(args.n, ==, 2);

    raft_free(args.entries);

    free(entries[0].buf.base);
    free(entries[1].buf.base);

    raft_free(buf1.base);
    free(buf2.base);

    return MUNIT_OK;
}

static MunitTest decode_append_entries_tests[] = {
    {"/0", test_decode_append_entries_0, setup, tear_down, 0, NULL},
    {"/1", test_decode_append_entries_1, setup, tear_down, 0, NULL},
    {"/pad", test_decode_append_entries_pad, setup, tear_down, 0, NULL},
    {"/2", test_decode_append_entries_2, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_encoding_suites[] = {
    {"/encode-configuration", encode_configuration_tests, NULL, 1, 0},
    {"/decode-configuration", decode_configuration_tests, NULL, 1, 0},
    {"/encode-append_entries", encode_append_entries_tests, NULL, 1, 0},
    {"/decode-append-entries", decode_append_entries_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
