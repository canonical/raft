#include <stdio.h>

#include "../../include/raft.h"

#include "../lib/munit.h"

/**
 * Helpers
 */

struct fixture
{
    unsigned short state;
    raft_term current_term;
    struct raft_context ctx;
    struct raft_logger logger;
    struct
    {
        int level;
        char *message;
    } last; /* Last message emitted. */
};

static void fixture__emit(void *data,
                          int level,
                          const char *format,
                          va_list args)
{
    struct fixture *f = data;
    int rv;

    // munit_assert_ptr_equal(ctx, &f->ctx);

    f->last.level = level;

    rv = vasprintf(&f->last.message, format, args);

    munit_assert_int(rv, >=, strlen(format));
}

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)user_data;
    (void)params;

    f->state = RAFT_STATE_LEADER;
    f->current_term = 1;

    f->ctx.state = &f->state;
    f->ctx.current_term = &f->current_term;

    f->logger.data = f;
    f->logger.emit = fixture__emit;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    free(f);
}

/**
 * raft__debugf
 */

/* Emit a message at debug level. */
static MunitResult test_debugf(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    raft_debugf((&f->logger), "hello");

    munit_assert_int(f->last.level, ==, RAFT_DEBUG);
    munit_assert_string_equal(f->last.message, "hello");

    free(f->last.message);

    /* Use the default logger */
    f->logger = raft_default_logger;
    raft_debugf((&f->logger), "hello");

    return MUNIT_OK;
}

/**
 * raft__infof
 */

/* Emit a message at info level, with arguments. */
static MunitResult test_infof(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    raft_infof((&f->logger), "hello %s", "world");

    munit_assert_int(f->last.level, ==, RAFT_INFO);
    munit_assert_string_equal(f->last.message, "hello world");

    free(f->last.message);

    /* Use the default logger */
    f->logger = raft_default_logger;
    raft_infof((&f->logger), "hello %s", "world");

    return MUNIT_OK;
}

/**
 * raft__warnf
 */

/* Emit a message at warn level, with arguments. */
static MunitResult test_warnf(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    raft_warnf((&f->logger), "hello %d", 123);

    munit_assert_int(f->last.level, ==, RAFT_WARN);
    munit_assert_string_equal(f->last.message, "hello 123");

    free(f->last.message);

    /* Use the default logger */
    f->logger = raft_default_logger;
    raft_warnf((&f->logger), "hello %d", 123);

    return MUNIT_OK;
}

/**
 * raft__errorf
 */

/* Emit a message at error level, with arguments. */
static MunitResult test_errorf(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    raft_errorf((&f->logger), "hello %d %s", 123, "world");

    munit_assert_int(f->last.level, ==, RAFT_ERROR);
    munit_assert_string_equal(f->last.message, "hello 123 world");

    free(f->last.message);

    /* Use the default logger */
    f->logger = raft_default_logger;
    raft_errorf((&f->logger), "hello %d %s", 123, "world");

    return MUNIT_OK;
}

/**
 * Default logger
 */

/* Emit a message at unknown level. */
static MunitResult test_unknown_level(const MunitParameter params[], void *data)
{
    va_list args;

    (void)data;
    (void)params;

    raft_default_logger.emit(NULL, 666, "hello", args);

    return MUNIT_OK;
}

/* The message is too long and gets truncated. */
static MunitResult test_too_long(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    char buf[2048];

    (void)params;

    memset(buf, 'a', sizeof buf - 1);
    buf[sizeof buf - 1] = 0;

    /* Use the default logger */
    f->logger = raft_default_logger;

    raft_errorf((&f->logger), buf);

    return MUNIT_OK;
}

static MunitTest macros_tests[] = {
    {"/debugf", test_debugf, setup, tear_down, 0, NULL},
    {"/infof", test_infof, setup, tear_down, 0, NULL},
    {"/warnf", test_warnf, setup, tear_down, 0, NULL},
    {"/errorf", test_errorf, setup, tear_down, 0, NULL},
    {"/unknown-level", test_unknown_level, setup, tear_down, 0, NULL},
    {"/too-long", test_too_long, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_logger_suites[] = {
    {"", macros_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
