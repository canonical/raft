#include <stdio.h>

#include "../../include/raft.h"

#include "../lib/runner.h"

TEST_MODULE(logger);

/**
 * Helpers
 */

struct fixture
{
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

TEST_SUITE(debugf);

static MunitTestSetup debugf__setup = setup;
static MunitTestTearDown debugf__tear_down = tear_down;

TEST_GROUP(debugf, success);

/* Emit a message at debug level. */
TEST_CASE(debugf, success, emit, NULL)
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

TEST_SUITE(infof);

static MunitTestSetup infof__setup = setup;
static MunitTestTearDown infof__tear_down = tear_down;

TEST_GROUP(infof, success);

/* Emit a message at info level, with arguments. */
TEST_CASE(infof, success, emit, NULL)
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

TEST_SUITE(warnf);

static MunitTestSetup warnf__setup = setup;
static MunitTestTearDown warnf__tear_down = tear_down;

TEST_GROUP(warnf, success);

/* Emit a message at warn level, with arguments. */
TEST_CASE(warnf, success, emit, NULL)
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

TEST_SUITE(errorf);

static MunitTestSetup errorf__setup = setup;
static MunitTestTearDown errorf__tear_down = tear_down;

TEST_GROUP(errorf, success);

/* Emit a message at error level, with arguments. */
TEST_CASE(errorf, success, emit, NULL)
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

TEST_SUITE(default);

static MunitTestSetup default__setup = setup;
static MunitTestTearDown default__tear_down = tear_down;

TEST_GROUP(default, success);

/* Emit a message at unknown level. */
TEST_CASE(default, success,_unknown_level, NULL)
{
    va_list args;

    (void)data;
    (void)params;

    raft_default_logger.emit(raft_default_logger.data, 666, "hello", args);

    return MUNIT_OK;
}

/* The message is too long and gets truncated. */
TEST_CASE(default, success, too_long, NULL)
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
;
