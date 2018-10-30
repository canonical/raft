#include "../../include/raft.h"

#include "../../src/context.h"

#include "../lib/munit.h"

/**
 *
 * Setup and tear down
 *
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct raft_context *ctx = munit_calloc(1, sizeof *ctx);

    (void)user_data;
    (void)params;

    return ctx;
}

static void tear_down(void *data)
{
    struct raft_context *ctx = data;
    free(ctx);
}

/**
 *
 * raft_context_format
 *
 */

/* Format an empty context. */
static MunitResult test_format_empty(const MunitParameter params[], void *data)
{
    struct raft_context *ctx = data;
    char buf[8];

    (void)params;

    raft_context_format(buf, sizeof buf, ctx);
    munit_assert_string_equal("()", buf);

    return MUNIT_OK;
}

/* Format server state code. */
static MunitResult test_format_state(const MunitParameter params[], void *data)
{
    struct raft_context *ctx = data;
    unsigned short state = RAFT_STATE_CANDIDATE;
    char buf[24];

    (void)params;

    ctx->state = &state;

    raft_context_format(buf, sizeof buf, ctx);
    munit_assert_string_equal("(state=candidate)", buf);

    return MUNIT_OK;
}

/* Format server state code and current term. */
static MunitResult test_format_state_and_term(const MunitParameter params[],
                                              void *data)
{
    struct raft_context *ctx = data;
    unsigned short state = RAFT_STATE_CANDIDATE;
    raft_term term = 2;
    char buf[64];

    (void)params;

    ctx->state = &state;
    ctx->current_term = &term;

    raft_context_format(buf, sizeof buf, ctx);
    munit_assert_string_equal("(state=candidate current-term=2)", buf);

    return MUNIT_OK;
}

/* The output string is truncated if it exceeds the given size. */
static MunitResult test_format_too_long(const MunitParameter params[],
                                        void *data)
{
    struct raft_context *ctx = data;
    unsigned short state = RAFT_STATE_CANDIDATE;
    raft_term term = 2;
    char buf[24];

    (void)params;

    ctx->state = &state;
    ctx->current_term = &term;

    raft_context_format(buf, sizeof buf, ctx);
    munit_assert_string_equal("(state=candidate curren", buf);

    return MUNIT_OK;
}

static MunitTest format_tests[] = {
    {"/empty", test_format_empty, setup, tear_down, 0, NULL},
    {"/state", test_format_state, setup, tear_down, 0, NULL},
    {"/state-and-term", test_format_state_and_term, setup, tear_down, 0, NULL},
    {"/too-long", test_format_too_long, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 *
 * raft_context__wrapf
 *
 */

/* Wrap an error message. */
static MunitResult test_wrapf(const MunitParameter params[], void *data)
{
    struct raft_context *ctx = data;

    (void)params;

    raft_context__errorf(ctx, "boom %d", 123);
    raft_context__wrapf(ctx, "major failure %s", "now");

    munit_assert_string_equal("major failure now: boom 123", ctx->errmsg);

    return MUNIT_OK;
}

static MunitTest wrapf_tests[] = {
    {"", test_wrapf, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 *
 * Test suite
 *
 */

MunitSuite raft_context_suites[] = {
    {"/format", format_tests, NULL, 1, 0},
    {"/wrapf", wrapf_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
