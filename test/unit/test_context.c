#include "../../include/raft.h"

#include "../lib/munit.h"

/**
 * Setup and tear down
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
 * raft_context_format
 */

/* The given buffer size is 0. */
static MunitResult test_format_zero_size(const MunitParameter params[],
                                         void *data)
{
    struct raft_context *ctx = data;
    char buf[1];

    (void)params;

    raft_context_format(buf, 0, ctx);

    return MUNIT_OK;
}

/* The given buffer is a NULL pointer. */
static MunitResult test_format_null_buffer(const MunitParameter params[],
                                           void *data)
{
    struct raft_context *ctx = data;

    (void)params;

    raft_context_format(NULL, 1, ctx);

    return MUNIT_OK;
}

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

static char *format_too_long_size[] = {"1", "2", "24", NULL};

static MunitParameterEnum format_too_long_params[] = {
    {"size", format_too_long_size},
    {NULL, NULL},
};

/* The output string is truncated if it exceeds the given size. */
static MunitResult test_format_too_long(const MunitParameter params[],
                                        void *data)
{
    struct raft_context *ctx = data;
    unsigned short state = RAFT_STATE_CANDIDATE;
    raft_term term = 2;
    const char *size = munit_parameters_get(params, "size");
    size_t n = atoi(size);
    char *buf = munit_malloc(n);

    ctx->state = &state;
    ctx->current_term = &term;

    raft_context_format(buf, n, ctx);

    switch (n) {
    case 1:
      munit_assert_string_equal("", buf);
      break;
    case 2:
      munit_assert_string_equal("(", buf);
      break;
    case 24:
      munit_assert_string_equal("(state=candidate curren", buf);
      break;
    }

    free(buf);

    return MUNIT_OK;
}

static MunitTest format_tests[] = {
    {"/zero-size", test_format_zero_size, setup, tear_down, 0, NULL},
    {"/null-buffer", test_format_null_buffer, setup, tear_down, 0, NULL},
    {"/empty", test_format_empty, setup, tear_down, 0, NULL},
    {"/state", test_format_state, setup, tear_down, 0, NULL},
    {"/state-and-term", test_format_state_and_term, setup, tear_down, 0, NULL},
    {"/too-long", test_format_too_long, setup, tear_down, 0,
     format_too_long_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_context_suites[] = {
    {"/format", format_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
