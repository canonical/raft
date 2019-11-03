#include <errno.h>
#include <stdio.h>

#include "../../src/err.h"
#include "../lib/heap.h"
#include "../lib/runner.h"

/* An error messages which is 249 characters. */
#define LONG_ERRMSG                                                          \
    "boom boom boom boom boom boom boom boom boom boom boom boom boom boom " \
    "boom boom boom boom boom boom boom boom boom boom boom boom boom boom " \
    "boom boom boom boom boom boom boom boom boom boom boom boom boom boom " \
    "boom boom boom boom boom boom boom boom"

/******************************************************************************
 *
 * ErrMsgPrintf
 *
 *****************************************************************************/

SUITE(ErrMsgPrintf)

/* The format string has no parameters. */
TEST(ErrMsgPrintf, noParams, NULL, NULL, 0, NULL)
{
    struct ErrMsg errmsg;
    ErrMsgPrintf(&errmsg, "boom");
    munit_assert_string_equal(ErrMsgString(&errmsg), "boom");
    return MUNIT_OK;
}

/* The format string has parameters. */
TEST(ErrMsgPrintf, params, NULL, NULL, 0, NULL)
{
    struct ErrMsg errmsg;
    ErrMsgPrintf(&errmsg, "boom %d", 123);
    munit_assert_string_equal(ErrMsgString(&errmsg), "boom 123");
    return MUNIT_OK;
}

/* The resulting message gets truncated. */
TEST(ErrMsgPrintf, truncate, NULL, NULL, 0, NULL)
{
    struct ErrMsg errmsg;
    ErrMsgPrintf(&errmsg, LONG_ERRMSG " %d", 123456);
    munit_assert_string_equal(ErrMsgString(&errmsg), LONG_ERRMSG " 12345");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * ErrMsgWrapf
 *
 *****************************************************************************/

SUITE(ErrMsgWrapf)

/* The wrapping format string has no parameters. */
TEST(ErrMsgWrapf, noParams, NULL, NULL, 0, NULL)
{
    struct ErrMsg errmsg;
    ErrMsgPrintf(&errmsg, "boom");
    ErrMsgWrapf(&errmsg, "no luck");
    munit_assert_string_equal(ErrMsgString(&errmsg), "no luck: boom");
    return MUNIT_OK;
}

/* The wrapping format string has parameters. */
TEST(ErrMsgWrapf, params, NULL, NULL, 0, NULL)
{
    struct ErrMsg errmsg;
    ErrMsgPrintf(&errmsg, "boom");
    ErrMsgWrapf(&errmsg, "no luck, %s", "joe");
    munit_assert_string_equal(ErrMsgString(&errmsg), "no luck, joe: boom");
    return MUNIT_OK;
}

/* The wrapped error message gets partially truncated. */
TEST(ErrMsgWrapf, partialTruncate, NULL, NULL, 0, NULL)
{
    struct ErrMsg errmsg;
    ErrMsgPrintf(&errmsg, "no luck");
    ErrMsgWrapf(&errmsg, LONG_ERRMSG);
    munit_assert_string_equal(ErrMsgString(&errmsg), LONG_ERRMSG ": no l");
    return MUNIT_OK;
}

/* The wrapped error message gets entirelly truncated. */
TEST(ErrMsgWrapf, fullTruncate, NULL, NULL, 0, NULL)
{
    struct ErrMsg errmsg;
    ErrMsgPrintf(&errmsg, "no luck");
    ErrMsgWrapf(&errmsg, LONG_ERRMSG " boom");
    munit_assert_string_equal(ErrMsgString(&errmsg), LONG_ERRMSG " boom");
    return MUNIT_OK;
}

static void *setupHeap(const MunitParameter params[],
                       MUNIT_UNUSED void *user_data)
{
    struct raft_heap *heap = munit_malloc(sizeof *heap);
    test_heap_setup(params, heap);
    test_heap_fault_enable(heap);
    return heap;
}

static void tearDownHeap(void *data)
{
    struct raft_heap *heap = data;
    test_heap_tear_down(heap);
    free(heap);
}

/******************************************************************************
 *
 * errMsgPrintf
 *
 *****************************************************************************/

SUITE(errMsgPrintf)

/* The format string has no parameters. */
TEST(errMsgPrintf, noParams, setupHeap, tearDownHeap, 0, NULL)
{
    char *errmsg = errMsgPrintf("boom");
    munit_assert_string_equal(errmsg, "boom");
    raft_free(errmsg);
    return MUNIT_OK;
}

/* The format string has parameters. */
TEST(errMsgPrintf, params, setupHeap, tearDownHeap, 0, NULL)
{
    char *errmsg = errMsgPrintf("boom %d", 123);
    munit_assert_string_equal(errmsg, "boom 123");
    raft_free(errmsg);
    return MUNIT_OK;
}

/* The format string is invalid. */
TEST(errMsgPrintf, badFormat, NULL, NULL, 0, NULL)
{
    char *errmsg = errMsgPrintf("%", 3);
    munit_assert_null(errmsg);
    return MUNIT_OK;
}

/* Out of memory. */
TEST(errMsgPrintf, oom, setupHeap, tearDownHeap, 0, NULL)
{
    struct raft_heap *heap = data;
    char *errmsg;
    test_heap_fault_config(heap, 0, 1);
    errmsg = errMsgPrintf("%d", 3);
    munit_assert_null(errmsg);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * errMsgWrapf
 *
 *****************************************************************************/

SUITE(errMsgWrapf)

/* The wrapping format string has no parameters. */
TEST(errMsgWrapf, noParams, NULL, NULL, 0, NULL)
{
    char *errmsg = errMsgPrintf("boom");
    errmsg = errMsgWrapf(errmsg, "no luck");
    munit_assert_string_equal(errmsg, "no luck: boom");
    raft_free(errmsg);
    return MUNIT_OK;
}

/* The wrapping format string has parameters. */
TEST(errMsgWrapf, params, NULL, NULL, 0, NULL)
{
    char *errmsg = errMsgPrintf("boom");
    errmsg = errMsgWrapf(errmsg, "no luck, %s", "joe");
    munit_assert_string_equal(errmsg, "no luck, joe: boom");
    raft_free(errmsg);
    return MUNIT_OK;
}

static char *errMsgWrapfOomDelay[] = {"1", "2", NULL};
static char *errMsgWrapfOomRepeat[] = {"1", NULL};

static MunitParameterEnum errMsgWrapfOomParams[] = {
    {TEST_HEAP_FAULT_DELAY, errMsgWrapfOomDelay},
    {TEST_HEAP_FAULT_REPEAT, errMsgWrapfOomRepeat},
    {NULL, NULL},
};

/* Out of memory. */
TEST(errMsgWrapf, oom, setupHeap, tearDownHeap, 0, errMsgWrapfOomParams)
{
    char *errmsg = errMsgPrintf("boom");
    errmsg = errMsgWrapf(errmsg, "no luck");
    munit_assert_string_equal(errmsg, "boom");
    raft_free(errmsg);
    return MUNIT_OK;
}

/* The format string is invalid. */
TEST(errMsgWrapf, badFormat, NULL, NULL, 0, NULL)
{
    char *errmsg = errMsgPrintf("boom");
    errmsg = errMsgWrapf(errmsg, "%", 123);
    munit_assert_string_equal(errmsg, "boom");
    raft_free(errmsg);
    return MUNIT_OK;
}
