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
