#include "../lib/dir.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

#include "../../src/byte.h"
#include "../../src/uv_encoding.h"

#define WORD_SIZE sizeof(uint64_t)

TEST_MODULE(uv_list)

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV;
    struct uvSnapshotInfo *snapshots;
    size_t n_snapshots;
    struct uvSegmentInfo *segments;
    size_t n_segments;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_UV;
    f->uv->tracer = &f->tracer;
    f->snapshots = NULL;
    f->segments = NULL;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    if (f->snapshots != NULL) {
        raft_free(f->snapshots);
    }
    if (f->segments != NULL) {
        raft_free(f->segments);
    }
    TEAR_DOWN_UV;
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Invoke uvList. */
#define LIST_RV \
    uvList(f->uv, &f->snapshots, &f->n_snapshots, &f->segments, &f->n_segments)
#define LIST munit_assert_int(LIST_RV, ==, 0);
#define LIST_ERROR(RV) munit_assert_int(LIST_RV, ==, RV);

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

#define ASSERT_SNAPSHOT(I, TERM, INDEX, TIMESTAMP)      \
    munit_assert_int(f->snapshots[I].term, ==, TERM);   \
    munit_assert_int(f->snapshots[I].index, ==, INDEX); \
    munit_assert_int(f->snapshots[I].timestamp, ==, TIMESTAMP);

/******************************************************************************
 *
 * Data directory has only open or closed segments.
 *
 *****************************************************************************/

TEST_SUITE(segments)
TEST_SETUP(segments, setup)
TEST_TEAR_DOWN(segments, tear_down)

/* Data directory is empty. */
TEST_CASE(segments, empty, NULL)
{
    struct fixture *f = data;
    (void)params;
    LIST;
    munit_assert_ptr_null(f->snapshots);
    munit_assert_int(f->n_snapshots, ==, 0);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * Data directory has one or more snapshots.
 *
 *****************************************************************************/

TEST_SUITE(snapshots)
TEST_SETUP(snapshots, setup)
TEST_TEAR_DOWN(snapshots, tear_down)

/* There is a single snapshot. */
TEST_CASE(snapshots, one, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8];
    (void)params;
    UV_WRITE_SNAPSHOT(f->dir, 1 /* term */, 8 /* index */, 123 /* timestamp */,
                      1 /* n servers */, 1 /* conf index */, buf /* data */,
                      sizeof buf);
    LIST;
    ASSERT_SNAPSHOT(0, 1, 8, 123);
    return MUNIT_OK;
}

/* There are several snapshots, including an incomplete one. */
TEST_CASE(snapshots, many, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8];
    (void)params;
    UV_WRITE_SNAPSHOT_META(f->dir, 1 /* term */, 8 /* index */,
                           123 /* timestamp */, 1 /* n servers */,
                           1 /* index */);

    UV_WRITE_SNAPSHOT(f->dir, 1 /* term */, 8 /* index */, 456 /* timestamp */,
                      1 /* n servers */, 1 /* conf index */, buf /* data */,
                      sizeof buf);

    UV_WRITE_SNAPSHOT(f->dir, 2 /* term */, 6 /* index */, 789 /* timestamp */,
                      2 /* n servers */, 3 /* conf index */, buf /* data */,
                      sizeof buf);

    UV_WRITE_SNAPSHOT(f->dir, 2 /* term */, 9 /* index */, 999 /* timestamp */,
                      2 /* n servers */, 3 /* conf index */, buf /* data */,
                      sizeof buf);

    LIST;

    munit_assert_ptr_not_null(f->snapshots);
    munit_assert_int(f->n_snapshots, ==, 3);

    ASSERT_SNAPSHOT(0, 1, 8, 456);
    ASSERT_SNAPSHOT(1, 2, 6, 789);
    ASSERT_SNAPSHOT(2, 2, 9, 999);

    return MUNIT_OK;
}
