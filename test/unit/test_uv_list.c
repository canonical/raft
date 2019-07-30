#include "../lib/dir.h"
#include "../lib/heap.h"
#include "../lib/uv.h"
#include "../lib/runner.h"

#include "../../src/byte.h"
#include "../../src/uv_encoding.h"

#define WORD_SIZE sizeof(uint64_t)

TEST_MODULE(io_uv_load);

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
    SETUP_UV;
    f->snapshots = NULL;
    f->segments = NULL;
    return f;
};

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
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

#define LIST(RV)						\
    {                                                                    \
        int rv;                                                          \
        rv = uvList(f->uv, &f->snapshots, &f->n_snapshots, &f->segments, \
                    &f->n_segments);                                     \
        munit_assert_int(rv, ==, RV);                                    \
    }

#define ASSERT_SNAPSHOT(I, TERM, INDEX, TIMESTAMP) \
    munit_assert_int(f->snapshots[I].term, ==, TERM);    \
    munit_assert_int(f->snapshots[I].index, ==, INDEX);  \
    munit_assert_int(f->snapshots[I].timestamp, ==, TIMESTAMP);

/******************************************************************************
 *
 * Success scenarios.
 *
 *****************************************************************************/

TEST_SUITE(success);
TEST_SETUP(success, setup);
TEST_TEAR_DOWN(success, tear_down);

/* There are no snapshot metadata files */
TEST_CASE(success, no_snapshots, NULL)
{
    struct fixture *f = data;

    (void)params;

    LIST(0);

    munit_assert_ptr_null(f->snapshots);
    munit_assert_int(f->n_snapshots, ==, 0);

    return MUNIT_OK;
}

/* There is a single snapshot metadata file */
TEST_CASE(success, one_snapshot, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8];

    (void)params;

    test_io_uv_write_snapshot_meta_file(f->dir, 1, 8, 123, 1, 1);
    test_io_uv_write_snapshot_data_file(f->dir, 1, 8, 123, buf, sizeof buf);

    LIST(0);

    munit_assert_ptr_not_null(f->snapshots);
    munit_assert_int(f->n_snapshots, ==, 1);

    ASSERT_SNAPSHOT(0, 1, 8, 123);

    return MUNIT_OK;
    return 0;
}

TEST_CASE(success, many_snapshots, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8];

    (void)params;

    test_io_uv_write_snapshot_meta_file(f->dir, 1, 8, 123, 1, 1);

    test_io_uv_write_snapshot_meta_file(f->dir, 1, 8, 456, 1, 1);
    test_io_uv_write_snapshot_data_file(f->dir, 1, 8, 456, buf, sizeof buf);

    test_io_uv_write_snapshot_meta_file(f->dir, 2, 6, 789, 2, 3);
    test_io_uv_write_snapshot_data_file(f->dir, 2, 6, 789, buf, sizeof buf);

    test_io_uv_write_snapshot_meta_file(f->dir, 2, 9, 999, 2, 3);
    test_io_uv_write_snapshot_data_file(f->dir, 2, 9, 999, buf, sizeof buf);

    LIST(0);

    munit_assert_ptr_not_null(f->snapshots);
    munit_assert_int(f->n_snapshots, ==, 3);

    ASSERT_SNAPSHOT(0, 1, 8, 456);
    ASSERT_SNAPSHOT(1, 2, 6, 789);
    ASSERT_SNAPSHOT(2, 2, 9, 999);

    return MUNIT_OK;
}
