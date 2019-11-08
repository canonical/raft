#include "../lib/runner.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture with a pristine libuv-based raft_io instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV;
};

static void *setupUv(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV;
    return f;
}

static void tearDownUv(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
    free(f);
}

/******************************************************************************
 *
 * raft_io->load()
 *
 *****************************************************************************/

/* Invoke f->io->load() and assert that it returns the given error code and
 * message. */
#define LOAD_ERROR(RV, ERRMSG)                                        \
    do {                                                              \
        int _rv;                                                      \
        raft_term _term;                                              \
        unsigned _voted_for;                                          \
        struct raft_snapshot *_snapshot;                              \
        raft_index _start_index;                                      \
        struct raft_entry *_entries;                                  \
        size_t _n;                                                    \
        _rv = f->io.load(&f->io, 10, &_term, &_voted_for, &_snapshot, \
                         &_start_index, &_entries, &_n);              \
        munit_assert_int(_rv, ==, RV);                                \
        munit_assert_string_equal(f->io.errmsg(&f->io), ERRMSG);      \
    } while (0)

/* Invoke f->io->load() and assert that it returns the given state. */
#define LOAD(TERM, VOTED_FOR, SNAPSHOT, START_INDEX, ENTRIES, N_ENTRIES) \
    do {                                                                 \
        int _rv;                                                         \
        raft_term _term;                                                 \
        unsigned _voted_for;                                             \
        struct raft_snapshot *_snapshot;                                 \
        raft_index _start_index;                                         \
        struct raft_entry *_entries;                                     \
        size_t _n;                                                       \
        _rv = f->io.load(&f->io, 10, &_term, &_voted_for, &_snapshot,    \
                         &_start_index, &_entries, &_n);                 \
        munit_assert_int(_rv, ==, 0);                                    \
        munit_assert_int(_term, ==, TERM);                               \
    } while (0)

SUITE(load)

/* Data directory not accessible */
TEST(load, emptyDir, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    LOAD(0, 0, NULL, 0, NULL, 0);
    return MUNIT_OK;
}

/* Data directory not accessible */
TEST(load, dirNotAccessible, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    test_dir_unexecutable(f->dir);
    LOAD_ERROR(RAFT_IOERR,
               "check if metadata1 exists: stat: permission denied");
    return MUNIT_OK;
}
