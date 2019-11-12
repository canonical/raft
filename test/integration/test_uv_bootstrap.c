#include "../lib/runner.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture with a libuv-based raft_io instance and an empty configuration.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV;
    struct raft_configuration conf;
};

static void *setupUv(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV;
    raft_configuration_init(&f->conf);
    return f;
}

static void tearDownUv(void *data)
{
    struct fixture *f = data;
    raft_configuration_close(&f->conf);
    TEAR_DOWN_UV;
    free(f);
}

/******************************************************************************
 *
 * raft_io->bootstrap()
 *
 *****************************************************************************/

SUITE(bootstrap)

/* Add a server to the fixture's configuration. */
#define CONFIGURATION_ADD(ID, ADDRESS)                             \
    {                                                              \
        int rv_;                                                   \
        rv_ = raft_configuration_add(&f->conf, ID, ADDRESS, true); \
        munit_assert_int(rv_, ==, 0);                              \
    }

/* Invoke f->io->bootstrap() and assert that no error occurs. */
#define BOOTSTRAP                                \
    {                                            \
        int rv_;                                 \
        rv_ = f->io.bootstrap(&f->io, &f->conf); \
        munit_assert_int(rv_, ==, 0);            \
    }

/* Invoke f->io->bootstrap() and assert that it returns the given error code and
 * message. */
#define BOOTSTRAP_ERROR(RV, ERRMSG)                               \
    {                                                             \
        int rv_;                                                  \
        rv_ = f->io.bootstrap(&f->io, &f->conf);                  \
        munit_assert_int(rv_, ==, RV);                            \
        munit_assert_string_equal(f->io.errmsg_(&f->io), ERRMSG); \
    }

/* The data directory already has metadata files with a non-zero term. */
TEST(bootstrap, termIsNonZero, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    CONFIGURATION_ADD(1, "1");
    BOOTSTRAP;
    BOOTSTRAP_ERROR(RAFT_CANTBOOTSTRAP, "metadata contain term 1");
    return MUNIT_OK;
}
