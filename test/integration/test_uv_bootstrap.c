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

/* Invoke f->io->bootstrap() and assert that it returns the given error code and
 * message. */
#define BOOTSTRAP_ERROR(RV, ERRMSG)                              \
    {                                                            \
        int rv_;                                                 \
        rv_ = f->io.bootstrap(&f->io, &f->conf);                 \
        munit_assert_int(rv_, ==, RV);                           \
        munit_assert_string_equal(f->io.errmsg(&f->io), ERRMSG); \
    }

/* The data directory does not exist. */
TEST(bootstrap, dirDoesNotExist, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    char errmsg[128];
    test_dir_remove(f->dir);
    sprintf(errmsg, "check data dir %s: stat: no such file or directory", f->dir);
    BOOTSTRAP_ERROR(RAFT_IOERR, errmsg);
    return MUNIT_OK;
}
