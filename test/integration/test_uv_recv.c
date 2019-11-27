#include "../lib/runner.h"
#include "../lib/tcp.h"
#include "../lib/uv_.h"

/******************************************************************************
 *
 * Fixture with a libuv-based raft_io instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_TCP;
    FIXTURE_UV;
    bool closed;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setUpDeps(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    SETUP_TCP;
    TCP_SERVER_LISTEN;
    f->io.data = f;
    f->closed = false;
    return f;
}

static void tearDownDeps(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_TCP;
    TEAR_DOWN_UV_DEPS;
    free(f);
}

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = setUpDeps(params, user_data);
    SETUP_UV;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
    tearDownDeps(f);
}

/******************************************************************************
 *
 * raft_io_recv_cb
 *
 *****************************************************************************/

SUITE(recv)

TEST(recv, first, setUp, tearDown, 0, NULL)
{
    return MUNIT_OK;
}
