#include <fcntl.h>
#include <unistd.h>

#include "../../src/uv_error.h"
#include "../../src/uv_file.h"
#include "../../src/uv_os.h"
#include "../lib/dir.h"
#include "../lib/loop.h"
#include "../lib/runner.h"

#define BLOCK_SIZE_ 4096
#define BUF_SIZE BLOCK_SIZE_
#define SEGMENT_SIZE (8 * 1024 * 1024)

/******************************************************************************
 *
 * Asynchronous writes with libuv
 *
 *****************************************************************************/

SUITE(uvWrite)

struct file
{
    FIXTURE_DIR;
    FIXTURE_LOOP;
    size_t block_size;
    struct uvFile file;
};

static void createCbAssertOk(struct uvFileCreate *req,
                             int status,
                             MUNIT_UNUSED const char *errmsg)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    *done = true;
}

static void *setupFile(MUNIT_UNUSED const MunitParameter params[],
                       MUNIT_UNUSED void *user_data)
{
    struct file *f = munit_malloc(sizeof *f);
    size_t direct_io;
    bool async_io;
    struct uvFileCreate req;
    bool done = false;
    int i;
    uvErrMsg errmsg;
    int rv;
    SETUP_DIR;
    if (f->dir == NULL) {
        free(f);
        return NULL;
    }
    SETUP_LOOP;

    /* Detect FS capabilities */
    rv = uvProbeIoCapabilities(f->dir, &direct_io, &async_io, errmsg);
    munit_assert_int(rv, ==, 0);
    rv = uvFileInit(&f->file, &f->loop, direct_io != 0, async_io, errmsg);
    munit_assert_int(rv, ==, 0);
    f->block_size = direct_io != 0 ? direct_io : 4096;

    /* Create an empty file */
    req.data = &done;
    rv = uvFileCreate(&f->file, &req, f->dir, "test", SEGMENT_SIZE, 1,
                      createCbAssertOk, errmsg);
    munit_assert_int(rv, ==, 0);
    for (i = 0; i < 2; i++) {
        LOOP_RUN(1);
        if (done) {
            break;
        }
    }
    munit_assert_true(done);

    return f;
}

static void tearDownFile(void *data)
{
    struct file *f = data;
    if (f == NULL) {
        return;
    }
    uvFileClose(&f->file, NULL);
    TEAR_DOWN_LOOP;
    TEAR_DOWN_DIR;
    free(f);
}

static char *appendN[] = {"1", "16", "256", "1024", NULL};

static char *dirFs[] = {"ext4", "btrfs", "xfs", "zfs", NULL};

static MunitParameterEnum appendParams[] = {
    {"n", appendN},
    {TEST_DIR_FS, dirFs},
    {NULL, NULL},
};

static void writeCbAssertOk(struct uvFileWrite *req,
                            int status,
                            MUNIT_UNUSED const char *errmsg)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    *done = true;
}

TEST(uvWrite, append, setupFile, tearDownFile, 0, appendParams)
{
    struct file *f = data;
    const char *n = munit_parameters_get(params, "n");
    struct uvFileWrite req;
    struct uv_buf_t buf;
    bool done = false;
    uvErrMsg errmsg;
    int rv;
    int i;
    int j;

    SKIP_IF_NO_FIXTURE;

    buf.len = BUF_SIZE;
    buf.base = aligned_alloc(BLOCK_SIZE_, buf.len);
    munit_assert_ptr_not_null(buf.base);

    for (i = 0; i < atoi(n); i++) {
        memset(buf.base, i, BUF_SIZE);
        req.data = &done;
        rv = uvFileWrite(&f->file, &req, &buf, 1, BUF_SIZE * i, writeCbAssertOk, errmsg);
        munit_assert_int(rv, ==, 0);
        for (j = 0; j < 2; j++) {
            LOOP_RUN(1);
            if (done) {
                break;
            }
        }
        munit_assert_true(done);
    }

    free(buf.base);

    return MUNIT_OK;
}
