/* Benchmark operating system write performance using various APIs. */

#include <fcntl.h>
#include <stdio.h>
#include <sys/uio.h>
#include <unistd.h>

#include "../lib/dir.h"
#include "../lib/runner.h"

#define BLOCK_SIZE_ 4096
#define FILE_SIZE (8 * 1024 * 1024)

/* Fixture with a pre-allocated open file. */
struct file
{
    FIXTURE_DIR;
    int fd;
};

static void *setupFile(MUNIT_UNUSED const MunitParameter params[],
                       MUNIT_UNUSED void *user_data)
{
    struct file *f = munit_malloc(sizeof *f);
    char path[1024];
    int flags = O_WRONLY | O_CREAT;
    int rv;
    SETUP_DIR;
    if (f->dir == NULL) {
        free(f);
        return NULL;
    }
    sprintf(path, "%s/x", f->dir);
    f->fd = open(path, flags, S_IRUSR | S_IWUSR);
    munit_assert_int(f->fd, >=, 0);
    rv = posix_fallocate(f->fd, 0, FILE_SIZE);
    munit_assert_int(rv, ==, 0);
    rv = fsync(f->fd);
    munit_assert_int(rv, ==, 0);
    return f;
}

static void tearDownFile(void *data)
{
    struct file *f = data;
    if (f == NULL) {
        return;
    }
    close(f->fd);
    TEAR_DOWN_DIR;
    free(f);
}

/* Number of writes each test should peform. Each write should write a new chuck
 * of bytes to the pre-allocated file, increasing the write offset
 * accordingly. */
#define N_WRITES "n-writes"

/* Size of the write buffer. */
#define BUF_SIZE "buf-size"

static char *nWrites[] = {"1", "16", "256", "1024", NULL};

static char *bufSize[] = {"64", "128", "256", "512", "1024", "2048", "4096", NULL};

static char *dirFs[] = {"ext4", "btrfs", "xfs", "zfs", NULL};

SUITE(write)

/******************************************************************************
 *
 * Synchronous writes with direct I/O.
 *
 *****************************************************************************/

#define SET_DIRECT_IO                                   \
    {                                                   \
        int flags_;                                     \
        int rv_;                                        \
        flags_ = fcntl(f->fd, F_GETFL);                 \
        rv_ = fcntl(f->fd, F_SETFL, flags_ | O_DIRECT); \
        munit_assert_int(rv_, ==, 0);                   \
    }

static MunitParameterEnum writeSyncDirectParams[] = {
    {N_WRITES, nWrites},
    {TEST_DIR_FS, dirFs},
    {NULL, NULL},
};

TEST(write, syncDirect, setupFile, tearDownFile, 0, writeSyncDirectParams)
{
#if defined(RWF_DSYNC) && defined(RWF_HIPRI)
    struct file *f = data;
    const char *n = munit_parameters_get(params, N_WRITES);
    struct iovec iov;
    int rv;
    int i;

    SKIP_IF_NO_FIXTURE;
    SET_DIRECT_IO;

    iov.iov_len = BLOCK_SIZE_;
    iov.iov_base = aligned_alloc(BLOCK_SIZE_, iov.iov_len);
    munit_assert_ptr_not_null(iov.iov_base);

    for (i = 0; i < atoi(n); i++) {
        memset(iov.iov_base, i, iov.iov_len);
        rv = pwritev2(f->fd, &iov, 1, i * iov.iov_len, RWF_DSYNC | RWF_HIPRI);
        munit_assert_int(rv, ==, iov.iov_len);
    }

    free(iov.iov_base);

    return MUNIT_OK;
#else
    return MUNIT_SKIP;
#endif
}

/******************************************************************************
 *
 * Synchronous writes with buffered I/O.
 *
 *****************************************************************************/

static MunitParameterEnum syncBufferedParams[] = {
    {N_WRITES, nWrites},
    {BUF_SIZE, bufSize},
    {TEST_DIR_FS, dirFs},
    {NULL, NULL},
};

TEST(write, syncBuffered, setupFile, tearDownFile, 0, syncBufferedParams)
{
    struct file *f = data;
    const char *n = munit_parameters_get(params, N_WRITES);
    const char *size = munit_parameters_get(params, BUF_SIZE);
    struct iovec iov;
    int rv;
    int i;

    SKIP_IF_NO_FIXTURE;

    iov.iov_len = atoi(size);
    iov.iov_base = munit_malloc(iov.iov_len);

    for (i = 0; i < atoi(n); i++) {
        memset(iov.iov_base, i, iov.iov_len);
#if defined(RWF_DSYNC) && defined(RWF_HIPRI)
        rv = pwritev2(f->fd, &iov, 1, i * iov.iov_len, RWF_DSYNC | RWF_HIPRI);
        munit_assert_int(rv, ==, iov.iov_len);
#else
        rv = pwritev(f->fd, &iov, 1, i * iov.iov_len);
        munit_assert_int(rv, ==, iov.iov_len);
        rv = fdatasync(f->fd);
        munit_assert_int(rv, ==, 0);
#endif
    }

    free(iov.iov_base);

    return MUNIT_OK;
}
