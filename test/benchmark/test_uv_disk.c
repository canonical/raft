#include <fcntl.h>
#include <unistd.h>

#include "../../src/uv_error.h"
#include "../../src/uv_os.h"
#include "../lib/dir.h"
#include "../lib/runner.h"

#define BLOCK_SIZE_ 4096
#define SEGMENT_SIZE (8 * 1024 * 1024)

SUITE(synWrite)

TEST(synWrite, append, setupDir, tearDownDir, 0, dir_all_params)
{
    const char *dir = data;
    int fd;
    void *buf;
    uvErrMsg errmsg;
    int rv;
    int i;

    if (dir == NULL) {
        return MUNIT_SKIP;
    }

    rv = uvOpenFile(dir, "test", O_WRONLY | O_CREAT | O_EXCL, &fd, errmsg);
    munit_assert_int(rv, ==, 0);
    rv = posix_fallocate(fd, 0, SEGMENT_SIZE);
    munit_assert_int(rv, ==, 0);
    rv = fsync(fd);
    munit_assert_int(rv, ==, 0);

    buf = munit_malloc(BLOCK_SIZE_);

    for (i = 0; i < SEGMENT_SIZE / BLOCK_SIZE_; i++) {
        memset(buf, i, BLOCK_SIZE_);
        rv = uvWriteFully(fd, buf, BLOCK_SIZE_, errmsg);
        munit_assert_int(rv, ==, 0);
        rv = fdatasync(fd);
        munit_assert_int(rv, ==, 0);
    }

    free(buf);
    close(fd);
    return MUNIT_OK;
}
