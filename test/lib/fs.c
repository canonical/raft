#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "fs.h"

#define TEST_DIR_TEMPLATE "./tmp/%s/raft-test-XXXXXX"

char *test_dir_setup(const MunitParameter params[])
{
    const char *fs_type = munit_parameters_get(params, TEST_DIR_FS_TYPE);
    char *dir;

    if (fs_type == NULL) {
        fs_type = "tmpfs";
    }

    dir = munit_malloc(strlen(TEST_DIR_TEMPLATE) + strlen(fs_type) + 1);

    sprintf(dir, TEST_DIR_TEMPLATE, fs_type);

    if (mkdtemp(dir) == NULL) {
        munit_error(strerror(errno));
    }

    return dir;
}

static int test_dir__remove(const char *path,
                            const struct stat *sbuf,
                            int type,
                            struct FTW *ftwb)
{
    (void)sbuf;
    (void)type;
    (void)ftwb;

    return remove(path);
}

void test_dir_tear_down(char *dir)
{
    int rv;

    rv = nftw(dir, test_dir__remove, 10, FTW_DEPTH | FTW_MOUNT | FTW_PHYS);
    munit_assert_int(rv, ==, 0);

    free(dir);
}

void test_dir_write_file(char *dir, const char *filename, void *buf, size_t n)
{
    char path[256];
    int fd;
    int rv;

    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);

    fd = open(path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

    munit_assert_int(fd, !=, -1);

    rv = write(fd, buf, n);
    munit_assert_int(rv, ==, n);

    close(fd);
}

void test_dir_read_file(char *dir, const char *filename, void *buf, size_t n)
{
    char path[256];
    int fd;
    int rv;

    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);

    fd = open(path, O_RDONLY);
    if (fd == -1) {
        munit_logf(MUNIT_LOG_ERROR, "read file '%s': %s", path,
                   strerror(errno));
    }

    rv = read(fd, buf, n);
    munit_assert_int(rv, ==, n);

    close(fd);
}
