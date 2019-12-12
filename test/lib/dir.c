#include "dir.h"

#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#define SEP "/"
#define TEMPLATE "raft-test-XXXXXX"

#define TEST_DIR_TEMPLATE "./tmp/%s/raft-test-XXXXXX"

char *test_dir_all[] = {"tmpfs", "ext4", "btrfs", "xfs", "zfs", NULL};

char *test_dir_tmpfs[] = {"tmpfs", NULL};

char *test_dir_btrfs[] = {"btrfs", NULL};

char *test_dir_zfs[] = {"zfs", NULL};

char *test_dir_aio[] = {"btrfs", "ext4", "xfs", NULL};

char *test_dir_no_aio[] = {"tmpfs", "zfs", NULL};

MunitParameterEnum dir_tmpfs_params[] = {
    {DIR_FS_PARAM, test_dir_tmpfs},
    {NULL, NULL},
};

MunitParameterEnum dir_btrfs_params[] = {
    {DIR_FS_PARAM, test_dir_btrfs},
    {NULL, NULL},
};

MunitParameterEnum dir_zfs_params[] = {
    {DIR_FS_PARAM, test_dir_zfs},
    {NULL, NULL},
};

MunitParameterEnum dir_all_params[] = {
    {DIR_FS_PARAM, test_dir_all},
    {NULL, NULL},
};

MunitParameterEnum dir_aio_params[] = {
    {DIR_FS_PARAM, test_dir_aio},
    {NULL, NULL},
};

MunitParameterEnum dir_no_aio_params[] = {
    {DIR_FS_PARAM, test_dir_no_aio},
    {NULL, NULL},
};

/* Create a temporary directory in the given parent directory. */
static char *mkTempDir(const char *parent)
{
    char *dir;
    if (parent == NULL) {
        return NULL;
    }
    dir = munit_malloc(strlen(parent) + strlen(SEP) + strlen(TEMPLATE) + 1);
    sprintf(dir, "%s%s%s", parent, SEP, TEMPLATE);
    if (mkdtemp(dir) == NULL) {
        munit_error(strerror(errno));
    }
    return dir;
}

void *setUpDir(MUNIT_UNUSED const MunitParameter params[],
               MUNIT_UNUSED void *user_data)
{
    const char *fs = munit_parameters_get(params, DIR_FS_PARAM);
    if (fs == NULL) {
        return mkTempDir("/tmp");
    } else if (strcmp(fs, "tmpfs") == 0) {
        return setUpTmpfsDir(params, user_data);
    } else if (strcmp(fs, "ext4") == 0) {
        return setUpExt4Dir(params, user_data);
    } else if (strcmp(fs, "btrfs") == 0) {
        return setUpBtrfsDir(params, user_data);
    } else if (strcmp(fs, "zfs") == 0) {
        return setUpZfsDir(params, user_data);
    } else if (strcmp(fs, "xfs") == 0) {
        return setUpXfsDir(params, user_data);
    }
    munit_errorf("Unsupported file system %s", fs);
    return NULL;
}

void *setUpTmpfsDir(MUNIT_UNUSED const MunitParameter params[],
                    MUNIT_UNUSED void *user_data)
{
    return mkTempDir(getenv("RAFT_TMP_TMPFS"));
}

void *setUpExt4Dir(MUNIT_UNUSED const MunitParameter params[],
                   MUNIT_UNUSED void *user_data)
{
    return mkTempDir(getenv("RAFT_TMP_EXT4"));
}

void *setUpBtrfsDir(MUNIT_UNUSED const MunitParameter params[],
                    MUNIT_UNUSED void *user_data)
{
    return mkTempDir(getenv("RAFT_TMP_BTRFS"));
}

void *setUpZfsDir(MUNIT_UNUSED const MunitParameter params[],
                  MUNIT_UNUSED void *user_data)
{
    return mkTempDir(getenv("RAFT_TMP_ZFS"));
}

void *setUpXfsDir(MUNIT_UNUSED const MunitParameter params[],
                  MUNIT_UNUSED void *user_data)
{
    return mkTempDir(getenv("RAFT_TMP_XFS"));
}

/* Wrapper around remove(), compatible with ntfw. */
static int removeFn(const char *path,
                    MUNIT_UNUSED const struct stat *sbuf,
                    MUNIT_UNUSED int type,
                    MUNIT_UNUSED struct FTW *ftwb)
{
    return remove(path);
}

void tearDownDir(void *data)
{
    char *dir = data;
    if (dir == NULL) {
        return;
    }
    if (test_dir_exists(dir)) {
        test_dir_remove(dir);
    }
    free(dir);
}

void test_dir_remove(char *dir)
{
    int rv;
    rv = chmod(dir, 0755);
    munit_assert_int(rv, ==, 0);

    rv = nftw(dir, removeFn, 10, FTW_DEPTH | FTW_MOUNT | FTW_PHYS);
    munit_assert_int(rv, ==, 0);
}

/* Join the given @dir and @filename into @path. */
static void joinPath(const char *dir, const char *filename, char *path)
{
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);
}

void test_dir_write_file(const char *dir,
                         const char *filename,
                         const void *buf,
                         const size_t n)
{
    char path[256];
    int fd;
    int rv;

    joinPath(dir, filename, path);

    fd = open(path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    munit_assert_int(fd, !=, -1);

    rv = write(fd, buf, n);
    munit_assert_int(rv, ==, n);

    close(fd);
}

void test_dir_write_file_with_zeros(const char *dir,
                                    const char *filename,
                                    const size_t n)
{
    void *buf = munit_malloc(n);

    test_dir_write_file(dir, filename, buf, n);

    free(buf);
}

void test_dir_append_file(const char *dir,
                          const char *filename,
                          const void *buf,
                          const size_t n)
{
    char path[256];
    int fd;
    int rv;

    joinPath(dir, filename, path);

    fd = open(path, O_APPEND | O_RDWR, S_IRUSR | S_IWUSR);

    munit_assert_int(fd, !=, -1);

    rv = write(fd, buf, n);
    munit_assert_int(rv, ==, n);

    close(fd);
}

void test_dir_overwrite_file(const char *dir,
                             const char *filename,
                             const void *buf,
                             const size_t n,
                             const off_t whence)
{
    char path[256];
    int fd;
    int rv;
    off_t size;

    joinPath(dir, filename, path);

    fd = open(path, O_RDWR, S_IRUSR | S_IWUSR);

    munit_assert_int(fd, !=, -1);

    /* Get the size of the file */
    size = lseek(fd, 0, SEEK_END);

    if (whence == 0) {
        munit_assert_int(size, >=, n);
        lseek(fd, 0, SEEK_SET);
    } else if (whence > 0) {
        munit_assert_int(whence, <=, size);
        munit_assert_int(size - whence, >=, n);
        lseek(fd, whence, SEEK_SET);
    } else {
        munit_assert_int(-whence, <=, size);
        munit_assert_int(-whence, >=, n);
        lseek(fd, whence, SEEK_END);
    }

    rv = write(fd, buf, n);
    munit_assert_int(rv, ==, n);

    close(fd);
}

void test_dir_overwrite_file_with_zeros(const char *dir,
                                        const char *filename,
                                        const size_t n,
                                        const off_t whence)
{
    void *buf;

    buf = munit_malloc(n);
    memset(buf, 0, n);

    test_dir_overwrite_file(dir, filename, buf, n, whence);

    free(buf);
}

void test_dir_truncate_file(const char *dir,
                            const char *filename,
                            const size_t n)
{
    char path[256];
    int fd;
    int rv;

    joinPath(dir, filename, path);

    fd = open(path, O_RDWR, S_IRUSR | S_IWUSR);
    munit_assert_int(fd, !=, -1);

    rv = ftruncate(fd, n);
    munit_assert_int(rv, ==, 0);

    rv = close(fd);
    munit_assert_int(rv, ==, 0);
}

void test_dir_grow_file(const char *dir, const char *filename, const size_t n)
{
    char path[256];
    int fd;
    struct stat sb;
    void *buf;
    size_t size;
    int rv;

    joinPath(dir, filename, path);

    fd = open(path, O_RDWR, S_IRUSR | S_IWUSR);
    munit_assert_int(fd, !=, -1);

    rv = fstat(fd, &sb);
    munit_assert_int(rv, ==, 0);
    munit_assert_int(sb.st_size, <=, n);

    /* Fill with zeros. */
    lseek(fd, sb.st_size, SEEK_SET);
    size = n - sb.st_size;
    buf = munit_malloc(size);
    rv = write(fd, buf, size);
    munit_assert_int(rv, ==, size);
    free(buf);

    rv = close(fd);
    munit_assert_int(rv, ==, 0);
}

void test_dir_rename_file(const char *dir,
                          const char *filename1,
                          const char *filename2)
{
    char path1[256];
    char path2[256];
    int rv;

    joinPath(dir, filename1, path1);
    joinPath(dir, filename2, path2);

    rv = rename(path1, path2);
    munit_assert_int(rv, ==, 0);
}

void test_dir_remove_file(const char *dir, const char *filename)
{
    char path[256];
    int rv;

    joinPath(dir, filename, path);
    rv = unlink(path);
    munit_assert_int(rv, ==, 0);
}

void test_dir_read_file(const char *dir,
                        const char *filename,
                        void *buf,
                        const size_t n)
{
    char path[256];
    int fd;
    int rv;

    joinPath(dir, filename, path);

    fd = open(path, O_RDONLY);
    if (fd == -1) {
        munit_logf(MUNIT_LOG_ERROR, "read file '%s': %s", path,
                   strerror(errno));
    }

    rv = read(fd, buf, n);
    munit_assert_int(rv, ==, n);

    close(fd);
}

bool test_dir_exists(const char *dir)
{
    struct stat sb;
    int rv;

    rv = stat(dir, &sb);
    if (rv == -1) {
        munit_assert_int(errno, ==, ENOENT);
        return false;
    }

    return true;
}

void test_dir_unexecutable(const char *dir)
{
    int rv;

    rv = chmod(dir, 0);
    munit_assert_int(rv, ==, 0);
}

void test_dir_unwritable(const char *dir)
{
    int rv;

    rv = chmod(dir, 0500);
    munit_assert_int(rv, ==, 0);
}

void test_dir_unreadable_file(const char *dir, const char *filename)
{
    char path[256];
    int rv;

    joinPath(dir, filename, path);

    rv = chmod(path, 0);
    munit_assert_int(rv, ==, 0);
}

bool test_dir_has_file(const char *dir, const char *filename)
{
    char path[256];
    int fd;

    joinPath(dir, filename, path);

    fd = open(path, O_RDONLY);
    if (fd == -1) {
        munit_assert_true(errno == ENOENT || errno == EACCES);
        return false;
    }

    close(fd);

    return true;
}

void test_dir_fill(const char *dir, const size_t n)
{
    char path[256];
    const char *filename = ".fill";
    struct statvfs fs;
    size_t size;
    int fd;
    int rv;

    rv = statvfs(dir, &fs);
    munit_assert_int(rv, ==, 0);

    size = fs.f_bsize * fs.f_bavail;

    if (n > 0) {
        munit_assert_int(size, >=, n);
    }

    joinPath(dir, filename, path);

    fd = open(path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    munit_assert_int(fd, !=, -1);

    rv = posix_fallocate(fd, 0, size - n);
    munit_assert_int(rv, ==, 0);

    /* If n is zero, make sure any further write fails with ENOSPC */
    if (n == 0) {
        char buf[4096];
        int i;

        rv = lseek(fd, 0, SEEK_END);
        munit_assert_int(rv, !=, -1);

        for (i = 0; i < 40; i++) {
            rv = write(fd, buf, sizeof buf);
            if (rv < 0) {
                break;
            }
        }

        munit_assert_int(rv, ==, -1);
        munit_assert_int(errno, ==, ENOSPC);
    }

    close(fd);
}

int test_aio_fill(aio_context_t *ctx, unsigned n)
{
    char buf[256];
    int fd;
    int rv;
    int limit;
    int used;

    /* Figure out how many events are available. */
    fd = open("/proc/sys/fs/aio-max-nr", O_RDONLY);
    munit_assert_int(fd, !=, -1);

    rv = read(fd, buf, sizeof buf);
    munit_assert_int(rv, !=, -1);

    close(fd);

    limit = atoi(buf);

    /* Figure out how many events are in use. */
    fd = open("/proc/sys/fs/aio-nr", O_RDONLY);
    munit_assert_int(fd, !=, -1);

    rv = read(fd, buf, sizeof buf);
    munit_assert_int(rv, !=, -1);

    close(fd);

    used = atoi(buf);

    /* Best effort check that nothing process is using AIO. Our own unit tests
     * case use up to 2 event slots at the time this function is called, so we
     * don't consider those. */
    if (used > 2) {
        return -1;
    }

    rv = syscall(__NR_io_setup, limit - used - n, ctx);
    munit_assert_int(rv, ==, 0);

    return 0;
}

void test_aio_destroy(aio_context_t ctx)
{
    int rv;

    rv = syscall(__NR_io_destroy, ctx);
    munit_assert_int(rv, ==, 0);
}
