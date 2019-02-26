#include <string.h>
#include <unistd.h>

#include "assert.h"
#include "io_uv_fs.h"
#include "uv_file.h"

void io_uv__join(const char *dir, const char *filename, char *path)
{
    assert(strlen(dir) < RAFT__IO_UV_FS_MAX_DIR_LEN);
    assert(strlen(filename) < RAFT__IO_UV_FS_MAX_FILENAME_LEN);

    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);
}

int io_uv__ensure_dir(struct raft_logger *logger, const char *dir)
{
    struct stat sb;
    int rv;

    /* Ensure that the given path doesn't exceed our static buffer limit */
    if (strlen(dir) > RAFT__IO_UV_FS_MAX_DIR_LEN) {
        return RAFT_ERR_IO_NAMETOOLONG;
    }

    /* Make sure we have a directory we can write into. */
    rv = stat(dir, &sb);
    if (rv == -1) {
        if (errno == ENOENT) {
            rv = mkdir(dir, 0700);
            if (rv != 0) {
                raft_errorf(logger, "create data directory %s: %s", dir,
                            uv_strerror(-errno));
                return RAFT_ERR_IO;
            }
        } else {
            raft_errorf(logger, "access data directory %s: %s", dir,
                        uv_strerror(-errno));
            return RAFT_ERR_IO;
        }
    } else if ((sb.st_mode & S_IFMT) != S_IFDIR) {
        raft_errorf(logger, "path %s is not a directory", dir);
        return RAFT_ERR_IO;
    }

    return 0;
}

int raft__io_uv_fs_open(const char *dir, const char *filename, int flags)
{
    io_uv__path path;

    io_uv__join(dir, filename, path);

    return open(path, flags, S_IRUSR | S_IWUSR);
}

int raft__io_uv_fs_stat(const char *dir, const char *filename, struct stat *sb)
{
    io_uv__path path;

    io_uv__join(dir, filename, path);

    return stat(path, sb);
}

int raft__io_uv_fs_unlink(const char *dir, const char *filename)
{
    io_uv__path path;
    int rv;

    io_uv__join(dir, filename, path);

    rv = unlink(path);
    if (rv == -1) {
        return uv_translate_sys_error(errno);
    }

    return 0;
}

int raft__io_uv_fs_truncate(const char *dir,
                            const char *filename,
                            size_t offset)
{
    io_uv__path path;
    int fd;
    int rv;

    io_uv__join(dir, filename, path);

    fd = open(path, O_RDWR);
    if (fd == -1) {
        return uv_translate_sys_error(errno);
    }

    rv = ftruncate(fd, offset);
    if (rv == -1) {
        close(fd);
        return uv_translate_sys_error(errno);
    }

    rv = fsync(fd);
    if (rv == -1) {
        close(fd);
        return uv_translate_sys_error(errno);
    }

    close(fd);

    return 0;
}

int raft__io_uv_fs_rename(const char *dir,
                          const char *filename1,
                          const char *filename2)
{
    io_uv__path path1;
    io_uv__path path2;
    int fd;
    int rv;

    io_uv__join(dir, filename1, path1);
    io_uv__join(dir, filename2, path2);

    /* TODO: double check that filename2 does not exist. */
    rv = rename(path1, path2);
    if (rv == -1) {
        return uv_translate_sys_error(errno);
    }

    fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        return uv_translate_sys_error(errno);
    }

    rv = fsync(fd);

    close(fd);

    if (rv == -1) {
        return uv_translate_sys_error(errno);
    }

    return 0;
}

int raft__io_uv_fs_sync_dir(const char *dir)
{
    int fd;
    int rv;

    fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        return uv_translate_sys_error(errno);
    }

    rv = fsync(fd);

    close(fd);

    if (rv == -1) {
        return uv_translate_sys_error(errno);
    }

    return 0;
}

int raft__io_uv_fs_is_empty(const char *dir, const char *filename, bool *empty)
{
    io_uv__path path;
    struct stat st;
    int rv;

    io_uv__join(dir, filename, path);

    rv = stat(path, &st);
    if (rv == -1) {
        return uv_translate_sys_error(errno);
    }

    *empty = st.st_size == 0 ? true : false;

    return 0;
}

int raft__io_uv_fs_is_all_zeros(const int fd, bool *flag)
{
    off_t size;
    off_t offset;
    uint8_t *data;
    size_t i;
    int rv;

    /* Save the current offset. */
    offset = lseek(fd, 0, SEEK_CUR);

    /* Figure the size of the rest of the file. */
    size = lseek(fd, 0, SEEK_END);
    if (size == -1) {
        return uv_translate_sys_error(errno);
    }
    size -= offset;

    /* Reposition the file descriptor offset to the original offset. */
    offset = lseek(fd, offset, SEEK_SET);
    if (offset == -1) {
        return uv_translate_sys_error(errno);
    }

    data = malloc(size);
    if (data == NULL) {
        return UV_ENOMEM;
    }

    rv = raft__io_uv_fs_read_n(fd, data, size);
    if (rv != 0) {
        return rv;
    }

    for (i = 0; i < (size_t)size; i++) {
        if (data[i] != 0) {
            *flag = false;
            goto done;
        }
    }

    *flag = true;

done:
    free(data);

    return 0;
}

int raft__io_uv_fs_read_n(const int fd, void *buf, size_t n)
{
    int rv;

    rv = read(fd, buf, n);

    if (rv == -1) {
        return uv_translate_sys_error(errno);
    }

    assert(rv >= 0);

    if ((size_t)rv < n) {
        return UV_EIO;
    }

    return 0;
}

bool raft__io_uv_fs_is_at_eof(const int fd)
{
    off_t offset; /* Current position */
    off_t size;   /* File size */

    offset = lseek(fd, 0, SEEK_CUR);
    size = lseek(fd, 0, SEEK_END);

    lseek(fd, offset, SEEK_SET);

    return offset == size;
}
