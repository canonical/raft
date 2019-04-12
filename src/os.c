#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "assert.h"
#include "os.h"

void osJoin(const osDir dir, const osFilename filename, osPath path)
{
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);
}

int osEnsureDir(const osDir dir)
{
    struct stat sb;
    int rv;

    /* Ensure that the given path doesn't exceed our static buffer limit */
    if (strnlen(dir, OS_MAX_DIR_LEN + 1) > OS_MAX_DIR_LEN) {
        return ENAMETOOLONG;
    }

    /* Make sure we have a directory we can write into. */
    rv = stat(dir, &sb);
    if (rv == -1) {
        if (errno == ENOENT) {
            rv = mkdir(dir, 0700);
            if (rv != 0) {
                return errno;
            }
        } else {
            return errno;
        }
    } else if ((sb.st_mode & S_IFMT) != S_IFDIR) {
        return ENOTDIR;
    }

    return 0;
}

int osOpen(const osDir dir, const osFilename filename, int flags, int *fd)
{
    osPath path;
    osJoin(dir, filename, path);
    *fd = open(path, flags, S_IRUSR | S_IWUSR);
    if (*fd == -1) {
        return errno;
    }
    return 0;
}

int osStat(const osDir dir, const osFilename filename, struct stat *sb)
{
    osPath path;
    int rv;
    osJoin(dir, filename, path);
    rv = stat(path, sb);
    if (rv == -1) {
        return errno;
    }
    return 0;
}

int osUnlink(const char *dir, const char *filename)
{
    osPath path;
    int rv;
    osJoin(dir, filename, path);
    rv = unlink(path);
    if (rv == -1) {
        return errno;
    }
    return 0;
}

int osTruncate(const osDir dir, const osFilename filename, size_t offset)
{
    osPath path;
    int fd;
    int rv;
    osJoin(dir, filename, path);
    fd = open(path, O_RDWR);
    if (fd == -1) {
        return errno;
    }
    rv = ftruncate(fd, offset);
    if (rv == -1) {
        close(fd);
        return errno;
    }
    rv = fsync(fd);
    if (rv == -1) {
        close(fd);
        return errno;
    }
    close(fd);
    return 0;
}

int osRename(const osDir dir,
             const osFilename filename1,
             const osFilename filename2)
{
    osPath path1;
    osPath path2;
    int fd;
    int rv;
    osJoin(dir, filename1, path1);
    osJoin(dir, filename2, path2);
    /* TODO: double check that filename2 does not exist. */
    rv = rename(path1, path2);
    if (rv == -1) {
        return errno;
    }
    fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        return errno;
    }
    rv = fsync(fd);
    close(fd);
    if (rv == -1) {
        return errno;
    }
    return 0;
}

int osSyncDir(const osDir dir)
{
    int fd;
    int rv;
    fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        return errno;
    }
    rv = fsync(fd);
    close(fd);
    if (rv == -1) {
        return errno;
    }
    return 0;
}

int osIsEmpty(const osDir dir, const osFilename filename, bool *empty)
{
    osPath path;
    struct stat sb;
    int rv;
    osJoin(dir, filename, path);
    rv = stat(path, &sb);
    if (rv == -1) {
        return errno;
    }
    *empty = sb.st_size == 0 ? true : false;
    return 0;
}

int osHasTrailingZeros(const int fd, bool *flag)
{
    off_t size;
    off_t offset;
    char *data;
    size_t i;
    int rv;

    /* Save the current offset. */
    offset = lseek(fd, 0, SEEK_CUR);

    /* Figure the size of the rest of the file. */
    size = lseek(fd, 0, SEEK_END);
    if (size == -1) {
        return errno;
    }
    size -= offset;

    /* Reposition the file descriptor offset to the original offset. */
    offset = lseek(fd, offset, SEEK_SET);
    if (offset == -1) {
        return errno;
    }

    data = malloc(size);
    if (data == NULL) {
        return ENOMEM;
    }

    rv = osReadN(fd, data, size);
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

int osReadN(const int fd, void *buf, const size_t n)
{
    int rv;
    rv = read(fd, buf, n);
    if (rv == -1) {
        return errno;
    }
    assert(rv >= 0);
    if ((size_t)rv < n) {
        return ENODATA;
    }
    return 0;
}

int osWriteN(const int fd, void *buf, const size_t n)
{
    int rv;
    rv = write(fd, buf, n);
    if (rv == -1) {
        return errno;
    }
    assert(rv >= 0);
    if ((size_t)rv < n) {
        return ENODATA;
    }
    return 0;
}

bool osIsAtEof(const int fd)
{
    off_t offset;
    off_t size;
    offset = lseek(fd, 0, SEEK_CUR); /* Get the current offset */
    size = lseek(fd, 0, SEEK_END);   /* Get file size */
    lseek(fd, offset, SEEK_SET);     /* Restore current offset */
    return offset == size;           /* Compare current offset and size */
}

char strErrorBuf[2048];

const char *osStrError(int rv)
{
    return strerror_r(rv, strErrorBuf, sizeof strErrorBuf);
}
