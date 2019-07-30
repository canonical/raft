/* Operating system related utilities. */

#ifndef UV_OS_H_
#define UV_OS_H_

#include <dirent.h>
#include <linux/aio_abi.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>

#include "../include/raft.h"

/* Maximum length of a file path. */
#define UV__PATH_MAX_LEN 1024

/* Maximum length of a filename. */
#define UV__FILENAME_MAX_LEN 128

/* Length of path separator. */
#define UV__SEP_LEN 1 /* strlen("/") */

/* Maximum length of a directory path. */
#define UV__DIR_MAX_LEN (UV__PATH_MAX_LEN - UV__SEP_LEN - UV__FILENAME_MAX_LEN)

/* Fixed length string that can hold a complete file system path. */
typedef char uvPath[UV__PATH_MAX_LEN];

/* Fixed length string that can hold a file name. */
typedef char uvFilename[UV__FILENAME_MAX_LEN];

/* Fixed length string that can hold a directory path. */
typedef char uvDir[UV__DIR_MAX_LEN];

/* Concatenate a directory and a file. */
void uvJoin(const uvDir dir, const uvFilename filename, uvPath path);

/* Extract the directory portion of the given path. */
void uvDirname(const uvPath path, uvDir dir);

/* Check that the given directory exists, and try to create it if it doesn't. */
int uvEnsureDir(const uvDir dir);

/* Open a file in a directory. */
int uvOpen(const uvDir dir, const uvFilename filename, int flags, int *fd);

/* Stat a file in a directory. */
int osStat(const uvDir dir, const uvFilename filename, struct stat *sb);

/* Delete a file in a directory. */
int osUnlink(const uvDir dir, const uvFilename filename);

/* Truncate a file in a directory. */
int osTruncate(const uvDir dir, const uvFilename filename, size_t offset);

/* Rename a file in a directory. */
int osRename(const uvDir dir,
             const uvFilename filename1,
             const uvFilename filename2);

/* Sync the given directory. */
int osSyncDir(const uvDir dir);

/* Return all entries of the given directory, in alphabetically sorted order. */
int osScanDir(const uvDir dir, struct dirent ***entries, int *n_entries);

/* Check whether the given file in the given directory is empty. */
int osIsEmpty(const uvDir dir, const uvFilename filename, bool *empty);

/* Check if the content of the file associated with the given file descriptor
 * contains all zeros from the current offset onward. */
int osHasTrailingZeros(int fd, bool *flag);

/* Read exactly @n bytes from the given file descriptor. */
int osReadN(int fd, void *buf, size_t n);

/* Write exactly @n bytes to the given file descriptor. */
int osWriteN(int fd, void *buf, size_t n);

/* Check if the given file descriptor has reached the end of the file. */
bool osIsAtEof(int fd);

/* Create a file with the given content. */
int osCreateFile(const uvDir dir,
                 const uvFilename filename,
                 struct raft_buffer *bufs,
                 unsigned n_bufs);

struct osFileSystemInfo
{
    size_t block_size; /* Block size to use when writing. */
    bool direct_io;    /* Whether direct I/O is supported. */
    bool async_io;     /* Whether fully asynchronous I/O is supported. */
};

/* Return information about the I/O capabilities of the underlying file
 * system.
 *
 * The @direct parameter will be set to zero if direct I/O is not possible, or
 * to the block size to use for direct I/O otherwise.
 *
 * The @async parameter will be set to true if fully asynchronous I/O is
 * possible using the KAIO API. */
int osProbeIO(const uvDir dir, size_t *direct, bool *async);

/* Configure the given file descriptor for direct I/O. */
int osSetDirectIO(int fd);

/* Return a human-readable description of the given OS error */
const char *osStrError(int rv);

/* Declaration of the kernel AIO APIs that we use. This avoids having to depend
 * on libaio. */
int osIoSetup(unsigned n, aio_context_t *ctx);

int osIoDestroy(aio_context_t ctx);

int osIoSubmit(aio_context_t ctx, long n, struct iocb **iocbs);

int osIoGetevents(aio_context_t ctx,
                  long min_nr,
                  long max_nr,
                  struct io_event *events,
                  struct timespec *timeout);

#endif /* UV_OS_H_ */
