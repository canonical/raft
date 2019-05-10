/* Operating system related utilities. */

#ifndef OS_H_
#define OS_H_

#include <dirent.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/stat.h>

#include "../include/raft.h"

/* Maximum length of a file path. */
#define OS_MAX_PATH_LEN 1024

/* Maximum length of a filename. */
#define OS_MAX_FILENAME_LEN 128

/* Length of path separator */
#define OS_SEP_LEN 1 /* strlen("/") */

/* Maximum length of a directory path. */
#define OS_MAX_DIR_LEN (OS_MAX_PATH_LEN - OS_SEP_LEN - OS_MAX_FILENAME_LEN)

/* Fixed length string that can hold a complete file system path. */
typedef char osPath[OS_MAX_PATH_LEN];

/* Fixed length string that can hold a file name. */
typedef char osFilename[OS_MAX_FILENAME_LEN];

/* Fixed length string that can hold a directory path. */
typedef char osDir[OS_MAX_DIR_LEN];

/* Concatenate a directory and a file. */
void osJoin(const osDir dir, const osFilename filename, osPath path);

/* Extract the directory portion of the given path. */
void osDirname(const osPath path, osDir dir);

/* Check that the given directory exists, and try to create it if it doesn't. */
int osEnsureDir(const osDir dir);

/* Open a file in a directory. */
int osOpen(const osDir dir, const osFilename filename, int flags, int *fd);

/* Stat a file in a directory. */
int osStat(const osDir dir, const osFilename filename, struct stat *sb);

/* Delete a file in a directory. */
int osUnlink(const osDir dir, const osFilename filename);

/* Truncate a file in a directory. */
int osTruncate(const osDir dir, const osFilename filename, size_t offset);

/* Rename a file in a directory. */
int osRename(const osDir dir,
             const osFilename filename1,
             const osFilename filename2);

/* Sync the given directory. */
int osSyncDir(const osDir dir);

/* Return all entries of the given directory, in alphabetically sorted order. */
int osScanDir(const osDir dir, struct dirent ***entries, int *n_entries);

/* Check whether the given file in the given directory is empty. */
int osIsEmpty(const osDir dir, const osFilename filename, bool *empty);

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
int osCreateFile(const osDir dir,
                 const osFilename filename,
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
int osProbeIO(const osDir dir, size_t *direct, bool *async);

/* Configure the given file descriptor for direct I/O. */
int osSetDirectIO(int fd);

/* Return a human-readable description of the given OS error */
const char *osStrError(int rv);

#endif /* OS_H_ */
