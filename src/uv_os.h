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
int uvEnsureDir(const uvDir dir, char *errmsg);

/* Sync the given directory. */
int uvSyncDir(const uvDir dir, char *errmsg);

/* Return all entries of the given directory, in alphabetically sorted order. */
int uvScanDir(const uvDir dir,
              struct dirent ***entries,
              int *n_entries,
              char *errmsg);

/* Open a file in a directory. */
int uvOpenFile(const uvDir dir,
               const uvFilename filename,
               int flags,
               int *fd,
               char *errmsg);

/* Stat a file in a directory. */
int uvStatFile(const uvDir dir,
               const uvFilename filename,
               struct stat *sb,
               char *errmsg);

/* Create a file and write the given content into it. */
int uvMakeFile(const uvDir dir,
               const uvFilename filename,
               struct raft_buffer *bufs,
               unsigned n_bufs,
               char *errmsg);

/* Delete a file in a directory. */
int uvUnlinkFile(const uvDir dir, const uvFilename filename, char *errmsg);

/* Truncate a file in a directory. */
int uvTruncateFile(const uvDir dir,
                   const uvFilename filename,
                   size_t offset,
                   char *errmsg);

/* Rename a file in a directory. */
int uvRenameFile(const uvDir dir,
                 const uvFilename filename1,
                 const uvFilename filename2,
                 char *errmsg);

/* Check whether the given file in the given directory is empty. */
int uvIsEmptyFile(const uvDir dir,
                  const uvFilename filename,
                  bool *empty,
                  char *errmsg);

/* Read exactly @n bytes from the given file descriptor. */
int uvReadFully(int fd, void *buf, size_t n, char *errmsg);

/* Write exactly @n bytes to the given file descriptor. */
int uvWriteFully(int fd, void *buf, size_t n, char *errmsg);

/* Check if the content of the file associated with the given file descriptor
 * contains all zeros from the current offset onward. */
int uvIsFilledWithTrailingZeros(int fd, bool *flag, char *errmsg);

/* Check if the given file descriptor has reached the end of the file. */
bool uvIsAtEof(int fd);

/* Return information about the I/O capabilities of the underlying file
 * system.
 *
 * The @direct parameter will be set to zero if direct I/O is not possible, or
 * to the block size to use for direct I/O otherwise.
 *
 * The @async parameter will be set to true if fully asynchronous I/O is
 * possible using the KAIO API. */
int uvProbeIoCapabilities(const uvDir dir,
                          size_t *direct,
                          bool *async,
                          char *errmsg);

/* Configure the given file descriptor for direct I/O. */
int uvSetDirectIo(int fd);

/* Return a human-readable description of the given OS error */
const char *osStrError(int rv);

/* Declaration of the kernel AIO APIs that we use. This avoids having to depend
 * on libaio. */
int uvIoSetup(unsigned n, aio_context_t *ctx, char *errmsg);

int uvIoDestroy(aio_context_t ctx);

int uvIoSubmit(aio_context_t ctx, long n, struct iocb **iocbs);

int uvIoGetevents(aio_context_t ctx,
                  long min_nr,
                  long max_nr,
                  struct io_event *events,
                  struct timespec *timeout);

#endif /* UV_OS_H_ */
