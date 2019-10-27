/* Operating system related utilities. */

#ifndef UV_OS_H_
#define UV_OS_H_

#include <linux/aio_abi.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>
#include <uv.h>

#include "../include/raft.h"

/* Maximum length of a file path. */
#define UV__PATH_MAX_LEN 1024

/* Maximum length of a filename. */
#define UV__FILENAME_MAX_LEN 128

/* Length of path separator. */
#define UV__SEP_LEN 1 /* strlen("/") */

/* Maximum length of a directory path. */
#define UV__DIR_MAX_LEN (UV__PATH_MAX_LEN - UV__SEP_LEN - UV__FILENAME_MAX_LEN)

/* True if the given DIR string has at most UV__DIR_MAX_LEN chars. */
#define UV__DIR_HAS_VALID_LEN(DIR) \
    (strnlen(DIR, UV__DIR_MAX_LEN + 1) <= UV__DIR_MAX_LEN)

/* True if the given FILENAME string has at most UV__FILENAME_MAX_LEN chars. */
#define UV__FILENAME_HAS_VALID_LEN(FILENAME) \
    (strnlen(FILENAME, UV__FILENAME_MAX_LEN + 1) <= UV__FILENAME_MAX_LEN)

/* Check that the given directory exists, and try to create it if it doesn't. */
int uvEnsureDir(const char *dir, char **errmsg);

/* Sync the given directory. */
int uvSyncDir(const char *dir, char **errmsg);

/* Open a file in a directory. */
int uvOpenFile(const char *dir,
               const char *filename,
               int flags,
               uv_file *fd,
               char **errmsg);

/* Stat a file in a directory. */
int uvStatFile(const char *dir,
               const char *filename,
               uv_stat_t *sb,
               char **errmsg);

/* Create a file and write the given content into it. */
int uvMakeFile(const char *dir,
               const char *filename,
               struct raft_buffer *bufs,
               unsigned n_bufs,
               char **errmsg);

/* Delete a file in a directory. */
int uvUnlinkFile(const char *dir, const char *filename, char **errmsg);

/* Like uvUnlinkFile, but ignoring errors. */
void uvTryUnlinkFile(const char *dir, const char *filename);

/* Truncate a file in a directory. */
int uvTruncateFile(const char *dir,
                   const char *filename,
                   size_t offset,
                   char **errmsg);

/* Rename a file in a directory. */
int uvRenameFile(const char *dir,
                 const char *filename1,
                 const char *filename2,
                 char **errmsg);

/* Check whether the given file in the given directory is empty. */
int uvIsEmptyFile(const char *dir,
                  const char *filename,
                  bool *empty,
                  char **errmsg);

/* Read exactly @n bytes from the given file descriptor. */
int uvReadFully(int fd, void *buf, size_t n, char **errmsg);

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
int uvProbeIoCapabilities(const char *dir,
                          size_t *direct,
                          bool *async,
                          char *errmsg);

/* Configure the given file descriptor for direct I/O. */
int uvSetDirectIo(int fd, char *errmsg);

/* Wrappers around the kernel AIO APIs that we use.. */
int uvIoSetup(unsigned n, aio_context_t *ctx, char *errmsg);

int uvIoDestroy(aio_context_t ctx, char *errmsg);

void uvTryIoDestroy(aio_context_t ctx);

int uvIoSubmit(aio_context_t ctx, long n, struct iocb **iocbs, char *errmsg);

int uvIoGetevents(aio_context_t ctx,
                  long min_nr,
                  long max_nr,
                  struct io_event *events,
                  struct timespec *timeout,
                  int *nr,
                  char *errmsg);

#endif /* UV_OS_H_ */
