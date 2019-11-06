/* File system related utilities. */

#ifndef UV_FS_H_
#define UV_FS_H_

#include <stdbool.h>
#include <uv.h>

#include "err.h"
#include "uv_os.h"

/* Check that the given directory exists, and try to create it if it doesn't. */
int UvFsEnsureDir(const char *dir, struct ErrMsg *errmsg);

/* Sync the given directory by calling fsync(). */
int UvFsSyncDir(const char *dir, struct ErrMsg *errmsg);

/* Check whether a the given file exists. */
int UvFsFileExists(const char *dir,
                   const char *filename,
                   bool *exists,
                   struct ErrMsg *errmsg);

/* Check whether the given file in the given directory is empty. */
int UvFsFileIsEmpty(const char *dir,
                    const char *filename,
                    bool *empty,
                    struct ErrMsg *errmsg);

/* Create the given file in the given directory and allocate the given size to
 * it, returning its file descriptor. The file must not exist yet. */
int UvFsAllocateFile(const char *dir,
                     const char *filename,
                     size_t size,
                     uv_file *fd,
                     struct ErrMsg *errmsg);

/* Create a file and write the given content into it. */
int UvFsMakeFile(const char *dir,
                 const char *filename,
                 struct raft_buffer *bufs,
                 unsigned n_bufs,
                 struct ErrMsg *errmsg);

/* Check if the content of the file associated with the given file descriptor
 * contains all zeros from the current offset onward. */
int UvFsFileHasOnlyTrailingZeros(uv_file fd, bool *flag, struct ErrMsg *errmsg);

/* Check if the given file descriptor has reached the end of the file. */
bool UvFsIsAtEof(uv_file fd);

/* Read exactly @n bytes from the given file descriptor. */
int UvFsReadFrom(uv_file fd, struct raft_buffer *buf, struct ErrMsg *errmsg);

/* Read all the content of the givn file. */
int UvFsReadFile(const char *dir,
                 const char *filename,
                 struct raft_buffer *buf,
                 struct ErrMsg *errmsg);

/* Synchronously remove a file, calling the unlink() system call. */
int UvFsRemoveFile(const char *dir,
                   const char *filename,
                   struct ErrMsg *errmsg);

/* Synchronously truncate a file to the given size and then rename it. */
int UvFsTruncateAndRenameFile(const char *dir,
                              size_t size,
                              const char *filename1,
                              const char *filename2,
                              struct ErrMsg *errmsg);

#endif /* UV_FS_H_ */
