/* File system related utilities. */

#ifndef UV_FS_H_
#define UV_FS_H_

#include <stdbool.h>
#include <uv.h>

#include "err.h"
#include "uv_os.h"

/* Check that the given directory exists, and try to create it if it doesn't. */
int UvFsEnsureDir(const char *dir, struct ErrMsg *errmsg);

/* Create the given file in the given directory, allocate the given size to it,
 * sync the underlying directory and return its file descriptor. The file must
 * not exist yet. */
int UvFsAllocateFile(const char *dir,
                     const char *filename,
                     size_t size,
                     uv_file *fd,
                     struct ErrMsg *errmsg);

/* Synchronously remove a file, calling the unlink() system call and fsync()'ing
 * the directory. */
int UvFsRemoveFile(const char *dir,
                   const char *filename,
                   struct ErrMsg *errmsg);

/* Synchronously truncate a file to the given size and then rename it. If the
 * size is zero, the file will be removed. */
int UvFsTruncateAndRenameFile(const char *dir,
                              size_t size,
                              const char *filename1,
                              const char *filename2,
                              struct ErrMsg *errmsg);

#endif /* UV_FS_H_ */
