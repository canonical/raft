/* File system related utilities. */

#ifndef UV_FS_H_
#define UV_FS_H_

#include <stdbool.h>
#include <uv.h>

#include "uv_os.h"

/* Abstract file system operations. */
struct UvFs
{
    struct uv_loop_s *loop; /* Event loop. */
    char *errmsg;           /* Description of last error occurred. */
};

/* Initialize a file system object. */
void UvFsInit(struct UvFs *fs, struct uv_loop_s *loop);

/* Release all reasources used by file system. */
void UvFsClose(struct UvFs *fs);

/* Return an error message describing the last error occurred. The pointer is
 * valid until a different error occurs or uvFsClose is called. */
const char *UvFsErrMsg(struct UvFs *fs);

/* Set the last error message, possibly replacing the former one. */
void UvFsSetErrMsg(struct UvFs *fs, char *errmsg);

/* Create the given file in the given directory, allocate the given size to it
 * and return its file descriptor. The file must not exist yet. */
int UvFsCreateFile(struct UvFs *fs,
                   const char *dir,
                   const char *filename,
                   size_t size,
                   uv_file *fd);

/* Synchronously remove a file, calling the unlink() system call and fsync()'ing
 * the directory. */
int UvFsRemoveFile(struct UvFs *fs, const char *dir, const char *filename);

/* Synchronously truncate a file to the given size and then rename it. If the
 * size is zero, the file will be removed. */
int UvFsTruncateAndRenameFile(struct UvFs *fs,
                              const char *dir,
                              size_t size,
                              const char *filename1,
                              const char *filename2);

#endif /* UV_FS_H_ */
