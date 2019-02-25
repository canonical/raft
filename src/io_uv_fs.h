/**
 * File-system related utilities.
 */

#ifndef RAFT_IO_UV_FS_H
#define RAFT_IO_UV_FS_H

#include <stdbool.h>
#include <stdlib.h>
#include <sys/stat.h>

/**
 * Maximum length of a file path.
 */
#define RAFT__IO_UV_FS_MAX_PATH_LEN 1024

/**
 * Maximum length of a filename.
 */
#define RAFT__IO_UV_FS_MAX_FILENAME_LEN 128

/**
 * Maximum length of a directory path.
 */
#define RAFT__IO_UV_FS_MAX_DIR_LEN \
    (RAFT__IO_UV_FS_MAX_PATH_LEN - \
     (strlen("/") + RAFT__IO_UV_FS_MAX_FILENAME_LEN))

/**
 * Convenience to declare a variable that can hold a file name.
 */
typedef char raft__io_uv_fs_filename[RAFT__IO_UV_FS_MAX_FILENAME_LEN];

/**
 * Convenience to declare a variable that can hold a file system path.
 */
typedef char raft__io_uv_fs_path[RAFT__IO_UV_FS_MAX_PATH_LEN];

/**
 * Convenience to concatenate a directory and a file.
 */
void raft__io_uv_fs_join(const char *dir, const char *filename, char *path);

/**
 * Open a file in a directory.
 */
int raft__io_uv_fs_open(const char *dir, const char *filename, int flags);

/**
 * Stat a file in a directory.
 */
int raft__io_uv_fs_stat(const char *dir, const char *filename, struct stat *sb);

/**
 * Synchronously delete a file in a directory. To be run in a threadpool.
 */
int raft__io_uv_fs_unlink(const char *dir, const char *filename);

/**
 * Synchronously truncate a file in a directory. To be run in a threadpool.
 */
int raft__io_uv_fs_truncate(const char *dir,
                            const char *filename,
                            size_t offset);

/**
 * Synchronously rename a file in a directory. To be run in a threadpool.
 */
int raft__io_uv_fs_rename(const char *dir,
                          const char *filename1,
                          const char *filename2);

/**
 * Sync the given directory.
 */
int raft__io_uv_fs_sync_dir(const char *dir);

/**
 * Check whether the given file in the given directory is empty.
 */
int raft__io_uv_fs_is_empty(const char *dir, const char *filename, bool *empty);

/**
 * Check if the content of the segment file associated with the given file
 * descriptor contains all zeros from the current offset onward.
 */
int raft__io_uv_fs_is_all_zeros(const int fd, bool *flag);

/**
 * Read exactly @n bytes from the given file descriptor.
 */
int raft__io_uv_fs_read_n(const int fd, void *buf, size_t n);

/**
 * Check if the given file descriptor has reached the end of the file.
 */
bool raft__io_uv_fs_is_at_eof(const int fd);

#endif /* RAFT_IO_UV_FS_H */
