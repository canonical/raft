/**
 * Integration between libuv and the Linux AIO file system APIs.
 */

#ifndef RAFT_UV_FS_H
#define RAFT_UV_FS_H

#include <linux/aio_abi.h>
#include <stdbool.h>

#include <uv.h>

/**
 * Maximum length of a file system path.
 */
#define RAFT_UV_FS_MAX_PATH_LEN 1024

/**
 * Maximum length of a filename.
 */
#define RAFT_UV_FS_MAX_FILENAME_LEN 128

/**
 * Maximum length of the data directory path.
 */
#define RAFT_UV_FS_MAX_DIR_LEN \
    (RAFT_UV_FS_MAX_PATH_LEN - RAFT_UV_FS_MAX_FILENAME_LEN - 1)

/**
 * Get the logical block size of the file system rooted at @dir.
 */
int raft_uv_fs__block_size(const char *dir, size_t *size);

/**
 * Handle to an open file descriptor.
 */
struct raft_uv_file
{
    struct uv_loop_s *loop;        /* libuv loop to hook into */
    int fd;                        /* write to this file descriptor */
    bool async;                    /* whether fully async I/O is supported */
    int event_fd;                  /* poll'ed to check if write has completed */
    struct uv_poll_s event_poller; /* makes libuv's loop poll event_fd */
    aio_context_t ctx;             /* AIO handle */
};

struct raft_uv_fs; /* Forward declaration */

/**
 * Callback for asynchronous file system operations.
 */
typedef void (*raft_uv_fs_cb)(struct raft_uv_fs *req);

/**
 * Request object for asynchronous file system operations.
 */
struct raft_uv_fs
{
    void *data;                /* user data */
    struct raft_uv_file *file; /* file handle */
    int status;                /* request result */
    struct uv_work_s work;     /* pending work thread request, if any */
    raft_uv_fs_cb cb;          /* callback to invoke upon completion */
    const char *path;          /* file path (for creation) */
    size_t size;               /* file size (for creation) */
    struct iocb iocb;          /* io_submit request (for writing) */
};

/**
 * Create the given file for subsequent non-blocking writing. The file must not
 * exist yet.
 */
int raft_uv_fs__create(struct raft_uv_file *f,
                       struct raft_uv_fs *req,
                       struct uv_loop_s *loop,
                       const char *path,
                       size_t size,
                       raft_uv_fs_cb cb);

/**
 * Close the given file and release all associated resources.
 */
int raft_uv_fs__close(struct raft_uv_file *f);

/**
 * Asynchronously write data to the file associated with the given handle.
 */
int raft_uv_fs__write(struct raft_uv_file *f,
                      struct raft_uv_fs *req,
                      const uv_buf_t bufs[],
                      unsigned n,
                      size_t offset,
                      raft_uv_fs_cb cb);

#endif /* RAFT_UV_FS_H */
