/**
 * Integration between libuv and the Linux AIO (aka KAIO) file system APIs.
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
 * Maximum length of a directory path.
 */
#define RAFT_UV_FS_MAX_DIR_LEN \
    (RAFT_UV_FS_MAX_PATH_LEN - (strlen("/") + RAFT_UV_FS_MAX_FILENAME_LEN))

/**
 * Convenience to declare a variable that can hold a file system path.
 */
typedef char raft_uv_path[RAFT_UV_FS_MAX_PATH_LEN];

/**
 * Convenience to concatenate a directory and a file.
 */
void raft_uv_fs__join(const char *dir, const char *filename, char *path);

/**
 * Get the logical block size of the file system rooted at @dir.
 */
int raft_uv_fs__block_size(const char *dir, size_t *size);

/**
 * Handle to an open file descriptor.
 */
struct raft_uv_file
{
    struct uv_loop_s *loop;        /* For eventd polling and thread work */
    int fd;                        /* Operating system file descriptor */
    bool async;                    /* Whether fully async I/O is supported */
    int event_fd;                  /* Poll'ed to check if write has completed */
    struct uv_poll_s event_poller; /* To make the loop poll for event_fd */
    aio_context_t ctx;             /* KAIO handle */
    bool busy;                     /* Whether a request is in progress */
};

struct raft_uv_fs; /* Forward declaration */

/**
 * Callback for asynchronous file system operations, such as file creation and
 * writing.
 */
typedef void (*raft_uv_fs_cb)(struct raft_uv_fs *req);

/**
 * Request object for asynchronous file system operations.
 */
struct raft_uv_fs
{
    void *data;                /* User data */
    struct raft_uv_file *file; /* File handle */
    int status;                /* Request result code */
    struct uv_work_s work;     /* To execute logic in the threadpool */
    raft_uv_fs_cb cb;          /* Callback to invoke upon request completion */
    const char *path;          /* File path (for creation) */
    size_t size;               /* File size (for creation) */
    struct iocb iocb;          /* KAIO request (for writing) */
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
 * Close the given file and release all associated resources. There must be no
 * request in progress.
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
