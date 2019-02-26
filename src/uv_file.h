/**
 * Create and write files asynchronously, using libuv on top of Linux AIO (aka
 * KAIO).
 */

#ifndef RAFT_UV_FILE_H_
#define RAFT_UV_FILE_H_

#include <linux/aio_abi.h>
#include <stdbool.h>

#include <uv.h>

#include "queue.h"

/**
 * Handle to an open file.
 */
struct uv__file;

/**
 * Create file request.
 */
struct uv__file_create;

/**
 * Write file request.
 */
struct uv__file_write;

/**
 * Callback called after a create file request has been completed.
 */
typedef void (*uv__file_create_cb)(struct uv__file_create *req, int status);

/**
 * Callback called after a write file request has been completed.
 */
typedef void (*uv__file_write_cb)(struct uv__file_write *req, int status);

/**
 * Callback called after the memory associated with a file handle can be
 * released.
 */
typedef void (*uv__file_close_cb)(struct uv__file *f);

/**
 * Get the logical block size of the file system rooted at @dir.
 */
int uv__file_block_size(const char *dir, size_t *size);

/**
 * Initialize a file handle.
 */
int uv__file_init(struct uv__file *f, struct uv_loop_s *loop);

/**
 * Create the given file for subsequent non-blocking writing. The file must not
 * exist yet.
 */
int uv__file_create(struct uv__file *f,
                    struct uv__file_create *req,
                    const char *path,
                    size_t size,
                    unsigned max_concurrent_writes,
                    uv__file_create_cb cb);

/**
 * Asynchronously write data to the file associated with the given handle.
 */
int uv__file_write(struct uv__file *f,
                   struct uv__file_write *req,
                   const uv_buf_t bufs[],
                   unsigned n_bufs,
                   size_t offset,
                   uv__file_write_cb cb);

/**
 * Close the given file and release all associated resources. There must be no
 * request in progress.
 */
void uv__file_close(struct uv__file *f, uv__file_close_cb cb);

/**
 * Return true if the file is closing or has been closed.
 */
bool uv__file_is_closing(struct uv__file *f);

struct uv__file
{
    void *data;                    /* User data */
    struct uv_loop_s *loop;        /* Event loop */
    int flags;                     /* State flags */
    int fd;                        /* Operating system file descriptor */
    bool async;                    /* Whether fully async I/O is supported */
    int event_fd;                  /* Poll'ed to check if write is finished */
    struct uv_poll_s event_poller; /* To make the loop poll for event_fd */
    aio_context_t ctx;             /* KAIO handle */
    struct io_event *events;       /* Array of KAIO response objects */
    unsigned n_events;             /* Length of the events array */
    raft__queue write_queue;       /* Queue of inflight write requests */
    uv__file_close_cb close_cb;    /* Close callback */
};

struct uv__file_create
{
    void *data;            /* User data */
    struct uv__file *file; /* File handle */
    int status;            /* Request result code */
    struct uv_work_s work; /* To execute logic in the threadpool */
    uv__file_create_cb cb; /* Callback to invoke upon request completion */
    const char *path;      /* File path */
    size_t size;           /* File size */
};

struct uv__file_write
{
    void *data;            /* User data */
    struct uv__file *file; /* File handle */
    int status;            /* Request result code */
    struct uv_work_s work; /* To execute logic in the threadpool */
    uv__file_write_cb cb;  /* Callback to invoke upon request completion */
    struct iocb iocb;      /* KAIO request (for writing) */
    raft__queue queue;     /* Prev/next links in the inflight queue */
};

#endif /* RAFT_UV_FILE_H_ */
