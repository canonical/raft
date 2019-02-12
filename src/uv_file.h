/**
 * Create and write files asynchronously, using libuv on top of Linux AIO (aka
 * KAIO)..
 */

#ifndef RAFT_UV_FILE_H_
#define RAFT_UV_FILE_H_

#include <linux/aio_abi.h>
#include <stdbool.h>

#include <uv.h>

#include "queue.h"

/**
 * Maximum length of a file path.
 */
#define RAFT__UV_FILE_MAX_PATH_LEN 1024

/**
 * Maximum length of a filename.
 */
#define RAFT__UV_FILE_MAX_FILENAME_LEN 128

/**
 * Maximum length of a directory path.
 */
#define RAFT__UV_FILE_MAX_DIR_LEN \
    (RAFT__UV_FILE_MAX_PATH_LEN - \
     (strlen("/") + RAFT__UV_FILE_MAX_FILENAME_LEN))

/**
 * Convenience to declare a variable that can hold a file name.
 */
typedef char raft__uv_file_name[RAFT__UV_FILE_MAX_FILENAME_LEN];

/**
 * Convenience to declare a variable that can hold a file system path.
 */
typedef char raft__uv_file_path[RAFT__UV_FILE_MAX_PATH_LEN];

/**
 * Handle to an open file.
 */
struct raft__uv_file;

/**
 * Create file request.
 */
struct raft__uv_file_create;

/**
 * Write file request.
 */
struct raft__uv_file_write;

/**
 * Callback called after a create file request has been completed.
 */
typedef void (*raft__uv_file_create_cb)(struct raft__uv_file_create *req,
                                        int status);

/**
 * Callback called after a write file request has been completed.
 */
typedef void (*raft__uv_file_write_cb)(struct raft__uv_file_write *req,
                                       int status);

/**
 * Callback called after the memory associated with a file handle can be
 * released.
 */
typedef void (*raft__uv_file_close_cb)(struct raft__uv_file *f);

/**
 * Convenience to concatenate a directory and a file.
 */
void raft__uv_file_join(const char *dir, const char *filename, char *path);

/**
 * Get the logical block size of the file system rooted at @dir.
 */
int raft__uv_file_block_size(const char *dir, size_t *size);

/**
 * Initialize a file handle.
 */
int raft__uv_file_init(struct raft__uv_file *f, struct uv_loop_s *loop);

/**
 * Create the given file for subsequent non-blocking writing. The file must not
 * exist yet.
 */
int raft__uv_file_create(struct raft__uv_file *f,
                         struct raft__uv_file_create *req,
                         const char *path,
                         size_t size,
                         unsigned max_concurrent_writes,
                         raft__uv_file_create_cb cb);

/**
 * Asynchronously write data to the file associated with the given handle.
 */
int raft__uv_file_write(struct raft__uv_file *f,
                        struct raft__uv_file_write *req,
                        const uv_buf_t bufs[],
                        unsigned n_bufs,
                        size_t offset,
                        raft__uv_file_write_cb cb);

/**
 * Close the given file and release all associated resources. There must be no
 * request in progress.
 */
void raft__uv_file_close(struct raft__uv_file *f, raft__uv_file_close_cb cb);

/**
 * Return true if the file is currengly being created.
 */
bool raft__uv_file_is_creating(struct raft__uv_file *f);

/**
 * Return true if there are pending write requests.
 */
bool raft__uv_file_is_writing(struct raft__uv_file *f);

/**
 * Return true if the file is closing or has been closed.
 */
bool raft__uv_file_is_closing(struct raft__uv_file *f);

struct raft__uv_file
{
    void *data;                      /* User data */
    struct uv_loop_s *loop;          /* Event loop */
    int flags;                       /* State flags */
    int fd;                          /* Operating system file descriptor */
    bool async;                      /* Whether fully async I/O is supported */
    int event_fd;                    /* Poll'ed to check if write is finished */
    struct uv_poll_s event_poller;   /* To make the loop poll for event_fd */
    aio_context_t ctx;               /* KAIO handle */
    struct io_event *events;         /* Array of KAIO response objects */
    unsigned n_events;               /* Length of the events array */
    raft__queue write_queue;         /* Queue of inflight write requests */
    raft__uv_file_close_cb close_cb; /* Close callback */
};

struct raft__uv_file_create
{
    void *data;                 /* User data */
    struct raft__uv_file *file; /* File handle */
    int status;                 /* Request result code */
    struct uv_work_s work;      /* To execute logic in the threadpool */
    raft__uv_file_create_cb cb; /* Callback to invoke upon request completion */
    const char *path;           /* File path */
    size_t size;                /* File size */
};

struct raft__uv_file_write
{
    void *data;                 /* User data */
    struct raft__uv_file *file; /* File handle */
    int status;                 /* Request result code */
    struct uv_work_s work;      /* To execute logic in the threadpool */
    raft__uv_file_write_cb cb;  /* Callback to invoke upon request completion */
    struct iocb iocb;           /* KAIO request (for writing) */
    raft__queue queue;          /* Prev/next links in the inflight queue */
};

#endif /* RAFT_UV_FILE_H_ */
