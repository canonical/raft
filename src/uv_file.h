/**
 * Create and write files asynchronously, using libuv on top of Linux AIO (aka
 * KAIO)..
 */

#ifndef RAFT_UV_FILE_H_
#define RAFT_UV_FILE_H_

#include <linux/aio_abi.h>
#include <stdbool.h>

#include <uv.h>

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
 * Convenience to concatenate a directory and a file.
 */
void raft__uv_file_join(const char *dir, const char *filename, char *path);

/**
 * Get the logical block size of the file system rooted at @dir.
 */
int raft__uv_file_block_size(const char *dir, size_t *size);

/**
 * Create the given file for subsequent non-blocking writing. The file must not
 * exist yet.
 */
int raft__uv_file_create(struct raft__uv_file *f,
                         struct raft__uv_file_create *req,
                         struct uv_loop_s *loop,
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
                        unsigned n,
                        size_t offset,
                        raft__uv_file_write_cb cb);

/**
 * Close the given file and release all associated resources. There must be no
 * request in progress.
 */
int raft__uv_file_close(struct raft__uv_file *f);

struct raft__uv_file
{
    struct uv_loop_s *loop;        /* For eventd polling and thread work */
    int fd;                        /* Operating system file descriptor */
    bool async;                    /* Whether fully async I/O is supported */
    int event_fd;                  /* Poll'ed to check if write has completed */
    struct uv_poll_s event_poller; /* To make the loop poll for event_fd */
    aio_context_t ctx;             /* KAIO handle */
    struct io_event *events;       /* Array of KAIO response objects */
    unsigned n_events;             /* Length of the events array */
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
};

#endif /* RAFT_UV_FILE_H_ */
