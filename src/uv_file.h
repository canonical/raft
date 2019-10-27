/* Create and write files asynchronously. */

#ifndef UV_FILE_H_
#define UV_FILE_H_

#include <linux/aio_abi.h>
#include <stdbool.h>
#include <uv.h>

#include "queue.h"
#include "uv_error.h"
#include "uv_os.h"

/* Handle to an open file. */
struct uvFile;

/* Create file request. */
struct uvFileCreate;

/* Write file request. */
struct uvFileWrite;

/* Callback called after a create file request has been completed. */
typedef void (*uvFileCreateCb)(struct uvFileCreate *req, int status);

/* Callback called after a write file request has been completed. */
typedef void (*uvFileWriteCb)(struct uvFileWrite *req,
                              int status,
                              const char *errmsg);

/* Callback called after the memory associated with a file handle can be
 * released. */
typedef void (*uvFileCloseCb)(struct uvFile *f);

/* Initialize a file handle. */
int uvFileInit(struct uvFile *f,
               struct uv_loop_s *loop,
               bool direct /* Whether to use direct I/O */,
               bool async /* Whether async I/O is available */);

/* Create the given file in the given directory for subsequent non-blocking
 * writing. The file must not exist yet. */
int uvFileCreate(struct uvFile *f,
                 struct uvFileCreate *req,
                 const char *dir,
                 const char *filename,
                 size_t size,
                 unsigned max_concurrent_writes,
                 uvFileCreateCb cb);

/* Asynchronously write data to the file associated with the given handle. */
int uvFileWrite(struct uvFile *f,
                struct uvFileWrite *req,
                const uv_buf_t bufs[],
                unsigned n_bufs,
                size_t offset,
                uvFileWriteCb cb,
                char *errmsg);

/* Return true if the given file is open. */
bool uvFileIsOpen(struct uvFile *f);

/* Return an error message describing the last error occurred. The pointer is
 * valid until a different error occurs or uvFileClose is called. */
const char *uvFileErrMsg(struct uvFile *f);

/* Close the given file and release all associated resources. */
void uvFileClose(struct uvFile *f, uvFileCloseCb cb);

struct uvFile
{
    void *data;                    /* User data */
    struct uv_loop_s *loop;        /* Event loop */
    int state;                     /* Current state code */
    int fd;                        /* Operating system file descriptor */
    bool direct;                   /* Whether direct I/O is supported */
    bool async;                    /* Whether fully async I/O is supported */
    int event_fd;                  /* Poll'ed to check if write is finished */
    struct uv_poll_s event_poller; /* To make the loop poll for event_fd */
    aio_context_t ctx;             /* KAIO handle */
    struct io_event *events;       /* Array of KAIO response objects */
    unsigned n_events;             /* Length of the events array */
    queue write_queue;             /* Queue of inflight write requests */
    bool closing;                  /* True during the close sequence */
    char *errmsg;                  /* Describe last error occured */
    uvFileCloseCb close_cb;        /* Close callback */
};

struct uvFileCreate
{
    void *data;            /* User data */
    struct uvFile *file;   /* File handle */
    int status;            /* Request result code */
    struct uv_work_s work; /* To execute logic in the threadpool */
    uvFileCreateCb cb;     /* Callback to invoke upon request completion */
    const char *dir;       /* File directory */
    const char *filename;  /* File name */
    size_t size;           /* File size */
};

struct uvFileWrite
{
    void *data;            /* User data */
    struct uvFile *file;   /* File handle */
    size_t len;            /* Total number of bytes to write */
    int status;            /* Request result code */
    uvErrMsg errmsg;       /* Error message (for status != 0) */
    struct uv_work_s work; /* To execute logic in the threadpool */
    uvFileWriteCb cb;      /* Callback to invoke upon request completion */
    struct iocb iocb;      /* KAIO request (for writing) */
    queue queue;           /* Prev/next links in the inflight queue */
};

#endif /* UV_FILE_H_ */
