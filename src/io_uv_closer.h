/**
 * Take care of truncating and renaming open segments that have become full, so
 * they become close.
 */

#ifndef RAFT_IO_UV_CLOSER_H_
#define RAFT_IO_UV_CLOSER_H_

#include "../include/raft.h"

#include "io_uv_loader.h"
#include "queue.h"

struct raft__io_uv_closer;

struct raft__io_uv_closer_segment;

/**
 * Truncation request.
 */
struct raft__io_uv_closer_truncate;

/**
 * Callback called after a truncation request has been completed.
 */
typedef void (*raft__io_uv_closer_truncate_cb)(
    struct raft__io_uv_closer_truncate *req,
    int status);

typedef void (*raft__io_uv_closer_close_cb)(struct raft__io_uv_closer *c);

int raft__io_uv_closer_init(struct raft__io_uv_closer *c,
                            struct uv_loop_s *loop,
                            struct raft_logger *logger,
                            struct raft__io_uv_loader *loader,
                            const char *dir);

void raft__io_uv_closer_close(struct raft__io_uv_closer *c,
                              raft__io_uv_closer_close_cb cb);

/**
 * Set the index of the last log entry. This should be called at start up right
 * after loading the state from disk.
 */
void raft__io_uv_closer_set_last_index(struct raft__io_uv_closer *c,
                                       raft_index index);
/**
 * Ask the closer to close the segment with the given counter. Requests are
 * processed one at a time, to avoid ending up closing open segment N + 1 before
 * closing open segment N.
 */
int raft__io_uv_closer_put(struct raft__io_uv_closer *c,
                           unsigned long long counter,
                           size_t used,
                           raft_index first_index,
                           raft_index last_index);

/**
 * Ask the closer to wait for the any segment being closed to be finalized and
 * then truncate the log from the given index included onwards.
 *
 * The @raft__io_uv_closer_put method must not be called until truncation is
 * completed.
 *
 * Requests are processed one at a time.
 */
int raft__io_uv_closer_truncate(struct raft__io_uv_closer *c,
                                raft_index index,
                                struct raft__io_uv_closer_truncate *req,
                                raft__io_uv_closer_truncate_cb cb);

/**
 * Return true if the closer is closing or has been closed.
 */
bool raft__io_uv_closer_is_closing(struct raft__io_uv_closer *c);

struct raft__io_uv_closer
{
    void *data;                           /* User data */
    struct uv_loop_s *loop;               /* Event loop */
    struct raft_logger *logger;           /* Logger */
    struct raft__io_uv_loader *loader;    /* Used for truncation */
    const char *dir;                      /* Data directory */
    raft_index last_index;                /* End index of last closed segment */
    int state;                            /* Current state code */
    raft__io_uv_closer_close_cb close_cb; /* Close callback */
    struct uv_work_s work;                /* Schedule work in the threadpool */

    /* Queue of pending put requests. */
    raft__queue put_queue;

    /* Open segment currently being closed. */
    struct raft__io_uv_closer_segment *closing;

    /* Queue of pending truncate requests. */
    raft__queue truncate_queue;

    /* Truncate request currently being processed. */
    struct raft__io_uv_closer_truncate *truncating;
};

struct raft__io_uv_closer_segment
{
    struct raft__io_uv_closer *closer;
    unsigned long long counter; /* Segment counter */
    size_t used;                /* Number of used bytes */
    raft_index first_index;     /* Index of first entry */
    raft_index last_index;      /* Index of last entry */
    int status;                 /* Status code of blocking syscalls */
    raft__queue queue;          /* Link to put queue */
};

struct raft__io_uv_closer_truncate
{
    void *data;                        /* User data */
    struct raft__io_uv_closer *closer; /* Reference to closer */
    raft_index index;                  /* Truncation point */
    raft__io_uv_closer_truncate_cb cb; /* Completion callback */
    int status;                        /* Status code */
    raft__queue queue;                 /* Link to truncate queue */
};

#endif /* RAFT_IO_UV_CLOSER_H */
