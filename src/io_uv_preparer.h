/**
 * Allocate new open segments ahead of time so they can be used immediately by
 * the writer once the current open segment becomes full.
 */

#ifndef RAFT_IO_UV_PREPARER_H_
#define RAFT_IO_UV_PREPARER_H_

#include "../include/raft.h"

#include "io_uv_fs.h"
#include "uv_file.h"

/**
 * Number of open segments that the preparer will try to keep ready for writing.
 */
#define RAFT__IO_UV_PREPARER_N 2

/**
 * Segment preparer.
 */
struct raft__io_uv_preparer;

/**
 * Hold details about a new open segment being prepared.
 */
struct raft__io_uv_preparer_segment;

/**
 * Get prepared open segment request.
 */
struct raft__io_uv_preparer_get;

/**
 * Callback called after a get prepared open segment request has been completed.
 */
typedef void (*raft__io_uv_preparer_get_cb)(
    struct raft__io_uv_preparer_get *req,
    struct raft__uv_file *file,
    unsigned long long counter,
    int status);

/**
 * Callback called after the memory associated with a preparer can be released.
 */
typedef void (*raft__io_uv_preparer_close_cb)(struct raft__io_uv_preparer *p);

int raft__io_uv_preparer_init(struct raft__io_uv_preparer *p,
                              struct uv_loop_s *loop,
                              struct raft_logger *logger,
                              const char *dir,
                              size_t block_size,
                              unsigned n_blocks);

void raft__io_uv_preparer_close(struct raft__io_uv_preparer *p,
                                raft__io_uv_preparer_close_cb cb);

/**
 * Submit a request to get a prepared open segment ready for writing.
 */
int raft__io_uv_preparer_get(struct raft__io_uv_preparer *p,
                             struct raft__io_uv_preparer_get *req,
                             raft__io_uv_preparer_get_cb cb);

/**
 * Return true if the preparer is closing or has been closed.
 */
bool raft__io_uv_preparer_is_closing(struct raft__io_uv_preparer *p);

struct raft__io_uv_preparer
{
    void *data;                          /* User data */
    struct uv_loop_s *loop;              /* Event loop */
    struct raft_logger *logger;          /* Logger */
    const char *dir;                     /* Data directory */
    int state;                           /* Current state code */
    size_t block_size;                   /* Block size of the data dir */
    unsigned n_blocks;                   /* N. of blocks in a segment */
    unsigned long long next_counter;     /* Counter of next segment */
    raft__io_uv_preparer_close_cb close_cb; /* Close callback */

    /* Queue of open segments that are being created. */
    raft__queue create_queue;

    /* Pool of prepared open segments ready to be used. The preparer will try to
     * keep the length of the queue equal to RAFT__IO_UV_PREPARER_N.*/
    raft__queue ready_queue;

    /* Queue of open segments that are being closed. */
    raft__queue close_queue;

    /* Queue of pending get requests. */
    raft__queue get_queue;
};

struct raft__io_uv_preparer_get
{
    void *data;                     /* User data */
    raft__io_uv_preparer_get_cb cb; /* Completion callback */
    raft__queue queue;              /* Links to the get queue */
};

struct raft__io_uv_preparer_segment
{
    struct raft__io_uv_preparer *preparer;
    struct raft__uv_file *file;
    struct raft__uv_file_create create; /* Create file request */
    unsigned long long counter;         /* Segment counter */
    raft__io_uv_fs_path path;           /* Path of the segment */
    raft__queue queue;                  /* Create, ready or close queue */
};

#endif /* RAFT_IO_UV_PREPARER_H */
