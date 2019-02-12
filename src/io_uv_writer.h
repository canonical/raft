/**
 * Write log entries to disk.
 */

#ifndef RAFT_IO_UV_WRITER_H_
#define RAFT_IO_UV_WRITER_H_

#include "../include/raft.h"

#include "io_uv_closer.h"
#include "io_uv_preparer.h"

/**
 * Current on-disk format version.
 */
#define RAFT__IO_UV_WRITER_FORMAT 1

struct raft__io_uv_writer;

struct raft__io_uv_writer_segment;

struct raft__io_uv_writer_append;

struct raft__io_uv_writer_truncate;

typedef void (*raft__io_uv_writer_append_cb)(
    struct raft__io_uv_writer_append *req,
    int status);

typedef void (*raft__io_uv_writer_close_cb)(struct raft__io_uv_writer *w);

int raft__io_uv_writer_init(struct raft__io_uv_writer *w,
                            struct uv_loop_s *loop,
                            struct raft_logger *logger,
                            struct raft__io_uv_preparer *preparer,
                            struct raft__io_uv_closer *closer,
                            const char *dir,
                            size_t block_size,
                            unsigned n_blocks);

void raft__io_uv_writer_close(struct raft__io_uv_writer *p,
                              raft__io_uv_writer_close_cb cb);

void raft__io_uv_writer_set_next_index(struct raft__io_uv_writer *w,
                                       raft_index index);

int raft__io_uv_writer_append(struct raft__io_uv_writer *w,
                              struct raft__io_uv_writer_append *req,
                              const struct raft_entry *entries,
                              unsigned n,
                              raft__io_uv_writer_append_cb cb);

int raft__io_uv_writer_truncate(struct raft__io_uv_writer *w, raft_index index);

/**
 * Return true if the writer is closing or has been closed.
 */
bool raft__io_uv_writer_is_closing(struct raft__io_uv_writer *p);

struct raft__io_uv_writer
{
    void *data;                            /* User data */
    struct uv_loop_s *loop;                /* Event loop */
    struct raft_logger *logger;            /* Logger */
    struct raft__io_uv_preparer *preparer; /* Segments preparer */
    struct raft__io_uv_closer *closer;     /* Segments cloer */
    const char *dir;                       /* Data directory */
    raft_index next_index;                 /* Index of next entry */
    raft_index block_size;                 /* Segment block size */
    unsigned n_blocks;                     /* N. of blocks in a segment */
    int state;                             /* Current state code */
    raft__io_uv_writer_close_cb close_cb;  /* Close callback */

    /* Open segments that we are currently writing. */
    raft__queue segment_queue;

    /* Queue of pending append requests. */
    raft__queue append_queue;

    /* Queue of pending write requests. */
    raft__queue write_queue;

    /* Queue of pending truncate requests. */
    raft__queue truncate_queue;
};

struct raft__io_uv_writer_segment
{
    struct raft__io_uv_writer *writer;   /* Our writer */
    struct raft__io_uv_preparer_get get; /* Get segment file request */
    struct raft__uv_file_write write;    /* Write request */
    unsigned long long counter;          /* Open segment counter */
    struct raft__uv_file *file;          /* File to write to */
    raft_index first_index;              /* Index of the first entry written */
    raft_index last_index;               /* Index of the last entry written */
    size_t size;                         /* Total number of bytes used */
    unsigned next_block;                 /* Next segment block to write */
    uv_buf_t arena;                      /* Memory for the write buffer */
    uv_buf_t buf;                        /* Write buffer */
    size_t scheduled;                    /* Number of bytes of the next write */
    size_t written;                      /* Number of bytes actually written */
    raft__queue queue;                   /* Segment queue */
};

struct raft__io_uv_writer_append
{
    void *data;                                 /* User data */
    const struct raft_entry *entries;           /* Entries to write */
    unsigned n;                                 /* Number of entries */
    size_t size;                                /* Size of this batch on disk */
    struct raft__io_uv_writer_segment *segment; /* Segment to write to */
    int status;
    raft__io_uv_writer_append_cb cb;
    raft__queue queue;
};

struct raft__io_uv_writer_truncate
{
    struct raft__io_uv_writer *writer;          /* Our writer */
    struct raft__io_uv_writer_segment *segment; /* Segment we were writing */
    raft_index index;
    struct raft__io_uv_closer_truncate *truncate;
    raft__queue queue;
};

#endif /* RAFT_IO_UV_WRITER_H */
