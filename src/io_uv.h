/**
 * Implementation of the @raft_io interface based on libuv.
 */

#ifndef RAFT_IO_UV_H_
#define RAFT_IO_UV_H_

#include "../include/raft.h"

#include "io_uv_metadata.h"
#include "uv_file.h"

/**
 * Current disk format version.
 */
#define IO_UV__DISK_FORMAT 1

/**
 * State codes.
 */
enum { IO_UV__ACTIVE = 1, IO_UV__CLOSING, IO_UV__CLOSED };

struct io_uv__client;
struct io_uv__server;

typedef unsigned long long io_uv__counter;

/**
 * Implementation of the raft_io interface.
 */
struct io_uv
{
    struct raft_io *io;                     /* I/O object we're implementing */
    struct uv_loop_s *loop;                 /* UV event loop */
    char *dir;                              /* Data directory */
    struct raft_io_uv_transport *transport; /* Network transport */
    unsigned id;                            /* Server ID */
    int state;                              /* Current state */
    bool errored;                           /* If a disk I/O error was hit */
    size_t block_size;                      /* Block size of the data dir */
    unsigned n_blocks;                      /* N. of blocks in a segment */
    struct io_uv__client **clients;         /* Outgoing connections */
    unsigned n_clients;                     /* Length of the clients array */
    struct io_uv__server **servers;         /* Incoming connections */
    unsigned n_servers;                     /* Length of the servers array */
    unsigned connect_retry_delay;           /* Client connection retry delay */
    struct uv__file *preparing;             /* File segment being prepared */
    raft__queue prepare_reqs;               /* Pending prepare requests. */
    raft__queue prepare_pool;               /* Prepared open segments */
    io_uv__counter prepare_next_counter;    /* Counter of next open segment */
    raft_index append_next_index;           /* Index of next entry to append */
    raft__queue append_segments;            /* Open segments in use. */
    raft__queue append_pending_reqs;        /* Pending append requests. */
    raft__queue append_writing_reqs;        /* Append requests in flight */
    raft__queue finalize_reqs;              /* Segments waiting to be closed */
    raft_index finalize_last_index;         /* Last index of last closed seg */
    struct uv_work_s finalize_work;         /* Resize and rename segments */
    raft__queue truncate_reqs;              /* Pending truncate requests */
    struct uv_work_s truncate_work;         /* Execute truncate log requests */
    raft__queue snapshot_put_reqs;          /* Inflight put snapshot requests */
    raft__queue snapshot_get_reqs;          /* Inflight get snapshot requests */
    struct uv_work_s snapshot_put_work;     /* Execute snapshot put requests */
    struct io_uv__metadata metadata;        /* Cache of metadata on disk */
    struct uv_timer_s timer;                /* Timer for periodic ticks */
    raft_io_tick_cb tick_cb;
    raft_io_recv_cb recv_cb;
    raft_io_close_cb close_cb;
};

/**
 * Request to obtain a newly prepared open segment.
 */
struct io_uv__prepare;
typedef void (*io_uv__prepare_cb)(struct io_uv__prepare *req,
                                  struct uv__file *file,
                                  unsigned long long counter,
                                  int status);
struct io_uv__prepare
{
    void *data;           /* User data */
    io_uv__prepare_cb cb; /* Completion callback */
    raft__queue queue;    /* Links in uv_io->prepare_reqs */
};

/**
 * Submit a request to get a prepared open segment ready for writing.
 */
void io_uv__prepare(struct io_uv *uv,
                    struct io_uv__prepare *req,
                    io_uv__prepare_cb cb);

/**
 * Cancel all pending prepare requests and remove all unused prepared open
 * segments. If a segment currently being created, wait for it to complete and
 * then remove it immediately.
 */
void io_uv__prepare_stop(struct io_uv *uv);

/**
 * Implementation of raft_io->append.
 */
int io_uv__append(struct raft_io *io,
                  const struct raft_entry entries[],
                  unsigned n,
                  void *data,
                  void (*cb)(void *data, int status));

/**
 * Callback invoked after completing a truncate request. If there are append
 * requests that have accumulated in while the truncate request was executed,
 * they will be processed now.
 */
void io_uv__append_unblock(struct io_uv *uv);

/**
 * Cancel all pending write requests and request the current segment to be
 * finalized. Must be invoked at closing time.
 */
void io_uv__append_stop(struct io_uv *uv);

/**
 * Tell the append implementation that the open segment currently being written
 * must be flushed. The implementation will:
 *
 * - Request a new prepared segment and target all newly submitted append
 *   requests to it.
 *
 * - Wait for any inflight write against the current segment to complete and
 *   then submit a request to finalize it.
 */
int io_uv__append_flush(struct io_uv *uv);

/**
 * Implementation of raft_io->send.
 */
int io_uv__send(struct raft_io *io,
                struct raft_io_send *req,
                const struct raft_message *message,
                raft_io_send_cb cb);

/**
 * Stop all clients by closing the outbound stream handles and canceling all
 * pending send requests.
 */
void io_uv__clients_stop(struct io_uv *uv);

/**
 * Start listening for new incoming connections.
 */
int io_uv__listen(struct io_uv *uv);

/**
 * Stop all servers by closing the inbound stream handles and aborting all
 * requests being received.
 */
void io_uv__servers_stop(struct io_uv *uv);

/**
 * Implementation of raft_io->truncate.
 */
int io_uv__truncate(struct raft_io *io, raft_index index);

/**
 * Cancel all pending truncate requests.
 */
void io_uv__truncate_stop(struct io_uv *uv);

/**
 * Callback invoked after a segment has been finalized. It will check if there
 * are pending truncate requests waiting for open segments to be finalized, and
 * possibly start executing the oldest one of them if no unfinalized open
 * segment is left.
 */
void io_uv__truncate_unblock(struct io_uv *uv);

/**
 * Submit a request to finalize the open segment with the given counter.
 *
 * Requests are processed one at a time, to avoid ending up closing open segment
 * N + 1 before closing open segment N.
 */
int io_uv__finalize(struct io_uv *uv,
                    unsigned long long counter,
                    size_t used,
                    raft_index first_index,
                    raft_index last_index);

/**
 * Callback that the segment finalize sub-system must invoke when it has
 * completed its pending tasks and.
 */
void io_uv__finalize_stop_cb(struct io_uv *uv);

/**
 * Implementation raft_io->snapshot_put.
 */
int io_uv__snapshot_put(struct raft_io *io,
                        struct raft_io_snapshot_put *req,
                        const struct raft_snapshot *snapshot,
                        raft_io_snapshot_put_cb cb);

/**
 * Callback invoked after truncation has completed, possibly unblocking pending
 * snapshot put requests.
 */
void io_uv__snapshot_put_unblock(struct io_uv *uv);

/**
 * Implementation of raft_io->snapshot_get.
 */
int io_uv__snapshot_get(struct raft_io *io,
                        struct raft_io_snapshot_get *req,
                        raft_io_snapshot_get_cb cb);

void io_uv__maybe_close(struct io_uv *uv);

#endif /* RAFT_IO_UV_H */
