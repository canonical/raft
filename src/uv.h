/* Implementation of the @raft_io interface based on libuv. */

#ifndef UV_H_
#define UV_H_

#include "../include/raft.h"
#include "err.h"
#include "queue.h"
#include "tracing.h"
#include "uv_fs.h"
#include "uv_os.h"

/* 8 Megabytes */
#define UV__MAX_SEGMENT_SIZE (8 * 1024 * 1024)

/* Template string for closed segment filenames: start index (inclusive), end
 * index (inclusive). */
#define UV__CLOSED_TEMPLATE "%016llu-%016llu"

/* Template string for open segment filenames: incrementing counter. */
#define UV__OPEN_TEMPLATE "open-%llu"

/* Template string for snapshot filenames: snapshot term, snapshot index,
 * creation timestamp (milliseconds since epoch). */
#define UV__SNAPSHOT_TEMPLATE "snapshot-%llu-%llu-%llu"

/* Template string for snapshot metadata filenames: snapshot term,  snapshot
 * index, creation timestamp (milliseconds since epoch). */
#define UV__SNAPSHOT_META_TEMPLATE UV__SNAPSHOT_TEMPLATE ".meta"

/* State codes. */
enum {
    UV__PRISTINE, /* Metadata cache populated and I/O capabilities probed */
    UV__ACTIVE,
    UV__CLOSED
};

/* Open segment counter type */
typedef unsigned long long uvCounter;

/* Information persisted in a single metadata file. */
struct uvMetadata
{
    unsigned long long version; /* Monotonically increasing version */
    raft_term term;             /* Current term */
    unsigned voted_for;         /* Server ID of last vote, or 0 */
};

/* Hold state of a libuv-based raft_io implementation. */
struct uv
{
    struct raft_io *io;                  /* I/O object we're implementing */
    struct uv_loop_s *loop;              /* UV event loop */
    char dir[UV__DIR_LEN];               /* Data directory */
    struct raft_uv_transport *transport; /* Network transport */
    struct raft_tracer *tracer;          /* Debug tracing */
    unsigned id;                         /* Server ID */
    int state;                           /* Current state */
    bool errored;                        /* If a disk I/O error was hit */
    bool direct_io;                      /* Whether direct I/O is supported */
    bool async_io;                       /* Whether async I/O is supported */
    size_t segment_size;                 /* Initial size of open segments. */
    size_t block_size;                   /* Block size of the data dir */
    queue clients;                       /* Outbound connections */
    queue servers;                       /* Inbound connections */
    unsigned connect_retry_delay;        /* Client connection retry delay */
    void *prepare_inflight;              /* Segment being prepared */
    queue prepare_reqs;                  /* Pending prepare requests. */
    queue prepare_pool;                  /* Prepared open segments */
    uvCounter prepare_next_counter;      /* Counter of next open segment */
    raft_index append_next_index;        /* Index of next entry to append */
    queue append_segments;               /* Open segments in use. */
    queue append_pending_reqs;           /* Pending append requests. */
    queue append_writing_reqs;           /* Append requests in flight */
    queue finalize_reqs;                 /* Segments waiting to be closed */
    raft_index finalize_last_index;      /* Last index of last closed seg */
    struct uv_work_s finalize_work;      /* Resize and rename segments */
    queue truncate_reqs;                 /* Pending truncate requests */
    struct uv_work_s truncate_work;      /* Execute truncate log requests */
    queue snapshot_put_reqs;             /* Inflight put snapshot requests */
    queue snapshot_get_reqs;             /* Inflight get snapshot requests */
    struct uv_work_s snapshot_put_work;  /* Execute snapshot put requests */
    struct uvMetadata metadata;          /* Cache of metadata on disk */
    struct uv_timer_s timer;             /* Timer for periodic ticks */
    raft_io_tick_cb tick_cb;             /* Invoked when the timer expires */
    raft_io_recv_cb recv_cb;             /* Invoked when upon RPC messages */
    queue aborting;                      /* Cleanups upon errors or shutdown */
    bool closing;                        /* True if we are closing */
    raft_io_close_cb close_cb;           /* Invoked when finishing closing */
};

/* Load Raft metadata from disk, choosing the most recent version (either the
 * metadata1 or metadata2 file). */
int uvMetadataLoad(const char *dir, struct uvMetadata *metadata, char *errmsg);

/* Store the given metadata to disk, writing the appropriate metadata file
 * according to the metadata version (if the version is odd, write metadata1,
 * otherwise write metadata2). */
int uvMetadataStore(struct uv *uv, const struct uvMetadata *metadata);

/* Metadata about a segment file. */
struct uvSegmentInfo
{
    bool is_open; /* Whether the segment is open */
    union {
        struct
        {
            raft_index first_index; /* First index in a closed segment */
            raft_index end_index;   /* Last index in a closed segment */
        };
        struct
        {
            unsigned long long counter; /* Open segment counter */
        };
    };
    char filename[UV__FILENAME_LEN]; /* Segment filename */
};

/* Append a new item to the given segment info list if the given filename
 * matches either the one of a closed segment (xxx-yyy) or the one of an open
 * segment (open-xxx). */
int uvSegmentInfoAppendIfMatch(const char *filename,
                               struct uvSegmentInfo *infos[],
                               size_t *n_infos,
                               bool *appended);

/* Sort the given list of segments by comparing their filenames. Closed segments
 * come before open segments. */
void uvSegmentSort(struct uvSegmentInfo *infos, size_t n_infos);

/* Keep only the closed segments whose entries are within the given trailing
 * amount past the given snapshot last index. If no error occurs the location
 * pointed by 'deleted' will contain the index of the last segment that got
 * deleted, or 'n' if no segment got deleted. */
int uvSegmentKeepTrailing(struct uv *uv,
                          struct uvSegmentInfo *segments,
                          size_t n,
                          raft_index last_index,
                          size_t trailing,
                          size_t *deleted);

/* Load all entries contained in the given closed segment. */
int uvSegmentLoadClosed(struct uv *uv,
                        struct uvSegmentInfo *segment,
                        struct raft_entry *entries[],
                        size_t *n);

/* Load raft entries from the given segments. The @start_index is the expected
 * index of the first entry of the first segment. */
int uvSegmentLoadAll(struct uv *uv,
                     const raft_index start_index,
                     struct uvSegmentInfo *segments,
                     size_t n_segments,
                     struct raft_entry **entries,
                     size_t *n_entries);

/* Return the number of blocks in a segments. */
#define uvSegmentBlocks(UV) (UV->segment_size / UV->block_size)

/* A dynamically allocated buffer holding data to be written into a segment
 * file.
 *
 * The memory is aligned at disk block boundary, to allow for direct I/O. */
struct uvSegmentBuffer
{
    size_t block_size; /* Disk block size for direct I/O */
    uv_buf_t arena;    /* Previously allocated memory that can be re-used */
    size_t n;          /* Write offset */
};

/* Initialize an empty buffer. */
void uvSegmentBufferInit(struct uvSegmentBuffer *b, size_t block_size);

/* Release all memory used by the buffer. */
void uvSegmentBufferClose(struct uvSegmentBuffer *b);

/* Encode the format version at the very beginning of the buffer. This function
 * must be called when the buffer is empty. */
int uvSegmentBufferFormat(struct uvSegmentBuffer *b);

/* Extend the segment's buffer by encoding the given entries.
 *
 * Previous data in the buffer will be retained, and data for these new entries
 * will be appended. */
int uvSegmentBufferAppend(struct uvSegmentBuffer *b,
                          const struct raft_entry entries[],
                          unsigned n_entries);

/* After all entries to write have been encoded, finalize the buffer by zeroing
 * the unused memory of the last block. The out parameter will point to the
 * memory to write. */
void uvSegmentBufferFinalize(struct uvSegmentBuffer *b, uv_buf_t *out);

/* Reset the buffer preparing it for the next segment write.
 *
 * If the retain parameter is greater than zero, then the data of the retain'th
 * block will be copied at the beginning of the buffer and the write offset will
 * be set accordingly. */
void uvSegmentBufferReset(struct uvSegmentBuffer *b, unsigned retain);

/* Write a closed segment, containing just one entry at the given index
 * for the given configuration. */
int uvSegmentCreateClosedWithConfiguration(
    struct uv *uv,
    raft_index index,
    const struct raft_configuration *configuration);

/* Write the first closed segment, containing just one entry for the given
 * configuration. */
int uvSegmentCreateFirstClosed(struct uv *uv,
                               const struct raft_configuration *configuration);

/* Truncate a segment that was already closed. */
int uvSegmentTruncate(struct uv *uv,
                      struct uvSegmentInfo *segment,
                      raft_index index);

/* Info about a persisted snapshot stored in snapshot metadata file. */
struct uvSnapshotInfo
{
    raft_term term;
    raft_index index;
    unsigned long long timestamp;
    char filename[UV__FILENAME_LEN];
};

/* Append a new item to the given snapshot info list if the given filename
 * matches the one of a snapshot metadata file (snapshot-xxx-yyy-zzz.meta) and
 * there is actually a matching snapshot file on disk. */
int uvSnapshotInfoAppendIfMatch(struct uv *uv,
                                const char *filename,
                                struct uvSnapshotInfo *infos[],
                                size_t *n_infos,
                                bool *appended);

/* Sort the given list of snapshots by comparing their filenames. Older
 * snapshots will come first. */
void uvSnapshotSort(struct uvSnapshotInfo *infos, size_t n_infos);

/* Load the snapshot associated with the given metadata. */
int uvSnapshotLoad(struct uv *uv,
                   struct uvSnapshotInfo *meta,
                   struct raft_snapshot *snapshot);

/* Remove all all snapshots except the last two. */
int uvSnapshotKeepLastTwo(struct uv *uv,
                          struct uvSnapshotInfo *snapshots,
                          size_t n);

/* Return a list of all snapshots and segments found in the data directory. Both
 * snapshots and segments are ordered by filename (closed segments come before
 * open ones). */
int uvList(struct uv *uv,
           struct uvSnapshotInfo *snapshots[],
           size_t *n_snapshots,
           struct uvSegmentInfo *segments[],
           size_t *n_segments);

/* Request to obtain a newly prepared open segment. */
struct uvPrepare;
typedef void (*uvPrepareCb)(struct uvPrepare *req, int status);
struct uvPrepare
{
    void *data;                 /* User data */
    uv_file fd;                 /* Resulting segment file descriptor */
    unsigned long long counter; /* Resulting segment counter */
    uvPrepareCb cb;             /* Completion callback */
    queue queue;                /* Links in uv_io->prepare_reqs */
};

/* Submit a request to get a prepared open segment ready for writing. */
void UvPrepare(struct uv *uv, struct uvPrepare *req, uvPrepareCb cb);

/* Cancel all pending prepare requests and start removing all unused prepared
 * open segments. If a segment currently being created, wait for it to complete
 * and then remove it immediately. */
void uvPrepareClose(struct uv *uv);

/* Callback invoked after completing a truncate request. If there are append
 * requests that have accumulated in while the truncate request was executed,
 * they will be processed now. */
void uvAppendMaybeProcessRequests(struct uv *uv);

/* Fix the first index of the last segment that we requested to prepare, to
 * reflect that we're restoring a snapshot. */
void uvAppendFixPreparedSegmentFirstIndex(struct uv *uv);

/* Cancel all pending write requests and request the current segment to be
 * finalized. Must be invoked at closing time. */
void uvAppendClose(struct uv *uv);

/* Tell the append implementation that the open segment currently being written
 * must be finalized, even if it's not full yet. The implementation will:
 *
 * - Request a new prepared segment and target all newly submitted append
 *   requests to it.
 *
 * - Wait for any inflight write against the current segment to complete and
 *   then submit a request to finalize it. */
int uvAppendForceFinalizingCurrentSegment(struct uv *uv);

/* Submit a request to finalize the open segment with the given counter.
 *
 * Requests are processed one at a time, to avoid ending up closing open segment
 * N + 1 before closing open segment N. */
int uvFinalize(struct uv *uv,
               unsigned long long counter,
               size_t used,
               raft_index first_index,
               raft_index last_index);

/* Cancel all pending truncate requests. */
void uvTruncateClose(struct uv *uv);

/* Callback invoked after a segment has been finalized. It will check if there
 * are pending truncate requests waiting for open segments to be finalized, and
 * possibly start executing the oldest one of them if no unfinalized open
 * segment is left. */
void uvTruncateMaybeProcessRequests(struct uv *uv);

/* Stop all clients by closing the outbound stream handles and canceling all
 * pending send requests.  */
void uvSendClose(struct uv *uv);

/* Start receiving messages from new incoming connections. */
int uvRecvStart(struct uv *uv);

/* Stop all servers by closing the inbound stream handles and aborting all
 * requests being received.  */
void uvRecvClose(struct uv *uv);

/* Callback invoked after truncation has completed, possibly unblocking pending
 * snapshot put requests. */
void uvSnapshotMaybeProcessRequests(struct uv *uv);

void uvMaybeFireCloseCb(struct uv *uv);

#endif /* UV_H_ */
