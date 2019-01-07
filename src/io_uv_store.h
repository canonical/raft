/**
 * Handle on-disk storage logic in the libuv-based @raft_io backend.
 */

#ifndef RAFT_IO_UV_STORE_H
#define RAFT_IO_UV_STORE_H

#include "uv_fs.h"

#include "../include/raft.h"

/**
 * Number of open segments that the store will try to prepare and keep ready for
 * writing.
 */
#define RAFT_IO_UV_STORE__N_PREPARED 3

/**
 * Status codes for prepared open segments.
 *
 * - pending: the segment is being prepared
 * - ready: the segment is ready to be written
 * - closing: the segment is being closed
 */
enum {
    RAFT_IO_UV_STORE__PREPARED_PENDING = 0,
    RAFT_IO_UV_STORE__PREPARED_READY,
    RAFT_IO_UV_STORE__PREPARED_CLOSING
};

/**
 * Information persisted in a single metadata file.
 */
struct raft_io_uv_metadata
{
    unsigned long long version; /* Monotonically increasing version */
    raft_term term;             /* Current term */
    unsigned voted_for;         /* Server ID of last vote, or 0 */
    raft_index start_index;     /* Raft log start index */
};

/**
 * A single prepared open segment that new entries can be written into, when
 * ready.
 */
struct raft_io_uv_prepared
{
    int state;                  /* Whether we're pending, ready or closing */
    unsigned long long counter; /* Segment counter, encoded in the filename */
    struct raft_uv_file file;   /* Open segment file */
    struct raft_uv_fs req;      /* File system request */
    unsigned block;             /* Next segment block to write */
    size_t offset;              /* Next free byte in the next segment block */
    size_t used;                /* How many bytes have been used in total */
    raft_index first_index;     /* Index of the first entry of the segment */
    raft_index end_index;       /* Index of the last entry of the segment */
    raft_uv_path path;          /* Full file system path */
};

struct raft_io_uv_store
{
    char *dir;               /* Data directory */
    size_t block_size;       /* File system block size */
    size_t max_segment_size; /* Maximum segment size */
    struct uv_loop_s *loop;  /* libuv loop to hook into */

    /* Cache of the last metadata file that was written (either metadata1 or
     * metadata2). */
    struct raft_io_uv_metadata metadata;

    /* State for the logic involved in preparing new open segments. */
    struct
    {
        uv_buf_t buf; /* Used to write the format version */
        struct raft_io_uv_prepared *segment; /* Segment being prepared */
    } preparer;

    /* State for the logic involved in writing log entries. */
    struct
    {
        const struct raft_entry *entries; /* Entries being written */
        unsigned n;                       /* Number of entries */
        void *p;                          /* Callback context */
        void (*cb)(void *p, const int status, const char *errmsg);

        /* Array of re-usable write buffers, each of block_size bytes. */
        uv_buf_t *bufs;
        unsigned n_bufs;

        /* The prepared open segment in the pool currently being written. */
        struct raft_io_uv_prepared *segment;

        bool submitted;
        char errmsg[RAFT_ERRMSG_SIZE];
        int status; /* Current result code */

        /* Index of the next entry to append. */
        raft_index next_index;
    } writer;

    /* State for the logic involved in closing open segments. */
    struct
    {
        struct uv_work_s work;               /* To run blocking syscalls. */
        struct raft_io_uv_prepared *segment; /* Segment being closed */
        int status;                          /* Current result code */
        char errmsg[RAFT_ERRMSG_SIZE];       /* Current error message */
    } closer;

    /* Pool of prepared open segments */
    struct raft_io_uv_prepared pool[RAFT_IO_UV_STORE__N_PREPARED];

    /* State for tracking a request to stop the store . */
    struct
    {
        void *p;
        void (*cb)(void *p);
    } stop;

    /* Whether we aborted operations, due to errors or a stop request. */
    bool aborted;
};

int raft_io_uv_store__init(struct raft_io_uv_store *s,
                           const char *dir,
                           struct uv_loop_s *loop,
                           char *errmsg);

/**
 * Synchronously load all state from disk.
 */
int raft_io_uv_store__load(struct raft_io_uv_store *s,
                           raft_term *term,
                           unsigned *voted_for,
                           raft_index *first_index,
                           struct raft_entry **entries,
                           size_t *n,
                           char *errmsg);

/**
 * Synchronously write the given encoded configuration as first entry and set
 * the term to 1.
 */
int raft_io_uv_store__bootstrap(struct raft_io_uv_store *s,
                                const struct raft_buffer *conf,
                                char *errmsg);

/**
 * Synchronously persist the term in the given request.
 */
int raft_io_uv_store__term(struct raft_io_uv_store *s,
                           const raft_term term,
                           char *errmsg);

/**
 * Synchronously persist the vote in the given request.
 */
int raft_io_uv_store__vote(struct raft_io_uv_store *s,
                           const unsigned server_id,
                           char *errmsg);

/**
 * Asynchronously persist the entries in the given request.
 */
int raft_io_uv_store__entries(struct raft_io_uv_store *s,
                              const struct raft_entry *entries,
                              const unsigned n,
                              void *p,
                              void (*cb)(void *p,
                                         const int status,
                                         const char *errmsg),
                              char *errmsg);

/**
 * Stop any on-going write as soon as possible. Invoke @cb when the dust is
 * settled.
 */
void raft_io_uv_store__stop(struct raft_io_uv_store *s,
                            void *p,
                            void (*cb)(void *p));

void raft_io_uv_store__close(struct raft_io_uv_store *s);

#endif /* RAFT_IO_UV_STORE_H */
