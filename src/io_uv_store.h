/**
 * Handle on-disk storage logic in the libuv-based @raft_io backend.
 */

#ifndef RAFT_IO_UV_STORE_H
#define RAFT_IO_UV_STORE_H

#include "uv_file.h"

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
    unsigned next_block;        /* Next segment block to write (starting at 0)*/
    size_t block_size;          /* Block size */
    size_t used;                /* How many bytes have been used in total */
    raft_index first_index;     /* Index of the first entry of the segment */
    raft_index end_index;       /* Index of the last entry of the segment */

    struct raft__uv_file file;          /* Open segment file */
    struct raft__uv_file_create create; /* Create file request */
    struct raft__uv_file_write write;   /* Write file system request */

    raft__uv_file_path path; /* Full file system path */
};

/**
 * Hold data buffers for N contiguous blocks of a segment.
 */
struct raft_io_uv_blocks
{
    size_t block_size; /* Block size */
    uv_buf_t *bufs;    /* Block buffers, each of block_size bytes */
    unsigned n_bufs;   /* Number of buffers in the array */
    size_t offset;     /* Offset of next byte to write */
};

/**
 * Hold details about a callback to invoke once a request to append entries has
 * completed.
 */
struct raft_io_uv_append_cb
{
    void *data;
    void (*f)(void *data, int status);
};

struct raft_io_uv_store
{
    struct raft_logger *logger; /* Logger to use */
    char *dir;                  /* Data directory */
    size_t block_size;          /* File system block size */
    size_t max_segment_size;    /* Maximum segment size */
    struct uv_loop_s *loop;     /* libuv loop to hook into */

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
        int state; /* Either idle, blocked, or writing */

        /* Buffers holding data for the blocks to write via direct I/O */
        struct raft_io_uv_blocks blocks;

        /* The prepared open segment in the pool currently being written. */
        struct raft_io_uv_prepared *segment;

        int status; /* Current result code */

        /* Index of the next entry to write to the block buffers */
        raft_index next_index;

        /* Index of the last entry that was actually written to disk. */
        raft_index last_index;

        /* Callbacks to invoke when a write operation has completed. */
        struct raft_io_uv_append_cb *cbs;
        size_t n_cbs;

    } writer;

    /* Queue of append entries request */
    struct
    {
        /* Enqueued entries */
        struct raft_entry *entries;
        size_t n_entries;

        /* Callbacks to invoke when the write operation for the enqueued entries
         * will complete. */
        struct raft_io_uv_append_cb *cbs;
        size_t n_cbs;
    } queue;

    /* State for the logic involved in closing open segments. */
    struct
    {
        struct uv_work_s work;               /* To run blocking syscalls. */
        struct raft_io_uv_prepared *segment; /* Segment being closed */
        int status;                          /* Current result code */
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
                           struct raft_logger *logger,
                           struct uv_loop_s *loop,
                           const char *dir);

void raft_io_uv_store__close(struct raft_io_uv_store *s);

/**
 * Start accepting disk I/O requests.
 */
int raft_io_uv_store__start(struct raft_io_uv_store *s);

/**
 * Stop any on-going write as soon as possible. Invoke @cb when the dust is
 * settled.
 */
void raft_io_uv_store__stop(struct raft_io_uv_store *s,
                            void *p,
                            void (*cb)(void *p));

/**
 * Synchronously load all state from disk.
 */
int raft_io_uv_store__load(struct raft_io_uv_store *s,
                           raft_term *term,
                           unsigned *voted_for,
                           raft_index *first_index,
                           struct raft_entry **entries,
                           size_t *n);

/**
 * Synchronously write the given encoded configuration as first entry and set
 * the term to 1.
 */
int raft_io_uv_store__bootstrap(struct raft_io_uv_store *s,
                                const struct raft_configuration *configuration);

/**
 * Synchronously persist the term in the given request.
 */
int raft_io_uv_store__term(struct raft_io_uv_store *s, const raft_term term);

/**
 * Synchronously persist the vote in the given request.
 */
int raft_io_uv_store__vote(struct raft_io_uv_store *s,
                           const unsigned server_id);

/**
 * Asynchronously append the entries in the given request.
 */
int raft_io_uv_store__append(struct raft_io_uv_store *s,
                             const struct raft_entry *entries,
                             const unsigned n,
                             void *data,
                             void (*cb)(void *data, int status));

/**
 * Asynchronously truncate any entry from @index onward.
 */
int raft_io_uv_store__truncate(struct raft_io_uv_store *s, raft_index index);

#endif /* RAFT_IO_UV_STORE_H */
