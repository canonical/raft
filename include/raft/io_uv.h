#ifndef RAFT_IO_UV_H
#define RAFT_IO_UV_H

#include <uv.h>

#define RAFT_IO_UV_METADATA_SIZE (8 * 5)              /* Five 64-bit words */
#define RAFT_IO_UV_MAX_SEGMENT_SIZE (8 * 1024 * 1024) /* 8 Megabytes */

struct raft_logger;
struct raft_io;

/**
 * Interface to establish outgoing connections to other Raft servers and to
 * accept incoming connections from them.
 */
struct raft_io_uv_transport
{
    /**
     * User defined data.
     */
    void *data;

    /**
     * Start accepting incoming connections.
     *
     * Once a new connection is accepted, the @cb callback passed in the
     * initializer must be invoked with the relevant details of the connecting
     * Raft server.
     */
    int (*start)(struct raft_io_uv_transport *t,
                 unsigned id,
                 const char *address,
                 void *data,
                 void (*cb)(void *data,
                            unsigned id,
                            const char *address,
                            uv_stream_t *stream));

    /**
     * Stop accepting incoming connections.
     *
     * The @cb callback must be invoked once done.
     */
    void (*stop)(struct raft_io_uv_transport *t,
                 void *data,
                 void (*cb)(void *data));

    /**
     * Establish a connection with the server with the given ID and address.
     *
     * If this function returns with no error, then the @stream pointer must be
     * initialized with a stream handle that calling code can start writing to:
     * the connection probably hasn't been established yet, but the OS will
     * buffer writes and flush them when ready. The handle pointed to by the
     * @stream pointer must have been allocated with @raft_malloc, and once this
     * function returns ownership of the memory is transferred to the caller,
     * which is in charge of releasing it.
     *
     * The @cb callback must be invoked when the connection has been established
     * or the connection attempt has failed.
     */
    int (*connect)(struct raft_io_uv_transport *t,
                   unsigned id,
                   const char *address,
                   struct uv_stream_s **stream,
                   void *data,
                   void (*cb)(void *data, int status));
};

/**
 * Init a transport interface that uses TCP sockets.
 */
int raft_io_uv_tcp_init(struct raft_io_uv_transport *t,
                        struct raft_logger *logger,
                        struct uv_loop_s *loop);

void raft_io_uv_tcp_close(struct raft_io_uv_transport *t);

/**
 * Configure the given @raft_io instance to use a libuv-based I/O
 * implementation.
 *
 * The @dir path will be copied, and its memory can possibly be reased once this
 * function returns.
 *
 * The implementation of metadata and log persistency is virtually the same as
 * the one found in LogCabin [0].
 *
 * The disk files consist of metadata files, closed segments, and open
 * segments. Metadata files are used to track Raft metadata, such as the
 * server's current term, vote, and log's start index. Segments contain
 * contiguous entries that are part of the log. Closed segments are never
 * written to again (but may be renamed and truncated if a suffix of the log is
 * truncated). Open segments are where newly appended entries go. Once an open
 * segment reaches RAFT_IO_UV_MAX_SEGMENT_SIZE, it is closed and a new one is
 * used.
 *
 * Metadata files are named "metadata1" and "metadata2". The code alternates
 * between these so that there is always at least one readable metadata file.
 * On boot, the readable metadata file with the higher version number is used.
 *
 * The format of a metadata file is:
 *
 * [8 bytes] Format (currently 1).
 * [8 bytes] Incremental version number.
 * [8 bytes] Current term.
 * [8 bytes] ID of server we voted for.
 * [8 bytes] First index of the log.
 *
 * Closed segments are named by the format string "%020lu-%020lu" with their
 * start and end indexes, both inclusive. Closed segments always contain at
 * least one entry; the end index is always at least as large as the start
 * index. Closed segment files may occasionally include data past their
 * filename's end index (these are ignored but a warning is logged). This can
 * happen if the suffix of the segment is truncated and a crash occurs at an
 * inopportune time (the segment file is first renamed, then truncated, and a
 * crash occurs in between).
 *
 * Open segments are named by the format string "open-%lu" with a unique
 * number. These should not exist when the server shuts down cleanly, but they
 * exist while the server is running and may be left around during a crash.
 * Open segments either contain entries which come after the last closed
 * segment or are full of zeros. When the server crashes while appending to an
 * open segment, the end of that file may be corrupt. We can't distinguish
 * between a corrupt file and a partially written entry. The code assumes it's
 * a partially written entry, logs a warning, and ignores it.
 *
 * Truncating a suffix of the log will remove all entries that are no longer
 * part of the log. Truncating a prefix of the log will only remove complete
 * segments that are before the new log start index. For example, if a
 * segment has entries 10 through 20 and the prefix of the log is truncated to
 * start at entry 15, that entire segment will be retained.
 *
 * Each segment file starts with a segment header, which currently contains
 * just an 8-byte version number for the format of that segment. The current
 * format (version 1) is just a concatenation of serialized entry batches.
 *
 * Each batch has the following format:
 *
 * [4 bytes] CRC32 checksum of the batch header, little endian.
 * [4 bytes] CRC32 checksum of the batch data, little endian.
 * [  ...  ] Batch (as described in @raft_decode_entries_batch).
 *
 * [0] https://github.com/logcabin/logcabin/blob/master/Storage/SegmentedLog.h
 */
int raft_io_uv_init(struct raft_io *io,
                    struct raft_logger *logger,
                    struct uv_loop_s *loop,
                    const char *dir,
                    struct raft_io_uv_transport *transport);

void raft_io_uv_close(struct raft_io *io);

#endif /* RAFT_IO_UV_H */
