#ifndef RAFT_IO_UV_H
#define RAFT_IO_UV_H

#include <uv.h>

#define RAFT_IO_UV_METADATA_SIZE (8 * 5)              /* Five 64-bit words */
#define RAFT_IO_UV_MAX_SEGMENT_SIZE (8 * 1024 * 1024) /* 8 Megabytes */

struct raft_io;

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
 * [0] https://github.com/logcabin/logcabin/blob/master/Storage/SegmentedLog.h
 */
int raft_io_uv_init(struct raft_io *io,
                    struct uv_loop_s *loop,
                    const char *dir);

#endif /* RAFT_IO_UV_H */
