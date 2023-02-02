/* Encoding routines for the the libuv-based @raft_io backend. */

#ifndef UV_ENCODING_H_
#define UV_ENCODING_H_

#include <uv.h>

#include "../include/raft.h"

/* Current disk format version. */
#define UV__DISK_FORMAT 1

#define UV__SEGMENT_DISK_FORMAT_LEGACY UV__DISK_FORMAT
#define UV__SEGMENT_DISK_FORMAT_2 2

int uvEncodeMessage(const struct raft_message *message,
                    uv_buf_t **bufs,
                    unsigned *n_bufs);

int uvDecodeMessage(uint16_t type,
                    const uv_buf_t *header,
                    struct raft_message *message,
                    size_t *payload_len);

int uvDecodeBatchHeader(const void *batch,
                        struct raft_entry **entries,
                        unsigned *n);

void uvDecodeEntriesBatch(uint8_t *batch,
                          size_t offset,
                          struct raft_entry *entries,
                          unsigned n);

/**
 * The layout of the memory pointed at by a @batch pointer is the following:
 *
 * [8 bytes] Number of entries in the batch, little endian.
 * [header1] Header data of the first entry of the batch.
 * [  ...  ] More headers
 * [headerN] Header data of the last entry of the batch.
 * [data1  ] Payload data of the first entry of the batch.
 * [  ...  ] More data
 * [dataN  ] Payload data of the last entry of the batch.
 *
 * An entry header is 16-byte long and has the following layout:
 *
 * [8 bytes] Term in which the entry was created, little endian.
 * [1 byte ] Message type (Either RAFT_COMMAND or RAFT_CHANGE)
 * [3 bytes] Currently unused.
 * [4 bytes] Size of the log entry data, little endian.
 *
 * A payload data section for an entry is simply a sequence of bytes of
 * arbitrary lengths, possibly padded with extra bytes to reach 8-byte boundary
 * (which means that all entry data pointers are 8-byte aligned).
 */
size_t uvSizeofBatchHeader(size_t n);

void uvEncodeBatchHeader(const struct raft_entry *entries,
                         unsigned n,
                         void *buf);

/* Validates the contents of the segment header in @buf of len @n.
 * Returns 0 when the segment header is valid. When the segment is valid,
 * @format contains the format version that can potentially be 0 and valid when
 * the file contains all 0's. It's the caller's responsibility to do further
 * validation. */
int uvDecodeSegmentHeaderValidate(void *buf, size_t n, uint64_t *format, char errmsg[RAFT_ERRMSG_BUF_SIZE]);

size_t uvSizeofSegmentHeader(uint64_t format);

void uvEncodeSegmentHeaderFormat2(uint64_t first_index, void *buf);
#endif /* UV_ENCODING_H_ */
