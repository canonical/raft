/**
 * Encoding routines for the the libuv-based @raft_io backend.
 */

#ifndef RAFT_IO_UV_ENCODING_H
#define RAFT_IO_UV_ENCODING_H

#include "../include/raft.h"

int raft_io_uv_encode__message(const struct raft_message *message,
                               uv_buf_t **bufs,
                               unsigned *n_bufs);

int raft_io_uv_decode__message(unsigned type,
                               const uv_buf_t *header,
                               struct raft_message *message,
                               size_t *payload_len);

int raft_io_uv_decode__batch_header(const void *batch,
                                    struct raft_entry **entries,
                                    unsigned *n);

void raft_io_uv_decode__entries_batch(const struct raft_buffer *buf,
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
 * [1 byte ] Message type (Either RAFT_LOG_COMMAND or RAFT_LOG_CONFIGURATION)
 * [3 bytes] Currently unused.
 * [4 bytes] Size of the log entry data, little endian.
 *
 * A payload data section for an entry is simply a sequence of bytes of
 * arbitrary lengths, possibly padded with extra bytes to reach 8-byte boundary
 * (which means that all entry data pointers are 8-byte aligned).
 */
size_t raft_io_uv_sizeof__batch_header(size_t n);

void raft_io_uv_encode__batch_header(const struct raft_entry *entries,
                                     unsigned n,
                                     void *buf);

#endif /* RAFT_IO_UV_ENCODING_H */
