/**
 * Encoding routines for the the libuv-based @raft_io backend.
 */

#ifndef RAFT_IO_UV_ENCODING_H
#define RAFT_IO_UV_ENCODING_H

#include "../include/raft.h"

int raft_io_uv_encode__configuration(const struct raft_configuration *c,
                                     struct raft_buffer *buf);

int raft_io_uv_encode__message(const struct raft_message *message,
                               uv_buf_t **bufs,
                               unsigned *n_bufs);

int raft_io_uv_decode__message(unsigned type,
                               const uv_buf_t *header,
                               struct raft_message *message,
                               size_t *payload_len);

void raft_io_uv_decode__entries_batch(const uv_buf_t *buf,
                                      struct raft_entry *entries,
                                      unsigned n);

#endif /* RAFT_IO_UV_ENCODING_H */
