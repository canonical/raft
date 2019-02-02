/**
 * I/O callbacks.
 */

#ifndef RAFT_IO_H
#define RAFT_IO_H

#include "../include/raft.h"

/**
 * Hold context for a #RAFT_IO_APPEND_ENTRIES send request that was submitted.
 */
struct raft_io__send_append_entries
{
    struct raft *raft;          /* Instance that has submitted the request */
    raft_index index;           /* Index of the first entry in the request. */
    struct raft_entry *entries; /* Entries referenced in the request. */
    unsigned n;                 /* Length of the entries array. */
};

/**
 * Hold context for a write log request that was submitted.
 */
struct raft_io__write_log
{
    struct raft *raft;          /* Instance that has submitted the request */
    raft_index index;           /* Index of the first entry in the request. */
    struct raft_entry *entries; /* Entries referenced in the request. */
    unsigned n;                 /* Length of the entries array. */
    unsigned leader_id;         /* Leader issuing the request */
    raft_index leader_commit;   /* For followers, commit index on the leader */
};

/**
 * Process the result of an asynchronous I/O request that involves raft entries
 * or snapshots (i.e. memory shared between a raft instance and its I/O
 * implementation).
 *
 * The @request parameter holds information about the entries or snapshosts
 * referenced in request that has been completed. The @status parameter must be
 * set to zero if the write was successful, or non-zero otherwise.
 */

/**
 * Handle and incoming message.
 */
void raft_io__recv(struct raft *r, struct raft_message *message);

/**
 * Callback to pass to raft_io.send.
 */
void raft_io__send_cb(void *data, int status);

/**
 * Callback to pass to raft_io.append.
 */
void raft_io__append_cb(void *data, int status);

/**
 * Callback to be passed to the @raft_io implementation. This is just a
 * trampiline to invoke @raft_io__recv.
 */
void raft__recv_cb(void *data, struct raft_message *message);

#endif /* RAFT_IO_H */
