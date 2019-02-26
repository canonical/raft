/**
 * In-memory cache of the persistent raft log stored on disk.
 */

#ifndef RAFT_LOG_H
#define RAFT_LOG_H

#include "../include/raft.h"

/**
 * Initial size of the entry reference count hash table.
 */
#define RAFT_LOG__REFS_INITIAL_SIZE 256

void raft_log__init(struct raft_log *l);

void raft_log__close(struct raft_log *l);

/**
 * Set the offset of the first entry in the log.
 */
void raft_log__set_offset(struct raft_log *l, raft_index offset);

/**
 * Append the an entry to the log.
 */
int raft_log__append(struct raft_log *l,
                     const raft_term term,
                     const int type,
                     const struct raft_buffer *buf,
                     void *batch);

/**
 * Convenience to append a series of RAFT_LOG_COMMAND entries.
 */
int raft_log__append_commands(struct raft_log *l,
                              const raft_term term,
                              const struct raft_buffer bufs[],
                              const unsigned n);

/**
 * Convenience to encode and append a single RAFT_LOG_CONFIGURATION entry.
 */
int raft_log__append_configuration(
    struct raft_log *l,
    const raft_term term,
    const struct raft_configuration *configuration);

/**
 * Get the current number of entries in the log.
 */
size_t raft_log__n_entries(struct raft_log *l);

/**
 * Get the index of the first entry in the log.
 */
raft_index raft_log__first_index(struct raft_log *l);

/**
 * Get the index of the last entry in the log.
 */
raft_index raft_log__last_index(struct raft_log *l);

/**
 * Get the term of the entry with the given index.
 */
raft_term raft_log__term_of(struct raft_log *l, raft_index index);

/**
 * Get the term of the last entry in the log.
 */
raft_term raft_log__last_term(struct raft_log *l);

/**
 * Get the entry with the given index.
 *
 * The returned pointer remains valid only as long as no API that might delete
 * the entry with the given index is invoked.
 */
const struct raft_entry *raft_log__get(struct raft_log *l,
                                       const raft_index index);

/**
 * Acquire an array of entries from the given index onwards.
 *
 * The payload memory referenced by the #buf attribute of the returned entries
 * is guaranteed to be valid until raft_log__release() is called.
 */
int raft_log__acquire(struct raft_log *l,
                      const raft_index index,
                      struct raft_entry *entries[],
                      unsigned *n);

/**
 * Release a previously acquired array of entries.
 */
void raft_log__release(struct raft_log *l,
                       const raft_index index,
                       struct raft_entry entries[],
                       const size_t n);

/**
 * Delete all entries from the given index (included) onwards.
 */
void raft_log__truncate(struct raft_log *l, const raft_index index);

/**
 * Discard all entries from the given index (included) onwards. This is exactly
 * the same as truncate, but the memory of the entries does not gets released.
 */
void raft_log__discard(struct raft_log *l, const raft_index index);

/**
 * Delete all entries up to the given index (included).
 */
void raft_log__shift(struct raft_log *l, const raft_index index);

#endif /* RAFT_LOG_H */
