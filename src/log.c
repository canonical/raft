#include <assert.h>
#include <string.h>

#include "../include/raft.h"

#include "log.h"

/**
 * Initial size of the entry reference count hash table.
 */
#define RAFT_LOG__REFS_INITIAL_SIZE 256

/**
 * Calculate the reference count hash table key for the given entry index in an
 * hash table with the given size.
 */
static size_t raft_log__refs_key(const raft_index index, const size_t size)
{
    assert(index > 0);

    return (index - 1) % size;
}

/**
 * Insert a new ref count item in the given hash table.
 *
 * A collision happens when the bucket associated with the hash key of the given
 * index is already used for entries with a different index. In that case the
 * collision output parameter will be set to true and no new entry is inserted
 * in the hash table.
 *
 * If two entries have the same index but different terms, the associated bucket
 * will be grown accordingly.
 */
static int raft_log__refs_insert(struct raft_entry_ref *refs,
                                 const size_t size,
                                 const raft_term term,
                                 const raft_index index,
                                 const unsigned short count,
                                 bool *collision)
{
    struct raft_entry_ref *bucket;
    struct raft_entry_ref *next;
    struct raft_entry_ref *last;
    struct raft_entry_ref *ref;
    size_t i;

    i = raft_log__refs_key(index, size);
    bucket = &refs[i];

    if (bucket->count == 0) {
        /* Empty bucket, no collision. */
        assert(bucket->next == NULL);
        ref = bucket;
        goto fill;
    }

    if (bucket->index != index) {
        /* The bucket is already used to refcount entries with a different
         * index. */
        *collision = true;
        return 0;
    }

    /* Find the last slot in the bucket. */
    for (next = bucket; next != NULL; next = next->next) {
        /* All entries in a bucket must have the same index. */
        assert(next->index == index);

        /* It should never happen that two entries with the same index and term
         * get appended. */
        assert(next->term != term);

        last = next;
    }

    assert(last->next == NULL);

    ref = raft_malloc(sizeof *ref);
    if (ref == NULL) {
        return RAFT_ERR_NOMEM;
    }

    last->next = ref;

fill:
    ref->term = term;
    ref->index = index;
    ref->count = count;
    ref->next = NULL;

    *collision = false;

    return 0;
}

/**
 * Move the slots of the given bucket into the given hash table. Slot indexes in
 * the given table will be calculated by re-keying the original indexes.
 */
static int raft_log__refs_move(struct raft_entry_ref *bucket,
                               struct raft_entry_ref *refs,
                               const size_t size)
{
    struct raft_entry_ref *ref;
    struct raft_entry_ref *prev;

    prev = NULL;

    for (ref = bucket; ref != NULL; ref = ref->next) {
        bool collision;
        int rv;

        if (prev != NULL) {
            raft_free(prev);
        }

        rv = raft_log__refs_insert(refs, size, ref->term, ref->index,
                                   ref->count, &collision);
        if (rv != 0) {
            return rv;
        }

        prev = ref;
    }

    return 0;
}

/**
 * Grow the size of the hash table.
 */
static int raft_log__refs_grow(struct raft_log *l)
{
    struct raft_entry_ref *refs;
    size_t size;
    size_t i;

    size = l->refs_size * 2; /* Double the table size */

    refs = raft_calloc(size, sizeof *refs);
    if (refs == NULL) {
        return RAFT_ERR_NOMEM;
    }

    /* Re-key */
    for (i = 0; i < l->refs_size; i++) {
        struct raft_entry_ref *bucket = &l->refs[i];
        if (bucket->count > 0) {
            int rv = raft_log__refs_move(bucket, refs, size);
            if (rv != 0) {
                return rv;
            }
        } else {
            /* If the count is zero, we expect that the bucket is unused. */
            assert(bucket->next == NULL);
        }
    }

    raft_free(l->refs);

    l->refs = refs;
    l->refs_size = size;

    return 0;
}

/**
 * Initialize the refcount of the entry with the given index, setting it to 1.
 */
static int raft_log__refs_init(struct raft_log *l,
                               const raft_term term,
                               const raft_index index)
{
    /* Initialize the hash map with a reasonable size */
    if (l->refs == NULL) {
        l->refs_size = RAFT_LOG__REFS_INITIAL_SIZE;

        l->refs = raft_calloc(l->refs_size, sizeof *l->refs);
        if (l->refs == NULL) {
            return RAFT_ERR_NOMEM;
        }
    }

    /* Check if the associated slot is available (i.e. there are no collisions),
     * or grow the table and re-key it otherwise. */
    do {
        bool collision;
        int rc;

        rc = raft_log__refs_insert(l->refs, l->refs_size, term, index, 1,
                                   &collision);
        if (rc != 0) {
            return RAFT_ERR_NOMEM;
        }

        if (!collision) {
            break;
        }

        rc = raft_log__refs_grow(l);
        if (rc != 0) {
            return rc;
        }

    } while (1);

    return 0;
}

/**
 * Increment the refcount of the entry with the given index.
 */
static void raft_log__refs_incr(struct raft_log *l,
                                const raft_term term,
                                const raft_index index)
{
    size_t i = raft_log__refs_key(index, l->refs_size);
    struct raft_entry_ref *ref;

    /* Lookup the slot associated with the given term/index. */
    for (ref = &l->refs[i]; ref != NULL; ref = ref->next) {
        assert(ref->index == index);
        if (ref->term == term) {
            break;
        }
    }

    assert(ref != NULL);

    ref->count++;
}

/**
 * Decrement the refcount of the entry with the given index. Return a boolean
 * indicating whether the entry has now zero references.
 */
static bool raft_log__refs_decr(struct raft_log *l,
                                const raft_term term,
                                const raft_index index)
{
    size_t i = raft_log__refs_key(index, l->refs_size);
    struct raft_entry_ref *prev;
    struct raft_entry_ref *ref;

    prev = NULL;

    /* Lookup the slot associated with the given term/index, keeping track of
     * its previous lost in the bucket list.. */
    for (ref = &l->refs[i]; ref != NULL; ref = ref->next) {
        assert(ref->index == index);
        if (ref->term == term) {
            break;
        }
        prev = ref;
    }

    assert(ref != NULL);

    ref->count--;

    if (ref->count > 0) {
        /* The entry is still referenced. */
        return false;
    }

    /* If the refcount has dropped to zero, delete the slot. */
    if (prev != NULL) {
        /* If this isn't the very first slot, simply unlink it from the
         * slot list. */
        prev->next = ref->next;
        raft_free(ref);
    } else if (ref->next != NULL) {
        /* If this is the very first slot, and slot list is not empty,
         * copy the second slot into the first one, then delete it. */
        struct raft_entry_ref *next;

        ref->term = ref->next->term;
        ref->index = ref->next->index;
        ref->count = ref->next->count;

        next = ref->next->next;
        raft_free(ref->next);
        ref->next = next;
    }

    return true;
}

void raft_log__init(struct raft_log *l)
{
    assert(l != NULL);

    l->entries = NULL;
    l->size = 0;
    l->front = l->back = 0;
    l->offset = 0;
    l->refs = NULL;
    l->refs_size = 0;
}

void raft_log__close(struct raft_log *l)
{
    void *batch = NULL; /* Last batch that has been freed */

    assert(l != NULL);

    if (l->entries != NULL) {
        size_t i;
        size_t n = raft_log__n_entries(l);

        for (i = 0; i < n; i++) {
            struct raft_entry *entry = &l->entries[(l->front + i) % l->size];
            raft_index index = l->offset + i + 1;
            size_t key = raft_log__refs_key(index, l->refs_size);
            struct raft_entry_ref *ref = &l->refs[key];

            /* We require that there are no outstanding references to active
             * entries. */
            assert(ref->count == 1);
            assert(ref->next == NULL);

            /* Release the memory used by the entry data (either directly or via
             * a batch). */
            if (entry->batch == NULL) {
                if (entry->buf.base != NULL) {
                    raft_free(entry->buf.base);
                }
            } else {
                if (entry->batch != batch) {
                    /* This batch was not released yet, so let's do it now. */
                    batch = entry->batch;
                    raft_free(entry->batch);
                }
            }
        }

        raft_free(l->entries);
    }

    if (l->refs != NULL) {
        raft_free(l->refs);
    }
}

/**
 * Ensure that the entries array has enough free slots for adding a new enty.
 */
static int raft_log__ensure_capacity(struct raft_log *l)
{
    struct raft_entry *entries; /* New entries array */
    size_t n;                   /* Current number of entries */
    size_t size;                /* Size of the new array */
    size_t i, j;

    n = raft_log__n_entries(l);

    if (n + 1 < l->size) {
        return 0;
    }

    /* Make the new size twice the current size plus one (for the new
     * entry). Over-allocating now avoids smaller allocations later. */
    size = (l->size + 1) * 2;

    entries = raft_calloc(size, sizeof *entries);
    if (entries == NULL) {
        return RAFT_ERR_NOMEM;
    }

    /* Copy all active old entries to the beginning of the newly allocated
     * array. */
    for (i = 0; i < l->size; i++) {
        j = (l->front + i) % l->size;
        memcpy(&entries[i], &l->entries[j], sizeof *entries);
    }

    /* Release the old entries array. */
    if (l->entries != NULL) {
        raft_free(l->entries);
    }

    l->entries = entries;
    l->size = size;
    l->front = 0;
    l->back = n;

    return 0;
}

int raft_log__append(struct raft_log *l,
                     const raft_term term,
                     const int type,
                     const struct raft_buffer *buf,
                     void *batch)
{
    int rv;
    struct raft_entry *entry;
    raft_index index;

    assert(l != NULL);
    assert(term > 0);
    assert(type == RAFT_LOG_CONFIGURATION || type == RAFT_LOG_COMMAND);
    assert(buf != NULL);

    rv = raft_log__ensure_capacity(l);
    if (rv != 0) {
        return rv;
    }

    index = raft_log__last_index(l) + 1;
    rv = raft_log__refs_init(l, term, index);
    if (rv != 0) {
        return rv;
    }

    entry = &l->entries[l->back];
    entry->term = term;
    entry->type = type;
    entry->buf = *buf;
    entry->batch = batch;

    l->back += 1;
    l->back = l->back % l->size;

    return 0;
}

size_t raft_log__n_entries(struct raft_log *l)
{
    assert(l != NULL);

    /* The circular buffer is not wrapped. */
    if (l->front <= l->back) {
        return l->back - l->front;
    }

    /* The circular buffer is wrapped. */
    return l->size - l->front + l->back;
}

raft_index raft_log__first_index(struct raft_log *l)
{
    if (raft_log__n_entries(l) == 0) {
        return 0;
    }
    return l->offset + 1;
}

raft_index raft_log__last_index(struct raft_log *l)
{
    return l->offset + raft_log__n_entries(l);
}

/**
 * Return the position of the entry with the given index in the entries array.
 *
 * If no entry with the given index is in the log return the size of the entries
 * array.
 */
static size_t raft_log__locate(struct raft_log *l, const uint64_t index)
{
    if (index < raft_log__first_index(l) || index > raft_log__last_index(l)) {
        return l->size;
    }

    /* Get the array index of the desired entry (log indexes start at 1, so we
     * subtract one to get array indexes). */
    return (l->front + (index - 1) - l->offset) % l->size;
}

raft_term raft_log__term_of(struct raft_log *l, const uint64_t index)
{
    size_t i;

    if (raft_log__n_entries(l) == 0) {
        return 0;
    }

    i = raft_log__locate(l, index);

    if (i == l->size) {
        return 0;
    }

    return l->entries[i].term;
}

raft_term raft_log__last_term(struct raft_log *l)
{
    return raft_log__term_of(l, raft_log__last_index(l));
}

const struct raft_entry *raft_log__get(struct raft_log *l, const raft_index index)
{
    size_t i;

    assert(l != NULL);

    /* Get the array index of the desired entry. */
    i = raft_log__locate(l, index);
    if (i == l->size) {
        return NULL;
    }

    return &l->entries[i];
}

int raft_log__acquire(struct raft_log *l,
                      const raft_index index,
                      struct raft_entry *entries[],
                      unsigned *n)
{
    size_t i;
    size_t j;
    struct raft_entry *e;

    assert(l != NULL);

    /* Get the array index of the first entry to acquire. */
    i = raft_log__locate(l, index);

    if (i == l->size) {
        *n = 0;
        *entries = NULL;
        return 0;
    }

    if (i < l->back) {
        /* The last entry does not wrap with respect to i, so the number of
         * entries is simply the length of the range [i...l->back). */
        *n = l->back - i;
    } else {
        /* The last entry wraps with respect to i, so the number of entries is
         * the sum of the lengths of the ranges [i...l->size) and [0...l->back),
         * which is l->size - i + l->back.*/
        *n = l->size - i + l->back;
    }

    assert(*n > 0);

    e = raft_calloc(*n, sizeof **entries);
    if (e == NULL) {
        return RAFT_ERR_NOMEM;
    }

    for (j = 0; j < *n; j++) {
        size_t k = (i + j) % l->size;
        e[j] = l->entries[k];
        raft_log__refs_incr(l, e[j].term, index + j);
    }

    *entries = e;

    return 0;
}

/**
 * Return true if the given batch is referenced by any entry currently in the
 * log.
 */
static bool raft_log__batch_is_referenced(struct raft_log *l, const void *batch)
{
    size_t n = raft_log__n_entries(l);
    size_t i;

    /* Iterate through all live entries to see if there's one
     * belonging to the same batch. This is slightly inefficient but
     * this code path should be taken very rarely in practice. */
    for (i = 0; i < n; i++) {
        struct raft_entry *entry;
        entry = &l->entries[(l->front + i) % l->size];

        if (entry->batch == batch) {
            return true;
        }
    }

    return false;
}

void raft_log__release(struct raft_log *l,
                       const raft_index index,
                       struct raft_entry entries[],
                       const size_t n)
{
    size_t i;
    void *batch = NULL; /* Last batch whose memory was freed */

    assert(l != NULL);
    assert((entries == NULL && n == 0) || (entries != NULL && n > 0));

    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        bool unref;

        unref = raft_log__refs_decr(l, entry->term, index + i);

        if (unref) {
            if (entries[i].batch == NULL) {
                if (entry->buf.base != NULL) {
                    raft_free(entries[i].buf.base);
                }
            } else {
                if (entry->batch != batch) {
                    if (!raft_log__batch_is_referenced(l, entry->batch)) {
                        batch = entry->batch;
                        raft_free(batch);
                    }
                }
            }
        }
    }

    if (entries != NULL) {
        raft_free(entries);
    }
}

/**
 * Clear the log if it became empty.
 */
static void raft_log__clear_if_empty(struct raft_log *l)
{
    if (raft_log__n_entries(l) == 0) {
        raft_free(l->entries);
        l->entries = NULL;
        l->size = 0;
        l->front = 0;
        l->back = 0;
    }
}

/**
 * Discard an entry, possibly releasing the memory of its buffer.
 */
static void raft_log__discard_entry(struct raft_log *l,
                                    struct raft_entry *entry)
{
    if (entry->batch == NULL) {
        if (entry->buf.base != NULL) {
            raft_free(entry->buf.base);
        }
    } else {
        if (!raft_log__batch_is_referenced(l, entry->batch)) {
            raft_free(entry->batch);
        }
    }
}

void raft_log__truncate(struct raft_log *l, const raft_index index)
{
    size_t i;
    size_t n;
    raft_index start = index;

    assert(l != NULL);
    assert(index > 0);
    assert(index <= raft_log__last_index(l));

    /* If we are asked to delete entries starting from an index which is lower
     * than our first one, just normalize the request and start from what we
     * have. */
    if (index < raft_log__first_index(l)) {
        start = raft_log__first_index(l);
    }

    /* Number of entries to delete */
    n = (raft_log__last_index(l) - start) + 1;

    for (i = 0; i < n; i++) {
        struct raft_entry *entry;
        bool unref;

        if (l->back == 0) {
            l->back = l->size - 1;
        } else {
            l->back--;
        }

        entry = &l->entries[l->back];

        unref = raft_log__refs_decr(l, entry->term, start + n - i - 1);

        if (unref) {
            raft_log__discard_entry(l, entry);
        }
    }

    raft_log__clear_if_empty(l);
}

void raft_log__shift(struct raft_log *l, const raft_index index)
{
    size_t i;
    size_t n;

    assert(l != NULL);
    assert(index > 0);
    assert(index <= raft_log__last_index(l));

    /* Number of entries to delete */
    n = (index - raft_log__first_index(l)) + 1;

    for (i = 0; i < n; i++) {
        struct raft_entry *entry;
        bool unref;

        entry = &l->entries[l->front];

        if (l->front == l->size - 1) {
            l->front = 0;
        } else {
            l->front++;
        }
        l->offset++;

        unref = raft_log__refs_decr(l, entry->term, l->offset);

        if (unref) {
            raft_log__discard_entry(l, entry);
        }
    }

    raft_log__clear_if_empty(l);
}
