#include <string.h>

#include "../include/raft.h"

#include "assert.h"
#include "configuration.h"
#include "log.h"

/**
 * Calculate the reference count hash table key for the given entry index in an
 * table with the given size.
 *
 * The hash is simply the index minus one modulo the size. This minimizes
 * conflicts in the most frequent case, where a new log entry is simpy appended
 * to the log and can use the hash table bucket next to the bucket for the entry
 * with the previous index (possibly resizing the table if its cap is reached).
 */
static size_t refs_key(const raft_index index, const size_t size)
{
    assert(index > 0);
    assert(size > 0);

    return (index - 1) % size;
}

/**
 * Try to insert a new ref count item into the given reference count hash table.
 *
 * A collision happens when the bucket associated with the hash key of the given
 * index is already used to refcount entries with a different index. In that
 * case the collision output parameter will be set to true and no new entry is
 * inserted into the hash table.
 *
 * If two entries have the same index but different terms, the associated bucket
 * will be grown accordingly.
 */
static int refs_try_insert(struct raft_entry_ref *table,
                           const size_t size,
                           const raft_term term,
                           const raft_index index,
                           const unsigned short count,
                           bool *collision)
{
    struct raft_entry_ref *bucket;    /* Bucket associated with this index. */
    struct raft_entry_ref *next_slot; /* For traversing the bucket slots */
    struct raft_entry_ref *last_slot; /* To track the last traversed slot */
    struct raft_entry_ref *slot;      /* Actual slot to use for this entry */
    size_t key;

    assert(table != NULL);
    assert(size > 0);
    assert(term > 0);
    assert(index > 0);
    assert(count > 0);
    assert(collision != NULL);

    /* Calculate the hash table key for the given index. */
    key = refs_key(index, size);
    bucket = &table[key];

    /* If a bucket is empty, then there's no collision and we can fill its first
     * slot. */
    if (bucket->count == 0) {
        assert(bucket->next == NULL);

        slot = bucket;
        goto fill;
    }

    /* If the bucket is already used to refcount entries with a different
     * index, then we have a collision and we must abort here. */
    if (bucket->index != index) {
        *collision = true;
        return 0;
    }

    /* If we get here it means that the bucket is in use to refcount one or more
     * entries with the same index as the given one, but different terms.
     *
     * We must append a newly allocated slot to refcount the entry with this
     * term.
     *
     * So first let's find the last slot in the bucket. */
    for (next_slot = bucket; next_slot != NULL; next_slot = next_slot->next) {
        /* All entries in a bucket must have the same index. */
        assert(next_slot->index == index);

        /* It should never happen that two entries with the same index and term
         * get appended. So no existing slot in this bucket must track an entry
         * with the same term as the given one. */
        assert(next_slot->term != term);

        last_slot = next_slot;
    }

    /* The last slot must have no next slot. */
    assert(last_slot->next == NULL);

    slot = raft_malloc(sizeof *slot);
    if (slot == NULL) {
        return RAFT_ENOMEM;
    }

    last_slot->next = slot;

fill:
    slot->term = term;
    slot->index = index;
    slot->count = count;
    slot->next = NULL;

    *collision = false;

    return 0;
}

/**
 * Move the slots of the given bucket into the given reference count hash
 * table. The key of the bucket to use in the given table will be re-calculated
 * according to the given size.
 */
static int refs_move(struct raft_entry_ref *bucket,
                     struct raft_entry_ref *table,
                     const size_t size)
{
    struct raft_entry_ref *slot;
    struct raft_entry_ref *next_slot;

    assert(bucket != NULL);
    assert(table != NULL);
    assert(size > 0);

    /* Only non-empty buckets should be moved. */
    assert(bucket->count > 0);

    /* For each slot in the bucket, insert the relevant entry in the given
     * table, then free it. */
    next_slot = bucket;
    while (next_slot != NULL) {
        bool collision;
        int rv;

        slot = next_slot;

        /* Insert the ref count for this entry into the new table. */
        rv = refs_try_insert(table, size, slot->term, slot->index, slot->count,
                             &collision);

        next_slot = slot->next;

        /* Unless this is the very first slot in the bucket, we need to free the
         * slot. */
        if (slot != bucket) {
            raft_free(slot);
        }

        if (rv != 0) {
            return rv;
        }

        /* The given hash table is assumed to be large enough to hold all ref
         * counts without any conflict. */
        assert(!collision);
    };

    return 0;
}

/**
 * Grow the size of the reference count hash table.
 */
static int refs_grow(struct raft_log *l)
{
    struct raft_entry_ref *table; /* New hash table. */
    size_t size;                  /* Size of the new hash table. */
    size_t i;

    assert(l != NULL);
    assert(l->refs_size > 0);

    size = l->refs_size * 2; /* Double the table size */

    table = raft_calloc(size, sizeof *table);
    if (table == NULL) {
        return RAFT_ENOMEM;
    }

    /* Populate the new hash table, inserting all entries existing in the
     * current hash table. Each bucket will have a different key in the new hash
     * table, since the size has changed. */
    for (i = 0; i < l->refs_size; i++) {
        struct raft_entry_ref *bucket = &l->refs[i];
        if (bucket->count > 0) {
            int rv = refs_move(bucket, table, size);
            if (rv != 0) {
                return rv;
            }
        } else {
            /* If the count is zero, we expect that the bucket is unused. */
            assert(bucket->next == NULL);
        }
    }

    raft_free(l->refs);

    l->refs = table;
    l->refs_size = size;

    return 0;
}

/**
 * Initialize the refcount of the entry with the given index, setting it to 1.
 */
static int refs_init(struct raft_log *l,
                     const raft_term term,
                     const raft_index index)
{
    int i;

    assert(l != NULL);
    assert(term > 0);
    assert(index > 0);

    /* Initialize the hash map with a reasonable size */
    if (l->refs == NULL) {
        l->refs_size = LOG__REFS_INITIAL_SIZE;

        l->refs = raft_calloc(l->refs_size, sizeof *l->refs);
        if (l->refs == NULL) {
            return RAFT_ENOMEM;
        }
    }

    /* Check if the bucket associated with the given index is available
     * (i.e. there are no collisions), or grow the table and re-key it
     * otherwise.
     *
     * We limit the number of times we try to grow the table to 10, to avoid
     * eating up too much memory. In practice, there should never be a case
     * where this is not enough. */
    for (i = 0; i < 10; i++) {
        bool collision;
        int rc;

        rc = refs_try_insert(l->refs, l->refs_size, term, index, 1, &collision);
        if (rc != 0) {
            return RAFT_ENOMEM;
        }

        if (!collision) {
            return 0;
        }

        rc = refs_grow(l);
        if (rc != 0) {
            return rc;
        }
    };

    return RAFT_ENOMEM;
}

/**
 * Increment the refcount of the entry with the given term and index.
 */
static void refs_incr(struct raft_log *l,
                      const raft_term term,
                      const raft_index index)
{
    size_t key;                  /* Hash table key for the given index. */
    struct raft_entry_ref *slot; /* Slot for the given term/index */

    assert(l != NULL);
    assert(term > 0);
    assert(index > 0);

    key = refs_key(index, l->refs_size);

    /* Lookup the slot associated with the given term/index. */
    for (slot = &l->refs[key]; slot != NULL; slot = slot->next) {
        assert(slot->index == index);
        if (slot->term == term) {
            break;
        }
    }

    assert(slot != NULL);

    slot->count++;
}

/**
 * Decrement the refcount of the entry with the given index. Return a boolean
 * indicating whether the entry has now zero references.
 */
static bool refs_decr(struct raft_log *l,
                      const raft_term term,
                      const raft_index index)
{
    size_t key;                       /* Hash table key for the given index. */
    struct raft_entry_ref *slot;      /* Slot for the given term/index */
    struct raft_entry_ref *prev_slot; /* Slot preceeding the one to decrement */

    assert(l != NULL);
    assert(term > 0);
    assert(index > 0);

    key = refs_key(index, l->refs_size);
    prev_slot = NULL;

    /* Lookup the slot associated with the given term/index, keeping track of
     * its previous slot in the bucket list. */
    for (slot = &l->refs[key]; slot != NULL; slot = slot->next) {
        assert(slot->index == index);
        if (slot->term == term) {
            break;
        }
        prev_slot = slot;
    }

    assert(slot != NULL);

    slot->count--;

    if (slot->count > 0) {
        /* The entry is still referenced. */
        return false;
    }

    /* If the refcount has dropped to zero, delete the slot. */
    if (prev_slot != NULL) {
        /* This isn't the very first slot, simply unlink it from the slot
         * list. */
        prev_slot->next = slot->next;
        raft_free(slot);
    } else if (slot->next != NULL) {
        /* This is the very first slot, and slot list is not empty. Copy the
         * second slot into the first one, then delete it. */
        struct raft_entry_ref *second_slot = slot->next;
        struct raft_entry_ref *tail_slots = second_slot->next;

        slot->term = second_slot->term;
        slot->index = second_slot->index;
        slot->count = second_slot->count;

        raft_free(second_slot);
        slot->next = tail_slots;
    }

    return true;
}

void log__init(struct raft_log *l)
{
    assert(l != NULL);

    l->entries = NULL;
    l->size = 0;
    l->front = l->back = 0;
    l->offset = 0;
    l->refs = NULL;
    l->refs_size = 0;
}

void log__set_offset(struct raft_log *l, raft_index offset)
{
    l->offset = offset;
}

/**
 * Return the index of the i'th entry in the log.
 */
static raft_index log__index(struct raft_log *l, size_t i)
{
    return l->offset + i + 1;
}

void log__close(struct raft_log *l)
{
    void *batch = NULL; /* Last batch that has been freed */

    assert(l != NULL);

    if (l->entries != NULL) {
        size_t i;
        size_t n = log__n_entries(l);

        for (i = 0; i < n; i++) {
            struct raft_entry *entry = &l->entries[(l->front + i) % l->size];
            raft_index index = log__index(l, i);
            size_t key = refs_key(index, l->refs_size);
            struct raft_entry_ref *slot = &l->refs[key];

            /* We require that there are no outstanding references to active
             * entries. */
            assert(slot->count == 1);
            assert(slot->next == NULL);

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
static int ensure_capacity(struct raft_log *l)
{
    struct raft_entry *entries; /* New entries array */
    size_t n;                   /* Current number of entries */
    size_t size;                /* Size of the new array */
    size_t i, j;

    n = log__n_entries(l);

    if (n + 1 < l->size) {
        return 0;
    }

    /* Make the new size twice the current size plus one (for the new
     * entry). Over-allocating now avoids smaller allocations later. */
    size = (l->size + 1) * 2;

    entries = raft_calloc(size, sizeof *entries);
    if (entries == NULL) {
        return RAFT_ENOMEM;
    }

    /* Copy all active old entries to the beginning of the newly allocated
     * array. */
    for (i = 0; i < n; i++) {
        j = (l->front + i) % l->size; /* Index in the current array */
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

int log__append(struct raft_log *l,
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
    assert(type == RAFT_CONFIGURATION || type == RAFT_COMMAND);
    assert(buf != NULL);

    rv = ensure_capacity(l);
    if (rv != 0) {
        return rv;
    }

    index = l->offset + log__n_entries(l) + 1;
    rv = refs_init(l, term, index);
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

int log__append_commands(struct raft_log *l,
                         const raft_term term,
                         const struct raft_buffer bufs[],
                         const unsigned n)
{
    unsigned i;
    int rv;

    assert(l != NULL);
    assert(term > 0);
    assert(bufs != NULL);
    assert(n > 0);

    for (i = 0; i < n; i++) {
        const struct raft_buffer *buf = &bufs[i];
        rv = log__append(l, term, RAFT_COMMAND, buf, NULL);
        if (rv != 0) {
            return rv;
        }
    }

    return 0;
}

int log__append_configuration(struct raft_log *l,
                              const raft_term term,
                              const struct raft_configuration *configuration)
{
    struct raft_buffer buf;
    int rv;

    assert(l != NULL);
    assert(term > 0);
    assert(configuration != NULL);

    /* Encode the configuration into a buffer. */
    rv = configuration__encode(configuration, &buf);
    if (rv != 0) {
        goto err;
    }

    /* Append the new entry to the log. */
    rv = log__append(l, term, RAFT_CONFIGURATION, &buf, NULL);
    if (rv != 0) {
        goto err_after_encode;
    }

    return 0;

err_after_encode:
    raft_free(buf.base);

err:
    assert(rv != 0);
    return rv;
}

size_t log__n_entries(struct raft_log *l)
{
    assert(l != NULL);

    /* The circular buffer is not wrapped. */
    if (l->front <= l->back) {
        return l->back - l->front;
    }

    /* The circular buffer is wrapped. */
    return l->size - l->front + l->back;
}

raft_index log__first_index(struct raft_log *l)
{
    if (log__n_entries(l) == 0) {
        return 0;
    }
    return log__index(l, 0);
}

raft_index log__last_index(struct raft_log *l)
{
    if (log__n_entries(l) == 0) {
        return 0;
    }
    return log__index(l, log__n_entries(l) - 1);
}

/**
 * Return the position of the entry with the given index in the entries array.
 *
 * If no entry with the given index is in the log return the size of the entries
 * array.
 */
static size_t locate_entry(struct raft_log *l, const raft_index index)
{
    if (index < log__first_index(l) || index > log__last_index(l)) {
        return l->size;
    }

    /* Get the array index of the desired entry. Log indexes start at 1, so we
     * subtract one to get array indexes. We also need to subtract any index
     * offset this log might start at. */
    return (l->front + (index - 1) - l->offset) % l->size;
}

raft_term log__term_of(struct raft_log *l, const raft_index index)
{
    size_t i;

    if (log__n_entries(l) == 0) {
        return 0;
    }

    i = locate_entry(l, index);

    if (i == l->size) {
        return 0;
    }

    assert(i < l->size);

    return l->entries[i].term;
}

raft_term log__last_term(struct raft_log *l)
{
    return log__term_of(l, log__last_index(l));
}

const struct raft_entry *log__get(struct raft_log *l, const raft_index index)
{
    size_t i;

    assert(l != NULL);

    /* Get the array index of the desired entry. */
    i = locate_entry(l, index);
    if (i == l->size) {
        return NULL;
    }

    assert(i < l->size);

    return &l->entries[i];
}

int log__acquire(struct raft_log *l,
                 const raft_index index,
                 struct raft_entry *entries[],
                 unsigned *n)
{
    size_t i;
    size_t j;

    assert(l != NULL);
    assert(index > 0);
    assert(entries != NULL);
    assert(n != NULL);

    /* Get the array index of the first entry to acquire. */
    i = locate_entry(l, index);

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

    *entries = raft_calloc(*n, sizeof **entries);
    if (*entries == NULL) {
        return RAFT_ENOMEM;
    }

    for (j = 0; j < *n; j++) {
        size_t k = (i + j) % l->size;
        struct raft_entry *entry = &(*entries)[j];
        *entry = l->entries[k];
        refs_incr(l, entry->term, index + j);
    }

    return 0;
}

/**
 * Return true if the given batch is referenced by any entry currently in the
 * log.
 */
static bool is_batch_referenced(struct raft_log *l, const void *batch)
{
    size_t n = log__n_entries(l);
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

void log__release(struct raft_log *l,
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

        unref = refs_decr(l, entry->term, index + i);

        /* If there are no outstanding references to this entry, free its
         * payload if it's not part of a batch, or check if we can free the
         * batch itself. */
        if (unref) {
            if (entries[i].batch == NULL) {
                if (entry->buf.base != NULL) {
                    raft_free(entries[i].buf.base);
                }
            } else {
                if (entry->batch != batch) {
                    if (!is_batch_referenced(l, entry->batch)) {
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
static void clear_if_empty(struct raft_log *l)
{
    if (log__n_entries(l) == 0) {
        raft_free(l->entries);
        l->entries = NULL;
        l->size = 0;
        l->front = 0;
        l->back = 0;
    }
}

/**
 * Destroy an entry, possibly releasing the memory of its buffer.
 */
static void destroy_entry(struct raft_log *l, struct raft_entry *entry)
{
    if (entry->batch == NULL) {
        if (entry->buf.base != NULL) {
            raft_free(entry->buf.base);
        }
    } else {
        if (!is_batch_referenced(l, entry->batch)) {
            raft_free(entry->batch);
        }
    }
}

/**
 * Core logic of @log__truncate and @log__discard, removing all
 * entries starting from @index.
 */
static void remove_suffix(struct raft_log *l,
                          const raft_index index,
                          bool destroy)
{
    size_t i;
    size_t n;
    raft_index start = index;

    assert(l != NULL);
    assert(index > 0);
    assert(index <= log__last_index(l));

    /* If we are asked to delete entries starting from an index which is lower
     * than our first one, just normalize the request and start from what we
     * have. */
    if (index < log__first_index(l)) {
        start = log__first_index(l);
    }

    /* Number of entries to delete */
    n = (log__last_index(l) - start) + 1;

    for (i = 0; i < n; i++) {
        struct raft_entry *entry;
        bool unref;

        if (l->back == 0) {
            l->back = l->size - 1;
        } else {
            l->back--;
        }

        entry = &l->entries[l->back];

        unref = refs_decr(l, entry->term, start + n - i - 1);

        if (unref && destroy) {
            destroy_entry(l, entry);
        }
    }

    clear_if_empty(l);
}

void log__truncate(struct raft_log *l, const raft_index index)
{
    if (log__n_entries(l) == 0) {
        return;
    }
    return remove_suffix(l, index, true);
}

void log__discard(struct raft_log *l, const raft_index index)
{
    return remove_suffix(l, index, false);
}

void log__shift(struct raft_log *l, const raft_index index)
{
    size_t i;
    size_t n;

    assert(l != NULL);
    assert(index > 0);
    assert(index <= log__last_index(l));

    /* Number of entries to delete */
    n = (index - log__first_index(l)) + 1;

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

        unref = refs_decr(l, entry->term, l->offset);

        if (unref) {
            destroy_entry(l, entry);
        }
    }

    clear_if_empty(l);
}
