#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "assert.h"
#include "byte.h"
#include "tracer.h"

/* Size of the metadata header (time, type, message len). */
#define METADATA_SIZE sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint8_t)

/* Maximum length of a single entry message, including the terminating null
 * byte. */
#define MAX_MESSAGE_LEN 255

/* Calculate the total size of an entry whose message has the given length,
 * including the terminating null byte. */
#define ENTRY_SIZE(MESSAGE_LEN) bytePad64(METADATA_SIZE + MESSAGE_LEN)

/* Minimum size of the entries buffer. We Require that at least one message of
 * maximum length can be stored in it. */
#define MIN_BUF_SIZE ENTRY_SIZE(MAX_MESSAGE_LEN)

/* Hold metadata about an single entry. */
struct metadata
{
    raft_time time; /* Entry timestamp. */
    unsigned type;  /* Entry type, must be greater than zero. */
    size_t len;     /* Message length, including the null byte. */
};

int raft_tracer_init(struct raft_tracer *t, size_t size)
{
    assert(size % sizeof(uint64_t) == 0); /* Require 8-byte alignment. */
    assert(size >= MIN_BUF_SIZE);
    t->buf = raft_malloc(size);
    if (t->buf == NULL) {
        return RAFT_NOMEM;
    }
    t->size = size;
    t->head = size;
    t->tail = size;

    return 0;
}

void raft_tracer_close(struct raft_tracer *t)
{
    raft_free(t->buf);
}

/* Return a cursor ponting at the given buffer offset. */
static void *cursorAtOffset(const struct raft_tracer *t, size_t offset)
{
    assert(offset < t->size);
    return (uint8_t *)t->buf + offset;
}

/* Return true if the buffer is wrapped (i.e. tail comes before head). */
static inline bool isWrapped(const struct raft_tracer *t)
{
    return t->tail < t->head;
}

/* Write the given entry at the given offset. */
static void putEntry(const struct raft_tracer *t,
                     const size_t offset,
                     const struct metadata *metadata,
                     const char *message)
{
    void *cursor = cursorAtOffset(t, offset);
    assert(metadata->len <= MAX_MESSAGE_LEN);
    assert(offset + ENTRY_SIZE(metadata->len) <= t->size);
    bytePut64(&cursor, metadata->time);
    bytePut8(&cursor, metadata->type);
    bytePut8(&cursor, metadata->len);
    strcpy(cursor, message);
}

/* Write a dummy entry at the given offset. A dummy entry is used to signal that
 * the last part of the buffer is unused and the actual next entry is a the
 * beginning of the buffer. */
static void putDummyEntry(const struct raft_tracer *t, const size_t offset)
{
    void *cursor = cursorAtOffset(t, offset);
    assert(offset + METADATA_SIZE <= t->size);
    memset(cursor, 0, METADATA_SIZE);
}

/* Read the metadata of the entry at the given offset. */
static void getEntryMetadata(const struct raft_tracer *t,
                             const size_t offset,
                             struct metadata *metadata)
{
    const void *cursor = cursorAtOffset(t, offset);

    assert(offset + METADATA_SIZE <= t->size); /* Can read metadata. */

    metadata->time = byteGet64(&cursor);
    metadata->type = byteGet8(&cursor);
    metadata->len = byteGet8(&cursor);

    assert(metadata->type > 0);                            /* Valid type. */
    assert(metadata->len > 0);                             /* Valid message */
    assert(offset + ENTRY_SIZE(metadata->len) <= t->size); /* Boundary check. */
}

/* Return the total size of the entry at the given offset. */
static size_t getEntrySize(const struct raft_tracer *t, size_t offset)
{
    struct metadata metadata;
    getEntryMetadata(t, offset, &metadata);
    return ENTRY_SIZE(metadata.len);
}

/* Read the entry at the given offset. */
static void getEntry(const struct raft_tracer *t,
                     const size_t offset,
                     struct metadata *metadata,
                     const char **message)
{
    getEntryMetadata(t, offset, metadata);
    *message = cursorAtOffset(t, offset + METADATA_SIZE);
    assert((*message)[metadata->len - 1] == 0);
}

/* Return true if the entry at the given offset is a dummy one. */
static bool hasDummyEntry(const struct raft_tracer *t, const size_t offset)
{
    const void *cursor = cursorAtOffset(t, offset);
    uint8_t metadata[METADATA_SIZE];
    assert(offset + METADATA_SIZE <= t->size); /* Can read metadata. */
    memset(&metadata, 0, sizeof metadata);
    return memcmp(cursor, metadata, sizeof metadata) == 0;
}

/* Return true if there is not a valid entry at the given offset, because the
 * given offset is past then end of the very last entry, and the next entry is
 * wrapped. */
static inline bool hasNoEntryAtOffset(const struct raft_tracer *t,
                                      const size_t offset)
{
    return offset + METADATA_SIZE > t->size || hasDummyEntry(t, offset);
}

void tracerEmit(struct raft_tracer *t,
                raft_time time,
                unsigned type,
                const char *format,
                va_list args)
{
    struct metadata metadata;      /* Entry's metadata. */
    char message[MAX_MESSAGE_LEN]; /* Buffer holding the entry's message. */
    size_t size;                   /* Entry size. */
    size_t offset;                 /* Position of the new entry. */

    /* The entry type 0 is reserved for the dummy entry. */
    assert(type > 0);

    assert(t->head <= t->size);
    assert(t->tail <= t->size);

    metadata.time = time;
    metadata.type = type;
    metadata.len = vsnprintf(message, sizeof message, format, args);

    /* If the message was truncated, adjust the actual length accordingly. */
    if (metadata.len >= MAX_MESSAGE_LEN) {
        metadata.len = MAX_MESSAGE_LEN - 1;
    }

    assert(metadata.len > 0); /* We don't allow empty messages. */

    metadata.len += 1; /* Add the null byte. */

    /* If this is the very first entry, put it at the beginning of the buffer
     * and initialize the head accordingly. */
    if (t->head == t->size) {
        assert(t->tail == t->size); /* The tail is at its initial state too. */
        t->head = 0;
        offset = 0;
        goto put;
    }

    /* Both tail an had must be set to a valid entry offset. */
    assert(t->head < t->size - METADATA_SIZE);
    assert(t->tail < t->size - METADATA_SIZE);

    /* If we aren't wrapped, the head must be at the beginning of the buffer. */
    assert(isWrapped(t) || t->head == 0);

    /* The candidate insertion point is right past the last entry we wrote. */
    offset = t->tail + getEntrySize(t, t->tail);

    /* Calculate how many bytes we need for this new entry. */
    size = ENTRY_SIZE(metadata.len);

    /* The happy case is that we can write this new entry right after the one
     * currently at t->tail. */
    if (offset + size <= (isWrapped(t) ? t->head : t->size)) {
        goto put;
    }

    /* If we are wrapped check if it's possible to make room for this new entry
     * by shifting the head forward: if it's not possible, we just delete all
     * entries and place this entry at the beginning of the buffer. Otherwise,
     * if we are not wrapped, we need to wrap and shift the head forward. */
    if (isWrapped(t)) {
        if (offset + size > t->size) {
            t->head = 0;
            offset = 0;
            goto put;
        }
    } else {
        if (offset + METADATA_SIZE <= t->size) {
            putDummyEntry(t, offset);
        }
        offset = 0;
    }

    /* Shift the head forward, deleting the older entries until enough bytes
     * become available. */
    while (offset + size > t->head) {
        t->head += getEntrySize(t, t->head);

        /* Check if the new head is still pointing to a valid entry or the old
         * head was actually the last one before the end of the buffer (either
         * because there's not room for a further entry or there is dummy entry
         * place holder): in the latter case we need to wrap the head back to
         * the beginning of the buffer. We can be sure that there is now enough
         * room for the new entry, because we checked earlier that was possible
         * to make enough room by deleting one more entries. */
        if (hasNoEntryAtOffset(t, t->head)) {
            t->head = 0;
            break;
        }
    }

put:
    putEntry(t, offset, &metadata, message);
    t->tail = offset;
}

void raft_tracer_walk(const struct raft_tracer *t,
                      raft_tracer_walk_cb cb,
                      void *data)
{
    size_t offset = t->head;

    /* If there are no entries, there's nothing to do. */
    if (t->head == t->size) {
        assert(t->tail == t->size);
        return;
    }

    while (1) {
        struct metadata metadata;
        const char *message;
        getEntry(t, offset, &metadata, &message);
        cb(data, metadata.time, metadata.type, message);

        /* Check if we have exhausted all entries. */
        if (offset == t->tail) {
            break;
        }

        /* Advance to the next entry. */
        offset += getEntrySize(t, offset);
        if (hasNoEntryAtOffset(t, offset)) {
            offset = 0;
        }
    }
}
