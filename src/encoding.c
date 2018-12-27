#include <assert.h>
#include <string.h>

#include "../include/raft.h"

#include "binary.h"

#define RAFT_ENCODING__VERSION 1

/* We rely on the size of char to be 8 bits. */
#ifdef static_assert
static_assert(sizeof(char) == sizeof(uint8_t), "Size of 'char' is not 8 bits");
#endif

size_t raft_batch_header_size(size_t n)
{
    return 8 + /* Number of entries in the batch, little endian */
           16 * n /* One header per entry */;
}

static size_t raft_encode__configuration_size(
    const struct raft_configuration *c)
{
    size_t n = 0;
    size_t i;

    /* We need one byte for the encoding format version */
    n++;

    /* Then 8 bytes for number of servers. */
    n += sizeof(uint64_t);

    /* Then some space for each server. */
    for (i = 0; i < c->n; i++) {
        struct raft_server *server = &c->servers[i];

        assert(server->address != NULL);

        n += sizeof(uint64_t) /* Server ID */;
        n += strlen(server->address) + 1;
        n++; /* Voting flag */
    };

    return n;
}

int raft_encode_configuration(const struct raft_configuration *c,
                              struct raft_buffer *buf)
{
    void *cursor;
    size_t i;

    assert(c != NULL);
    assert(buf != NULL);

    /* The configuration can't be empty. */
    if (c->n == 0) {
        return RAFT_ERR_EMPTY_CONFIGURATION;
    }

    buf->len = raft_encode__configuration_size(c);
    buf->base = raft_malloc(buf->len);

    if (buf->base == NULL) {
        return RAFT_ERR_NOMEM;
    }

    cursor = buf->base;

    /* Encoding version */
    raft__put8(&cursor, RAFT_ENCODING__VERSION);

    /* Number of servers */
    raft__put64(&cursor, c->n);

    for (i = 0; i < c->n; i++) {
        struct raft_server *server = &c->servers[i];

        assert(server->address != NULL);

        raft__put64(&cursor, server->id);

        strcpy((char *)cursor, server->address);
        cursor += strlen(server->address) + 1;

        raft__put8(&cursor, server->voting);
    };

    return 0;
}

int raft_decode_configuration(const struct raft_buffer *buf,
                              struct raft_configuration *c)
{
    const void *cursor;
    size_t i;
    size_t n;

    assert(c != NULL);
    assert(buf != NULL);

    /* TODO: use 'if' instead of assert for checking buffer boundaries */
    assert(buf->len > 0);

    /* Check that the target configuration is empty. */
    if (c->n != 0) {
        return RAFT_ERR_CONFIGURATION_NOT_EMPTY;
    }
    assert(c->servers == NULL);

    cursor = buf->base;

    /* Check the encoding format version */
    if (raft__get8(&cursor) != RAFT_ENCODING__VERSION) {
        return RAFT_ERR_MALFORMED;
    }

    /* Read the number of servers. */
    n = raft__get64(&cursor);

    /* Decode the individual servers. */
    for (i = 0; i < n; i++) {
        unsigned id;
        size_t address_len = 0;
        const char *address;
        bool voting;
        int rv;

        /* Server ID. */
        id = raft__get64(&cursor);

        /* Server Address. */
        while (cursor + address_len < buf->base + buf->len) {
            if (*(char *)(cursor + address_len) == 0) {
                break;
            }
            address_len++;
        }
        if (cursor + address_len == buf->base + buf->len) {
            return RAFT_ERR_MALFORMED;
        }
        address = (const char *)cursor;
        cursor += address_len + 1;

        /* Voting flag. */
        voting = raft__get8(&cursor);

        rv = raft_configuration_add(c, id, address, voting);
        if (rv != 0) {
            return rv;
        }
    }

    return 0;
}

static void raft_encode__batch_header(const struct raft_entry *entries,
                                      size_t n,
                                      void *batch)
{
    size_t i;
    void *cursor;

    assert((entries == NULL && n == 0) || (entries != NULL && n != 0));
    assert(batch != NULL);

    cursor = batch;

    /* Number of entries in the batch, little endian */
    raft__put64(&cursor, n);

    for (i = 0; i < n; i++) {
        const struct raft_entry *entry = &entries[i];

        /* Term in which the entry was created, little endian. */
        raft__put64(&cursor, entry->term);

        /* Message type (Either RAFT_LOG_COMMAND or RAFT_LOG_CONFIGURATION) */
        raft__put8(&cursor, entry->type);

        cursor += 3; /* Unused */

        /* Size of the log entry data, little endian. */
        raft__put32(&cursor, entry->buf.len);
    }
}

int raft_decode_batch_header(const void *batch,
                             struct raft_entry **entries,
                             unsigned *n)
{
    const void *cursor = batch;
    size_t i;
    int rv;

    *n = raft__get64(&cursor);

    if (*n == 0) {
        *entries = NULL;
        return 0;
    }

    *entries = raft_malloc(*n * sizeof **entries);

    if (*entries == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    for (i = 0; i < *n; i++) {
        struct raft_entry *entry = &(*entries)[i];

        entry->term = raft__get64(&cursor);
        entry->type = raft__get8(&cursor);

        if (entry->type != RAFT_LOG_COMMAND &&
            entry->type != RAFT_LOG_CONFIGURATION) {
            rv = RAFT_ERR_MALFORMED;
            goto err_after_alloc;
        }

        cursor += 3; /* Unused */

        /* Size of the log entry data, little endian. */
        entry->buf.len = raft__get32(&cursor);
    }

    return 0;

err_after_alloc:
    raft_free(entries);

err:
    assert(rv != 0);

    return rv;
}

int raft_decode_entries_batch(const struct raft_buffer *buf,
                              struct raft_entry *entries,
                              unsigned n)
{
    void *cursor;
    size_t i;

    assert(buf != NULL);

    cursor = buf->base;

    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        entry->batch = buf->base;

        if (entry->buf.len == 0) {
            entry->buf.base = NULL;
            continue;
        }

        entry->buf.base = cursor;

        cursor += entry->buf.len;
        if (entry->buf.len % 8 != 0) {
            /* Add padding */
            cursor += 8 - (entry->buf.len % 8);
        }
    }

    return 0;
}

int raft_encode_append_entries(const struct raft_append_entries_args *args,
                               struct raft_buffer *buf)
{
    void *cursor;

    assert(args != NULL);
    assert(buf != NULL);

    buf->len = 0;

    buf->len += 8; /* Slot for protocol version and message type. */
    buf->len += 8; /* Slot for the message size. */
    buf->len += 8; /* Leader's term. */
    buf->len += 8; /* Leader ID. */
    buf->len += 8; /* Previous log entry index. */
    buf->len += 8; /* Previous log entry term. */
    buf->len += 8; /* Leader's commit index. */
    buf->len += raft_batch_header_size(args->n);

    buf->base = raft_malloc(buf->len);

    if (buf->base == NULL) {
        return RAFT_ERR_NOMEM;
    }

    cursor = buf->base;

    raft__put32(&cursor, RAFT_ENCODING__VERSION); /* Encode version */
    raft__put32(&cursor, RAFT_IO_APPEND_ENTRIES); /* Message type */

    raft__put64(&cursor, buf->len - 16); /* Exclude the message header */

    raft__put64(&cursor, args->term);           /* Leader's term. */
    raft__put64(&cursor, args->leader_id);      /* Leader ID. */
    raft__put64(&cursor, args->prev_log_index); /* Previous index. */
    raft__put64(&cursor, args->prev_log_term);  /* Previous term. */
    raft__put64(&cursor, args->leader_commit);  /* Commit index. */

    raft_encode__batch_header(args->entries, args->n, cursor);

    return 0;
}

int raft_decode_append_entries(const struct raft_buffer *buf,
                               struct raft_append_entries_args *args)
{
    const void *cursor;
    int rv;

    assert(buf != NULL);
    assert(args != NULL);

    cursor = buf->base;

    args->term = raft__get64(&cursor);
    args->leader_id = raft__get64(&cursor);
    args->prev_log_index = raft__get64(&cursor);
    args->prev_log_term = raft__get64(&cursor);
    args->leader_commit = raft__get64(&cursor);

    rv = raft_decode_batch_header(cursor, &args->entries, &args->n);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_encode_append_entries_result(
    const struct raft_append_entries_result *result,
    struct raft_buffer *buf)
{
    void *cursor;

    assert(result != NULL);
    assert(buf != NULL);

    buf->len = 0;

    buf->len += 8; /* Slot for protocol version and message type. */
    buf->len += 8; /* Slot for the message size. */
    buf->len += 8; /* Term. */
    buf->len += 8; /* Success. */
    buf->len += 8; /* Last log index. */

    buf->base = raft_malloc(buf->len);

    if (buf->base == NULL) {
        return RAFT_ERR_NOMEM;
    }

    cursor = buf->base;

    raft__put32(&cursor, RAFT_ENCODING__VERSION);        /* Encode version */
    raft__put32(&cursor, RAFT_IO_APPEND_ENTRIES_RESULT); /* Message type */
    raft__put64(&cursor, buf->len - 16); /* Exclude the message header */

    raft__put64(&cursor, result->term);
    raft__put64(&cursor, result->success);
    raft__put64(&cursor, result->last_log_index);

    return 0;
}

int raft_decode_append_entries_result(const struct raft_buffer *buf,
                                      struct raft_append_entries_result *result)
{
    const void *cursor;

    assert(buf != NULL);
    assert(result != NULL);

    cursor = buf->base;

    result->term = raft__get64(&cursor);
    result->success = raft__get64(&cursor);
    result->last_log_index = raft__get64(&cursor);

    return 0;
}

int raft_encode_request_vote(const struct raft_request_vote_args *args,
                             struct raft_buffer *buf)
{
    void *cursor;

    assert(args != NULL);
    assert(buf != NULL);

    buf->len = 0;

    buf->len += 8; /* Slot for protocol version and message type. */
    buf->len += 8; /* Slot for the message size. */
    buf->len += 8; /* Term. */
    buf->len += 8; /* Candidate ID. */
    buf->len += 8; /* Last log index. */
    buf->len += 8; /* Last log term. */

    buf->base = raft_malloc(buf->len);

    if (buf->base == NULL) {
        return RAFT_ERR_NOMEM;
    }

    cursor = buf->base;

    raft__put32(&cursor, RAFT_ENCODING__VERSION); /* Encode version */
    raft__put32(&cursor, RAFT_IO_REQUEST_VOTE);   /* Message type */
    raft__put64(&cursor, buf->len - 16); /* Exclude the message header */

    raft__put64(&cursor, args->term);
    raft__put64(&cursor, args->candidate_id);
    raft__put64(&cursor, args->last_log_index);
    raft__put64(&cursor, args->last_log_term);

    return 0;
}

int raft_decode_request_vote(const struct raft_buffer *buf,
                             struct raft_request_vote_args *args)
{
    const void *cursor;

    assert(buf != NULL);
    assert(args != NULL);

    cursor = buf->base;

    args->term = raft__get64(&cursor);
    args->candidate_id = raft__get64(&cursor);
    args->last_log_index = raft__get64(&cursor);
    args->last_log_term = raft__get64(&cursor);

    return 0;
}

int raft_encode_request_vote_result(
    const struct raft_request_vote_result *result,
    struct raft_buffer *buf)
{
    void *cursor;

    assert(result != NULL);
    assert(buf != NULL);

    buf->len = 0;

    buf->len += 8; /* Slot for protocol version and message type. */
    buf->len += 8; /* Slot for the message size. */
    buf->len += 8; /* Term. */
    buf->len += 8; /* Vote granted. */

    buf->base = raft_malloc(buf->len);

    if (buf->base == NULL) {
        return RAFT_ERR_NOMEM;
    }

    cursor = buf->base;

    raft__put32(&cursor, RAFT_ENCODING__VERSION);
    raft__put32(&cursor, RAFT_IO_REQUEST_VOTE_RESULT);
    raft__put64(&cursor, buf->len - 16);

    raft__put64(&cursor, result->term);
    raft__put64(&cursor, result->vote_granted);

    return 0;
}

int raft_decode_request_vote_result(const struct raft_buffer *buf,
                                    struct raft_request_vote_result *result)
{
    const void *cursor;

    assert(buf != NULL);
    assert(result != NULL);

    cursor = buf->base;

    result->term = raft__get64(&cursor);
    result->vote_granted = raft__get64(&cursor);

    return 0;
}
