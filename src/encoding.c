#include <assert.h>
#include <string.h>

#include "../include/raft.h"

#include "binary.h"

#define RAFT_ENCODING__VERSION 1

static void raft_encode__uint8(void **cursor, uint8_t value)
{
    *(uint8_t *)(*cursor) = value;
    *cursor += sizeof(uint8_t);
}

static void raft_encode__uint32(void **cursor, uint32_t value)
{
    *(uint32_t *)(*cursor) = value;
    *cursor += sizeof(uint32_t);
}

static void raft_encode__uint64(void **cursor, uint64_t value)
{
    *(uint64_t *)(*cursor) = raft__flip64(value);
    *cursor += sizeof(uint64_t);
}

static uint8_t raft_decode__uint8(void **cursor)
{
    uint8_t value = *(uint8_t *)(*cursor);
    *cursor += sizeof(uint8_t);
    return value;
}

static uint32_t raft_decode__uint32(void **cursor)
{
    uint32_t value = raft__flip32(*(uint32_t *)(*cursor));
    *cursor += sizeof(uint32_t);
    return value;
}

static uint64_t raft_decode__uint64(void **cursor)
{
    uint64_t value = raft__flip64(*(uint64_t *)(*cursor));
    *cursor += sizeof(uint64_t);
    return value;
}

static size_t raft_encode__batch_header_size(size_t n)
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
    raft_encode__uint8(&cursor, RAFT_ENCODING__VERSION);

    /* Number of servers */
    raft_encode__uint64(&cursor, c->n);

    for (i = 0; i < c->n; i++) {
        struct raft_server *server = &c->servers[i];

        assert(server->address != NULL);

        raft_encode__uint64(&cursor, server->id);

        strcpy((char *)cursor, server->address);
        cursor += strlen(server->address) + 1;

        raft_encode__uint8(&cursor, server->voting);
    };

    return 0;
}

static size_t raft_decode__configuration_size(const struct raft_buffer *buf)
{
    void *cursor = buf->base;
    size_t size = 0;
    size_t n_servers;
    size_t i;

    assert(buf->base != NULL);
    assert(buf->len > 0);

    /* Check the encoding format version */
    if (raft_decode__uint8(&cursor) != RAFT_ENCODING__VERSION) {
        return 0;
    }

    assert(cursor < buf->base + buf->len);

    /* Read the number of servers. */
    n_servers = raft_decode__uint64(&cursor);

    /* Space needed for the raft_server array */
    size += n_servers * sizeof(struct raft_server);

    assert(cursor < buf->base + buf->len);

    /* Figure the space needed for the addresses. */
    for (i = 0; i < n_servers; i++) {
        size_t address_len = 0;

        /* Skip server ID. */
	raft_decode__uint64(&cursor);

        /* Server Address. */
        while (cursor < buf->base + buf->len) {
            address_len++;
            if (*(char*)cursor == 0) {
                break;
            }
            cursor++;
        }
        if (cursor == buf->base + buf->len) {
            return 0;
        }
        cursor++;

        size += address_len;

        /* Skip voting flag. */
	raft_decode__uint8(&cursor);
    };

    return size;
}

int raft_decode_configuration(const struct raft_buffer *buf,
                              struct raft_configuration *c)
{
    void *cursor;
    uint8_t *addresses;
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

    n = raft_decode__configuration_size(buf);
    if (n == 0) {
        return RAFT_ERR_MALFORMED;
    }

    /* Allocate enough space for the raft_server objects and the addresses
       strings. */
    c->servers = raft_malloc(n);
    if (c->servers == NULL) {
        return RAFT_ERR_NOMEM;
    }

    cursor = buf->base;

    /* Skip the version byte */
    raft_decode__uint8(&cursor);

    assert(cursor < buf->base + n);

    /* Read the number of servers. */
    c->n = raft_decode__uint64(&cursor);

    assert(cursor < buf->base + n);

    /* Set addresses buffer, just after the servers array. */
    addresses = (uint8_t *)(c->servers) + c->n * sizeof(struct raft_server);

    /* Decode the individual server objects. */
    for (i = 0; i < c->n; i++) {
        size_t address_len;

        /* Server ID. */
        c->servers[i].id = raft_decode__uint64(&cursor);

        /* Server Address. */
        c->servers[i].address = (char *)addresses;
        strcpy((char *)addresses, (const char *)cursor);

        address_len = strlen((const char *)cursor) + 1;
        cursor += address_len;
        addresses += address_len;

        /* Voting flag. */
        c->servers[i].voting = raft_decode__uint8(&cursor);
    };

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
    raft_encode__uint64(&cursor, n);

    for (i = 0; i < n; i++) {
        const struct raft_entry *entry = &entries[i];

        /* Term in which the entry was created, little endian. */
        raft_encode__uint64(&cursor, entry->term);

        /* Message type (Either RAFT_LOG_COMMAND or RAFT_LOG_CONFIGURATION) */
        raft_encode__uint8(&cursor, entry->type);

        cursor += 3; /* Unused */

        /* Size of the log entry data, little endian. */
        raft_encode__uint32(&cursor, entry->buf.len);
    }
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
    buf->len += raft_encode__batch_header_size(args->n);

    buf->base = raft_malloc(buf->len);

    if (buf->base == NULL) {
        return RAFT_ERR_NOMEM;
    }

    cursor = buf->base;

    raft_encode__uint32(&cursor, RAFT_ENCODING__VERSION); /* Encode version */
    raft_encode__uint32(&cursor, RAFT_IO_APPEND_ENTRIES); /* Message type */

    raft_encode__uint64(&cursor,
                        buf->len - 16); /* Exclude the message header */

    raft_encode__uint64(&cursor, args->term);           /* Leader's term. */
    raft_encode__uint64(&cursor, args->leader_id);      /* Leader ID. */
    raft_encode__uint64(&cursor, args->prev_log_index); /* Previous index. */
    raft_encode__uint64(&cursor, args->prev_log_term);  /* Previous term. */
    raft_encode__uint64(&cursor, args->leader_commit);  /* Commit index. */

    raft_encode__batch_header(args->entries, args->n, cursor);

    return 0;
}

static int raft_decode__batch_header(void *batch,
                                     struct raft_entry **entries,
                                     unsigned *n)
{
    void *cursor = batch;
    size_t i;

    *n = raft_decode__uint64(&cursor);

    if (*n == 0) {
        *entries = NULL;
        return 0;
    }

    *entries = raft_malloc((*n) * sizeof **entries);

    if (*entries == NULL) {
        return RAFT_ERR_NOMEM;
    }

    for (i = 0; i < *n; i++) {
        struct raft_entry *entry = &(*entries)[i];

        entry->term = raft_decode__uint64(&cursor);
        entry->type = raft_decode__uint8(&cursor);

        if (entry->type != RAFT_LOG_COMMAND &&
            entry->type != RAFT_LOG_CONFIGURATION) {
            goto err;
        }

        cursor += 3; /* Unused */

        /* Size of the log entry data, little endian. */
        entry->buf.len = raft_decode__uint32(&cursor);
        entry->batch = batch;
    }

    return 0;

err:
    raft_free(*entries);
    return RAFT_ERR_MALFORMED;
}

int raft_decode_append_entries(const struct raft_buffer *buf,
                               struct raft_append_entries_args *args)
{
    void *cursor;
    int rv;

    assert(buf != NULL);
    assert(args != NULL);

    cursor = buf->base;

    args->term = raft_decode__uint64(&cursor);
    args->leader_id = raft_decode__uint64(&cursor);
    args->prev_log_index = raft_decode__uint64(&cursor);
    args->prev_log_term = raft_decode__uint64(&cursor);
    args->leader_commit = raft_decode__uint64(&cursor);

    rv = raft_decode__batch_header(cursor, &args->entries, &args->n);
    if (rv != 0) {
        return rv;
    }

    return 0;
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

    raft_encode__uint32(&cursor, RAFT_ENCODING__VERSION); /* Encode version */
    raft_encode__uint32(&cursor,
                        RAFT_IO_APPEND_ENTRIES_RESULT); /* Message type */
    raft_encode__uint64(&cursor,
                        buf->len - 16); /* Exclude the message header */

    raft_encode__uint64(&cursor, result->term);
    raft_encode__uint64(&cursor, result->success);
    raft_encode__uint64(&cursor, result->last_log_index);

    return 0;
}

int raft_decode_append_entries_result(const struct raft_buffer *buf,
                                      struct raft_append_entries_result *result)
{
    void *cursor;

    assert(buf != NULL);
    assert(result != NULL);

    cursor = buf->base;

    result->term = raft_decode__uint64(&cursor);
    result->success = raft_decode__uint64(&cursor);
    result->last_log_index = raft_decode__uint64(&cursor);

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

    raft_encode__uint32(&cursor, RAFT_ENCODING__VERSION); /* Encode version */
    raft_encode__uint32(&cursor, RAFT_IO_REQUEST_VOTE);   /* Message type */
    raft_encode__uint64(&cursor,
                        buf->len - 16); /* Exclude the message header */

    raft_encode__uint64(&cursor, args->term);
    raft_encode__uint64(&cursor, args->candidate_id);
    raft_encode__uint64(&cursor, args->last_log_index);
    raft_encode__uint64(&cursor, args->last_log_term);

    return 0;
}

int raft_decode_request_vote(const struct raft_buffer *buf,
                             struct raft_request_vote_args *args)
{
    void *cursor;

    assert(buf != NULL);
    assert(args != NULL);

    cursor = buf->base;

    args->term = raft_decode__uint64(&cursor);
    args->candidate_id = raft_decode__uint64(&cursor);
    args->last_log_index = raft_decode__uint64(&cursor);
    args->last_log_term = raft_decode__uint64(&cursor);

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

    raft_encode__uint32(&cursor, RAFT_ENCODING__VERSION);
    raft_encode__uint32(&cursor, RAFT_IO_REQUEST_VOTE_RESULT);
    raft_encode__uint64(&cursor, buf->len - 16);

    raft_encode__uint64(&cursor, result->term);
    raft_encode__uint64(&cursor, result->vote_granted);

    return 0;
}

int raft_decode_request_vote_result(const struct raft_buffer *buf,
                                    struct raft_request_vote_result *result)
{
    void *cursor;

    assert(buf != NULL);
    assert(result != NULL);

    cursor = buf->base;

    result->term = raft_decode__uint64(&cursor);
    result->vote_granted = raft_decode__uint64(&cursor);

    return 0;
}
