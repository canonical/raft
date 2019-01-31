#include "io_uv_encoding.h"
#include <assert.h>
#include <string.h>
#include "binary.h"

/**
 * Size of the request preable.
 */
#define RAFT_IO_UV__PREAMBLE_SIZE           \
    (sizeof(uint64_t) /* Message type. */ + \
     sizeof(uint64_t) /* Message size. */)

size_t raft_io_uv_sizeof__request_vote()
{
    return sizeof(uint64_t) + /* Term. */
           sizeof(uint64_t) + /* Candidate ID. */
           sizeof(uint64_t) + /* Last log index. */
           sizeof(uint64_t) /* Last log term. */;
}

static size_t raft_io_uv_sizeof__request_vote_result()
{
    return sizeof(uint64_t) + /* Term. */
           sizeof(uint64_t) /* Vote granted. */;
}

static size_t raft_io_uv_sizeof__append_entries(
    const struct raft_append_entries *p)
{
    return sizeof(uint64_t) + /* Leader's term. */
           sizeof(uint64_t) + /* Leader ID */
           sizeof(uint64_t) + /* Previous log entry index */
           sizeof(uint64_t) + /* Previous log entry term */
           sizeof(uint64_t) + /* Leader's commit index */
           sizeof(uint64_t) + /* Number of entries in the batch */
           16 * p->n_entries /* One header per entry */;
}

static size_t raft_io_uv_sizeof__append_entries_result()
{
    return sizeof(uint64_t) + /* Term. */
           sizeof(uint64_t) + /* Success. */
           sizeof(uint64_t) /* Last log index. */;
}

size_t raft_io_uv_sizeof__batch_header(size_t n)
{
    return 8 + /* Number of entries in the batch, little endian */
           16 * n /* One header per entry */;
}

static void raft_io_uv_encode__request_vote(const struct raft_request_vote *p,
                                            void *buf)
{
    void *cursor = buf;

    raft__put64(&cursor, p->term);
    raft__put64(&cursor, p->candidate_id);
    raft__put64(&cursor, p->last_log_index);
    raft__put64(&cursor, p->last_log_term);
}

static void raft_io_uv_encode__request_vote_result(
    const struct raft_request_vote_result *p,
    void *buf)
{
    void *cursor = buf;

    raft__put64(&cursor, p->term);
    raft__put64(&cursor, p->vote_granted);
}

static void raft_io_uv_encode__append_entries(
    const struct raft_append_entries *p,
    void *buf)
{
    size_t i;
    void *cursor;

    cursor = buf;

    raft__put64(&cursor, p->term);           /* Leader's term. */
    raft__put64(&cursor, p->leader_id);      /* Leader ID. */
    raft__put64(&cursor, p->prev_log_index); /* Previous index. */
    raft__put64(&cursor, p->prev_log_term);  /* Previous term. */
    raft__put64(&cursor, p->leader_commit);  /* Commit index. */

    /* Number of entries in the batch, little endian */
    raft__put64(&cursor, p->n_entries);

    for (i = 0; i < p->n_entries; i++) {
        const struct raft_entry *entry = &p->entries[i];

        /* Term in which the entry was created, little endian. */
        raft__put64(&cursor, entry->term);

        /* Message type (Either RAFT_LOG_COMMAND or RAFT_LOG_CONFIGURATION) */
        raft__put8(&cursor, entry->type);

        cursor += 3; /* Unused */

        /* Size of the log entry data, little endian. */
        raft__put32(&cursor, entry->buf.len);
    }
}

static void raft_io_uv_encode__append_entries_result(
    const struct raft_append_entries_result *p,
    void *buf)
{
    void *cursor = buf;

    raft__put64(&cursor, p->term);
    raft__put64(&cursor, p->success);
    raft__put64(&cursor, p->last_log_index);
}

int raft_io_uv_encode__message(const struct raft_message *message,
                               uv_buf_t **bufs,
                               unsigned *n_bufs)
{
    uv_buf_t header;
    void *cursor;

    /* Figure out the length of the header for this request and allocate a
     * buffer for it. */
    header.len = RAFT_IO_UV__PREAMBLE_SIZE;
    switch (message->type) {
        case RAFT_IO_REQUEST_VOTE:
            header.len += raft_io_uv_sizeof__request_vote();
            break;
        case RAFT_IO_REQUEST_VOTE_RESULT:
            header.len += raft_io_uv_sizeof__request_vote_result();
            break;
        case RAFT_IO_APPEND_ENTRIES:
            header.len +=
                raft_io_uv_sizeof__append_entries(&message->append_entries);
            break;
        case RAFT_IO_APPEND_ENTRIES_RESULT:
            header.len += raft_io_uv_sizeof__append_entries_result();
            break;
        default:
            return RAFT_ERR_IO_MALFORMED;
    };

    header.base = raft_malloc(header.len);
    if (header.base == NULL) {
        goto oom;
    }

    cursor = header.base;

    /* Encode the request preamble, with message type and message size. */
    raft__put64(&cursor, message->type);
    raft__put64(&cursor, header.len - RAFT_IO_UV__PREAMBLE_SIZE);

    /* Encode the request header. */
    switch (message->type) {
        case RAFT_IO_REQUEST_VOTE:
            raft_io_uv_encode__request_vote(&message->request_vote, cursor);
            break;
        case RAFT_IO_REQUEST_VOTE_RESULT:
            raft_io_uv_encode__request_vote_result(
                &message->request_vote_result, cursor);
            break;
        case RAFT_IO_APPEND_ENTRIES:
            raft_io_uv_encode__append_entries(&message->append_entries, cursor);
            break;
        case RAFT_IO_APPEND_ENTRIES_RESULT:
            raft_io_uv_encode__append_entries_result(
                &message->append_entries_result, cursor);
            break;
    };

    *n_bufs = 1;

    /* For AppendEntries request we also send the entries payload. */
    if (message->type == RAFT_IO_APPEND_ENTRIES) {
        *n_bufs += message->append_entries.n_entries;
    }

    *bufs = raft_calloc(*n_bufs, sizeof **bufs);
    if (*bufs == NULL) {
        goto oom_after_header_alloc;
    }

    (*bufs)[0] = header;

    if (message->type == RAFT_IO_APPEND_ENTRIES) {
        unsigned i;
        for (i = 0; i < message->append_entries.n_entries; i++) {
            const struct raft_entry *entry =
                &message->append_entries.entries[i];
            (*bufs)[i + 1].base = entry->buf.base;
            (*bufs)[i + 1].len = entry->buf.len;
        }
    }

    return 0;

oom_after_header_alloc:
    raft_free(header.base);

oom:
    return RAFT_ERR_NOMEM;
}

static void raft_io_uv_decode__request_vote(const uv_buf_t *buf,
                                            struct raft_request_vote *p)
{
    const void *cursor;

    cursor = buf->base;

    p->term = raft__get64(&cursor);
    p->candidate_id = raft__get64(&cursor);
    p->last_log_index = raft__get64(&cursor);
    p->last_log_term = raft__get64(&cursor);
}

static void raft_io_uv_decode__request_vote_result(
    const uv_buf_t *buf,
    struct raft_request_vote_result *p)
{
    const void *cursor;

    cursor = buf->base;

    p->term = raft__get64(&cursor);
    p->vote_granted = raft__get64(&cursor);
}

int raft_io_uv_decode__batch_header(const void *batch,
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

static int raft_io_uv_decode__append_entries(const uv_buf_t *buf,
                                             struct raft_append_entries *args)
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

    rv = raft_io_uv_decode__batch_header(cursor, &args->entries,
                                         &args->n_entries);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static void raft_io_uv_decode__append_entries_result(
    const uv_buf_t *buf,
    struct raft_append_entries_result *p)
{
    const void *cursor;

    cursor = buf->base;

    p->term = raft__get64(&cursor);
    p->success = raft__get64(&cursor);
    p->last_log_index = raft__get64(&cursor);
}

int raft_io_uv_decode__message(unsigned type,
                               const uv_buf_t *header,
                               struct raft_message *message,
                               size_t *payload_len)
{
    unsigned i;
    int rv = 0;

    message->type = type;

    *payload_len = 0;

    /* Decode the header. */
    switch (type) {
        case RAFT_IO_REQUEST_VOTE:
            raft_io_uv_decode__request_vote(header, &message->request_vote);
            break;
        case RAFT_IO_REQUEST_VOTE_RESULT:
            raft_io_uv_decode__request_vote_result(
                header, &message->request_vote_result);
            break;
        case RAFT_IO_APPEND_ENTRIES:
            rv = raft_io_uv_decode__append_entries(header,
                                                   &message->append_entries);
            for (i = 0; i < message->append_entries.n_entries; i++) {
                *payload_len += message->append_entries.entries[i].buf.len;
            }
            break;
        case RAFT_IO_APPEND_ENTRIES_RESULT:
            raft_io_uv_decode__append_entries_result(
                header, &message->append_entries_result);
            break;
        default:
            rv = RAFT_ERR_IO;
            break;
    };

    return rv;
}

void raft_io_uv_decode__entries_batch(const struct raft_buffer *buf,
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
}
