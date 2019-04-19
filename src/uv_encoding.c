#include <string.h>

#include "../include/raft/uv.h"

#include "assert.h"
#include "byte.h"
#include "configuration.h"
#include "uv_encoding.h"

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

static size_t raft_io_uv_sizeof__install_snapshot(
    const struct raft_install_snapshot *p)
{
    size_t conf_size = configuration__encoded_size(&p->conf);
    return sizeof(uint64_t) + /* Leader's term. */
           sizeof(uint64_t) + /* Leader ID */
           sizeof(uint64_t) + /* Snapshot's last index */
           sizeof(uint64_t) + /* Term of last index */
           sizeof(uint64_t) + /* Configuration's index */
           sizeof(uint64_t) + /* Length of configuration */
           conf_size +        /* Configuration data */
           sizeof(uint64_t);  /* Length of snapshot data */
}

size_t uvSizeofBatchHeader(size_t n)
{
    return 8 + /* Number of entries in the batch, little endian */
           16 * n /* One header per entry */;
}

static void raft_io_uv_encode__request_vote(const struct raft_request_vote *p,
                                            void *buf)
{
    void *cursor = buf;

    byte__put64(&cursor, p->term);
    byte__put64(&cursor, p->candidate_id);
    byte__put64(&cursor, p->last_log_index);
    byte__put64(&cursor, p->last_log_term);
}

static void raft_io_uv_encode__request_vote_result(
    const struct raft_request_vote_result *p,
    void *buf)
{
    void *cursor = buf;

    byte__put64(&cursor, p->term);
    byte__put64(&cursor, p->vote_granted);
}

static void raft_io_uv_encode__append_entries(
    const struct raft_append_entries *p,
    void *buf)
{
    void *cursor;

    cursor = buf;

    byte__put64(&cursor, p->term);           /* Leader's term. */
    byte__put64(&cursor, p->leader_id);      /* Leader ID. */
    byte__put64(&cursor, p->prev_log_index); /* Previous index. */
    byte__put64(&cursor, p->prev_log_term);  /* Previous term. */
    byte__put64(&cursor, p->leader_commit);  /* Commit index. */

    uvEncodeBatchHeader(p->entries, p->n_entries, cursor);
}

static void raft_io_uv_encode__append_entries_result(
    const struct raft_append_entries_result *p,
    void *buf)
{
    void *cursor = buf;

    byte__put64(&cursor, p->term);
    byte__put64(&cursor, p->rejected);
    byte__put64(&cursor, p->last_log_index);
}

static void raft_io_uv_encode__install_snapshot(
    const struct raft_install_snapshot *p,
    void *buf)
{
    void *cursor;
    size_t conf_size = configuration__encoded_size(&p->conf);

    cursor = buf;

    byte__put64(&cursor, p->term);       /* Leader's term. */
    byte__put64(&cursor, p->leader_id);  /* Leader ID. */
    byte__put64(&cursor, p->last_index); /* Snapshot last index. */
    byte__put64(&cursor, p->last_term);  /* Term of last index. */
    byte__put64(&cursor, p->conf_index); /* Configuration index. */
    byte__put64(&cursor, conf_size);     /* Configuration length. */
    configuration__encode_to_buf(&p->conf, cursor);
    cursor += conf_size;
    byte__put64(&cursor, p->data.len); /* Snapshot data size. */
}

int uvEncodeMessage(const struct raft_message *message,
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
        case RAFT_IO_INSTALL_SNAPSHOT:
            header.len +=
                raft_io_uv_sizeof__install_snapshot(&message->install_snapshot);
            break;
        default:
            return RAFT_MALFORMED;
    };

    header.base = raft_malloc(header.len);
    if (header.base == NULL) {
        goto oom;
    }

    cursor = header.base;

    /* Encode the request preamble, with message type and message size. */
    byte__put64(&cursor, message->type);
    byte__put64(&cursor, header.len - RAFT_IO_UV__PREAMBLE_SIZE);

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
        case RAFT_IO_INSTALL_SNAPSHOT:
            raft_io_uv_encode__install_snapshot(&message->install_snapshot,
                                                cursor);
            break;
    };

    *n_bufs = 1;

    /* For AppendEntries request we also send the entries payload. */
    if (message->type == RAFT_IO_APPEND_ENTRIES) {
        *n_bufs += message->append_entries.n_entries;
    }

    /* For InstallSnapshot request we also send the snapshot payload. */
    if (message->type == RAFT_IO_INSTALL_SNAPSHOT) {
        *n_bufs += 1;
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

    if (message->type == RAFT_IO_INSTALL_SNAPSHOT) {
        (*bufs)[1].base = message->install_snapshot.data.base;
        (*bufs)[1].len = message->install_snapshot.data.len;
    }

    return 0;

oom_after_header_alloc:
    raft_free(header.base);

oom:
    return RAFT_NOMEM;
}

void uvEncodeBatchHeader(const struct raft_entry *entries,
                                unsigned n,
                                void *buf)
{
    unsigned i;
    void *cursor = buf;

    /* Number of entries in the batch, little endian */
    byte__put64(&cursor, n);

    for (i = 0; i < n; i++) {
        const struct raft_entry *entry = &entries[i];

        /* Term in which the entry was created, little endian. */
        byte__put64(&cursor, entry->term);

        /* Message type (Either RAFT_COMMAND or RAFT_CHANGE) */
        byte__put8(&cursor, entry->type);

        cursor += 3; /* Unused */

        /* Size of the log entry data, little endian. */
        byte__put32(&cursor, entry->buf.len);
    }
}

static void raft_io_uv_decode__request_vote(const uv_buf_t *buf,
                                            struct raft_request_vote *p)
{
    const void *cursor;

    cursor = buf->base;

    p->term = byte__get64(&cursor);
    p->candidate_id = byte__get64(&cursor);
    p->last_log_index = byte__get64(&cursor);
    p->last_log_term = byte__get64(&cursor);
}

static void raft_io_uv_decode__request_vote_result(
    const uv_buf_t *buf,
    struct raft_request_vote_result *p)
{
    const void *cursor;

    cursor = buf->base;

    p->term = byte__get64(&cursor);
    p->vote_granted = byte__get64(&cursor);
}

int uvDecodeBatchHeader(const void *batch,
                               struct raft_entry **entries,
                               unsigned *n)
{
    const void *cursor = batch;
    size_t i;
    int rv;

    *n = byte__get64(&cursor);

    if (*n == 0) {
        *entries = NULL;
        return 0;
    }

    *entries = raft_malloc(*n * sizeof **entries);

    if (*entries == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    for (i = 0; i < *n; i++) {
        struct raft_entry *entry = &(*entries)[i];

        entry->term = byte__get64(&cursor);
        entry->type = byte__get8(&cursor);

        if (entry->type != RAFT_COMMAND && entry->type != RAFT_BARRIER &&
            entry->type != RAFT_CHANGE) {
            rv = RAFT_MALFORMED;
            goto err_after_alloc;
        }

        cursor += 3; /* Unused */

        /* Size of the log entry data, little endian. */
        entry->buf.len = byte__get32(&cursor);
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

    args->term = byte__get64(&cursor);
    args->leader_id = byte__get64(&cursor);
    args->prev_log_index = byte__get64(&cursor);
    args->prev_log_term = byte__get64(&cursor);
    args->leader_commit = byte__get64(&cursor);

    rv = uvDecodeBatchHeader(cursor, &args->entries, &args->n_entries);
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

    p->term = byte__get64(&cursor);
    p->rejected = byte__get64(&cursor);
    p->last_log_index = byte__get64(&cursor);
}

static int raft_io_uv_decode__install_snapshot(
    const uv_buf_t *buf,
    struct raft_install_snapshot *args)
{
    const void *cursor;
    struct raft_buffer conf;
    int rv;

    assert(buf != NULL);
    assert(args != NULL);

    cursor = buf->base;

    args->term = byte__get64(&cursor);
    args->leader_id = byte__get64(&cursor);
    args->last_index = byte__get64(&cursor);
    args->last_term = byte__get64(&cursor);
    args->conf_index = byte__get64(&cursor);
    conf.len = byte__get64(&cursor);
    conf.base = (void *)cursor;
    raft_configuration_init(&args->conf);
    rv = configuration__decode(&conf, &args->conf);
    if (rv != 0) {
        return rv;
    }
    cursor += conf.len;
    args->data.len = byte__get64(&cursor);

    return 0;
}

int uvDecodeMessage(unsigned type,
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
        case RAFT_IO_INSTALL_SNAPSHOT:
            rv = raft_io_uv_decode__install_snapshot(
                header, &message->install_snapshot);
            *payload_len += message->install_snapshot.data.len;
            break;
        default:
            rv = RAFT_IOERR;
            break;
    };

    return rv;
}

void uvDecodeEntriesBatch(const struct raft_buffer *buf,
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
