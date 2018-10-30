#include <stdbool.h>
#include <stdio.h>

#include "fault.h"
#include "io.h"

/* Set to 1 to enable logging. */
#if 0
#define __log(MSG) munit_log(MUNIT_LOG_INFO, MSG)
#define __logf(MSG, ...) munit_logf(MUNIT_LOG_INFO, MSG, __VA_ARGS__)
#else
#define __log(MSG)
#define __logf(MSG, ...)
#endif

/**
 * In-memory implementation of the raft_io interface.
 */
struct test_io
{
    /* Term and vote */
    raft_term term;
    unsigned voted_for;

    /* Log */
    struct raft_entry *entries; /* Entries array */
    uint64_t first_index;       /* Index of the first entry */
    size_t n;                   /* Size of the entries array */
    bool pending_write_log;     /* Whether a write log request is in flight */

    /* Queue of in-flight I/O requests. */
    struct test_io_request requests[TEST_IO_REQUEST_QUEUE_SIZE];
    size_t n_requests;

    struct test_fault fault; /* Fault injection state */

    struct test_network *network; /* Optional test network  */
    uint64_t id;                  /* Server ID in the cluster. */
};

/**
 * Initialize the in-memory I/O implementation data.
 * */
static void test_io__init(struct test_io *t)
{
    int i;

    t->term = 0;
    t->voted_for = 0;

    t->entries = NULL;
    t->first_index = 0;
    t->n = 0;

    /* Reset the events array. */
    for (i = 0; i < TEST_IO_REQUEST_QUEUE_SIZE; i++) {
        t->requests[i].type = RAFT_IO_NULL;
    }

    t->n_requests = 0;
    t->pending_write_log = false;

    test_fault_init(&t->fault);

    t->network = NULL;
    t->id = 0;
}

/**
 * Enqueue a pending I/O request.
 */
struct test_io_request *test_io__queue_push(struct raft_io *io,
                                            const size_t id,
                                            const int type)
{
    struct test_io *t = io->data;
    struct test_io_request *request;

    /* Make sure that there's enough capacity in the requests buffer. */
    munit_assert_int(t->n_requests, <, TEST_IO_REQUEST_QUEUE_SIZE);

    request = &t->requests[t->n_requests];
    munit_assert_int(request->type, ==, RAFT_IO_NULL);

    request->io = io;
    request->type = type;
    request->id = id;

    t->n_requests++;

    return request;
}

/**
 * Execute a write log I/O request.
 */
static void test_io__write_log_cb(struct raft_io *io,
                                  struct test_io_request *request)
{
    struct test_io *t = io->data;
    size_t n = request->write_log.n;
    struct raft_entry *all_entries;
    const struct raft_entry *new_entries = request->write_log.entries;
    size_t i;

    munit_assert_ptr_not_null(t);
    munit_assert_ptr_not_null(new_entries);
    munit_assert_int(n, >, 0);

    if (test_fault_tick(&t->fault)) {
        /* TODO: handle faults */
        __logf("io: fail to append %ld log entries", n);
    }

    __logf("io: append %ld log entries (%ld existing)", n, t->n);

    /* If it's the very first write, set the initial index. */
    if (t->first_index == 0) {
        t->first_index = 1;
    }

    /* Allocate an array for the old entries plus ne the new ons. */
    all_entries = munit_malloc((t->n + n) * sizeof *all_entries);

    /* If it's not the very first write, copy the existing entries into the new
     * array. */
    if (t->n > 0) {
        munit_assert_ptr_not_null(t->entries);
        memcpy(all_entries, t->entries, t->n * sizeof *t->entries);
    }

    /* Copy the new entries into the new array. */
    memcpy(all_entries + t->n, new_entries, n * sizeof *new_entries);
    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &all_entries[t->n + i];

        /* Make a copy of the actual entry data. */
        entry->buf.base = munit_malloc(entry->buf.len);
        memcpy(entry->buf.base, new_entries[i].buf.base, entry->buf.len);
    }

    if (t->entries != NULL) {
        free(t->entries);
    }

    t->entries = all_entries;
    t->n += n;
}

void test_io__request_vote_cb(struct raft_io *io,
                              struct test_io_request *request)
{
    struct test_io *t = io->data;
    struct test_host *host;
    struct test_message message;
    int rv;

    if (t->network == NULL) {
        return;
    }

    munit_assert_int(request->request_vote.server.id, !=, 0);

    host = test_network_host(t->network, request->request_vote.server.id);
    munit_assert_ptr_not_null(host);

    rv = raft_encode_request_vote(&request->request_vote.args, &message.header);
    munit_assert_int(rv, ==, 0);

    message.payload.base = NULL;

    munit_assert_int(t->id, !=, 0);
    message.sender_id = t->id;

    test_host_enqueue(host, &message);
}

void test_io__request_vote_response_cb(struct raft_io *io,
                                       struct test_io_request *request)
{
    struct test_io *t = io->data;
    struct test_host *host;
    struct test_message message;
    int rv;

    if (t->network == NULL) {
        return;
    }

    munit_assert_int(request->request_vote_response.server.id, !=, 0);

    host =
        test_network_host(t->network, request->request_vote_response.server.id);
    munit_assert_ptr_not_null(host);

    rv = raft_encode_request_vote_result(&request->request_vote_response.result,
                                         &message.header);
    munit_assert_int(rv, ==, 0);

    message.payload.base = NULL;

    munit_assert_int(t->id, !=, 0);
    message.sender_id = t->id;

    test_host_enqueue(host, &message);
}

void test_io__append_entries_cb(struct raft_io *io,
                                struct test_io_request *request)
{
    struct test_io *t = io->data;
    struct test_host *host;
    struct test_message message;
    size_t i;
    int rv;

    if (t->network == NULL) {
        return;
    }

    munit_assert_int(request->append_entries.server.id, !=, 0);

    host = test_network_host(t->network, request->append_entries.server.id);
    munit_assert_ptr_not_null(host);

    rv = raft_encode_append_entries(&request->append_entries.args,
                                    &message.header);
    munit_assert_int(rv, ==, 0);

    /* Calculate the size of the entry data payload. */
    message.payload.len = 0;
    for (i = 0; i < request->append_entries.args.n; i++) {
        struct raft_entry *entry = &request->append_entries.args.entries[i];
        message.payload.len += entry->buf.len;
        if (entry->buf.len % 8 != 0) {
            /* Add padding */
            message.payload.len += 8 - (entry->buf.len % 8);
        }
    }

    /* Populate the entry data payload. */
    if (message.payload.len > 0) {
        message.payload.base = raft_malloc(message.payload.len);
        munit_assert_ptr_not_null(message.payload.base);

        void *cursor = message.payload.base;

        for (i = 0; i < request->append_entries.args.n - 1; i++) {
            struct raft_entry *entry = &request->append_entries.args.entries[i];

            if (entry->buf.base == NULL) {
                continue;
            }

            memcpy(cursor, entry->buf.base, entry->buf.len);
            cursor += entry->buf.len;
            if (entry->buf.len % 8 != 0) {
                /* Add padding */
                cursor += 8 - (entry->buf.len % 8);
            }
        }
    } else {
        message.payload.base = NULL;
    }

    munit_assert_int(t->id, !=, 0);
    message.sender_id = t->id;

    test_host_enqueue(host, &message);
}

void test_io__append_entries_response_cb(struct raft_io *io,
                                         struct test_io_request *request)
{
    struct test_io *t = io->data;
    struct test_host *host;
    struct test_message message;
    int rv;

    if (t->network == NULL) {
        return;
    }

    munit_assert_int(request->append_entries_response.server.id, !=, 0);

    host = test_network_host(t->network,
                             request->append_entries_response.server.id);
    munit_assert_ptr_not_null(host);

    rv = raft_encode_append_entries_result(
        &request->append_entries_response.result, &message.header);
    munit_assert_int(rv, ==, 0);

    message.payload.base = NULL;

    munit_assert_int(t->id, !=, 0);
    message.sender_id = t->id;

    test_host_enqueue(host, &message);
}

/**
 * Execute all pending I/O requets.
 */
void test_io__queue_flush(struct raft_io *io)
{
    struct test_io *t = io->data;
    int i;
    int n = t->n_requests;

    for (i = 0; i < n; i++) {
        struct test_io_request *request = &t->requests[i];

        if (test_fault_tick(&t->fault)) {
            /* TODO: handle failures */
        }

        switch (request->type) {
            case RAFT_IO_WRITE_LOG:
                test_io__write_log_cb(io, request);
                t->pending_write_log = false;
                break;
            case RAFT_IO_REQUEST_VOTE:
                test_io__request_vote_cb(io, request);
                break;
            case RAFT_IO_REQUEST_VOTE_RESULT:
                test_io__request_vote_response_cb(io, request);
                break;
            case RAFT_IO_APPEND_ENTRIES:
                test_io__append_entries_cb(io, request);
                break;
            case RAFT_IO_APPEND_ENTRIES_RESULT:
                test_io__append_entries_response_cb(io, request);
                break;
        }

        request->type = RAFT_IO_NULL;

        t->n_requests--;
    }

    munit_assert_int(t->n_requests, ==, 0);
}

static int test_io__write_term(struct raft_io *io, const raft_term term)
{
    struct test_io *t = io->data;

    munit_assert_ptr_not_null(t);

    if (test_fault_tick(&t->fault)) {
        __logf("io: write term %ld: error", term);
        return RAFT_ERR_NO_SPACE;
    }

    __logf("io: write term %ld", term);

    t->term = term;
    t->voted_for = 0;

    return 0;
}

static int test_io__write_vote(struct raft_io *io, const unsigned node_id)
{
    struct test_io *t = io->data;

    munit_assert_ptr_not_null(t);

    if (test_fault_tick(&t->fault)) {
        __logf("io: write vote for %ld: error", node_id);
        return RAFT_ERR_SHUTDOWN;
    }

    __logf("io: write vote for %ld", node_id);
    t->voted_for = node_id;

    return 0;
}

static int test_io__write_log(struct raft_io *io,
                              const unsigned request_id,
                              const struct raft_entry entries[],
                              const unsigned n)
{
    struct test_io *t = io->data;
    struct test_io_request *request;

    munit_assert_ptr_not_null(t);
    munit_assert_int(n, >, 0);

    if (t->pending_write_log) {
        return RAFT_ERR_IO_BUSY;
    }

    if (test_fault_tick(&t->fault)) {
        __logf("io: fail to append %ld log entries", n);
        return RAFT_ERR_SHUTDOWN;
    }

    request = test_io__queue_push(io, request_id, RAFT_IO_WRITE_LOG);
    request->write_log.entries = entries;
    request->write_log.n = n;

    t->pending_write_log = true;

    return 0;
}

static int test_io__truncate_log(struct raft_io *io, const raft_index index)
{
    struct test_io *t = io->data;
    size_t n;

    munit_assert_ptr_not_null(t);
    munit_assert_true(index >= t->first_index);

    if (test_fault_tick(&t->fault)) {
        __logf("io: truncate log entries from %ld onward: error", index);
        return RAFT_ERR_SHUTDOWN;
    }

    __logf("io: truncate log entries from %ld onward", index);

    n = index - 1;

    if (n > 0) {
        struct raft_entry *new_entries;
        new_entries = munit_malloc((index - 1) * sizeof *new_entries);
        memcpy(new_entries, t->entries, n * sizeof *t->entries);
        if (t->entries != NULL) {
            size_t i;
            for (i = n; i < t->n; i++) {
                free(t->entries[i].buf.base);
            }
            free(t->entries);
        }
        t->entries = new_entries;
    } else {
        free(t->entries);
        t->entries = NULL;
    }
    t->n = n;

    return 0;
}

int test_io__send_request_vote_request(
    struct raft_io *io,
    const struct raft_server *server,
    const struct raft_request_vote_args *args)
{
    struct test_io *t = io->data;
    struct test_io_request *request;

    munit_assert_ptr_not_null(t);
    munit_assert_ptr_not_null(server);
    munit_assert_ptr_not_null(args);

    if (test_fault_tick(&t->fault)) {
        __logf("io: fail to send request vote to %ld", server->id);
        return RAFT_ERR_SHUTDOWN;
    }

    __logf("io: send request vote to %ld", server->id);

    request = test_io__queue_push(io, 0, RAFT_IO_REQUEST_VOTE);
    request->request_vote.server = *server;
    request->request_vote.args = *args;

    return 0;
}

int test_io__send_request_vote_response(
    struct raft_io *io,
    const struct raft_server *server,
    const struct raft_request_vote_result *result)
{
    struct test_io *t = io->data;
    struct test_io_request *request;

    munit_assert_ptr_not_null(t);
    munit_assert_ptr_not_null(server);
    munit_assert_ptr_not_null(result);

    if (test_fault_tick(&t->fault)) {
        __logf("io: fail to send request vote response to %ld", server->id);
        return RAFT_ERR_SHUTDOWN;
    }

    request = test_io__queue_push(io, 0, RAFT_IO_REQUEST_VOTE_RESULT);
    request->request_vote_response.server = *server;
    request->request_vote_response.result = *result;

    return 0;
}

int test_io__send_append_entries_request(
    struct raft_io *io,
    const unsigned request_id,
    const struct raft_server *server,
    const struct raft_append_entries_args *args)
{
    struct test_io *t = io->data;
    struct test_io_request *request;

    munit_assert_ptr_not_null(t);
    munit_assert_ptr_not_null(server);
    munit_assert_ptr_not_null(args);
    munit_assert_int(server->id, !=, 0);

    if (test_fault_tick(&t->fault)) {
        __logf("io: fail to send append entries to %ld", server->id);
        return RAFT_ERR_NO_SPACE;
    }

    request = test_io__queue_push(io, request_id, RAFT_IO_APPEND_ENTRIES);
    request->append_entries.server = *server;
    request->append_entries.args = *args;

    return 0;
}

int test_io__send_append_entries_response(
    struct raft_io *io,
    const struct raft_server *server,
    const struct raft_append_entries_result *result)
{
    struct test_io *t = io->data;
    struct test_io_request *request;

    munit_assert_ptr_not_null(t);
    munit_assert_ptr_not_null(server);
    munit_assert_ptr_not_null(result);

    if (test_fault_tick(&t->fault)) {
        __logf("io: fail to send append entries response to %ld", server->id);
        return RAFT_ERR_SHUTDOWN;
    }

    munit_assert_int(server->id, !=, 0);

    request = test_io__queue_push(io, 0, RAFT_IO_APPEND_ENTRIES_RESULT);
    request->append_entries_response.server = *server;
    request->append_entries_response.result = *result;

    return 0;
}

void test_io_setup(const MunitParameter params[], struct raft_io *io)
{
    struct test_io *t = munit_malloc(sizeof *t);

    /* Fault injection. */
    const char *delay = munit_parameters_get(params, TEST_IO_FAULT_DELAY);
    const char *repeat = munit_parameters_get(params, TEST_IO_FAULT_REPEAT);

    munit_assert_ptr_not_null(io);

    test_io__init(t);

    if (delay != NULL) {
        t->fault.countdown = atoi(delay);
    }
    if (repeat != NULL) {
        t->fault.n = atoi(repeat);
    }

    io->version = 1;
    io->data = t;
    io->write_term = test_io__write_term;
    io->write_vote = test_io__write_vote;
    io->write_log = test_io__write_log;
    io->truncate_log = test_io__truncate_log;
    io->send_request_vote_request = test_io__send_request_vote_request;
    io->send_request_vote_response = test_io__send_request_vote_response;
    io->send_append_entries_request = test_io__send_append_entries_request;
    io->send_append_entries_response = test_io__send_append_entries_response;
}

void test_io_tear_down(struct raft_io *io)
{
    struct test_io *t = io->data;
    size_t i;

    for (i = 0; i < t->n; i++) {
        struct raft_entry *entry = &t->entries[i];
        free(entry->buf.base);
    }

    free(t->entries);

    free(t);
}

void test_io_bootstrap(struct raft_io *io,
                       const int n_servers,
                       const int voting_a,
                       const int voting_b)
{
    struct raft_configuration configuration;
    struct raft_entry entry;
    int i;
    int rv;

    munit_assert_ptr_not_null(io);
    munit_assert_int(n_servers, >=, 1);
    munit_assert_int(voting_a, >=, 1);
    munit_assert_int(voting_a, <=, voting_b);
    munit_assert_int(voting_b, >=, 1);
    munit_assert_int(voting_b, <=, n_servers);

    /* Set initial term to 1 and vote to nil. */
    test_io_write_term_and_vote(io, 1, 0);

    /* Populate the configuration. */
    raft_configuration_init(&configuration);

    for (i = 0; i < n_servers; i++) {
        uint64_t id = i + 1;
        char *address = munit_malloc(4);
        bool voting = (int)id >= voting_a && (int)id <= voting_b;

        sprintf(address, "%ld", id);
        rv = raft_configuration_add(&configuration, id, address, voting);
        munit_assert_int(rv, ==, 0);
    }

    /* Encode the configuration into a log entry */
    entry.term = 1;
    entry.type = RAFT_LOG_CONFIGURATION;

    rv = raft_encode_configuration(&configuration, &entry.buf);
    munit_assert_int(rv, ==, 0);

    /* Write the log entry. */
    test_io_write_entry(io, &entry);

    raft_free(entry.buf.base);

    for (i = 0; i < n_servers; i++) {
        struct raft_server *server = &configuration.servers[i];
        free((void *)server->address);
    }

    raft_configuration_close(&configuration);
}

void test_io_write_term_and_vote(struct raft_io *io,
                                 const uint64_t term,
                                 const uint64_t vote)
{
    int rv;

    rv = io->write_term(io, term);
    munit_assert_int(rv, ==, 0);

    rv = io->write_vote(io, vote);
    munit_assert_int(rv, ==, 0);
}

void test_io_write_entry(struct raft_io *io, const struct raft_entry *entry)
{
    int rv;

    rv = test_io__write_log(io, 0, entry, 1);
    munit_assert_int(rv, ==, 0);

    test_io_flush(io);
}

void test_io_flush(struct raft_io *io)
{
    test_io__queue_flush(io);
}

uint64_t test_io_get_term(struct raft_io *io)
{
    struct test_io *t = io->data;

    return t->term;
}

uint64_t test_io_get_vote(struct raft_io *io)
{
    struct test_io *t = io->data;

    return t->voted_for;
}

void test_io_get_entries(struct raft_io *io,
                         const struct raft_entry *entries[],
                         size_t *n)
{
    struct test_io *t = io->data;

    *entries = t->entries;
    *n = t->n;
}

void test_io_get_requests(struct raft_io *io,
                          int type,
                          struct test_io_request **requests,
                          size_t *n)
{
    struct test_io *t = io->data;
    size_t i;
    size_t j = 0;

    *n = 0;

    for (i = 0; i < t->n_requests; i++) {
        if (t->requests[i].type == type) {
            (*n)++;
        }
    }

    *requests = munit_malloc(*n * sizeof **requests);

    for (i = 0; i < t->n_requests; i++) {
        if (t->requests[i].type == type) {
            (*requests)[j] = t->requests[i];
            j++;
        }
    }
}

void test_io_get_one_request(struct raft_io *io,
                             int type,
                             struct test_io_request *request)
{
    struct test_io *t = io->data;
    size_t i;
    bool found = false;

    for (i = 0; i < t->n_requests; i++) {
        if (t->requests[i].type == type) {
            if (found) {
                munit_errorf("more than one request of type %d", type);
            }
            *request = t->requests[i];
            found = true;
        }
    }

    if (!found) {
        munit_errorf("no request of type %d", type);
    }
}

void test_io_fault(struct raft_io *io, int delay, int repeat)
{
    struct test_io *t = io->data;

    test_fault_config(&t->fault, delay, repeat);
}

void test_io_set_network(struct raft_io *io,
                         struct test_network *network,
                         uint64_t id)
{
    struct test_io *t = io->data;

    t->network = network;
    t->id = id;
}
