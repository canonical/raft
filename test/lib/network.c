#include <stdio.h>

#include "../../src/binary.h"

#include "network.h"

void test_network_setup(const MunitParameter params[],
                        struct test_network *n,
                        size_t n_hosts)
{
    const char *min_latency =
        munit_parameters_get(params, TEST_NETWORK_MIN_LATENCY);
    const char *max_latency =
        munit_parameters_get(params, TEST_NETWORK_MAX_LATENCY);
    size_t i, j;

    (void)params;

    if (min_latency == NULL) {
        min_latency = "5";
    }

    if (max_latency == NULL) {
        max_latency = "50";
    }

    n->n = n_hosts;
    n->hosts = munit_calloc(n->n, sizeof *n->hosts);
    n->min_latency = atoi(min_latency);
    n->max_latency = atoi(max_latency);
    n->connectivity = munit_malloc(n->n * n->n * sizeof(bool));

    for (i = 0; i < n->n; i++) {
        struct test_host *host = &n->hosts[i];
        host->network = n;

        /* By default all hosts are connected */
        for (j = 0; j < n->n; j++) {
            n->connectivity[i * n->n + j] = true;
        }
    }
}

void test_network_add_host(struct test_network *n)
{
    bool *connectivity; /* New connectivity matrix */
    size_t i, j;

    n->n++;

    n->hosts = realloc(n->hosts, n->n * sizeof *n->hosts);
    munit_assert_ptr_not_null(n->hosts);

    connectivity = munit_malloc(n->n * n->n * sizeof(bool));
    munit_assert_ptr_not_null(connectivity);

    for (i = 0; i < n->n; i++) {
        struct test_host *host = &n->hosts[i];

        host->network = n;

        for (j = 0; j < n->n; j++) {
            /* The new host is connected, the other ones maintain their
             * connectivity state. */
            bool connected;

            if (i == n->n - 1 || j == n->n - 1) {
                connected = true;
            } else {
                connected = n->connectivity[i * (n->n - 1) + j];
            }
            connectivity[i * n->n + j] = connected;
        }
    }

    free(n->connectivity);
    n->connectivity = connectivity;
}

int test_message_type(const struct test_message *m)
{
    if (m->header.base == NULL) {
        return RAFT_IO_NULL;
    }

    return raft__flip32(*(uint32_t *)(m->header.base + 4));
}

void test_network_tear_down(struct test_network *n)
{
    size_t i, j;

    for (i = 0; i < n->n; i++) {
        struct test_host *host = &n->hosts[i];

        for (j = 0; j < TEST_NETWORK_INCOMING_QUEUE_SIZE; j++) {
            struct test_message *message = &host->incoming[j];
            int type = test_message_type(message);

            if (type != RAFT_IO_NULL) {
                raft_free(message->header.base);
                message->header.base = NULL;

                if (message->payload.base != NULL) {
                    raft_free(message->payload.base);
                    message->payload.base = NULL;
                }
            }
        }
    }

    free(n->connectivity);
    free(n->hosts);
}

struct test_host *test_network_host(struct test_network *n, unsigned id)
{
    size_t i;

    munit_assert_int(id, >, 0);

    i = id - 1;

    munit_assert_int(i, <, n->n);

    return &n->hosts[i];
}

void test_host_enqueue(struct test_host *h, struct test_message *message)
{
    struct test_network *network = h->network;
    size_t i, j;

    munit_assert_int(message->sender_id, >, 0);

    i = message->sender_id - 1;
    j = h->raft->id - 1;

    /* If server and receiver are disconnected, just drop the message. */
    if (!network->connectivity[i * network->n + j]) {
        raft_free(message->header.base);
        message->header.base = NULL;

        if (message->payload.base != NULL) {
            raft_free(message->payload.base);
            message->payload.base = NULL;
        }

        return;
    }

    for (i = 0; i < TEST_NETWORK_INCOMING_QUEUE_SIZE; i++) {
        struct test_message *incoming = &h->incoming[i];
        int type = test_message_type(incoming);

        if (type != RAFT_IO_NULL) {
            continue;
        }

        /* We found a free slot in the incoming queue. */
        *incoming = *message;

        /* Assign a random message latency in the configured range. */
        incoming->timer = munit_rand_int_range(h->network->min_latency,
                                               h->network->max_latency);

        return;
    }

    munit_error("incoming message queue is full");
}

struct test_message *test_host_peek(struct test_host *h)
{
    struct test_message *message_with_lowest_timer = NULL;
    size_t i;

    for (i = 0; i < TEST_NETWORK_INCOMING_QUEUE_SIZE; i++) {
        struct test_message *message = &h->incoming[i];
        int type = test_message_type(message);

        if (type == RAFT_IO_NULL) {
            continue;
        }

        if (message_with_lowest_timer == NULL) {
            message_with_lowest_timer = message;
            continue;
        }

        if (message->timer < message_with_lowest_timer->timer) {
            message_with_lowest_timer = message;
            continue;
        }
    }

    if (message_with_lowest_timer != NULL) {
        munit_assert_int(message_with_lowest_timer->sender_id, >, 0);
    }

    return message_with_lowest_timer;
}

static void test_host__request_vote(struct test_host *h,
                                    const unsigned id,
                                    const char *address,
                                    const struct raft_buffer *buf)
{
    struct raft_request_vote_args args;
    int rv;

    rv = raft_decode_request_vote(buf, &args);
    munit_assert_int(rv, ==, 0);

    raft_handle_request_vote(h->raft, id, address, &args);
}

static void test_host__request_vote_response(struct test_host *h,
                                             const unsigned id,
                                             const char *address,
                                             const struct raft_buffer *buf)
{
    struct raft_request_vote_result result;
    int rv;

    rv = raft_decode_request_vote_result(buf, &result);
    munit_assert_int(rv, ==, 0);

    raft_handle_request_vote_response(h->raft, id, address, &result);
}

static void test_host__append_entries(struct test_host *h,
                                      const unsigned id,
                                      const char *address,
                                      const struct raft_buffer *buf,
                                      const struct raft_buffer *payload)
{
    struct raft_append_entries_args args;
    int rv;

    rv = raft_decode_append_entries(buf, &args);
    munit_assert_int(rv, ==, 0);

    rv = raft_decode_entries_batch(payload, args.entries, args.n);
    munit_assert_int(rv, ==, 0);

    raft_handle_append_entries(h->raft, id, address, &args);
}

static void test_host__append_entries_response(struct test_host *h,
                                               const unsigned id,
                                               const char *address,
                                               const struct raft_buffer *buf)
{
    struct raft_append_entries_result result;
    int rv;

    rv = raft_decode_append_entries_result(buf, &result);
    munit_assert_int(rv, ==, 0);

    raft_handle_append_entries_response(h->raft, id, address, &result);
}

void test_host_receive(struct test_host *h, struct test_message *message)
{
    struct raft_buffer buf;
    int type = test_message_type(message);
    unsigned id;
    char address[4];

    munit_assert_int(type, !=, RAFT_IO_NULL);
    munit_assert_int(message->sender_id, !=, 0);
    munit_assert_int(message->sender_id, <=, h->network->n);

    id = message->sender_id;
    sprintf(address, "%d", id);

    /* Skip message header. */
    buf.base = message->header.base + 16;
    buf.len = message->header.len - 16;

    switch (type) {
        case RAFT_IO_REQUEST_VOTE:
            test_host__request_vote(h, id, address, &buf);
            break;
        case RAFT_IO_REQUEST_VOTE_RESULT:
            test_host__request_vote_response(h, id, address, &buf);
            break;
        case RAFT_IO_APPEND_ENTRIES:
            test_host__append_entries(h, id, address, &buf, &message->payload);
            break;
        case RAFT_IO_APPEND_ENTRIES_RESULT:
            test_host__append_entries_response(h, id, address, &buf);
            break;
    }

    raft_free(message->header.base);

    message->header.base = NULL;
    message->payload.base = NULL;
}
