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

/**
 * Release all memory linked to the given message.
 */
void test_network_close_message(struct test_message *m)
{
    if (m->sender_id == 0) {
        return;
    }

    switch (m->message.type) {
        case RAFT_IO_APPEND_ENTRIES:
            if (m->message.append_entries.entries != NULL) {
                raft_free(m->message.append_entries.entries[0].batch);
                raft_free(m->message.append_entries.entries);
            }
            break;
    }

    m->sender_id = 0;
}

void test_network_tear_down(struct test_network *n)
{
    size_t i, j;

    for (i = 0; i < n->n; i++) {
        struct test_host *host = &n->hosts[i];

        for (j = 0; j < TEST_NETWORK_INCOMING_QUEUE_SIZE; j++) {
            struct test_message *m = &host->incoming[j];

            test_network_close_message(m);
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

void test_host_enqueue(struct test_host *h,
                       struct test_message *m)
{
    struct test_network *network = h->network;
    size_t i, j;

    munit_assert_int(m->sender_id, >, 0);

    i = m->sender_id - 1;
    j = h->raft->id - 1;

    /* If server and receiver are disconnected, just drop the message. */
    if (!network->connectivity[i * network->n + j]) {
        test_network_close_message(m);
        return;
    }

    for (i = 0; i < TEST_NETWORK_INCOMING_QUEUE_SIZE; i++) {
        struct test_message *incoming = &h->incoming[i];

        if (incoming->sender_id != 0) {
            continue;
        }

        /* We found a free slot in the incoming queue. */
        incoming->sender_id = m->sender_id;
        incoming->message = m->message;

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

        if (message->sender_id == 0) {
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
