#include <stdio.h>
#include <stdlib.h>

#include "../../src/log.h"
#include "../../src/tick.h"

#include "../../include/raft/io_stub.h"

#include "cluster.h"
#include "munit.h"
#include "raft.h"

/**
 * Maximum number of servers the cluster can grow to.
 */
#define TEST_CLUSTER__N 16

static int test_cluster__rand()
{
    return munit_rand_uint32();
}

/**
 * Return the global time of the cluster, which is the same for all servers.
 */
static int test_cluster__time(void *data)
{
    struct test_cluster *c = data;

    return c->time;
}

void test_cluster_setup(const MunitParameter params[], struct test_cluster *c)
{
    const char *n = munit_parameters_get(params, TEST_CLUSTER_SERVERS);
    const char *n_voting = munit_parameters_get(params, TEST_CLUSTER_VOTING);
    size_t i;

    if (n == NULL) {
        n = "3";
    }
    if (n_voting == NULL) {
        n_voting = n;
    }

    c->n = atoi(n);
    c->n_voting = atoi(n_voting);

    munit_assert_int(c->n, >, 0);
    munit_assert_int(c->n_voting, >, 0);
    munit_assert_int(c->n_voting, <=, c->n);
    munit_assert_int(c->n, <=, TEST_CLUSTER__N);

    c->loggers = raft_calloc(TEST_CLUSTER__N, sizeof *c->loggers);
    c->ios = raft_calloc(TEST_CLUSTER__N, sizeof *c->ios);
    c->fsms = raft_calloc(TEST_CLUSTER__N, sizeof *c->fsms);
    c->rafts = raft_calloc(TEST_CLUSTER__N, sizeof *c->rafts);
    c->alive = raft_calloc(TEST_CLUSTER__N, sizeof *c->alive);

    c->time = 0;
    c->leader_id = 0;

    test_network_setup(params, &c->network, c->n);

    for (i = 0; i < c->n; i++) {
        unsigned id = i + 1;
        struct raft_logger *logger = &c->loggers[i];
        struct raft_io *io = &c->ios[i];
        struct raft_fsm *fsm = &c->fsms[i];
        struct raft *raft = &c->rafts[i];
        struct test_host *host = test_network_host(&c->network, id);
        char address[4];

        sprintf(address, "%d", id);

        c->alive[i] = true;

        test_logger_setup(params, logger, id);
        test_logger_time(logger, c, test_cluster__time);

        test_io_setup(params, io, logger);

        test_fsm_setup(params, fsm);

        raft_init(raft, logger, io, fsm, c, id, address);

        raft_set_rand(raft, test_cluster__rand);
        raft_set_election_timeout(raft, 250);

        test_bootstrap_and_start(raft, c->n, 1, c->n_voting);

        host->raft = raft;
    }

    c->commit_index = 1; /* The initial configuration is committed. */

    raft_log__init(&c->log);
}

void test_cluster_tear_down(struct test_cluster *c)
{
    size_t i;

    raft_log__close(&c->log);

    for (i = 0; i < c->n; i++) {
        struct raft_logger *logger = &c->loggers[i];
        struct raft_io *io = &c->ios[i];
        struct raft_fsm *fsm = &c->fsms[i];
        struct raft *raft = &c->rafts[i];

        raft_io_stub_flush(io);

        raft_close(raft, NULL);
        test_fsm_tear_down(fsm);
        test_io_tear_down(io);

        test_logger_tear_down(logger);
    }

    test_network_tear_down(&c->network);

    raft_free(c->loggers);
    raft_free(c->fsms);
    raft_free(c->ios);
    raft_free(c->rafts);
    raft_free(c->alive);
}

/**
 * Copy all entries in @src into @dst.
 */
static void test_cluster__copy_entries(const struct raft_entry *src,
                                       struct raft_entry **dst,
                                       unsigned n)
{
    size_t size = 0;
    void *batch;
    void *cursor;
    unsigned i;

    if (n == 0) {
        *dst = NULL;
        return;
    }

    /* Calculate the total size of the entries content and allocate the
     * batch. */
    for (i = 0; i < n; i++) {
        size += src[i].buf.len;
    }

    batch = raft_malloc(size);
    munit_assert_ptr_not_null(batch);

    /* Copy the entries. */
    *dst = raft_malloc(n * sizeof **dst);
    munit_assert_ptr_not_null(*dst);

    cursor = batch;

    for (i = 0; i < n; i++) {
        (*dst)[i] = src[i];

        (*dst)[i].buf.base = cursor;
        memcpy((*dst)[i].buf.base, src[i].buf.base, src[i].buf.len);

        (*dst)[i].batch = batch;

        cursor += src[i].buf.len;
    }
}

/**
 * Make a copy of the content of the given message and push it to the incoming
 * queue of the receiver.
 */
static void test_cluster__enqueue_message(struct test_cluster *c,
                                          struct raft *sender,
                                          struct raft_message *src)
{
    struct test_host *host = test_network_host(&c->network, src->server_id);
    struct raft_message dst;
    struct test_message msg;

    dst = *src;
    dst.server_id = sender->id;
    dst.server_address = sender->address;

    switch (src->type) {
        case RAFT_IO_APPEND_ENTRIES:
            /* Make a copy of the entries being sent */
            test_cluster__copy_entries(src->append_entries.entries,
                                       &dst.append_entries.entries,
                                       src->append_entries.n_entries);
            dst.append_entries.n_entries = src->append_entries.n_entries;
            break;
    }

    msg.sender_id = sender->id;
    msg.message = dst;

    test_host_enqueue(host, &msg);
}

/**
 * Flush any pending write to the disk and any pending message into the network
 * buffers (this will assign them a latency timer).
 */
static void test_cluster__flush_io(struct test_cluster *c)
{
    size_t i;
    for (i = 0; i < c->n; i++) {
        struct raft *raft = &c->rafts[i];
        struct raft_io *io = &c->ios[i];
        struct raft_message *messages;
        unsigned n;
        unsigned i;

        raft_io_stub_flush(io);

        raft_io_stub_sent(io, &messages, &n);

        for (i = 0; i < n; i++) {
            struct raft_message *message = &messages[i];
            test_cluster__enqueue_message(c, raft, message);
        }
    }
}

/**
 * Figure what's the message with the lowest timer, i.e. the
 * message that should be delivered first (if any is pending).
 */
static void test_cluster__message_with_lowest_timer(
    struct test_cluster *c,
    struct test_message **message,
    struct test_host **host)
{
    size_t i;

    *message = NULL;

    for (i = 0; i < c->n; i++) {
        struct test_host *h = test_network_host(&c->network, i + 1);
        struct test_message *m = test_host_peek(h);

        if (m == NULL) {
            continue;
        }

        if (!c->alive[h->raft->id - 1]) {
            /* Drop the message */
            test_network_close_message(m);
            continue;
        }

        if (*message == NULL) {
            *host = h;
            *message = m;
            continue;
        }

        if (m->timer < (*message)->timer) {
            *host = h;
            *message = m;
            continue;
        }
    }
}

/**
 * Get the amount of milliseconds left before the timer of the given raft
 * instance expires (either triggering a heartbeat or an election).
 */
static int test_cluster__raft_remaining_time(struct raft *r)
{
    if (r->state == RAFT_STATE_LEADER) {
        return r->heartbeat_timeout - r->timer;
    } else {
        return r->election_timeout_rand - r->timer;
    }
}

/**
 * Check what's the raft instance whose timer is closest to expiration.
 */
static void test_cluster__raft_with_lowest_timer(struct test_cluster *c,
                                                 struct raft **raft)
{
    size_t i;
    int lowest = 0; /* Lowest remaining time before expiration */

    *raft = NULL;

    for (i = 0; i < c->n; i++) {
        struct raft *r = &c->rafts[i];
        int remaining; /* Milliseconds remaining before expiration. */

        if (!c->alive[i]) {
            continue;
        }

        remaining = test_cluster__raft_remaining_time(r);

        if (*raft == NULL) {
            *raft = r;
            lowest = remaining;
            continue;
        }

        if (remaining < lowest) {
            *raft = r;
            lowest = remaining;
        }
    }

    munit_assert_ptr_not_null(*raft);
}

/**
 * Fire either a message delivery or a raft tick, depending on whose timer is
 * closest to expiration.
 */
static void test_cluster__deliver_or_tick(struct test_cluster *c,
                                          struct test_message *next_message,
                                          struct test_host *next_host,
                                          struct raft *next_raft)
{
    size_t i, j;
    int remaining = test_cluster__raft_remaining_time(next_raft);
    int elapse;

    if (next_message != NULL && next_message->timer < remaining) {
        elapse = next_message->timer + 1;
        raft_io_stub_dispatch(next_host->raft->io, &next_message->message);
        next_message->sender_id = 0;
    } else {
        elapse = remaining + 1;
    }

    if (elapse < 0) {
        elapse = 0;
    }

    for (i = 0; i < c->n; i++) {
        struct raft *raft = &c->rafts[i];
        struct test_host *host = &c->network.hosts[i];

        if (!c->alive[i]) {
            continue;
        }

        for (j = 0; j < TEST_NETWORK_INCOMING_QUEUE_SIZE; j++) {
            struct test_message *incoming = &host->incoming[j];

            if (incoming->sender_id == 0) {
                continue;
            }
            incoming->timer -= elapse;
        }

	raft_io_stub_advance(raft->io, elapse);
    }

    c->time += elapse;
}

/**
 * Update the leader and check for election safety.
 *
 * From figure 3.2:
 *
 *   Election Safety -> At most one leader can be elected in a given
 *   term.
 *
 * Return true if the current leader turns out to be different from the one at
 * the time this function was called.
 */
static bool test_cluster__update_leader(struct test_cluster *c)
{
    unsigned leader_id = 0;
    raft_term leader_term = 0;
    size_t i, j;
    bool changed;

    for (i = 0; i < c->n; i++) {
        struct raft *raft = &c->rafts[i];

        if (!c->alive[i]) {
            continue;
        }

        if (raft->state == RAFT_STATE_LEADER) {
            /* No other server is leader for this term. */
            for (j = 0; j < c->n; j++) {
                struct raft *other = &c->rafts[j];

                if (other->id == raft->id) {
                    continue;
                }

                if (other->state == RAFT_STATE_LEADER) {
                    if (other->current_term == raft->current_term) {
                        munit_errorf(
                            "server %u and %u are both leaders in term %llu",
                            raft->id, other->id, raft->current_term);
                    }
                }
            }

            if (raft->current_term > leader_term) {
                leader_id = raft->id;
                leader_term = raft->current_term;
            }
        }
    }

    /* Check that the leader is stable, in the sense that it has been
     * acknowledged by all alive servers connected to it, and those servers
     * together with the leader form a majority. */
    if (leader_id != 0) {
        size_t n = 0;
        bool acked = true;

        j = leader_id - 1;

        for (i = 0; i < c->n; i++) {
            struct raft *raft = &c->rafts[i];

            if (raft->id == leader_id) {
                continue;
            }

            if (!c->alive[i] || !c->network.connectivity[i * c->n + j] ||
                !c->network.connectivity[j * c->n + i]) {
                /* This server is not alive or not connected to this leader, so
                 * don't count it in for stability. */
                continue;
            }

            if (raft->current_term != leader_term) {
                acked = false;
                break;
            }

            if (raft->state != RAFT_STATE_FOLLOWER) {
                acked = false;
                break;
            }

            if (raft->follower_state.current_leader_id == 0) {
                acked = false;
                break;
            }

            if (raft->follower_state.current_leader_id != leader_id) {
                acked = false;
                break;
            }

            n++;
        }

        if (!acked || n < (c->n / 2)) {
            leader_id = 0;
        }
    }

    changed = leader_id != c->leader_id;
    c->leader_id = leader_id;

    return changed;
}

/**
 * Check for leader append-only.
 *
 * From figure 3.2:
 *
 *   Leader Append-Only -> A leader never overwrites or deletes entries in its
 *   own log; it only appends new entries.
 */
static void test_cluster__check_leader_append_only(struct test_cluster *c)
{
    struct raft *raft = &c->rafts[c->leader_id - 1];
    raft_index index;
    raft_index last = raft_log__last_index(&c->log);

    if (last == 0) {
        /* If the cached log is empty it means there was no leader before. */
        return;
    }

    /* If there's no new leader, just return. */
    if (c->leader_id == 0) {
        return;
    }

    for (index = 1; index <= last; index++) {
        const struct raft_entry *entry1;
        const struct raft_entry *entry2;

        entry1 = raft_log__get(&c->log, index);
        entry2 = raft_log__get(&raft->log, index);

        munit_assert_ptr_not_null(entry1);

        /* Entry was not deleted. */
        munit_assert_ptr_not_null(entry2);

        /* TODO: check other entry types too. */
        if (entry1->type != RAFT_LOG_COMMAND) {
            continue;
        }

        /* Entry was not overwritten. */
        munit_assert_int(entry1->term, ==, entry2->term);
        munit_assert_int(*(uint32_t *)entry1->buf.base, ==,
                         *(uint32_t *)entry2->buf.base);
    }
}

/* Make a copy of the the current leader log, in order to perform the Leader
 * Append-Only check at the next iteration. */
static void test_cluster__copy_leader_log(struct test_cluster *c)
{
    struct raft *raft = &c->rafts[c->leader_id - 1];
    struct raft_entry *entries;
    unsigned n;
    size_t i;
    int rv;

    /* Log copy */
    raft_log__close(&c->log);
    raft_log__init(&c->log);

    rv = raft_log__acquire(&raft->log, 1, &entries, &n);
    munit_assert_int(rv, ==, 0);

    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        struct raft_buffer buf;

        buf.len = entry->buf.len;
        buf.base = raft_malloc(buf.len);
        memcpy(buf.base, entry->buf.base, buf.len);

        rv = raft_log__append(&c->log, entry->term, entry->type, &buf, NULL);
        munit_assert_int(rv, ==, 0);
    }

    raft_log__release(&raft->log, 1, entries, n);
}

/* Update the commit index to match the one from the current leader. */
static void test_cluster__update_commit_index(struct test_cluster *c)
{
    struct raft *raft = &c->rafts[c->leader_id - 1];
    if (raft->commit_index > c->commit_index) {
        c->commit_index = raft->commit_index;
    }
}

void test_cluster_run_once(struct test_cluster *c)
{
    struct test_host *next_host;
    struct test_message *next_message;
    struct raft *next_raft;

    /* First flush I/O operations. */
    test_cluster__flush_io(c);

    /* Second, figure what's the message with the lowest timer (i.e. the
     * message that should be delivered first) */
    test_cluster__message_with_lowest_timer(c, &next_message, &next_host);

    /* Now check what's the raft instance whose timer is closest to
     * expiration. */
    test_cluster__raft_with_lowest_timer(c, &next_raft);

    /* Fire either a raft tick or a message delivery. */
    test_cluster__deliver_or_tick(c, next_message, next_host, next_raft);

    /* If the leader has not changed check the Leader Append-Only
     * guarantee. */
    if (!test_cluster__update_leader(c)) {
        test_cluster__check_leader_append_only(c);
    }

    /* If we have a leader, update leader-related state . */
    if (c->leader_id != 0) {
        /* Log copy */
        test_cluster__copy_leader_log(c);

        /* Commit index. */
        test_cluster__update_commit_index(c);
    }
}

int test_cluster_run_until(struct test_cluster *c,
                           bool (*stop)(struct test_cluster *c),
                           int max_msecs)
{
    int start = c->time;

    while (!stop(c) && (c->time - start) < max_msecs) {
        test_cluster_run_once(c);
    }

    return c->time < max_msecs ? 0 : -1;
}

unsigned test_cluster_leader(struct test_cluster *c)
{
    return c->leader_id;
}

bool test_cluster_has_leader(struct test_cluster *c)
{
    return test_cluster_leader(c) != 0;
}

bool test_cluster_has_no_leader(struct test_cluster *c)
{
    return test_cluster_leader(c) == 0;
}

void test_cluster_propose(struct test_cluster *c)
{
    unsigned leader_id = test_cluster_leader(c);
    uint32_t *entry_id = raft_malloc(sizeof *entry_id);
    struct raft_buffer buf;
    struct raft *raft;
    int rv;

    munit_assert_ptr_not_null(entry_id);
    *entry_id = munit_rand_uint32();

    munit_assert_int(leader_id, !=, 0);

    raft = &c->rafts[leader_id - 1];

    buf.base = entry_id;
    buf.len = sizeof *entry_id;

    rv = raft_propose(raft, &buf, 1);
    munit_assert_int(rv, ==, 0);
}

void test_cluster_add_server(struct test_cluster *c)
{
    unsigned leader_id = test_cluster_leader(c);
    unsigned id = c->n + 1;
    char *address = munit_malloc(4);
    struct raft_logger *logger;
    struct raft_io *io;
    struct raft_fsm *fsm;
    struct raft *raft;
    struct test_host *host;
    struct raft *leader;
    MunitParameter params[] = {{NULL, NULL}};
    int rv;

    /* Create a new raft instance for the new server. */
    c->n++;

    sprintf(address, "%d", id);

    munit_assert_int(c->n, <=, TEST_CLUSTER__N);

    logger = &c->loggers[c->n - 1];
    io = &c->ios[c->n - 1];
    fsm = &c->fsms[c->n - 1];
    raft = &c->rafts[c->n - 1];

    test_network_add_host(&c->network);
    host = test_network_host(&c->network, id);

    c->alive[c->n - 1] = true;

    test_logger_setup(params, logger, id);
    test_logger_time(logger, c, test_cluster__time);

    test_io_setup(params, io, logger);

    test_fsm_setup(params, fsm);

    raft_init(raft, logger, io, fsm, c, id, address);

    rv = raft_start(raft);
    munit_assert_int(rv, ==, 0);

    raft_set_rand(raft, test_cluster__rand);
    raft_set_election_timeout(raft, 250);

    host->raft = raft;

    leader = &c->rafts[leader_id - 1];

    rv = raft_add_server(leader, id, address);
    munit_assert_int(rv, ==, 0);

    free(address);
}

void test_cluster_promote(struct test_cluster *c)
{
    unsigned leader_id = test_cluster_leader(c);
    unsigned id;
    struct raft *leader;
    int rv;

    leader = &c->rafts[leader_id - 1];

    id = c->n; /* Last server that was added. */

    rv = raft_promote(leader, id);
    munit_assert_int(rv, ==, 0);
}

bool test_cluster_committed_2(struct test_cluster *c)
{
    return c->commit_index >= 2;
}

bool test_cluster_committed_3(struct test_cluster *c)
{
    return c->commit_index >= 3;
}

void test_cluster_kill(struct test_cluster *c, unsigned id)
{
    size_t i = id - 1;

    c->alive[i] = false;
}

void test_cluster_kill_majority(struct test_cluster *c)
{
    size_t i;
    size_t n;

    for (i = 0, n = 0; n < (c->n / 2) + 1; i++) {
        uint64_t id = i + 1;
        if (id == c->leader_id) {
            continue;
        }
        test_cluster_kill(c, id);
        n++;
    }
}

bool test_cluster_connected(struct test_cluster *c, unsigned id1, unsigned id2)
{
    size_t i, j;

    i = id1 - 1;
    j = id2 - 1;

    return c->network.connectivity[i * c->n + j] &&
           c->network.connectivity[j * c->n + i];
}

void test_cluster_disconnect(struct test_cluster *c, unsigned id1, unsigned id2)
{
    size_t i, j;

    i = id1 - 1;
    j = id2 - 1;

    c->network.connectivity[i * c->n + j] = false;
    c->network.connectivity[j * c->n + i] = false;
}

void test_cluster_reconnect(struct test_cluster *c, unsigned id1, unsigned id2)
{
    size_t i, j;

    i = id1 - 1;
    j = id2 - 1;

    c->network.connectivity[i * c->n + j] = true;
    c->network.connectivity[j * c->n + i] = true;
}
