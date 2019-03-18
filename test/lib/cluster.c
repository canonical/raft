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
    size_t j;

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

    for (i = 0; i < c->n; i++) {
        unsigned id = i + 1;
        struct raft_logger *logger = &c->loggers[i];
        struct raft_io *io = &c->ios[i];
        struct raft_fsm *fsm = &c->fsms[i];
        struct raft *raft = &c->rafts[i];
        char address[4];

        sprintf(address, "%d", id);

        c->alive[i] = true;

        test_logger_setup(params, logger, id);
        test_logger_time(logger, c, test_cluster__time);

        test_io_setup(params, io, logger);
        raft_io_stub_set_latency(io, 5, 50);

        test_fsm_setup(params, fsm);

        raft_init(raft, io, fsm, c, id, address);

	raft->heartbeat_timeout = 50;
        raft_set_election_timeout(raft, 250);

        test_bootstrap_and_start(raft, c->n, 1, c->n_voting);
    }

    /* Connect all servers to each another */
    for (i = 0; i < c->n; i++) {
        for (j = 0; j < c->n; j++) {
            struct raft_io *io1 = &c->ios[i];
            struct raft_io *io2 = &c->ios[j];
            if (i == j) {
                continue;
            }
            raft_io_stub_connect(io1, io2);
        }
    }

    c->commit_index = 1; /* The initial configuration is committed. */

    log__init(&c->log);
}

void test_cluster_tear_down(struct test_cluster *c)
{
    size_t i;

    log__close(&c->log);

    for (i = 0; i < c->n; i++) {
        struct raft_logger *logger = &c->loggers[i];
        struct raft_io *io = &c->ios[i];
        struct raft_fsm *fsm = &c->fsms[i];
        struct raft *raft = &c->rafts[i];

        raft_io_stub_flush_all(io);

        raft_close(raft, NULL);
        test_fsm_tear_down(fsm);
        test_io_tear_down(io);

        test_logger_tear_down(logger);
    }

    raft_free(c->loggers);
    raft_free(c->fsms);
    raft_free(c->ios);
    raft_free(c->rafts);
    raft_free(c->alive);
}

/**
 * Flush any pending write to the disk and any pending message into the network
 * buffers (this will assign them a latency timer).
 */
static void test_cluster__flush_io(struct test_cluster *c)
{
    size_t i;
    for (i = 0; i < c->n; i++) {
        struct raft_io *io = &c->ios[i];
        raft_io_stub_flush_all(io);
    }
}

/**
 * Figure what's the lowest delivery timer across all stub I/O instances,
 * i.e. the time at which the next message should be delivered (if any is
 * pending).
 */
static int test_cluster__next_deliver_timeout(struct test_cluster *c)
{
    int min_timeout = -1;
    size_t i;

    for (i = 0; i < c->n; i++) {
        struct raft_io *io = &c->ios[i];
        int timeout;
        timeout = raft_io_stub_next_deliver_timeout(io);
	if (timeout == -1) {
	  continue;
	}
        if (min_timeout == -1 || timeout < min_timeout) {
            min_timeout = timeout;
        }
    }

    return min_timeout;
}

/**
 * Check what's the raft instance whose timer is closest to expiration.
 */
static unsigned test_cluster__next_raft_timeout(struct test_cluster *c)
{
    size_t i;
    unsigned min_timeout = 0; /* Lowest remaining time before expiration */

    for (i = 0; i < c->n; i++) {
        struct raft *r = &c->rafts[i];
        unsigned timeout; /* Milliseconds remaining before expiration. */

        if (!c->alive[i]) {
            continue;
        }

        timeout = raft_next_timeout(r);
        if (min_timeout == 0 || timeout < min_timeout) {
            min_timeout = timeout;
        }
    }

    return min_timeout;
}

/**
 * Fire either a message delivery or a raft tick, depending on whose timer is
 * closest to expiration.
 */
static void test_cluster__advance(struct test_cluster *c, unsigned msecs)
{
    size_t i;

    c->time += msecs;

    for (i = 0; i < c->n; i++) {
        struct raft *raft = &c->rafts[i];

        if (!c->alive[i]) {
            continue;
        }

        raft_io_stub_advance(raft->io, msecs);
    }
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

        if (raft->state == RAFT_LEADER) {
            /* No other server is leader for this term. */
            for (j = 0; j < c->n; j++) {
                struct raft *other = &c->rafts[j];

                if (other->id == raft->id) {
                    continue;
                }

                if (other->state == RAFT_LEADER) {
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

            if (!c->alive[i] || !test_cluster_connected(c, leader_id, raft->id)) {
                /* This server is not alive or not connected to this leader, so
                 * don't count it in for stability. */
                continue;
            }

            if (raft->current_term != leader_term) {
                acked = false;
                break;
            }

            if (raft->state != RAFT_FOLLOWER) {
                acked = false;
                break;
            }

            if (raft->follower_state.current_leader.id == 0) {
                acked = false;
                break;
            }

            if (raft->follower_state.current_leader.id != leader_id) {
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
    raft_index last = log__last_index(&c->log);

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

        entry1 = log__get(&c->log, index);
        entry2 = log__get(&raft->log, index);

        munit_assert_ptr_not_null(entry1);

        /* Entry was not deleted. */
        munit_assert_ptr_not_null(entry2);

        /* TODO: check other entry types too. */
        if (entry1->type != RAFT_COMMAND) {
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
    log__close(&c->log);
    log__init(&c->log);

    rv = log__acquire(&raft->log, 1, &entries, &n);
    munit_assert_int(rv, ==, 0);

    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        struct raft_buffer buf;

        buf.len = entry->buf.len;
        buf.base = raft_malloc(buf.len);
        memcpy(buf.base, entry->buf.base, buf.len);

        rv = log__append(&c->log, entry->term, entry->type, &buf, NULL);
        munit_assert_int(rv, ==, 0);
    }

    log__release(&raft->log, 1, entries, n);
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
    int deliver_timeout;
    unsigned raft_timeout;
    unsigned timeout;

    /* First flush I/O operations. */
    test_cluster__flush_io(c);

    /* Second, figure what's the message with the lowest timer (i.e. the
     * message that should be delivered first) */
    deliver_timeout = test_cluster__next_deliver_timeout(c);

    /* Now check what's the raft instance whose timer is closest to
     * expiration. */
    raft_timeout = test_cluster__next_raft_timeout(c);

    /* Fire either a raft tick or a message delivery. */
    if (deliver_timeout != -1 && (unsigned)deliver_timeout < raft_timeout) {
      timeout = deliver_timeout;
    } else {
      timeout = raft_timeout;
    }

    test_cluster__advance(c, timeout + 1);

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

static void apply_cb(struct raft_apply *req, int status)
{
    (void)status;
    free(req);
}

void test_cluster_propose(struct test_cluster *c)
{
    unsigned leader_id = test_cluster_leader(c);
    uint32_t *entry_id = raft_malloc(sizeof *entry_id);
    struct raft_buffer buf;
    struct raft_apply *req = munit_malloc(sizeof *req);
    struct raft *raft;
    int rv;

    munit_assert_ptr_not_null(entry_id);
    *entry_id = munit_rand_uint32();

    munit_assert_int(leader_id, !=, 0);

    raft = &c->rafts[leader_id - 1];

    buf.base = entry_id;
    buf.len = sizeof *entry_id;

    rv = raft_apply(raft, req, &buf, 1, apply_cb);
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
    struct raft *leader;
    MunitParameter params[] = {{NULL, NULL}};
    int rv;
    unsigned i;

    /* Create a new raft instance for the new server. */
    c->n++;

    sprintf(address, "%d", id);

    munit_assert_int(c->n, <=, TEST_CLUSTER__N);

    logger = &c->loggers[c->n - 1];
    io = &c->ios[c->n - 1];
    fsm = &c->fsms[c->n - 1];
    raft = &c->rafts[c->n - 1];

    c->alive[c->n - 1] = true;

    test_logger_setup(params, logger, id);
    test_logger_time(logger, c, test_cluster__time);

    test_io_setup(params, io, logger);
    raft_io_stub_set_latency(io, 5, 50);

    for (i = 0; i < c->n - 1; i++) {
        struct raft_io *other = &c->ios[i];
        raft_io_stub_connect(io, other);
        raft_io_stub_connect(other, io);
    }

    test_fsm_setup(params, fsm);

    raft_init(raft, io, fsm, c, id, address);

    rv = raft_start(raft);
    munit_assert_int(rv, ==, 0);

    raft_set_election_timeout(raft, 250);

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

    return raft_io_stub_connected(&c->ios[i], &c->ios[j]) &&
           raft_io_stub_connected(&c->ios[j], &c->ios[i]);
}

void test_cluster_disconnect(struct test_cluster *c, unsigned id1, unsigned id2)
{
    size_t i, j;

    i = id1 - 1;
    j = id2 - 1;

    raft_io_stub_disconnect(&c->ios[i], &c->ios[j]);
    raft_io_stub_disconnect(&c->ios[j], &c->ios[i]);
}

void test_cluster_reconnect(struct test_cluster *c, unsigned id1, unsigned id2)
{
    size_t i, j;

    i = id1 - 1;
    j = id2 - 1;

    raft_io_stub_reconnect(&c->ios[i], &c->ios[j]);
    raft_io_stub_reconnect(&c->ios[j], &c->ios[i]);
}
