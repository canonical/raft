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

void test_cluster_setup(const MunitParameter params[], struct test_cluster *c)
{
    const char *n = munit_parameters_get(params, TEST_CLUSTER_SERVERS);
    const char *n_voting = munit_parameters_get(params, TEST_CLUSTER_VOTING);
    int i;
    int rc;
    if (n == NULL) {
        n = "3";
    }
    if (n_voting == NULL) {
        n_voting = n;
    }
    for (i = 0; i < atoi(n); i++) {
        struct raft_fsm *fsm = &c->fsms[i];
        test_fsm_setup(params, fsm);
    }
    rc = raft_fixture_setup(&c->fixture, atoi(n), atoi(n_voting), c->fsms,
                            munit_rand_int_range);
    munit_assert_int(rc, ==, 0);
    c->commit_index = 1; /* The initial configuration is committed. */
    log__init(&c->log);
}

void test_cluster_tear_down(struct test_cluster *c)
{
    unsigned i;
    log__close(&c->log);
    raft_fixture_tear_down(&c->fixture);
    for (i = 0; i < c->fixture.n; i++) {
        struct raft_fsm *fsm = &c->fsms[i];
        test_fsm_tear_down(fsm);
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
    unsigned n = raft_fixture_n(&c->fixture);
    size_t i, j;
    bool changed;

    for (i = 0; i < n; i++) {
        struct raft *raft = raft_fixture_get(&c->fixture, i);

        if (!raft_fixture_alive(&c->fixture, i)) {
            continue;
        }

        if (raft->state == RAFT_LEADER) {
            /* No other server is leader for this term. */
            for (j = 0; j < c->fixture.n; j++) {
                struct raft *other = raft_fixture_get(&c->fixture, j);

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
        unsigned n_acks = 0;
        bool acked = true;

        j = leader_id - 1;

        for (i = 0; i < n; i++) {
            struct raft *raft = raft_fixture_get(&c->fixture, i);

            if (raft->id == leader_id) {
                continue;
            }

            if (!raft_fixture_alive(&c->fixture, i) ||
                !raft_fixture_connected(&c->fixture, leader_id, raft->id)) {
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

            n_acks++;
        }

        if (!acked || n_acks < (n / 2)) {
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
    struct raft *raft;
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

    raft = raft_fixture_get(&c->fixture, c->leader_id - 1);
    last = log__last_index(&c->log);

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
    struct raft *raft = &c->fixture.servers[c->leader_id - 1].raft;
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
    struct raft *raft = &c->fixture.servers[c->leader_id - 1].raft;
    if (raft->commit_index > c->commit_index) {
        c->commit_index = raft->commit_index;
    }
}

void test_cluster_run_once(struct test_cluster *c)
{
    raft_fixture_step(&c->fixture);

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
                           unsigned max_msecs)
{
    int start = c->fixture.time;

    while (!stop(c) && (c->fixture.time - start) < max_msecs) {
        test_cluster_run_once(c);
    }

    return c->fixture.time < max_msecs ? 0 : -1;
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

    raft = &c->fixture.servers[leader_id - 1].raft;

    buf.base = entry_id;
    buf.len = sizeof *entry_id;

    rv = raft_apply(raft, req, &buf, 1, apply_cb);
    munit_assert_int(rv, ==, 0);
}

void test_cluster_add_server(struct test_cluster *c)
{
    unsigned leader_id = test_cluster_leader(c);
    unsigned id = c->fixture.n + 1;
    char *address = munit_malloc(4);
    struct raft *leader;
    MunitParameter params[] = {{NULL, NULL}};
    int rc;

    /* Create a new raft instance for the new server. */
    test_fsm_setup(params, &c->fsms[c->fixture.n]);

    rc = raft_fixture_add_server(&c->fixture, &c->fsms[c->fixture.n]);
    munit_assert_int(rc, ==, 0);

    sprintf(address, "%u", id);
    leader = &c->fixture.servers[leader_id - 1].raft;

    rc = raft_add_server(leader, id, address);
    munit_assert_int(rc, ==, 0);

    free(address);
}

void test_cluster_promote(struct test_cluster *c)
{
    unsigned leader_id = test_cluster_leader(c);
    unsigned id;
    struct raft *leader;
    int rv;

    leader = &c->fixture.servers[leader_id - 1].raft;

    id = c->fixture.n; /* Last server that was added. */

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
    raft_fixture_kill(&c->fixture, id);
}

void test_cluster_kill_majority(struct test_cluster *c)
{
    size_t i;
    size_t n;

    for (i = 0, n = 0; n < (c->fixture.n / 2) + 1; i++) {
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
    return raft_fixture_connected(&c->fixture, id1, id2);
}

void test_cluster_disconnect(struct test_cluster *c, unsigned id1, unsigned id2)
{
    raft_fixture_disconnect(&c->fixture, id1, id2);
}

void test_cluster_reconnect(struct test_cluster *c, unsigned id1, unsigned id2)
{
    raft_fixture_reconnect(&c->fixture, id1, id2);
}
