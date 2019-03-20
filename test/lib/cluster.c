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
}

void test_cluster_tear_down(struct test_cluster *c)
{
    unsigned i;
    raft_fixture_tear_down(&c->fixture);
    for (i = 0; i < c->fixture.n; i++) {
        struct raft_fsm *fsm = &c->fsms[i];
        test_fsm_tear_down(fsm);
    }
}

void test_cluster_run_once(struct test_cluster *c)
{
    raft_fixture_step(&c->fixture);
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
    return c->fixture.leader_id;
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
    leader = raft_fixture_get(&c->fixture, leader_id - 1);

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

    leader = raft_fixture_get(&c->fixture, leader_id - 1);

    id = c->fixture.n; /* Last server that was added. */

    rv = raft_promote(leader, id);
    munit_assert_int(rv, ==, 0);
}

bool test_cluster_committed_2(struct test_cluster *c)
{
    return c->fixture.commit_index >= 2;
}

bool test_cluster_committed_3(struct test_cluster *c)
{
    return c->fixture.commit_index >= 3;
}

void test_cluster_kill(struct test_cluster *c, unsigned id)
{
    raft_fixture_kill(&c->fixture, id - 1);
}

void test_cluster_kill_majority(struct test_cluster *c)
{
    size_t i;
    size_t n;

    for (i = 0, n = 0; n < (c->fixture.n / 2) + 1; i++) {
        uint64_t id = i + 1;
        if (id == c->fixture.leader_id) {
            continue;
        }
        test_cluster_kill(c, id);
        n++;
    }
}

void test_cluster_disconnect(struct test_cluster *c, unsigned id1, unsigned id2)
{
    raft_fixture_disconnect(&c->fixture, id1 - 1, id2 - 1);
}

void test_cluster_reconnect(struct test_cluster *c, unsigned id1, unsigned id2)
{
    raft_fixture_reconnect(&c->fixture, id1 - 1, id2 - 1);
}
