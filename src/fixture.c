#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "../include/raft/fixture.h"
#include "../include/raft/io_stub.h"

#include "assert.h"
#include "log.h"

/* Maximum number of cluster steps to perform when waiting for a certain state
 * to be reached. */
#define MAX_STEPS 100

#define HEARTBEAT_TIMEOUT 50
#define ELECTION_TIMEOUT 250

static int setup_server(unsigned i,
                        struct raft_fixture_server *s,
                        struct raft_fsm *fsm,
                        int (*random)(int, int))
{
    int rc;
    s->alive = true;
    s->id = i + 1;
    sprintf(s->address, "%u", s->id);
    rc = raft_io_stub_init(&s->io);
    if (rc != 0) {
        return rc;
    }
    raft_io_stub_set_random(&s->io, random);
    raft_io_stub_set_latency(&s->io, 5, 50);
    rc = raft_init(&s->raft, &s->io, fsm, s->id, s->address);
    if (rc != 0) {
        return rc;
    }
    raft_set_election_timeout(&s->raft, ELECTION_TIMEOUT);
    raft_set_heartbeat_timeout(&s->raft, HEARTBEAT_TIMEOUT);
    return 0;
}

static void tear_down_server(struct raft_fixture_server *s)
{
    raft_close(&s->raft, NULL);
    raft_io_stub_close(&s->io);
}

/* Connect the server with the given index to all others */
static void connect_to_all(struct raft_fixture *f, unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        struct raft_io *io1 = &f->servers[i].io;
        struct raft_io *io2 = &f->servers[j].io;
        if (i == j) {
            continue;
        }
        raft_io_stub_connect(io1, io2);
    }
}

int raft_fixture_setup(struct raft_fixture *f,
                       unsigned n,
                       unsigned n_voting,
                       struct raft_fsm *fsms,
                       int (*random)(int, int))
{
    struct raft_configuration configuration;
    unsigned i;
    int rc;
    assert(n >= 1);
    assert(n_voting >= 1);
    assert(n_voting <= n);

    f->time = 0;
    f->n = n;
    f->random = random;

    /* Setup all servers */
    raft_configuration_init(&configuration);
    for (i = 0; i < n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        bool voting = i < n_voting;
        rc = setup_server(i, s, &fsms[i], random);
        if (rc != 0) {
            return rc;
        }
        rc = raft_configuration_add(&configuration, s->id, s->address, voting);
        if (rc != 0) {
            return rc;
        }
    }

    /* Connect all servers to each another */
    for (i = 0; i < f->n; i++) {
        connect_to_all(f, i);
    }

    /* Bootstrap and start */
    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        rc = raft_bootstrap(&s->raft, &configuration);
        if (rc != 0) {
            return rc;
        }
        rc = raft_start(&s->raft);
        if (rc != 0) {
            return rc;
        }
    }

    raft_configuration_close(&configuration);

    log__init(&f->log);
    f->commit_index = 1; /* The initial configuration is committed. */

    return 0;
}

void raft_fixture_tear_down(struct raft_fixture *f)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        tear_down_server(&f->servers[i]);
    }
    log__close(&f->log);
}

unsigned raft_fixture_n(struct raft_fixture *f)
{
    return f->n;
}

struct raft *raft_fixture_get(struct raft_fixture *f, unsigned i)
{
    assert(i < f->n);
    return &f->servers[i].raft;
}

bool raft_fixture_alive(struct raft_fixture *f, unsigned i)
{
    assert(i < f->n);
    return f->servers[i].alive;
}

/* Flush any pending write to the disk and any pending message into the network
 * buffers (this will assign them a latency timer). */
static void flush_io(struct raft_fixture *f)
{
    size_t i;
    for (i = 0; i < f->n; i++) {
        struct raft_io *io = &f->servers[i].io;
        raft_io_stub_flush_all(io);
    }
}

/* Figure what's the lowest delivery timer across all stub I/O instances,
 * i.e. the time at which the next message should be delivered (if any is
 * pending). */
static int lowest_deliver_timeout(struct raft_fixture *f)
{
    int min_timeout = -1;
    size_t i;

    for (i = 0; i < f->n; i++) {
        struct raft_io *io = &f->servers[i].io;
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

/* Check what's the raft instance whose timer is closest to expiration. */
static unsigned lowest_raft_timeout(struct raft_fixture *f)
{
    size_t i;
    unsigned min_timeout = 0; /* Lowest remaining time before expiration */

    for (i = 0; i < f->n; i++) {
        struct raft *r = &f->servers[i].raft;
        unsigned timeout; /* Milliseconds remaining before expiration. */

        timeout = raft_next_timeout(r);
        if (i == 0 || timeout <= min_timeout) {
            min_timeout = timeout;
        }
    }

    return min_timeout;
}

/* Fire either a message delivery or a raft tick, depending on whose timer is
 * closest to expiration. */
static void advance(struct raft_fixture *f, unsigned msecs)
{
    size_t i;

    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        if (!s->alive) {
            continue;
        }
        raft_io_stub_advance(&s->io, msecs);
    }
    f->time += msecs;
}

/* Update the leader and check for election safety.
 *
 * From figure 3.2:
 *
 *   Election Safety -> At most one leader can be elected in a given
 *   term.
 *
 * Return true if the current leader turns out to be different from the one at
 * the time this function was called.
 */
static bool update_leader(struct raft_fixture *f)
{
    unsigned leader_id = 0;
    unsigned leader_i = 0;
    raft_term leader_term = 0;
    unsigned i;
    bool changed;

    for (i = 0; i < f->n; i++) {
        struct raft *raft = raft_fixture_get(f, i);
        unsigned j;

        if (!raft_fixture_alive(f, i)) {
            continue;
        }

        if (raft_state(raft) == RAFT_LEADER) {
            /* No other server is leader for this term. */
            for (j = 0; j < f->n; j++) {
                struct raft *other = raft_fixture_get(f, j);

                if (other->id == raft->id) {
                    continue;
                }

                if (other->state == RAFT_LEADER) {
                    if (other->current_term == raft->current_term) {
                        fprintf(
                            stderr,
                            "server %u and %u are both leaders in term %llu",
                            raft->id, other->id, raft->current_term);
                        abort();
                    }
                }
            }

            if (raft->current_term > leader_term) {
                leader_id = raft->id;
                leader_i = i;
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

        for (i = 0; i < f->n; i++) {
            struct raft *raft = raft_fixture_get(f, i);
            if (i == leader_i) {
                continue;
            }
            if (!raft_fixture_alive(f, i) ||
                !raft_fixture_connected(f, leader_i, i)) {
                /* This server is not alive or not connected to the leader, so
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

        if (!acked || n_acks < (f->n / 2)) {
            leader_id = 0;
        }
    }

    changed = leader_id != f->leader_id;
    f->leader_id = leader_id;

    return changed;
}

/* Check for leader append-only.
 *
 * From figure 3.2:
 *
 *   Leader Append-Only -> A leader never overwrites or deletes entries in its
 *   own log; it only appends new entries.
 */
static void check_leader_append_only(struct raft_fixture *f)
{
    struct raft *raft;
    raft_index index;
    raft_index last = log__last_index(&f->log);

    /* If the cached log is empty it means there was no leader before. */
    if (last == 0) {
        return;
    }

    /* If there's no new leader, just return. */
    if (f->leader_id == 0) {
        return;
    }

    raft = raft_fixture_get(f, f->leader_id - 1);
    last = log__last_index(&f->log);

    for (index = 1; index <= last; index++) {
        const struct raft_entry *entry1;
        const struct raft_entry *entry2;

        entry1 = log__get(&f->log, index);
        entry2 = log__get(&raft->log, index);

        assert(entry1 != NULL);

        /* Entry was not deleted. */
        assert(entry2 != NULL);

        /* TODO: check other entry types too. */
        if (entry1->type != RAFT_COMMAND) {
            continue;
        }

        /* Entry was not overwritten. TODO: check all content. */
        assert(entry1->term == entry2->term);
        assert(*(uint32_t *)entry1->buf.base == *(uint32_t *)entry2->buf.base);
    }
}

/* Make a copy of the the current leader log, in order to perform the Leader
 * Append-Only check at the next iteration. */
static void copy_leader_log(struct raft_fixture *f)
{
    struct raft *raft = raft_fixture_get(f, f->leader_id - 1);
    struct raft_entry *entries;
    unsigned n;
    size_t i;
    int rc;

    log__close(&f->log);
    log__init(&f->log);

    rc = log__acquire(&raft->log, 1, &entries, &n);
    assert(rc == 0);

    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        struct raft_buffer buf;
        buf.len = entry->buf.len;
        buf.base = raft_malloc(buf.len);
        memcpy(buf.base, entry->buf.base, buf.len);
        rc = log__append(&f->log, entry->term, entry->type, &buf, NULL);
        assert(rc == 0);
    }

    log__release(&raft->log, 1, entries, n);
}

/* Update the commit index to match the one from the current leader. */
static void update_commit_index(struct raft_fixture *f)
{
    struct raft *raft = raft_fixture_get(f, f->leader_id - 1);
    if (raft->commit_index > f->commit_index) {
        f->commit_index = raft->commit_index;
    }
}

void raft_fixture_step(struct raft_fixture *f)
{
    int deliver_timeout;
    unsigned raft_timeout;
    unsigned timeout;

    /* First flush I/O operations. */
    flush_io(f);

    /* Second, figure what's the message with the lowest timer (i.e. the
     * message that should be delivered first) */
    deliver_timeout = lowest_deliver_timeout(f);

    /* Now check what's the raft instance whose timer is closest to
     * expiration. */
    raft_timeout = lowest_raft_timeout(f);

    /* Fire either a raft tick or a message delivery. */
    if (deliver_timeout != -1 && (unsigned)deliver_timeout < raft_timeout) {
        timeout = deliver_timeout;
    } else {
        timeout = raft_timeout;
    }

    advance(f, timeout + 1);

    /* If the leader has not changed check the Leader Append-Only
     * guarantee. */
    if (!update_leader(f)) {
        check_leader_append_only(f);
    }

    /* If we have a leader, update leader-related state . */
    if (f->leader_id != 0) {
        copy_leader_log(f);
        update_commit_index(f);
    }
}

/* Return the index of the current leader, or -1 */
static int current_leader_index(struct raft_fixture *f)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        if (raft_state(&s->raft) == RAFT_LEADER) {
            return i;
        }
    }
    return -1;
}

/* Enable/disable dropping outgoing messages of a certain type from all servers
 * except one. */
static void drop_all_except(struct raft_fixture *f,
                            int type,
                            bool flag,
                            unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        struct raft_fixture_server *s = &f->servers[j];
        if (j == i) {
            continue;
        }
        raft_io_stub_drop(&s->io, type, flag);
    }
}

/* Set the election timeout on all servers except the given one. */
static void set_all_election_timeouts_except(struct raft_fixture *f,
                                             unsigned msecs,
                                             unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        struct raft *raft = &f->servers[j].raft;
        if (j == i) {
            continue;
        }
        raft->election_timeout = msecs;
    }
}

void raft_fixture_elect(struct raft_fixture *f, unsigned i)
{
    unsigned j;

    /* Make sure there's currently no leader. */
    assert(current_leader_index(f) == -1);

    /* TODO: make sure that the server with the given id is a voting one */

    /* Prevent all servers from sending request vote messages, except for the
     * one to be elected. */
    drop_all_except(f, RAFT_IO_REQUEST_VOTE, true, i);
    set_all_election_timeouts_except(f, ELECTION_TIMEOUT * 3, i);

    /* Set a very large election timeout on all servers, except the one being
     * elected. This effectively avoids competition. */

    for (j = 0; j < MAX_STEPS; j++) {
        int leader;
        raft_fixture_step(f);
        leader = current_leader_index(f);
        if (leader == -1) {
            continue;
        }
        assert((unsigned)leader == i);
        drop_all_except(f, RAFT_IO_REQUEST_VOTE, false, i);
        set_all_election_timeouts_except(f, ELECTION_TIMEOUT, i);
        return;
    }

    assert(0);
}

void raft_fixture_wait_applied(struct raft_fixture *f, raft_index index)
{
    unsigned applied;
    unsigned i;
    for (i = 0; i < MAX_STEPS; i++) {
        unsigned j;
        applied = 0;
        raft_fixture_step(f);
        for (j = 0; j < f->n; j++) {
            struct raft *raft = &f->servers[j].raft;
            if (raft_last_applied(raft) >= index) {
                applied++;
            }
        }
        if (applied == f->n) {
            return;
        }
    }
    assert(0);
}

void raft_fixture_depose(struct raft_fixture *f)
{
    int leader = current_leader_index(f);
    unsigned i;

    assert(leader != -1);

    /* Prevent all servers from sending append entries results, so the leader
     * will eventually step down. */
    drop_all_except(f, RAFT_IO_APPEND_ENTRIES_RESULT, true, leader);

    for (i = 0; i < MAX_STEPS; i++) {
        raft_fixture_step(f);
        leader = current_leader_index(f);
        if (leader == -1) {
            drop_all_except(f, RAFT_IO_APPEND_ENTRIES_RESULT, false, leader);
            return;
        }
    }

    assert(0);
}

void raft_fixture_disconnect(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    raft_io_stub_disconnect(io1, io2);
    raft_io_stub_disconnect(io2, io1);
}

void raft_fixture_disconnect_from_all(struct raft_fixture *f, unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        if (j == i) {
            continue;
        }
        raft_fixture_disconnect(f, i, j);
    }
}

bool raft_fixture_connected(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    return raft_io_stub_connected(io1, io2) && raft_io_stub_connected(io2, io1);
}

void raft_fixture_reconnect(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    raft_io_stub_reconnect(io1, io2);
    raft_io_stub_reconnect(io2, io1);
}

void raft_fixture_reconnect_to_all(struct raft_fixture *f, unsigned i)
{
    unsigned j;
    for (j = 0; i < f->n; j++) {
        if (j == i) {
            continue;
        }
        raft_fixture_reconnect(f, i, j);
    }
}

void raft_fixture_kill(struct raft_fixture *f, unsigned i)
{
    raft_fixture_disconnect_from_all(f, i);
    f->servers[i].alive = false;
}

int raft_fixture_add_server(struct raft_fixture *f, struct raft_fsm *fsm)
{
    struct raft_fixture_server *s;
    unsigned i;
    unsigned j;
    int rc;
    i = f->n;
    f->n++;
    s = &f->servers[i];

    rc = setup_server(i, s, fsm, f->random);
    if (rc != 0) {
        return rc;
    }

    connect_to_all(f, i);
    for (j = 0; j < f->n; j++) {
        struct raft_io *io1 = &f->servers[i].io;
        struct raft_io *io2 = &f->servers[j].io;
        raft_io_stub_connect(io2, io1);
    }

    rc = raft_start(&s->raft);
    if (rc != 0) {
        return rc;
    }

    return 0;
}
