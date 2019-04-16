#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/raft/fixture.h"
#include "../include/raft/io_stub.h"

#include "assert.h"
#include "configuration.h"
#include "log.h"

/* Maximum number of cluster steps to perform when waiting for a certain state
 * to be reached. */
#define MAX_STEPS 100

#define HEARTBEAT_TIMEOUT 100
#define ELECTION_TIMEOUT 1000
#define MIN_LATENCY 5
#define MAX_LATENCY 50

static int init_server(unsigned i,
                       struct raft_fixture_server *s,
                       struct raft_fsm *fsm)
{
    int rc;
    s->alive = true;
    s->id = i + 1;
    sprintf(s->address, "%u", s->id);
    rc = raft_io_stub_init(&s->io);
    if (rc != 0) {
        return rc;
    }
    raft_io_stub_set_latency(&s->io, MIN_LATENCY, MAX_LATENCY);
    rc = raft_init(&s->raft, &s->io, fsm, s->id, s->address);
    if (rc != 0) {
        return rc;
    }
    raft_set_election_timeout(&s->raft, ELECTION_TIMEOUT);
    raft_set_heartbeat_timeout(&s->raft, HEARTBEAT_TIMEOUT);
    return 0;
}

static void close_server(struct raft_fixture_server *s)
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

int raft_fixture_init(struct raft_fixture *f, unsigned n, struct raft_fsm *fsms)
{
    unsigned i;
    int rc;
    assert(n >= 1);

    f->time = 0;
    f->n = n;

    /* Initialize all servers */
    for (i = 0; i < n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        rc = init_server(i, s, &fsms[i]);
        if (rc != 0) {
            return rc;
        }
    }

    /* Connect all servers to each another */
    for (i = 0; i < f->n; i++) {
        connect_to_all(f, i);
    }

    log__init(&f->log);
    f->commit_index = 0;

    return 0;
}

void raft_fixture_close(struct raft_fixture *f)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        close_server(&f->servers[i]);
    }
    log__close(&f->log);
}

int raft_fixture_configuration(struct raft_fixture *f,
                               unsigned n_voting,
                               struct raft_configuration *configuration)
{
    unsigned i;

    assert(f->n > 0);
    assert(n_voting > 0);
    assert(n_voting <= f->n);

    raft_configuration_init(configuration);

    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s;
        bool voting = i < n_voting;
        int rc;
        s = &f->servers[i];
        rc = raft_configuration_add(configuration, s->id, s->address, voting);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

int raft_fixture_bootstrap(struct raft_fixture *f,
                           struct raft_configuration *configuration)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        struct raft *raft = raft_fixture_get(f, i);
        int rc;
        rc = raft_bootstrap(raft, configuration);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

int raft_fixture_start(struct raft_fixture *f)
{
    unsigned i;
    int rc;
    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        rc = raft_start(&s->raft);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
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

unsigned raft_fixture_leader_index(struct raft_fixture *f)
{
    if (f->leader_id != 0) {
        return f->leader_id - 1;
    }
    return f->n;
}

unsigned raft_fixture_voted_for(struct raft_fixture *f, unsigned i)
{
    return raft_io_stub_vote(&f->servers[i].io);
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

void raft_fixture_advance(struct raft_fixture *f, unsigned msecs)
{
    unsigned i;
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

        /* Check if the entry was snapshotted. */
        if (entry2 == NULL) {
            assert(raft->log.snapshot.last_index >= index);
	    continue;
        }

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

    raft_fixture_advance(f, timeout + 1);

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

bool raft_fixture_step_until(struct raft_fixture *f,
                             bool (*stop)(struct raft_fixture *f, void *arg),
                             void *arg,
                             unsigned max_msecs)
{
    unsigned start = f->time;
    while (!stop(f, arg) && (f->time - start) < max_msecs) {
        raft_fixture_step(f);
    }
    return f->time - start < max_msecs;
}

static bool spin(struct raft_fixture *f, void *arg)
{
    (void)f;
    (void)arg;
    return false;
}

void raft_fixture_step_until_elapsed(struct raft_fixture *f, unsigned msecs)
{
    raft_fixture_step_until(f, spin, NULL, msecs);
}

static bool has_leader(struct raft_fixture *f, void *arg)
{
    (void)arg;
    return f->leader_id != 0;
}

bool raft_fixture_step_until_has_leader(struct raft_fixture *f,
                                        unsigned max_msecs)
{
    return raft_fixture_step_until(f, has_leader, NULL, max_msecs);
}

static bool has_no_leader(struct raft_fixture *f, void *arg)
{
    (void)arg;
    return f->leader_id == 0;
}

bool raft_fixture_step_until_has_no_leader(struct raft_fixture *f,
                                           unsigned max_msecs)
{
    return raft_fixture_step_until(f, has_no_leader, NULL, max_msecs);
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

/* Reset the election timeout on all servers except the given one. */
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
        raft_set_election_timeout(raft, msecs);
    }
}

void raft_fixture_elect(struct raft_fixture *f, unsigned i)
{
    struct raft *raft = raft_fixture_get(f, i);
    unsigned j;

    /* Make sure there's currently no leader. */
    assert(f->leader_id == 0);

    /* Make sure that the given server is voting. */
    assert(configuration__get(&raft->configuration, raft->id)->voting);

    /* Make sure all servers are currently followers */
    for (j = 0; j < f->n; j++) {
        assert(raft_state(&f->servers[j].raft) == RAFT_FOLLOWER);
    }

    /* Set a very large election timeout on all servers, except the one being
     * elected, effectively preventing competition. */
    set_all_election_timeouts_except(f, ELECTION_TIMEOUT * 10, i);

    raft_fixture_step_until_has_leader(f, ELECTION_TIMEOUT * 20);
    assert(f->leader_id == raft->id);

    set_all_election_timeouts_except(f, ELECTION_TIMEOUT, i);
}

void raft_fixture_depose(struct raft_fixture *f)
{
    unsigned leader_i;

    assert(f->leader_id != 0);
    leader_i = f->leader_id - 1;

    /* Set a very large election timeout on all followers, to prevent them from
     * starting an election. */
    set_all_election_timeouts_except(f, ELECTION_TIMEOUT * 10, leader_i);

    /* Prevent all servers from sending append entries results, so the leader
     * will eventually step down. */
    drop_all_except(f, RAFT_IO_APPEND_ENTRIES_RESULT, true, leader_i);

    raft_fixture_step_until_has_no_leader(f, ELECTION_TIMEOUT * 3);
    assert(f->leader_id == 0);

    drop_all_except(f, RAFT_IO_APPEND_ENTRIES_RESULT, false, leader_i);
    set_all_election_timeouts_except(f, ELECTION_TIMEOUT, leader_i);
}

struct step_apply
{
    unsigned i;
    raft_index index;
};

static bool has_applied_index(struct raft_fixture *f, void *arg)
{
    struct step_apply *apply = (struct step_apply *)arg;
    struct raft *raft;
    unsigned n = 0;
    unsigned i;

    if (apply->i < f->n) {
        raft = raft_fixture_get(f, apply->i);
        return raft_last_applied(raft) >= apply->index;
    }

    for (i = 0; i < f->n; i++) {
        raft = raft_fixture_get(f, i);
        if (raft_last_applied(raft) >= apply->index) {
            n++;
        }
    }
    return n == f->n;
}

bool raft_fixture_step_until_applied(struct raft_fixture *f,
                                     unsigned i,
                                     raft_index index,
                                     unsigned max_msecs)
{
    struct step_apply apply = {i, index};
    return raft_fixture_step_until(f, has_applied_index, &apply, max_msecs);
}

void raft_fixture_disconnect(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    raft_io_stub_disconnect(io1, io2);
    raft_io_stub_disconnect(io2, io1);
}

static void disconnect_from_all(struct raft_fixture *f, unsigned i)
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

void raft_fixture_kill(struct raft_fixture *f, unsigned i)
{
    disconnect_from_all(f, i);
    f->servers[i].alive = false;
}

int raft_fixture_grow(struct raft_fixture *f, struct raft_fsm *fsm)
{
    struct raft_fixture_server *s;
    unsigned i;
    unsigned j;
    int rc;
    i = f->n;
    f->n++;
    s = &f->servers[i];

    rc = init_server(i, s, fsm);
    if (rc != 0) {
        return rc;
    }

    connect_to_all(f, i);
    for (j = 0; j < f->n; j++) {
        struct raft_io *io1 = &f->servers[i].io;
        struct raft_io *io2 = &f->servers[j].io;
        raft_io_stub_connect(io2, io1);
    }

    return 0;
}

void raft_fixture_set_random(struct raft_fixture *f,
                             unsigned i,
                             int (*random)(int, int))
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_set_random(&s->io, random);
}

void raft_fixture_set_latency(struct raft_fixture *f,
                              unsigned i,
                              unsigned min,
                              unsigned max)
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_set_latency(&s->io, min, max);
}

void raft_fixture_set_term(struct raft_fixture *f, unsigned i, raft_term term)
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_set_term(&s->io, term);
}

void raft_fixture_set_snapshot(struct raft_fixture *f,
                               unsigned i,
                               struct raft_snapshot *snapshot)
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_set_snapshot(&s->io, snapshot);
}

void raft_fixture_set_entries(struct raft_fixture *f,
                              unsigned i,
                              struct raft_entry *entries,
                              unsigned n)
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_set_entries(&s->io, entries, n);
}

void raft_fixture_add_entry(struct raft_fixture *f,
                            unsigned i,
                            struct raft_entry *entry) {
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_add_entry(&s->io, entry);
}

void raft_fixture_io_fault(struct raft_fixture *f,
                           unsigned i,
                           int delay,
                           int repeat)
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_fault(&s->io, delay, repeat);
}
