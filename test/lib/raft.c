#include <stdbool.h>
#include <stdio.h>

#include "../../src/configuration.h"
#include "../../src/log.h"
#include "../../src/tick.h"

#include "io.h"
#include "munit.h"
#include "raft.h"

void test_start(struct raft *r)
{
    int rv;

    rv = raft_start(r);
    munit_assert_int(rv, ==, 0);
}

void test_stop(struct raft *r)
{
    int rv;

    rv = raft_stop(r, NULL, NULL);
    munit_assert_int(rv, ==, 0);
}

void test_bootstrap_and_start(struct raft *r,
                              int n_servers,
                              int voting_a,
                              int voting_b)
{
    struct raft_configuration configuration;
    int i;
    int rv;

    munit_assert_int(n_servers, >=, 1);
    munit_assert_int(voting_a, >=, 1);
    munit_assert_int(voting_a, <=, voting_b);
    munit_assert_int(voting_b, >=, 1);
    munit_assert_int(voting_b, <=, n_servers);

    /* Populate the configuration. */
    raft_configuration_init(&configuration);

    for (i = 0; i < n_servers; i++) {
        unsigned id = i + 1;
        char address[4];
        bool voting = (int)id >= voting_a && (int)id <= voting_b;

        sprintf(address, "%d", id);
        rv = raft_configuration_add(&configuration, id, address, voting);
        munit_assert_int(rv, ==, 0);
    }

    /* Bootstrap the instance */
    rv = raft_bootstrap(r, &configuration);
    munit_assert_int(rv, ==, 0);

    /* Cleanup */
    raft_configuration_close(&configuration);

    test_start(r);
}

void test_load(struct raft *r)
{
    const struct raft_entry *entries;
    size_t i;
    size_t n;
    int rv;

    r->current_term = test_io_get_term(r->io);
    r->voted_for = test_io_get_vote(r->io);

    test_io_get_entries(r->io, &entries, &n);

    munit_assert_int(n, >, 0);
    munit_assert_int(entries[0].type, ==, RAFT_LOG_CONFIGURATION);

    for (i = 0; i < n; i++) {
        struct raft_buffer buf;

        buf.len = entries[i].buf.len;
        buf.base = raft_malloc(buf.len);
        memcpy(buf.base, entries[i].buf.base, buf.len);

        rv = raft_log__append(&r->log, entries[i].term, entries[i].type, &buf,
                              NULL);
        munit_assert_int(rv, ==, 0);
    }

    rv = raft_configuration_decode(&entries[0].buf, &r->configuration);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(r->configuration.n, >, 0);

    r->configuration_index = 1;

    r->state = RAFT_STATE_FOLLOWER;
}

void test_bootstrap_and_load(struct raft *r,
                             int n_servers,
                             int voting_a,
                             int voting_b)
{
    munit_assert_ptr_not_null(r);

    /* Encode and flush the configuration to persistent storage as entry 1. */
    test_io_bootstrap(r->io, n_servers, voting_a, voting_b);

    /* Load back the persistent storage and cache it on the raft instance */
    test_load(r);

    r->commit_index = 1;
    r->last_applied = 1;
}

void test_become_candidate(struct raft *r)
{
    int rv;

    /* Become candidate */
    rv = raft__tick(r, r->election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(r->state, ==, RAFT_STATE_CANDIDATE);
}

void test_become_leader(struct raft *r)
{
    size_t votes = raft_configuration__n_voting(&r->configuration) / 2;
    size_t i;
    struct raft_request_vote_result result;
    int rv;

    test_become_candidate(r);

    for (i = 0; i < r->configuration.n; i++) {
        const struct raft_server *server = &r->configuration.servers[i];

        if (server->id == r->id || !server->voting) {
            continue;
        }

        result.term = r->current_term;
        result.vote_granted = 1;

        rv = raft_handle_request_vote_response(r, server->id, server->address,
                                               &result);
        munit_assert_int(rv, ==, 0);

        votes--;
        if (votes == 0) {
            break;
        }
    }

    if (votes > 0) {
        munit_error("could not get all required votes");
    }

    munit_assert_int(r->state, ==, RAFT_STATE_LEADER);

    raft_io_stub_flush(r->io);
}

void test_receive_heartbeat(struct raft *r, unsigned leader_id)
{
    struct raft_message message;
    struct raft_append_entries *args = &message.append_entries;
    char address[4];

    munit_assert_int(leader_id, !=, r->id);
    sprintf(address, "%d", leader_id);

    message.type = RAFT_IO_APPEND_ENTRIES;
    message.server_id = leader_id;
    message.server_address = address;

    args->term = r->current_term;
    args->leader_id = leader_id;

    args->prev_log_index = raft_log__last_index(&r->log);
    args->prev_log_term = raft_log__last_term(&r->log);

    args->entries = NULL;
    args->n_entries = 0;
    args->leader_commit = r->commit_index;


    raft_io_stub_dispatch(r->io, &message);
}
