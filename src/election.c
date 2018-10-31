#include <assert.h>

#include "configuration.h"
#include "election.h"
#include "error.h"
#include "log.h"
#include "logger.h"

void raft_election__reset_timer(struct raft *r)
{
    assert(r != NULL);

    /* [election_timeout, 2 * election_timeout) */
    r->election_timeout_rand =
        r->election_timeout + (abs(r->rand()) % r->election_timeout);

    r->timer = 0;
}

/**
 * Send a RequestVote RPC to the given server.
 */
static int raft_election__send_request_vote(struct raft *r,
                                            struct raft_server *server)
{
    struct raft_request_vote_args args;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_STATE_CANDIDATE);

    assert(server != NULL);
    assert(server->id != r->id);
    assert(server->id != 0);

    /* TODO: account for snapshots */

    args.term = r->current_term;
    args.candidate_id = r->id;
    args.last_log_index = raft_log__last_index(&r->log);
    args.last_log_term = raft_log__last_term(&r->log);

    rv = r->io->send_request_vote_request(r->io, server, &args);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_election__start(struct raft *r)
{
    raft_term term;
    size_t n_voting;
    size_t voting_index;
    size_t i;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_STATE_FOLLOWER || r->state == RAFT_STATE_CANDIDATE);

    n_voting = raft_configuration__n_voting(&r->configuration);
    voting_index = raft_configuration__voting_index(&r->configuration, r->id);

    /* This function should not be invoked if we are not a voting server, hence
     * voting_index must be lower than the number of servers in the
     * configuration (meaning that we are a voting server). */
    assert(voting_index < r->configuration.n);

    /* Sanity check that raft_configuration__n_voting and
     * raft_configuration__votes_index have returned somethig that makes
     * sense. */
    assert(n_voting <= r->configuration.n);
    assert(voting_index < n_voting);

    /* Increment current term */
    term = r->current_term + 1;
    rv = r->io->write_term(r->io, term);
    if (rv != 0) {
        raft_error__printf(r, rv, "write term");
        goto err;
    }

    /* Vote for self */
    rv = r->io->write_vote(r->io, r->id);
    if (rv != 0) {
        raft_error__printf(r, rv, "write vote");
        goto err;
    }

    /* Update our cache too. */
    r->current_term = term;
    r->voted_for = r->id;

    /* Reset election timer. */
    raft_election__reset_timer(r);

    assert(r->candidate_state.votes != NULL);

    /* Initialize the votes array and send vote requests. */
    for (i = 0; i < n_voting; i++) {
        if (i == voting_index) {
            r->candidate_state.votes[i] = true; /* We vote for ourselves */
        } else {
            r->candidate_state.votes[i] = false;
        }
    }
    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        int rv;

        if (server->id == r->id || !server->voting) {
            continue;
        }

        rv = raft_election__send_request_vote(r, server);
        if (rv != 0) {
            /* This is not a critical failure, let's just log it. */
            raft__warnf(r, "failed to send vote request to server %ld: %s (%d)",
                        server->id, raft_strerror(rv), rv);
        }
    }

    return 0;

 err:
    assert(rv != 0);
    return rv;
}

int raft_election__maybe_grant_vote(struct raft *r,
                                    const struct raft_request_vote_args *args,
                                    bool *granted)
{
    const struct raft_server *local_server;
    uint64_t local_last_log_index;
    uint64_t local_last_log_term;
    int rv;

    local_server = raft_configuration__get(&r->configuration, r->id);

    *granted = false;

    if (local_server == NULL || !local_server->voting) {
        raft__debugf(r, "local server is not voting -> not granting vote");
        return 0;
    }

    if (r->voted_for != 0 && r->voted_for != args->candidate_id) {
        raft__debugf(r, "local server already voted -> not granting vote");
        return 0;
    }

    local_last_log_index = raft_log__last_index(&r->log);

    /* Our log is definitely not more up-to-date if it's empty! */
    if (local_last_log_index == 0) {
        raft__debugf(r, "local log is empty -> granting vote");
        goto grant_vote;
    }

    /* TODO: account for snapshots */
    local_last_log_term = raft_log__last_term(&r->log);

    if (args->last_log_term < local_last_log_term) {
        /* The requesting server has last entry's log term lower than ours. */
        raft__debugf(
            r, "local log last entry has higher last term -> not granting");
        return 0;
    }

    if (args->last_log_term > local_last_log_term) {
        /* The requesting server has a more up-to-date log. */
        raft__debugf(r,
                     "remote log last entry has higher term -> granting vote");
        goto grant_vote;
    }

    /* The term of the last log entry is the same, so let's compare the length
     * of the log. */
    assert(args->last_log_term == local_last_log_term);

    if (local_last_log_index <= args->last_log_index) {
        /* Our log is shorter or equal to the one of the requester. */
        raft__debugf(r,
                     "remote log equal or longer than local -> granting vote");
        goto grant_vote;
    }

    raft__debugf(r, "remote log shorter than local -> not granting vote");

    return 0;

grant_vote:
    rv = r->io->write_vote(r->io, args->candidate_id);
    if (rv != 0) {
        return rv;
    }

    *granted = true;
    r->voted_for = args->candidate_id;

    /* Reset the timer. */
    r->timer = 0;

    return 0;
}

bool raft_election__maybe_win(struct raft *r, size_t votes_index)
{
    size_t n_voting = raft_configuration__n_voting(&r->configuration);
    size_t votes = 0;
    size_t i;
    size_t half = n_voting / 2;

    assert(r != NULL);
    assert(r->state == RAFT_STATE_CANDIDATE);
    assert(r->candidate_state.votes != NULL);

    r->candidate_state.votes[votes_index] = true;

    for (i = 0; i < n_voting; i++) {
        if (r->candidate_state.votes[i]) {
            votes++;
        }
    }

    return votes >= half + 1;
}
