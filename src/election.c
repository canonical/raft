#include "election.h"
#include "assert.h"
#include "configuration.h"
#include "log.h"
#include "logging.h"

/* Set to 1 to enable tracing. */
#if 0
#define tracef(MSG, ...) debugf(r->io, MSG, __VA_ARGS__)
#else
#define tracef(MSG, ...)
#endif

void election__reset_timer(struct raft *r)
{
    assert(r != NULL);
    r->randomized_election_timeout =
        r->io->random(r->io, r->election_timeout, 2 * r->election_timeout);
    r->election_elapsed = 0;
}

static void send_request_vote_cb(struct raft_io_send *req, int status)
{
    (void)status;
    raft_free(req);
}

/* Send a RequestVote RPC to the given server. */
static int send_request_vote(struct raft *r, const struct raft_server *server)
{
    struct raft_message message;
    struct raft_io_send *req;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_CANDIDATE);

    assert(server != NULL);
    assert(server->id != r->id);
    assert(server->id != 0);

    message.type = RAFT_IO_REQUEST_VOTE;
    message.request_vote.term = r->current_term;
    message.request_vote.candidate_id = r->id;
    message.request_vote.last_log_index = log__last_index(&r->log);
    message.request_vote.last_log_term = log__last_term(&r->log);
    message.server_id = server->id;
    message.server_address = server->address;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }

    rv = r->io->send(r->io, req, &message, send_request_vote_cb);
    if (rv != 0) {
        raft_free(req);
        return rv;
    }

    return 0;
}

int election__start(struct raft *r)
{
    raft_term term;
    size_t n_voting;
    size_t voting_index;
    size_t i;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_CANDIDATE);

    n_voting = configuration__n_voting(&r->configuration);
    voting_index = configuration__index_of_voting(&r->configuration, r->id);

    /* This function should not be invoked if we are not a voting server, hence
     * voting_index must be lower than the number of servers in the
     * configuration (meaning that we are a voting server). */
    assert(voting_index < r->configuration.n);

    /* Sanity check that configuration__n_voting and
     * configuration__index_of_voting have returned somethig that makes
     * sense. */
    assert(n_voting <= r->configuration.n);
    assert(voting_index < n_voting);

    /* Increment current term */
    term = r->current_term + 1;
    rv = r->io->set_term(r->io, term);
    if (rv != 0) {
        goto err;
    }

    /* Vote for self */
    rv = r->io->set_vote(r->io, r->id);
    if (rv != 0) {
        goto err;
    }

    /* Update our cache too. */
    r->current_term = term;
    r->voted_for = r->id;

    /* Reset election timer. */
    election__reset_timer(r);

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
        const struct raft_server *server = &r->configuration.servers[i];
        if (server->id == r->id || !server->voting) {
            continue;
        }
        rv = send_request_vote(r, server);
        if (rv != 0) {
            /* This is not a critical failure, let's just log it. */
            warnf(r->io, "failed to send vote request to server %ld: %s (%d)",
                  server->id, raft_strerror(rv), rv);
        }
    }

    return 0;

err:
    assert(rv != 0);
    return rv;
}

int election__vote(struct raft *r,
                   const struct raft_request_vote *args,
                   bool *granted)
{
    const struct raft_server *local_server;
    raft_index local_last_index;
    raft_term local_last_term;
    int rv;

    assert(r != NULL);
    assert(args != NULL);
    assert(granted != NULL);

    local_server = configuration__get(&r->configuration, r->id);

    *granted = false;

    if (local_server == NULL || !local_server->voting) {
        tracef("local server is not voting -> not granting vote");
        return 0;
    }

    if (r->voted_for != 0 && r->voted_for != args->candidate_id) {
        tracef("local server already voted -> not granting vote");
        return 0;
    }

    local_last_index = log__last_index(&r->log);

    /* Our log is definitely not more up-to-date if it's empty! */
    if (local_last_index == 0) {
        tracef("local log is empty -> granting vote");
        goto grant_vote;
    }

    local_last_term = log__last_term(&r->log);

    if (args->last_log_term < local_last_term) {
        /* The requesting server has last entry's log term lower than ours. */
        tracef(
            "local last entry %llu has term %llu higher than %llu -> not "
            "granting",
            local_last_index, local_last_term, args->last_log_term);
        return 0;
    }

    if (args->last_log_term > local_last_term) {
        /* The requesting server has a more up-to-date log. */
        tracef(
            "remote last entry %llu has term %llu higher than %llu -> "
            "granting vote",
            args->last_log_index, args->last_log_term, local_last_term);
        goto grant_vote;
    }

    /* The term of the last log entry is the same, so let's compare the length
     * of the log. */
    assert(args->last_log_term == local_last_term);

    if (local_last_index <= args->last_log_index) {
        /* Our log is shorter or equal to the one of the requester. */
        tracef("remote log equal or longer than local -> granting vote");
        goto grant_vote;
    }

    tracef("remote log shorter than local -> not granting vote");

    return 0;

grant_vote:
    rv = r->io->set_vote(r->io, args->candidate_id);
    if (rv != 0) {
        return rv;
    }

    *granted = true;
    r->voted_for = args->candidate_id;

    /* Reset the election timer. */
    r->election_elapsed = 0;

    return 0;
}

bool election__tally(struct raft *r, size_t votes_index)
{
    size_t n_voting = configuration__n_voting(&r->configuration);
    size_t votes = 0;
    size_t i;
    size_t half = n_voting / 2;

    assert(r != NULL);
    assert(r->state == RAFT_CANDIDATE);
    assert(r->candidate_state.votes != NULL);

    r->candidate_state.votes[votes_index] = true;

    for (i = 0; i < n_voting; i++) {
        if (r->candidate_state.votes[i]) {
            votes++;
        }
    }

    return votes >= half + 1;
}
