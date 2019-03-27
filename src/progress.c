#include "progress.h"
#include "assert.h"
#include "configuration.h"
#include "log.h"

/* Possible values for the state field of struct raft_progress. */
enum {
    PROBE = 0, /* At most one AppendEntries per heartbeat interval */
    PIPELINE,  /* Optimistically stream AppendEntries */
    SNAPSHOT   /* Sending a snapshot */
};

static void init_progress(struct raft_progress *p, raft_index last_index)
{
    p->next_index = last_index + 1;
    p->match_index = 0;
    p->snapshot_index = 0;
    p->recent_activity = false;
    p->state = PROBE;
}

int progress__create_array(struct raft *r)
{
    struct raft_progress *p;
    unsigned i;
    raft_index last_index = log__last_index(&r->log);
    p = raft_malloc(r->configuration.n * sizeof *p);
    if (p == NULL) {
        return RAFT_ENOMEM;
    }
    for (i = 0; i < r->configuration.n; i++) {
        init_progress(&p[i], last_index);
    }
    r->leader_state.progress = p;
    return 0;
}

int progress__update_array(struct raft *r,
                           const struct raft_configuration *configuration)
{
    raft_index last_index = log__last_index(&r->log);
    struct raft_progress *p;
    unsigned i;

    p = raft_malloc(configuration->n * sizeof *p);
    if (p == NULL) {
        return RAFT_ENOMEM;
    }

    /* First copy the progress information for the servers that exists both in
     * the current and in the new configuration. */
    for (i = 0; i < r->configuration.n; i++) {
        unsigned id = r->configuration.servers[i].id;
        size_t j = configuration__index_of(configuration, id);
        if (j == configuration->n) {
            /* This server is not present in the new configuration, so we just
             * skip it. */
            continue;
        }
        p[j] = r->leader_state.progress[i];
    }

    /* Then reset the replication state for servers that are present in the new
     * configuration, but not in the current one. */
    for (i = 0; i < configuration->n; i++) {
        unsigned id = configuration->servers[i].id;
        size_t j = configuration__index_of(&r->configuration, id);
        if (j < r->configuration.n) {
            /* This server is present both in the new and in the current
             * configuration, so we have already copied its next/match index
             * value in the loop above. */
            continue;
        }
        assert(j == r->configuration.n);
        init_progress(&p[i], last_index);
    }

    raft_free(r->leader_state.progress);
    r->leader_state.progress = p;

    return 0;
}

bool progress__check_quorum(struct raft *r)
{
    unsigned i;
    unsigned contacts = 0;

    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        struct raft_progress *progress = &r->leader_state.progress[i];
	bool recent_activity = progress->recent_activity;
	progress->recent_activity = false;
        if (!server->voting) {
            continue;
        }
        if (server->id == r->id) {
            contacts++;
            continue;
        }
        if (recent_activity) {
            contacts++;
        }
    }

    return contacts > configuration__n_voting(&r->configuration) / 2;
}
