#include "progress.h"
#include "configuration.h"
#include "assert.h"

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
    p->last_contact = 0;
    p->state = PROBE;
}

struct raft_progress *progress__create_array(unsigned n_servers,
                                             raft_index last_index)
{
    struct raft_progress *p;
    unsigned i;
    p = raft_malloc(n_servers * sizeof *p);
    if (p == NULL) {
        return NULL;
    }
    for (i = 0; i < n_servers; i++) {
        init_progress(&p[i], last_index);
    }

    return p;
}

struct raft_progress *progress__update_array(
    struct raft_progress *cur_p,
    raft_index last_index,
    const struct raft_configuration *cur_configuration,
    const struct raft_configuration *new_configuration)
{
    struct raft_progress *p;
    unsigned i;

    p = raft_malloc(new_configuration->n * sizeof *p);
    if (p == NULL) {
        return NULL;
    }

    /* First copy the progress information for the servers that exists both in
     * the current and in the new configuration. */
    for (i = 0; i < cur_configuration->n; i++) {
        unsigned id = cur_configuration->servers[i].id;
        size_t j = configuration__index_of(new_configuration, id);
        if (j == new_configuration->n) {
            /* This server is not present in the new configuration, so we just
             * skip it. */
            continue;
        }
        p[j] = cur_p[i];
    }

    /* Then reset the replication state for servers that are present in the new
     * configuration, but not in the current one. */
    for (i = 0; i < new_configuration->n; i++) {
        unsigned id = new_configuration->servers[i].id;
        size_t j = configuration__index_of(cur_configuration, id);
        if (j < cur_configuration->n) {
            /* This server is present both in the new and in the current
             * configuration, so we have already copied its next/match index
             * value in the loop above. */
            continue;
        }
        assert(j == cur_configuration->n);
	init_progress(&p[i], last_index);
    }

    return p;
}
