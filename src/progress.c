#include "progress.h"

/* Possible values for the state field of struct raft_progress. */
enum {
    PROBE = 0, /* At most one AppendEntries per heartbeat interval */
    PIPELINE,  /* Optimistically stream AppendEntries */
    SNAPSHOT   /* Sending a snapshot */
};

void progress__init(struct raft_progress *p, raft_index last_index)
{
    p->next_index = last_index + 1;
    p->match_index = 0;
    p->last_contact = 0;
    p->state = PROBE;
}
