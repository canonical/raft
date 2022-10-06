#include "lifecycle.h"
#include "queue.h"

#include <stdlib.h>

void lifecycleRequestStart(struct raft *r, struct request *req)
{
    QUEUE_PUSH(&r->leader_state.requests, &req->queue);
}

void lifecycleRequestEnd(struct raft *r, struct request *req)
{
    (void)r;
    QUEUE_REMOVE(&req->queue);
}
