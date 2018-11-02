#include <assert.h>

#include "watch.h"

void raft_watch(struct raft *r, int event, void (*cb)(void *, int, void *))
{
    assert(r != NULL);
    assert(event == RAFT_EVENT_STATE_CHANGE ||
           event == RAFT_EVENT_COMMAND_APPLIED ||
           event == RAFT_EVENT_CONFIGURATION_APPLIED);
    assert(cb != NULL);

    r->watchers[event] = cb;
}

/**
 * Fire an event if the relevant callback is registered.
 */
static void raft_watch__fire(struct raft *r, int event, void *data)
{
    void (*cb)(void *, int, void *);

    cb = r->watchers[event];

    if (cb == NULL) {
        return;
    }

    cb(r->data, event, data);
}

void raft_watch__state_change(struct raft *r, const unsigned short old_state)
{
    assert(r != NULL);

    raft_watch__fire(r, RAFT_EVENT_STATE_CHANGE, (void*)(&old_state));
}

void raft_watch__command_applied(struct raft *r, const raft_index index)
{
    assert(r != NULL);
    assert(index > 0);

    raft_watch__fire(r, RAFT_EVENT_COMMAND_APPLIED, (void*)(&index));
}

void raft_watch__configuration_applied(struct raft *r)
{
    assert(r != NULL);
    raft_watch__fire(r, RAFT_EVENT_CONFIGURATION_APPLIED, &r->configuration);
}

void raft_watch__promotion_aborted(struct raft *r, const unsigned id)
{
    assert(r != NULL);
    assert(id > 0);

    raft_watch__fire(r, RAFT_EVENT_PROMOTION_ABORTED, (void*)(&id));
}
