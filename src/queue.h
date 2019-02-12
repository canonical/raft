#ifndef RAFT_QUEUE_H_
#define RAFT_QUEUE_H_

#include <stddef.h>

typedef void *raft__queue[2];

/* Private macros. */
#define RAFT__QUEUE_NEXT(q) (*(raft__queue **)&((*(q))[0]))
#define RAFT__QUEUE_PREV(q) (*(raft__queue **)&((*(q))[1]))

#define RAFT__QUEUE_PREV_NEXT(q) (RAFT__QUEUE_NEXT(RAFT__QUEUE_PREV(q)))
#define RAFT__QUEUE_NEXT_PREV(q) (RAFT__QUEUE_PREV(RAFT__QUEUE_NEXT(q)))

/**
 * Initialize an empty queue.
 */
#define RAFT__QUEUE_INIT(q)        \
    {                              \
        RAFT__QUEUE_NEXT(q) = (q); \
        RAFT__QUEUE_PREV(q) = (q); \
    }

/**
 * Return true if the queue has no element.
 */
#define RAFT__QUEUE_IS_EMPTY(q) \
    ((const raft__queue *)(q) == (const raft__queue *)RAFT__QUEUE_NEXT(q))

/**
 * Insert an element at the back of a queue.
 */
#define RAFT__QUEUE_PUSH(q, e)                     \
    {                                              \
        RAFT__QUEUE_NEXT(e) = (q);                 \
        RAFT__QUEUE_PREV(e) = RAFT__QUEUE_PREV(q); \
        RAFT__QUEUE_PREV_NEXT(e) = (e);            \
        RAFT__QUEUE_PREV(q) = (e);                 \
    }

/**
 * Remove the given element from the queue. Any element can be removed at any
 * time.
 */
#define RAFT__QUEUE_REMOVE(e)                           \
    {                                                   \
        RAFT__QUEUE_PREV_NEXT(e) = RAFT__QUEUE_NEXT(e); \
        RAFT__QUEUE_NEXT_PREV(e) = RAFT__QUEUE_PREV(e); \
    }

/**
 * Return the element at the front of the queue.
 */
#define RAFT__QUEUE_HEAD(q) (RAFT__QUEUE_NEXT(q))

/**
 * Return the element at the back of the queue.
 */
#define RAFT__QUEUE_TAIL(q) (RAFT__QUEUE_PREV(q))

/**
 * Iternate over the element of a queue.
 *
 * Mutating the queue while iterating results in undefined behavior.
 */
#define RAFT__QUEUE_FOREACH(q, e) \
    for ((q) = RAFT__QUEUE_NEXT(e); (q) != (e); (q) = RAFT__QUEUE_NEXT(q))

/**
 * Return the structure holding the given element.
 */
#define RAFT__QUEUE_DATA(e, type, field) \
    ((type *)((char *)(e)-offsetof(type, field)))

#endif /* RAFT_QUEUE_H_*/
