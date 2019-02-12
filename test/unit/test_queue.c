#include "../../src/queue.h"

#include "../lib/munit.h"

/**
 * Helpers
 */

struct fixture
{
    void *queue[2];
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)params;
    (void)user_data;

    RAFT__QUEUE_INIT(&f->queue);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    free(f);
}

struct __item
{
    int value;
    void *queue[2];
};

/**
 * Initialize and push the given items to the queue. Each item will have a value
 * equal to its index plus one.
 */
#define __push(F, ITEMS)                               \
    {                                                  \
        int n = sizeof ITEMS / sizeof ITEMS[0];        \
        int i;                                         \
                                                       \
        for (i = 0; i < n; i++) {                      \
            struct __item *item = &items[i];           \
            item->value = i + 1;                       \
            RAFT__QUEUE_INIT(&item->queue);            \
            RAFT__QUEUE_PUSH(&F->queue, &item->queue); \
        }                                              \
    }

/**
 * Remove the i'th item among the given ones.
 */
#define __remove(ITEMS, I)                   \
    {                                        \
        RAFT__QUEUE_REMOVE(&ITEMS[I].queue); \
    }

/**
 * Assert that the item at the head of the queue has the given value.
 */
#define __assert_head(F, VALUE)                              \
    {                                                        \
        raft__queue *head = RAFT__QUEUE_HEAD(&F->queue);     \
        struct __item *item;                                 \
                                                             \
        item = RAFT__QUEUE_DATA(head, struct __item, queue); \
        munit_assert_int(item->value, ==, VALUE);            \
    }

/**
 * Assert that the item at the tail of the queue has the given value.
 */
#define __assert_tail(F, VALUE)                              \
    {                                                        \
        raft__queue *tail = RAFT__QUEUE_TAIL(&F->queue);     \
        struct __item *item;                                 \
                                                             \
        item = RAFT__QUEUE_DATA(tail, struct __item, queue); \
        munit_assert_int(item->value, ==, VALUE);            \
    }

/**
 * Assert that the queue is empty.
 */
#define __assert_is_empty(F)                                \
    {                                                       \
        munit_assert_true(RAFT__QUEUE_IS_EMPTY(&F->queue)); \
    }

/**
 * Assert that the queue is not empty.
 */
#define __assert_is_not_empty(F)                             \
    {                                                        \
        munit_assert_false(RAFT__QUEUE_IS_EMPTY(&F->queue)); \
    }

/**
 * RAFT__QUEUE_IS_EMPTY
 */

static MunitResult test_is_empty_true(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __assert_is_empty(f);

    return MUNIT_OK;
}

static MunitResult test_is_empty_false(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    struct __item items[1];

    (void)params;

    __push(f, items);

    __assert_is_not_empty(f);

    return MUNIT_OK;
}

static MunitTest is_empty_tests[] = {
    {"/true", test_is_empty_true, setup, tear_down, 0, NULL},
    {"/false", test_is_empty_false, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * RAFT__QUEUE_PUSH
 */

static MunitResult test_push_one(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct __item items[1];

    (void)params;

    __push(f, items);

    __assert_head(f, 1);

    return MUNIT_OK;
}

static MunitResult test_push_two(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct __item items[2];
    int i;

    (void)params;

    __push(f, items);

    for (i = 0; i < 2; i++) {
        __assert_head(f, i + 1);

        __remove(items, i);
    }

    __assert_is_empty(f);

    return MUNIT_OK;
}

static MunitTest push_tests[] = {
    {"/one", test_push_one, setup, tear_down, 0, NULL},
    {"/two", test_push_two, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * RAFT__QUEUE_REMOVE
 */

static MunitResult test_remove_first(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct __item items[3];

    (void)params;

    __push(f, items);

    __remove(items, 0);

    __assert_head(f, 2);

    return MUNIT_OK;
}

static MunitResult test_remove_second(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct __item items[3];

    (void)params;

    __push(f, items);

    __remove(items, 1);

    __assert_head(f, 1);

    return MUNIT_OK;
}

static MunitResult test_remove_third(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct __item items[3];

    (void)params;

    __push(f, items);

    __remove(items, 2);

    __assert_head(f, 1);

    return MUNIT_OK;
}

static MunitTest remove_tests[] = {
    {"/first", test_remove_first, setup, tear_down, 0, NULL},
    {"/second", test_remove_second, setup, tear_down, 0, NULL},
    {"/third", test_remove_third, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * RAFT__QUEUE_TAIL
 */

static MunitResult test_tail_one(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct __item items[1];

    (void)params;

    __push(f, items);

    __assert_tail(f, 1);

    return MUNIT_OK;
}

static MunitResult test_tail_two(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct __item items[2];

    (void)params;

    __push(f, items);

    __assert_tail(f, 2);

    return MUNIT_OK;
}

static MunitResult test_tail_three(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct __item items[3];

    (void)params;

    __push(f, items);

    __assert_tail(f, 3);

    return MUNIT_OK;
}

static MunitTest tail_tests[] = {
    {"/one", test_tail_one, setup, tear_down, 0, NULL},
    {"/two", test_tail_two, setup, tear_down, 0, NULL},
    {"/three", test_tail_three, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Suite
 */
MunitSuite raft_queue_suites[] = {
    {"/is-empty", is_empty_tests, NULL, 1, 0},
    {"/push", push_tests, NULL, 1, 0},
    {"/remove", remove_tests, NULL, 1, 0},
    {"/tail", tail_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
