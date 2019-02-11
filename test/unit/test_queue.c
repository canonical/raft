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
 * RAFT__QUEUE_IS_EMPTY
 */

static MunitResult test_is_empty_true(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    munit_assert_true(RAFT__QUEUE_IS_EMPTY(&f->queue));

    return MUNIT_OK;
}

static MunitResult test_is_empty_false(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    struct __item item;

    (void)params;

    RAFT__QUEUE_INIT(&item.queue);
    RAFT__QUEUE_PUSH(&item.queue, &f->queue);

    munit_assert_false(RAFT__QUEUE_IS_EMPTY(&f->queue));

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
    struct __item item;
    raft__queue *head;

    (void)params;

    item.value = 1;

    RAFT__QUEUE_INIT(&item.queue);
    RAFT__QUEUE_PUSH(&f->queue, &item.queue);

    head = RAFT__QUEUE_HEAD(&f->queue);
    item = *RAFT__QUEUE_DATA(head, struct __item, queue);

    munit_assert_int(item.value, ==, 1);

    return MUNIT_OK;
}

static MunitResult test_push_two(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct __item item1;
    struct __item item2;
    int value;

    (void)params;

    item1.value = 1;
    item2.value = 2;

    RAFT__QUEUE_INIT(&item1.queue);
    RAFT__QUEUE_INIT(&item2.queue);

    RAFT__QUEUE_PUSH(&f->queue, &item1.queue);
    RAFT__QUEUE_PUSH(&f->queue, &item2.queue);

    for (value = 1; value <= 2; value++) {
        struct __item *item;
	raft__queue *head;

        head = RAFT__QUEUE_HEAD(&f->queue);

        item = RAFT__QUEUE_DATA(head, struct __item, queue);
        munit_assert_int(item->value, ==, value);

        RAFT__QUEUE_POP(head);
    }

    munit_assert_true(RAFT__QUEUE_IS_EMPTY(&f->queue));

    return MUNIT_OK;
}

static MunitTest push_tests[] = {
    {"/one", test_push_one, setup, tear_down, 0, NULL},
    {"/two", test_push_two, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Suite
 */
MunitSuite raft_queue_suites[] = {
    {"/is-empty", is_empty_tests, NULL, 1, 0},
    {"/push", push_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
