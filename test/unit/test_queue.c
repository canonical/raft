#include "../../src/queue.h"

#include "../lib/runner.h"

TEST_MODULE(queue)

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    void *queue[2];
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)params;
    (void)user_data;
    QUEUE_INIT(&f->queue);
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    free(f);
}

struct item
{
    int value;
    void *queue[2];
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Initialize and push the given items to the fixture's queue. Each item will
 * have a value equal to its index plus one. */
#define PUSH(ITEMS)                              \
    {                                            \
        int n_ = sizeof ITEMS / sizeof ITEMS[0]; \
        int i_;                                  \
        for (i_ = 0; i_ < n_; i_++) {            \
            struct item *item = &items[i_];      \
            item->value = i_ + 1;                \
            QUEUE_PUSH(&f->queue, &item->queue); \
        }                                        \
    }

/* Remove the i'th item among the given ones. */
#define REMOVE(ITEMS, I) QUEUE_REMOVE(&ITEMS[I].queue)

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert that the item at the head of the fixture's queue has the given
 * value. */
#define ASSERT_HEAD(VALUE)                             \
    {                                                  \
        queue *head_ = QUEUE_HEAD(&f->queue);          \
        struct item *item_;                            \
        item_ = QUEUE_DATA(head_, struct item, queue); \
        munit_assert_int(item_->value, ==, VALUE);     \
    }

/* Assert that the item at the tail of the queue has the given value. */
#define ASSERT_TAIL(VALUE)                             \
    {                                                  \
        queue *tail_ = QUEUE_TAIL(&f->queue);          \
        struct item *item_;                            \
        item_ = QUEUE_DATA(tail_, struct item, queue); \
        munit_assert_int(item_->value, ==, VALUE);     \
    }

/* Assert that the fixture's queue is empty. */
#define ASSERT_EMPTY munit_assert_true(QUEUE_IS_EMPTY(&f->queue))

/* Assert that the fixture's queue is not empty. */
#define ASSERT_NOT_EMPTY munit_assert_false(QUEUE_IS_EMPTY(&f->queue))

/******************************************************************************
 *
 * QUEUE_IS_EMPTY
 *
 *****************************************************************************/

TEST_SUITE(is_empty)

TEST_SETUP(is_empty, setup)
TEST_TEAR_DOWN(is_empty, tear_down)

TEST_CASE(is_empty, yes, NULL)
{
    struct fixture *f = data;
    (void)params;
    ASSERT_EMPTY;
    return MUNIT_OK;
}

TEST_CASE(is_empty, no, NULL)
{
    struct fixture *f = data;
    struct item items[1];
    (void)params;
    PUSH(items);
    ASSERT_NOT_EMPTY;
    return MUNIT_OK;
}

/******************************************************************************
 *
 * QUEUE_PUSH
 *
 *****************************************************************************/

TEST_SUITE(push)

TEST_SETUP(push, setup)
TEST_TEAR_DOWN(push, tear_down)

TEST_CASE(push, one, NULL)
{
    struct fixture *f = data;
    struct item items[1];
    (void)params;
    PUSH(items);
    ASSERT_HEAD(1);
    return MUNIT_OK;
}

TEST_CASE(push, two, NULL)
{
    struct fixture *f = data;
    struct item items[2];
    int i;
    (void)params;
    PUSH(items);
    for (i = 0; i < 2; i++) {
        ASSERT_HEAD(i + 1);
        REMOVE(items, i);
    }
    ASSERT_EMPTY;
    return MUNIT_OK;
}

/******************************************************************************
 *
 * QUEUE_REMOVE
 *
 *****************************************************************************/

TEST_SUITE(remove)

TEST_SETUP(remove, setup)
TEST_TEAR_DOWN(remove, tear_down)

TEST_CASE(remove, first, NULL)
{
    struct fixture *f = data;
    struct item items[3];
    (void)params;
    PUSH(items);
    REMOVE(items, 0);
    ASSERT_HEAD(2);
    return MUNIT_OK;
}

TEST_CASE(remove, second, NULL)
{
    struct fixture *f = data;
    struct item items[3];
    (void)params;
    PUSH(items);
    REMOVE(items, 1);
    ASSERT_HEAD(1);
    return MUNIT_OK;
}

TEST_CASE(remove, success, NULL)
{
    struct fixture *f = data;
    struct item items[3];
    (void)params;
    PUSH(items);
    REMOVE(items, 2);
    ASSERT_HEAD(1);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * QUEUE_TAIL
 *
 *****************************************************************************/

TEST_SUITE(tail)

TEST_SETUP(tail, setup)
TEST_TEAR_DOWN(tail, tear_down)

TEST_CASE(tail, one, NULL)
{
    struct fixture *f = data;
    struct item items[1];
    (void)params;
    PUSH(items);
    ASSERT_TAIL(1);
    return MUNIT_OK;
}

TEST_CASE(tail, two, NULL)
{
    struct fixture *f = data;
    struct item items[2];
    (void)params;
    PUSH(items);
    ASSERT_TAIL(2);
    return MUNIT_OK;
}

TEST_CASE(tail, three, NULL)
{
    struct fixture *f = data;
    struct item items[3];
    (void)params;
    PUSH(items);
    ASSERT_TAIL(3);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * QUEUE_FOREACH
 *
 *****************************************************************************/

TEST_SUITE(foreach)

TEST_SETUP(foreach, setup)
TEST_TEAR_DOWN(foreach, tear_down)

/* Loop through a queue of zero items. */
TEST_CASE(foreach, zero, NULL)
{
    struct fixture *f = data;
    queue *head;
    int count = 0;
    (void)params;
    QUEUE_FOREACH(head, &f->queue) { count++; }
    munit_assert_int(count, ==, 0);
    return MUNIT_OK;
}

/* Loop through a queue of one item. */
TEST_CASE(foreach, one, NULL)
{
    struct fixture *f = data;
    struct item items[1];
    queue *head;
    int count = 0;
    (void)params;
    PUSH(items);
    QUEUE_FOREACH(head, &f->queue) { count++; }
    munit_assert_int(count, ==, 1);
    return MUNIT_OK;
}

/* Loop through a queue of two items. */
TEST_CASE(foreach, two, NULL)
{
    struct fixture *f = data;
    struct item items[2];
    queue *head;
    int count = 0;
    (void)params;
    PUSH(items);
    QUEUE_FOREACH(head, &f->queue) { count++; }
    munit_assert_int(count, ==, 2);
    return MUNIT_OK;
}
