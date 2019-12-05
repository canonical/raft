#include "../../src/queue.h"
#include "../lib/runner.h"

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

SUITE(QUEUE_IS_EMPTY)

TEST(QUEUE_IS_EMPTY, yes, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    ASSERT_EMPTY;
    return MUNIT_OK;
}

TEST(QUEUE_IS_EMPTY, no, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct item items[1];
    PUSH(items);
    ASSERT_NOT_EMPTY;
    return MUNIT_OK;
}

/******************************************************************************
 *
 * QUEUE_PUSH
 *
 *****************************************************************************/

TEST_SUITE(QUEUE_PUSH)

TEST(QUEUE_PUSH, one, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct item items[1];
    (void)params;
    PUSH(items);
    ASSERT_HEAD(1);
    return MUNIT_OK;
}

TEST(QUEUE_PUSH, two, setup, tear_down, 0, NULL)
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

TEST_SUITE(QUEUE_REMOVE)

TEST(QUEUE_REMOVE, first, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct item items[3];
    (void)params;
    PUSH(items);
    REMOVE(items, 0);
    ASSERT_HEAD(2);
    return MUNIT_OK;
}

TEST(QUEUE_REMOVE, second, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct item items[3];
    (void)params;
    PUSH(items);
    REMOVE(items, 1);
    ASSERT_HEAD(1);
    return MUNIT_OK;
}

TEST(QUEUE_REMOVE, success, setup, tear_down, 0, NULL)
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

SUITE(QUEUE_TAIL)

TEST(QUEUE_TAIL, one, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct item items[1];
    PUSH(items);
    ASSERT_TAIL(1);
    return MUNIT_OK;
}

TEST(QUEUE_TAIL, two, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct item items[2];
    PUSH(items);
    ASSERT_TAIL(2);
    return MUNIT_OK;
}

TEST(QUEUE_TAIL, three, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct item items[3];
    PUSH(items);
    ASSERT_TAIL(3);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * QUEUE_FOREACH
 *
 *****************************************************************************/

SUITE(QUEUE_FOREACH)

/* Loop through a queue of zero items. */
TEST(QUEUE_FOREACH, zero, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    queue *head;
    int count = 0;
    QUEUE_FOREACH(head, &f->queue) { count++; }
    munit_assert_int(count, ==, 0);
    return MUNIT_OK;
}

/* Loop through a queue of one item. */
TEST(QUEUE_FOREACH, one, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct item items[1];
    queue *head;
    int count = 0;
    PUSH(items);
    QUEUE_FOREACH(head, &f->queue) { count++; }
    munit_assert_int(count, ==, 1);
    return MUNIT_OK;
}

/* Loop through a queue of two items. The order of the loop is from the head to
 * the tail. */
TEST(QUEUE_FOREACH, two, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct item items[2];
    queue *head;
    int values[2] = {0, 0};
    int i = 0;
    PUSH(items);
    QUEUE_FOREACH(head, &f->queue)
    {
        struct item *item;
        item = QUEUE_DATA(head, struct item, queue);
        values[i] = item->value;
        i++;
    }
    munit_assert_int(values[0], ==, 1);
    munit_assert_int(values[1], ==, 2);
    return MUNIT_OK;
}
