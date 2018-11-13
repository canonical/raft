#include "fsm.h"

/**
 * In-memory implementation of the raft_fsm interface.
 */
struct test_fsm
{
    int x;
    int y;
};

static int test_fsm__apply(struct raft_fsm *fsm, const struct raft_buffer *buf)
{
    struct test_fsm *t = fsm->data;
    unsigned command;
    int value;

    if (buf->len != 16) {
        return -1;
    }

    command = *(uint64_t *)buf->base;
    value = *(int64_t *)(buf->base + 8);

    switch (command) {
        case 1:
            t->x = value;
            break;
        case 2:
            t->x = value;
            break;
        default:
            return -1;
    }

    return 0;
}

void test_fsm_setup(const MunitParameter params[], struct raft_fsm *fsm)
{
    struct test_fsm *t = munit_malloc(sizeof *fsm);

    (void)params;

    t->x = 0;
    t->y = 0;

    fsm->version = 1;
    fsm->data = t;
    fsm->apply = test_fsm__apply;
}

void test_fsm_tear_down(struct raft_fsm *fsm)
{
    struct test_fsm *t = fsm->data;
    free(t);
}

void test_fsm_encode_set_x(const int value, struct raft_buffer *buf)
{
    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    *(uint64_t *)buf->base = 1;
    *(int64_t *)(buf->base + 8) = value;
}

void test_fsm_encode_set_y(const int value, struct raft_buffer *buf)
{
    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    *(uint64_t *)buf->base = 2;
    *(int64_t *)(buf->base + 8) = value;
}
