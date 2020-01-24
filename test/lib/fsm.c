#include "fsm.h"

#include "../../src/byte.h"
#include "munit.h"

/* In-memory implementation of the raft_fsm interface. */
struct test_fsm
{
    int x;
    int y;
};

/* Command codes */
enum { SET_X = 1, SET_Y, ADD_X, ADD_Y };

static int fsmApply(struct raft_fsm *fsm,
                    const struct raft_buffer *buf,
                    void **result)
{
    struct test_fsm *t = fsm->data;
    unsigned command;
    int value;

    if (buf->len != 16) {
        return -1;
    }

    command = *(uint64_t *)buf->base;
    value = *((int64_t *)buf->base + 1);

    switch (command) {
        case SET_X:
            t->x = value;
            break;
        case SET_Y:
            t->y = value;
            break;
        case ADD_X:
            t->x += value;
            break;
        case ADD_Y:
            t->y += value;
            break;
        default:
            return -1;
    }

    *result = NULL;

    return 0;
}

static int fsmRestore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
    struct test_fsm *t = fsm->data;
    const void *cursor = buf->base;

    munit_assert_int(buf->len, ==, sizeof(uint64_t) * 2);

    t->x = byteGet64(&cursor);
    t->y = byteGet64(&cursor);

    raft_free(buf->base);

    return 0;
}

static int fsmEncodeSnapshot(int x,
                             int y,
                             struct raft_buffer *bufs[],
                             unsigned *n_bufs)
{
    struct raft_buffer *buf;
    void *cursor;

    *n_bufs = 1;

    *bufs = raft_malloc(sizeof **bufs);
    if (*bufs == NULL) {
        return RAFT_NOMEM;
    }

    buf = &(*bufs)[0];
    buf->len = sizeof(uint64_t) * 2;
    buf->base = raft_malloc(buf->len);
    if (buf->base == NULL) {
        return RAFT_NOMEM;
    }

    cursor = (*bufs)[0].base;

    bytePut64(&cursor, x);
    bytePut64(&cursor, y);

    return 0;
}

static int fsmSnapshot(struct raft_fsm *fsm,
                       struct raft_buffer *bufs[],
                       unsigned *n_bufs)
{
    struct test_fsm *t = fsm->data;
    return fsmEncodeSnapshot(t->x, t->y, bufs, n_bufs);
}

void test_fsm_init(struct raft_fsm *fsm)
{
    struct test_fsm *t = munit_malloc(sizeof *fsm);

    t->x = 0;
    t->y = 0;

    fsm->version = 1;
    fsm->data = t;
    fsm->apply = fsmApply;
    fsm->snapshot = fsmSnapshot;
    fsm->restore = fsmRestore;
}

void test_fsm_close(struct raft_fsm *fsm)
{
    struct test_fsm *t = fsm->data;
    free(t);
}

void test_fsm_encode_set_x(const int value, struct raft_buffer *buf)
{
    void *cursor;

    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    cursor = buf->base;
    bytePut64(&cursor, SET_X);
    bytePut64(&cursor, value);
}

void test_fsm_encode_add_x(const int value, struct raft_buffer *buf)
{
    void *cursor;

    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    cursor = buf->base;
    bytePut64(&cursor, ADD_X);
    bytePut64(&cursor, value);
}

void test_fsm_encode_set_y(const int value, struct raft_buffer *buf)
{
    void *cursor;

    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    cursor = buf->base;
    bytePut64(&cursor, SET_Y);
    bytePut64(&cursor, value);
}

void test_fsm_encode_add_y(const int value, struct raft_buffer *buf)
{
    void *cursor;

    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    cursor = buf->base;
    bytePut64(&cursor, ADD_Y);
    bytePut64(&cursor, value);
}

void test_fsm_encode_snapshot(int x,
                              int y,
                              struct raft_buffer *bufs[],
                              unsigned *n_bufs)
{
    int rc;
    rc = fsmEncodeSnapshot(x, y, bufs, n_bufs);
    munit_assert_int(rc, ==, 0);
}

int test_fsm_get_x(struct raft_fsm *fsm)
{
    struct test_fsm *t = fsm->data;
    return t->x;
}

int test_fsm_get_y(struct raft_fsm *fsm)
{
    struct test_fsm *t = fsm->data;
    return t->y;
}

void test_fsm_set_x(struct raft_fsm *fsm, int value)
{
    struct test_fsm *t = fsm->data;
    t->x = value;
}

void test_fsm_set_y(struct raft_fsm *fsm, int value)
{
    struct test_fsm *t = fsm->data;
    t->y = value;
}
