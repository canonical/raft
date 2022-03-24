#include "fsm.h"

#include "../../src/byte.h"
#include "munit.h"

/* In-memory implementation of the raft_fsm interface. */
struct fsm
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
    struct fsm *f = fsm->data;
    const void *cursor = buf->base;
    unsigned command;
    int value;

    if (buf->len != 16) {
        return -1;
    }

    command = (unsigned)byteGet64(&cursor);
    value = (int)byteGet64(&cursor);

    switch (command) {
        case SET_X:
            f->x = value;
            break;
        case SET_Y:
            f->y = value;
            break;
        case ADD_X:
            f->x += value;
            break;
        case ADD_Y:
            f->y += value;
            break;
        default:
            return -1;
    }

    *result = NULL;

    return 0;
}

static int fsmRestore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
    struct fsm *f = fsm->data;
    const void *cursor = buf->base;

    munit_assert_int(buf->len, ==, sizeof(uint64_t) * 2);

    f->x = byteGet64(&cursor);
    f->y = byteGet64(&cursor);

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
    struct fsm *f = fsm->data;
    return fsmEncodeSnapshot(f->x, f->y, bufs, n_bufs);
}

void FsmInit(struct raft_fsm *fsm)
{
    struct fsm *f = munit_malloc(sizeof *fsm);

    f->x = 0;
    f->y = 0;

    fsm->version = 1;
    fsm->data = f;
    fsm->apply = fsmApply;
    fsm->snapshot = fsmSnapshot;
    fsm->restore = fsmRestore;
}

void FsmClose(struct raft_fsm *fsm)
{
    struct fsm *f = fsm->data;
    free(f);
}

void FsmEncodeSetX(const int value, struct raft_buffer *buf)
{
    void *cursor;

    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    cursor = buf->base;
    bytePut64(&cursor, SET_X);
    bytePut64(&cursor, value);
}

void FsmEncodeAddX(const int value, struct raft_buffer *buf)
{
    void *cursor;

    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    cursor = buf->base;
    bytePut64(&cursor, ADD_X);
    bytePut64(&cursor, value);
}

void FsmEncodeSetY(const int value, struct raft_buffer *buf)
{
    void *cursor;

    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    cursor = buf->base;
    bytePut64(&cursor, SET_Y);
    bytePut64(&cursor, value);
}

void FsmEncodeAddY(const int value, struct raft_buffer *buf)
{
    void *cursor;

    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    cursor = buf->base;
    bytePut64(&cursor, ADD_Y);
    bytePut64(&cursor, value);
}

void FsmEncodeSnapshot(int x,
                       int y,
                       struct raft_buffer *bufs[],
                       unsigned *n_bufs)
{
    int rc;
    rc = fsmEncodeSnapshot(x, y, bufs, n_bufs);
    munit_assert_int(rc, ==, 0);
}

int FsmGetX(struct raft_fsm *fsm)
{
    struct fsm *f = fsm->data;
    return f->x;
}

int FsmGetY(struct raft_fsm *fsm)
{
    struct fsm *f = fsm->data;
    return f->y;
}
