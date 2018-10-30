#include <stdarg.h>
#include <stdio.h>

#include "logger.h"
#include "munit.h"

struct test_logger
{
    unsigned id;
    void *data;
    int (*time)(void *data);
};

static void test_logger__emit(void *data,
                              struct raft_context *ctx,
                              int level,
                              const char *format,
                              ...)
{
    struct test_logger *t = data;
    char buf[1024];
    const char *level_name;
    va_list args;
    int i;

    (void)data;

    switch (level) {
        case RAFT_DEBUG:
            level_name = "DEBUG";
            break;
        case RAFT_INFO:
            level_name = "INFO ";
            break;
        case RAFT_WARN:
            level_name = "WARN ";
            break;
        case RAFT_ERROR:
            level_name = "ERROR";
            break;
    };

    buf[0] = 0;

    if (t->time != NULL) {
        sprintf(buf, "%6d: ", t->time(t->data));
    }

    sprintf(buf + strlen(buf), "%2d -> [%s] ", t->id, level_name);

    va_start(args, format);
    vsnprintf(buf + strlen(buf), 1024 - strlen(buf), format, args);
    va_end(args);

    snprintf(buf + strlen(buf), 1024 - strlen(buf), " ");

    for (i = strlen(buf); i < 85; i++) {
        buf[i] = ' ';
    }

    raft_context_format(buf + 85, 1024 - strlen(buf), ctx);

    munit_log(MUNIT_LOG_INFO, buf);
}

void test_logger_setup(const MunitParameter params[],
                       struct raft_logger *l,
                       uint64_t id)
{
    struct test_logger *t;

    (void)params;

    t = raft_malloc(sizeof *t);
    t->id = id;
    t->data = NULL;
    t->time = NULL;

    l->data = t;
    l->emit = test_logger__emit;
}

void test_logger_tear_down(struct raft_logger *l)
{
    raft_free(l->data);
}

void test_logger_time(struct raft_logger *l,
                      void *data,
                      int (*time)(void *data))
{
    struct test_logger *t = l->data;

    t->data = data;
    t->time = time;
}
