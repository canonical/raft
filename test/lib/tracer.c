#include <stdio.h>
#include <string.h>
#include <time.h>

#include "munit.h"
#include "tracer.h"

void walkCb(void *data, raft_time time, unsigned type, const char *message)
{
    char buf[1024];
    char *cursor = buf;
    time_t secs = time / 1000;
    unsigned msecs = time % 1000;
    struct tm tm;
    (void)data;
    (void)type;

    memset(buf, 0, sizeof buf);
    gmtime_r(&secs, &tm);

    strftime(cursor, 10, "%H:%M:%S", &tm);
    cursor = buf + strlen(buf);

    sprintf(cursor, ".%03d: %s ", msecs, message);
    munit_log(MUNIT_LOG_INFO, message);
}

void test_tracer_on_exit(int status, void *arg)
{
    struct raft_tracer *t = arg;
    if (status != 0) {
        raft_tracer_walk(t, walkCb, NULL);
    }
}
