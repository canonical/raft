#include <time.h>
#include <string.h>
#include <stdio.h>

#include "tracer.h"
#include "munit.h"

void test_tracer_walk_cb(void *data,
                         raft_time time,
                         unsigned type,
                         const char *message) {
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
