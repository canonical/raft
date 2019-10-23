#include <argp.h>
#include <assert.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

static char doc[] = "Benchmark operating system disk write performance";

#define SYNC_PWRITEV2_BUFFERED 0
#define SYNC_PWRITEV2_DIRECT 1
#define ASYNC_URING_BUFFERED 2
#define ASYNC_URING_DIRECT 3

static const char *engines[] = {
    [SYNC_PWRITEV2_BUFFERED] = "sync-pwritev2-buffered",
    [SYNC_PWRITEV2_DIRECT] = "sync-pwritev2-direct",
    [ASYNC_URING_BUFFERED] = "async-uring-buffered",
    [ASYNC_URING_DIRECT] = "async-uring-direct",
    NULL};

/* Order of fields: {NAME, KEY, ARG, FLAGS, DOC, GROUP}.*/
static struct argp_option options[] = {
    {"dir", 'd', "DIR", 0, "Directory to use for temp files (default /tmp)", 0},
    {"buf", 'b', "BUF", 0, "Write buffer size (default st_blksize)", 0},
    {"writes", 'n', "N", 0, "Number of writes to perform (default 1024)", 0},
    {"engine", 'e', "ENGINE", 0,
     "I/O engine to use (default sync-buffered-pwritev2)", 0},
    {0}};

struct arguments
{
    int engine;
    int n;
    char *dir;
    int buf;
};

static int engineCode(const char *engine)
{
    int i = 0;
    while (engines[i] != NULL) {
        if (strcmp(engines[i], engine) == 0) {
            return i;
        }
        i++;
    }
    return -1;
}

static error_t argumentsParse(int key, char *arg, struct argp_state *state)
{
    struct arguments *arguments = state->input;
    switch (key) {
        case 'd':
            arguments->dir = arg;
            break;
        case 'b':
            arguments->buf = atoi(arg);
            break;
        case 'n':
            arguments->n = atoi(arg);
            break;
        case 'e':
            arguments->engine = engineCode(arg);
            if (arguments->engine == -1) {
                return ARGP_ERR_UNKNOWN;
            }
            break;
        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

static char *makeTempFileTemplate(const char *dir)
{
    char *path;
    path = malloc(strlen(dir) + strlen("/bench-XXXXXX") + 1);
    assert(path != NULL);
    sprintf(path, "%s/bench-XXXXXX", dir);
    return path;
}

static int allocateTempFile(int fd, const char *dir, int n_writes, int buf)
{
    int dirfd;
    int rv;

    rv = posix_fallocate(fd, 0, n_writes * buf);
    if (rv != 0) {
        errno = rv;
        printf("posiz_fallocate: %s\n", strerror(errno));
        return -1;
    }

    /* Sync the file and its directory. */
    rv = fsync(fd);
    assert(rv == 0);

    dirfd = open(dir, O_RDONLY | O_DIRECTORY);
    assert(dirfd != -1);

    rv = fsync(dirfd);
    assert(rv == 0);

    close(dirfd);

    return 0;
}

/* Save current time in 'time'. */
static void timeNow(struct timespec *time)
{
    int rv;
    rv = clock_gettime(CLOCK_MONOTONIC, time);
    assert(rv == 0);
}

/* Calculate how much time has elapsed since 'start', in microseconds. */
static int timeSince(struct timespec *start)
{
    struct timespec now;
    long nsecs;
    timeNow(&now);
    if (start->tv_sec == now.tv_sec) {
        nsecs = now.tv_nsec - start->tv_nsec;
    } else {
        nsecs = (now.tv_sec - start->tv_sec) * 1000 * 1000 * 1000 -
                start->tv_nsec + now.tv_nsec;
    }
    return nsecs / 1000;
}

/* Allocate a buffer of the given size. */
static void allocBuffer(struct iovec *iov, int size)
{
    iov->iov_len = size;
    iov->iov_base = aligned_alloc(iov->iov_len, iov->iov_len);
    assert(iov->iov_base != NULL);
}

static void setDirectIO(int fd)
{
    int flags; /* Current fcntl flags */
    int rv;
    flags = fcntl(fd, F_GETFL);
    rv = fcntl(fd, F_SETFL, flags | O_DIRECT);
    assert(rv == 0);
}

int main(int argc, char *argv[])
{
    struct argp argp = {options, argumentsParse, NULL, doc, 0, 0, 0};
    struct arguments arguments;
    struct stat st;
    char *path;
    int fd;
    struct iovec iov;
    struct timespec start;
    int i;
    int rv;

    arguments.dir = "/tmp";
    arguments.buf = 0;
    arguments.n = 1024;
    arguments.engine = SYNC_PWRITEV2_BUFFERED;

    argp_parse(&argp, argc, argv, 0, 0, &arguments);

    rv = stat(arguments.dir, &st);
    if (rv != 0) {
        printf("stat '%s': %s\n", arguments.dir, strerror(errno));
        return -1;
    }

    if (arguments.buf == 0) {
        arguments.buf = st.st_blksize;
    }

    path = makeTempFileTemplate(arguments.dir);
    fd = mkstemp(path);
    if (fd == -1) {
        printf("mstemp '%s': %s\n", path, strerror(errno));
        return -1;
    }

    rv = allocateTempFile(fd, arguments.dir, arguments.n, arguments.buf);
    if (rv != 0) {
        unlink(path);
        return -1;
    }

    allocBuffer(&iov, arguments.buf);

    timeNow(&start);
    for (i = 0; i < arguments.n; i++) {
        switch (arguments.engine) {
            case SYNC_PWRITEV2_BUFFERED:
                rv = pwritev2(fd, &iov, 1, 0, RWF_DSYNC | RWF_HIPRI);
                assert(rv == (int)iov.iov_len);
                break;
            case SYNC_PWRITEV2_DIRECT:
                setDirectIO(fd);
                rv = pwritev2(fd, &iov, 1, 0, RWF_DSYNC | RWF_HIPRI);
                assert(rv == (int)iov.iov_len);
                break;
            default:
                assert(0);
        }
    }

    printf("%-22s: writing %4d bytes takes %4d microsecs on average\n",
           engines[arguments.engine], arguments.buf,
           timeSince(&start) / arguments.n);

    close(fd);
    unlink(path);
    free(path);

    return 0;
}
