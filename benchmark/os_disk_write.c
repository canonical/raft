#include <argp.h>
#include <assert.h>
#include <fcntl.h>
#include <liburing.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

static char doc[] = "Benchmark operating system disk write performance";

#define PWRITEV2 0
#define URING 1

static const char *engines[] =
    {[PWRITEV2] = "pwritev2", [URING] = "uring", NULL};

/* Order of fields: {NAME, KEY, ARG, FLAGS, DOC, GROUP}.*/
static struct argp_option options[] = {
    {"dir", 'd', "DIR", 0, "Directory to use for temp files (default /tmp)", 0},
    {"buf", 'b', "BUF", 0, "Write buffer size (default st_blksize)", 0},
    {"writes", 'n', "N", 0, "Number of writes to perform (default 1024)", 0},
    {"engine", 'e', "ENGINE", 0, "I/O engine to use (default pwritev2)", 0},
    {"direct", 'D', NULL, 0, "Use direct I/O", 0},
    {0}};

struct arguments
{
    char *dir;
    int buf;
    int n;
    int engine;
    bool direct;
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
        case 'D':
            arguments->direct = true;
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

static int writeWithPwriteV2(int fd, struct iovec *iov, int i)
{
    int rv;
    rv = pwritev2(fd, iov, 1, i * iov->iov_len, RWF_DSYNC | RWF_HIPRI);
    if (rv == -1) {
        perror("pwritev2");
        return -1;
    }
    assert(rv == (int)iov->iov_len);
    return 0;
}

static struct io_uring uring;

static void initUring(int fd, struct iovec *iov)
{
    int rv;
    rv = io_uring_queue_init(4, &uring, 0);
    assert(rv == 0);
    rv = io_uring_register_files(&uring, &fd, 1);
    assert(rv == 0);

    rv = io_uring_register_buffers(&uring, iov, 1);
    assert(rv == 0);
}

static int writeWithUring(int fd, struct iovec *iov, int i)
{
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    int rv;

    if (i == 0) {
        initUring(fd, iov);
    }

    sqe = io_uring_get_sqe(&uring);
    io_uring_prep_write_fixed(sqe, 0, iov->iov_base, iov->iov_len,
                              i * iov->iov_len, 0);
    io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
    sqe->rw_flags = RWF_DSYNC;

    rv = io_uring_submit(&uring);
    assert(rv == 1);

    io_uring_wait_cqe(&uring, &cqe);
    if (cqe->res < 0) {
        printf("sqe failed: %s\n", strerror(-cqe->res));
        return -1;
    }
    assert(cqe->res == (int)iov->iov_len);

    io_uring_cqe_seen(&uring, cqe);

    return 0;
}

int benchmarkWritePerformance(const char *dir,
                              int buf,
                              int n,
                              int engine,
                              bool direct)
{
    char *path;
    int fd;
    struct iovec iov;
    struct timespec start;
    int i;
    int rv;

    path = makeTempFileTemplate(dir);
    fd = mkstemp(path);
    if (fd == -1) {
        printf("mstemp '%s': %s\n", path, strerror(errno));
        return -1;
    }

    rv = allocateTempFile(fd, dir, n, buf);
    if (rv != 0) {
        unlink(path);
        return -1;
    }

    allocBuffer(&iov, buf);

    if (direct) {
        setDirectIO(fd);
    }

    timeNow(&start);
    for (i = 0; i < n; i++) {
        switch (engine) {
            case PWRITEV2:
                rv = writeWithPwriteV2(fd, &iov, i);
                break;
            case URING:
                rv = writeWithUring(fd, &iov, i);
                break;
            default:
                assert(0);
        }
    }

    if (rv != 0) {
        return -1;
    }

    printf("%-8s:  %8s writes of %4d bytes take %4d microsecs on average\n",
           engines[engine], direct ? "direct" : "buffered", buf,
           timeSince(&start) / n);

    close(fd);
    unlink(path);
    free(path);

    return 0;
}

int main(int argc, char *argv[])
{
    struct argp argp = {options, argumentsParse, NULL, doc, 0, 0, 0};
    struct arguments arguments;
    struct stat st;
    int rv;

    arguments.dir = "/tmp";
    arguments.buf = 0;
    arguments.n = 1024;
    arguments.engine = PWRITEV2;
    arguments.direct = false;

    argp_parse(&argp, argc, argv, 0, 0, &arguments);

    rv = stat(arguments.dir, &st);
    if (rv != 0) {
        printf("stat '%s': %s\n", arguments.dir, strerror(errno));
        return -1;
    }

    if (arguments.buf == 0) {
        arguments.buf = st.st_blksize;
    }

    rv = benchmarkWritePerformance(arguments.dir, arguments.buf, arguments.n,
                                   arguments.engine, arguments.direct);
    if (rv != 0) {
        return -1;
    }

    return 0;
}
