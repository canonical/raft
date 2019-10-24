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

/* Minimum buffer size to benchmark. */
#define MIN_BUF_SIZE 64

/* Maximum buffer size to benchmark. */
#define MAX_BUF_SIZE 4096

/* Minimum physical block size for direct I/O that we expect to detect. */
#define MIN_BLOCK_SIZE 512

/* Maximum physical block size for direct I/O that we expect to detect. */
#define MAX_BLOCK_SIZE 4096

/* Engines */
#define PWRITEV2 0
#define URING 1

/* Modes */
#define BUFFERED 0
#define DIRECT 1

static const char *engines[] =
    {[PWRITEV2] = "pwritev2", [URING] = "uring", NULL};

static const char *modes[] =
    {[BUFFERED] = "buffered", [DIRECT] = "direct", NULL};

/* Order of fields: {NAME, KEY, ARG, FLAGS, DOC, GROUP}.*/
static struct argp_option options[] = {
    {"dir", 'd', "DIR", 0, "Directory to use for temp files (default /tmp)", 0},
    {"buf", 'b', "BUF", 0, "Write buffer size (default st_blksize)", 0},
    {"writes", 'n', "N", 0, "Number of writes to perform (default 1024)", 0},
    {"engine", 'e', "ENGINE", 0, "I/O engine to use (default all)", 0},
    {"mode", 'm', "MODE", 0, "Use 'buffered' or 'direct' I/O", 0},
    {0}};

struct arguments
{
    char *dir;
    int buf;
    int n;
    int engine;
    int mode;
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

static int modeCode(const char *mode)
{
    int i = 0;
    while (modes[i] != NULL) {
        if (strcmp(modes[i], mode) == 0) {
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
        case 'm':
            arguments->mode = modeCode(arg);
            if (arguments->mode == -1) {
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

static int createTempFile(const char *dir, int size, char **path, int *fd)
{
    int dirfd;
    int rv;

    *path = makeTempFileTemplate(dir);
    *fd = mkstemp(*path);
    if (*fd == -1) {
        printf("mstemp '%s': %s\n", *path, strerror(errno));
        return -1;
    }

    rv = posix_fallocate(*fd, 0, size);
    if (rv != 0) {
        errno = rv;
        printf("posiz_fallocate: %s\n", strerror(errno));
        return -1;
    }

    /* Sync the file and its directory. */
    rv = fsync(*fd);
    assert(rv == 0);

    dirfd = open(dir, O_RDONLY | O_DIRECTORY);
    assert(dirfd != -1);

    rv = fsync(dirfd);
    assert(rv == 0);

    close(dirfd);

    return 0;
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

/* Detect all suitable block size we can use to write to the underlying device
 * using direct I/O. */
static int detectSuitableBlockSizesForDirectIO(const char *dir,
                                               int **block_size,
                                               int *n_block_size)
{
    char *path;
    int fd;
    int size;
    int rv;

    rv = createTempFile(dir, MAX_BLOCK_SIZE, &path, &fd);
    if (rv != 0) {
        unlink(path);
        return -1;
    }

    setDirectIO(fd);

    *block_size = NULL;
    *n_block_size = 0;

    for (size = MIN_BLOCK_SIZE; size <= MAX_BLOCK_SIZE; size *= 2) {
        struct iovec iov;
        allocBuffer(&iov, size);
        rv = pwritev2(fd, &iov, 1, 0, RWF_DSYNC | RWF_HIPRI);
        free(iov.iov_base);
        if (rv == -1) {
            assert(errno == EINVAL);
            continue; /* Try with a bigger buffer size */
        }
        assert(rv == size);
        *n_block_size += 1;
        *block_size = realloc(*block_size, *n_block_size * sizeof **block_size);
        assert(*block_size != NULL);
        (*block_size)[*n_block_size - 1] = size;
    }

    close(fd);
    unlink(path);

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

/* Benchmark the performance of a single disk write. */
int benchmarkWritePerformance(const char *dir,
                              int buf,
                              int n,
                              int engine,
                              int mode)
{
    char *path;
    int fd;
    struct iovec iov;
    struct timespec start;
    int i;
    int rv;

    rv = createTempFile(dir, n * buf, &path, &fd);
    if (rv != 0) {
        unlink(path);
        return -1;
    }

    allocBuffer(&iov, buf);

    if (mode == DIRECT) {
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
           engines[engine], modes[mode], buf, timeSince(&start) / n);

    if (engine == URING) {
        io_uring_queue_exit(&uring);
    }

    close(fd);
    unlink(path);
    free(path);

    return 0;
}

static bool isSuitableBufSizeForDirectIO(int *block_size,
                                         int n_block_size,
                                         int buf)
{
    int i;
    for (i = 0; i < n_block_size; i++) {
        if (block_size[i] == buf) {
            return true;
        }
    }
    return false;
}

int main(int argc, char *argv[])
{
    struct argp argp = {options, argumentsParse, NULL, doc, 0, 0, 0};
    struct arguments arguments;
    struct stat st;
    int engine;
    int mode;
    int buf;
    int *block_size;
    int n_block_size;
    int i;
    int rv;

    arguments.dir = "/tmp";
    arguments.buf = -1;
    arguments.n = 1024;
    arguments.engine = -1;
    arguments.mode = -1;

    argp_parse(&argp, argc, argv, 0, 0, &arguments);

    rv = stat(arguments.dir, &st);
    if (rv != 0) {
        printf("stat '%s': %s\n", arguments.dir, strerror(errno));
        return -1;
    }

    rv = detectSuitableBlockSizesForDirectIO(arguments.dir, &block_size,
                                             &n_block_size);
    if (rv != 0) {
        return rv;
    }

    if (arguments.buf != -1) {
        if (arguments.mode == -1 || arguments.mode == DIRECT) {
            if (!isSuitableBufSizeForDirectIO(block_size, n_block_size,
                                              arguments.buf)) {
                printf("suitable buffer sizes for direct I/O:");
                for (i = 0; i < n_block_size; i++) {
                    printf(" %4d", block_size[i]);
                }
                printf("\n");
                return -1;
            }
        }
    }

    for (engine = PWRITEV2; engine <= URING; engine++) {
        if (arguments.engine != engine && arguments.engine != -1) {
            continue;
        }
        for (mode = BUFFERED; mode <= DIRECT; mode++) {
            if (arguments.mode != mode && arguments.mode != -1) {
                continue;
            }
            for (buf = MIN_BUF_SIZE; buf <= MAX_BUF_SIZE; buf *= 2) {
                if (arguments.buf != buf && arguments.buf != -1) {
                    continue;
                }
                if (mode == DIRECT) {
                    if (!isSuitableBufSizeForDirectIO(block_size, n_block_size,
                                                      buf)) {
                        continue;
                    }
                }
                rv = benchmarkWritePerformance(arguments.dir, buf, arguments.n,
                                               engine, mode);
                if (rv != 0) {
                    return -1;
                }
            }
        }
    }

    return 0;
}
