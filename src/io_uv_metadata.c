#include <string.h>

#include "../include/raft/io_uv.h"

#include "assert.h"
#include "byte.h"
#include "io_uv_fs.h"
#include "io_uv_metadata.h"

/* Current on-disk format version. */
#define RAFT__IO_UV_METADATA_FORMAT 1

/* Encode the content of a metadata file. */
static void encode(const struct io_uv__metadata *metadata, void *buf);

/* Decode the content of a metadata file. */
static int decode(const void *buf, struct io_uv__metadata *metadata);

/* Read the @n'th metadata file (with @n equal to 1 or 2) and decode the content
 * of the file, populating the given metadata buffer accordingly. */
static int load_n(struct raft_logger *logger,
                  const char *dir,
                  const unsigned short n,
                  struct io_uv__metadata *metadata);

/* Update both metadata files using the given one as seed, so they are created
 * if they didn't exist. */
static int ensure(struct raft_logger *logger,
                  const char *dir,
                  struct io_uv__metadata *metadata);

/* Return the metadata file index associated with the given version. */
static int index_n(int version);

/* Render the file system path of the metadata file with index @n. */
static void render_path(const char *dir, const unsigned short n, char *path);

int io_uv__metadata_load(struct raft_logger *logger,
                         const char *dir,
                         struct io_uv__metadata *metadata)
{
    struct io_uv__metadata metadata1;
    struct io_uv__metadata metadata2;
    int rv;

    /* Read the two metadata files (if available). */
    rv = load_n(logger, dir, 1, &metadata1);
    if (rv != 0) {
        return rv;
    }

    rv = load_n(logger, dir, 2, &metadata2);
    if (rv != 0) {
        return rv;
    }

    /* Check the versions. */
    if (metadata1.version == 0 && metadata2.version == 0) {
        /* Neither metadata file exists: have a brand new server. */
        metadata->version = 0;
        metadata->term = 0;
        metadata->voted_for = 0;
    } else if (metadata1.version == metadata2.version) {
        /* The two metadata files can't have the same version. */
        raft_errorf(logger, "metadata1 and metadata2 are both at version %d",
                    metadata1.version);
        return RAFT_ERR_IO_CORRUPT;
    } else {
        /* Pick the metadata with the grater version. */
        if (metadata1.version > metadata2.version) {
            *metadata = metadata1;
        } else {
            *metadata = metadata2;
        }
    }

    /* Update the metadata files, so they are created if they did not exist. */
    rv = ensure(logger, dir, metadata);
    if (rv != 0) {
        return rv;
    }

    return 0;
}
int io_uv__metadata_store(struct raft_logger *logger,
                          const char *dir,
                          const struct io_uv__metadata *metadata)
{
    io_uv__path path;                      /* Full path of metadata file */
    uint8_t buf[RAFT_IO_UV_METADATA_SIZE]; /* Content of metadata file */
    unsigned short n;
    int fd;
    int rv;

    assert(metadata->version > 0);

    /* Encode the given metadata. */
    encode(metadata, buf);

    /* Render the metadata file name. */
    n = index_n(metadata->version);
    render_path(dir, n, path);

    /* Write the metadata file, creating it if it does not exist. */
    fd = open(path, O_WRONLY | O_CREAT | O_SYNC | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        raft_errorf(logger, "open %s: %s", path, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    do {
        rv = write(fd, buf, sizeof buf);
    } while (rv == -1 && errno == EINTR);

    close(fd);

    if (rv == -1) {
        raft_errorf(logger, "write %s: %s", path, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    assert(rv >= 0);

    if ((size_t)rv < sizeof buf) {
        raft_errorf(logger, "write %s: only %d bytes written", path, rv);
        return RAFT_ERR_IO;
    };

    return 0;
}

static void encode(const struct io_uv__metadata *metadata, void *buf)
{
    void *cursor = buf;

    byte__put64(&cursor, RAFT__IO_UV_METADATA_FORMAT);
    byte__put64(&cursor, metadata->version);
    byte__put64(&cursor, metadata->term);
    byte__put64(&cursor, metadata->voted_for);
}

static int decode(const void *buf, struct io_uv__metadata *metadata)
{
    const void *cursor = buf;
    unsigned format;

    format = byte__get64(&cursor);

    if (format != RAFT__IO_UV_METADATA_FORMAT) {
        return RAFT_ERR_IO_CORRUPT;
    }

    metadata->version = byte__get64(&cursor);
    metadata->term = byte__get64(&cursor);
    metadata->voted_for = byte__get64(&cursor);

    return 0;
}

static int load_n(struct raft_logger *logger,
                  const char *dir,
                  const unsigned short n,
                  struct io_uv__metadata *metadata)
{
    io_uv__path path;                      /* Full path of metadata file */
    uint8_t buf[RAFT_IO_UV_METADATA_SIZE]; /* Content of metadata file */
    int fd;
    int rv;

    assert(n == 1 || n == 2);

    /* Render the metadata path */
    render_path(dir, n, path);

    /* Open the metadata file, if it exists. */
    fd = open(path, O_RDONLY);
    if (fd == -1) {
        if (errno != ENOENT) {
            raft_errorf(logger, "open %s: %s", path, uv_strerror(-errno));
            return RAFT_ERR_IO;
        }

        /* The file does not exist, just return. */
        metadata->version = 0;

        return 0;
    }

    /* Read the content of the metadata file. */
    rv = read(fd, buf, sizeof buf);
    if (rv == -1) {
        raft_errorf(logger, "read %s: %s", path, uv_strerror(-errno));
        close(fd);
        return RAFT_ERR_IO;
    }
    if (rv != sizeof buf) {
        /* Assume that the server crashed while writing this metadata file, and
         * pretend it has not been written at all. */
        metadata->version = 0;
        close(fd);
        return 0;
    };

    close(fd);

    /* Decode the content of the metadata file. */
    rv = decode(buf, metadata);
    if (rv != 0) {
        raft_errorf(logger, "decode %s: %s", path, raft_strerror(rv));
        return RAFT_ERR_IO;
    }

    /* Sanity checks that values make sense */
    if (metadata->version == 0) {
        raft_errorf(logger, "metadata %s: version is set to zero", path);
        return RAFT_ERR_IO;
    }

    return 0;
}

static int ensure(struct raft_logger *logger,
                  const char *dir,
                  struct io_uv__metadata *metadata)
{
    int i;
    int rv;

    /* Update both metadata files, so they are created if they didn't
     * exist. Also sync the data directory so the entries get created. */
    for (i = 0; i < 2; i++) {
        metadata->version++;
        rv = io_uv__metadata_store(logger, dir, metadata);
        if (rv != 0) {
            return rv;
        }
    }

    /* Also sync the data directory so the entries get created. */
    rv = raft__io_uv_fs_sync_dir(dir);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static int index_n(int version)
{
    return version % 2 == 1 ? 1 : 2;
}

static void render_path(const char *dir, const unsigned short n, char *path)
{
    char filename[strlen("metadataN") + 1]; /* Pattern of metadata filename */

    sprintf(filename, "metadata%d", n);
    io_uv__join(dir, filename, path);
}
