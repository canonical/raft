#include <errno.h>
#include <fcntl.h>
#include <string.h>

#include "assert.h"
#include "byte.h"
#include "logging.h"
#include "uv_metadata.h"

/* Current on-disk format version. */
#define FORMAT 1

/* Format, version, term, vote */
#define SIZE (8 * 4)

/* Encode the content of a metadata file. */
static void encode(const struct uvMetadata *metadata, void *buf)
{
    void *cursor = buf;
    byte__put64(&cursor, FORMAT);
    byte__put64(&cursor, metadata->version);
    byte__put64(&cursor, metadata->term);
    byte__put64(&cursor, metadata->voted_for);
}

/* Decode the content of a metadata file. */
static int decode(const void *buf, struct uvMetadata *metadata)
{
    const void *cursor = buf;
    unsigned format;
    format = byte__get64(&cursor);
    if (format != FORMAT) {
        return EPROTO;
    }
    metadata->version = byte__get64(&cursor);
    metadata->term = byte__get64(&cursor);
    metadata->voted_for = byte__get64(&cursor);
    return 0;
}

/* Render the filename of the metadata file with index @n. */
static void filenameOf(const unsigned short n, osFilename filename)
{
    sprintf(filename, "metadata%d", n);
}

/* Read the n'th metadata file (with n equal to 1 or 2) and decode the content
 * of the file, populating the given metadata buffer accordingly. */
static int loadFile(struct raft_io *io,
                    const osDir dir,
                    const unsigned short n,
                    struct uvMetadata *metadata)
{
    osFilename filename; /* Filename of the metadata file */
    uint8_t buf[SIZE];   /* Content of metadata file */
    int fd;
    int rv;

    assert(n == 1 || n == 2);

    /* Render the metadata path */
    filenameOf(n, filename);

    /* Open the metadata file, if it exists. */
    rv = osOpen(dir, filename, O_RDONLY, &fd);
    if (rv != 0) {
        if (rv != ENOENT) {
            errorf(io, "load %s: %s", filename, osStrError(rv));
            return rv;
        }
        /* The file does not exist, just return. */
        metadata->version = 0;
        return 0;
    }

    /* Read the content of the metadata file. */
    rv = osReadN(fd, buf, sizeof buf);
    if (rv != 0) {
        if (rv != ENODATA) {
            errorf(io, "read %s: %s", filename, osStrError(rv));
            return rv;
        }
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
        assert(rv == EPROTO);
        errorf(io, "decode %s: bad format version", filename);
        return rv;
    }

    /* Sanity checks that values make sense */
    if (metadata->version == 0) {
        errorf(io, "metadata %s: version is set to zero", filename);
        return EPROTO;
    }

    return 0;
}

/* Update both metadata files using the given one as seed, so they are created
 * if they didn't exist. */
static int ensure(struct raft_io *io,
                  const char *dir,
                  struct uvMetadata *metadata)
{
    int i;
    int rv;

    /* Update both metadata files, so they are created if they didn't
     * exist. Also sync the data directory so the entries get created. */
    for (i = 0; i < 2; i++) {
        metadata->version++;
        rv = uvMetadataStore(io, dir, metadata);
        if (rv != 0) {
            return rv;
        }
    }

    /* Also sync the data directory so the entries get created. */
    rv = osSyncDir(dir);
    if (rv != 0) {
        errorf(io, "sync dir %s: %s", dir, osStrError(rv));
        return rv;
    }

    return 0;
}

/* Return the metadata file index associated with the given version. */
static int indexOf(int version)
{
    return version % 2 == 1 ? 1 : 2;
}

int uvMetadataLoad(struct raft_io *io,
                   const osDir dir,
                   struct uvMetadata *metadata)
{
    struct uvMetadata metadata1;
    struct uvMetadata metadata2;
    int rv;

    /* Read the two metadata files (if available). */
    rv = loadFile(io, dir, 1, &metadata1);
    if (rv != 0) {
        return rv;
    }

    rv = loadFile(io, dir, 2, &metadata2);
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
        errorf(io, "metadata1 and metadata2 are both at version %d",
               metadata1.version);
        return EPROTO;
    } else {
        /* Pick the metadata with the grater version. */
        if (metadata1.version > metadata2.version) {
            *metadata = metadata1;
        } else {
            *metadata = metadata2;
        }
    }

    /* Update the metadata files, so they are created if they did not exist. */
    rv = ensure(io, dir, metadata);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int uvMetadataStore(struct raft_io *io,
                    const osDir dir,
                    const struct uvMetadata *metadata)
{
    osFilename filename; /* Filename of the metadata file */
    uint8_t buf[SIZE];   /* Content of metadata file */
    const int flags = O_WRONLY | O_CREAT | O_SYNC | O_TRUNC;
    unsigned short n;
    int fd;
    int rv;

    assert(metadata->version > 0);

    /* Encode the given metadata. */
    encode(metadata, buf);

    /* Render the metadata file name. */
    n = indexOf(metadata->version);
    filenameOf(n, filename);

    /* Write the metadata file, creating it if it does not exist. */
    rv = osOpen(dir, filename, flags, &fd);
    if (rv != 0) {
        errorf(io, "open %s: %s", filename, osStrError(rv));
        return rv;
    }

    rv = osWriteN(fd, buf, sizeof buf);
    close(fd);

    if (rv != 0) {
        errorf(io, "write %s: %s", filename, osStrError(rv));
        return rv;
    }

    return 0;
}
