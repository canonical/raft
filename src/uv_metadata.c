#include "assert.h"
#include "byte.h"
#include "uv.h"
#include "uv_encoding.h"

/* We have metadata1 and metadata2. */
#define METADATA_FILENAME_PREFIX "metadata"
#define METADATA_FILENAME_SIZE (sizeof(METADATA_FILENAME_PREFIX) + 2)

/* Format, version, term, vote */
#define METADATA_CONTENT_SIZE (8 * 4)

/* Encode the content of a metadata file. */
static void encode(const struct uvMetadata *metadata, void *buf)
{
    void *cursor = buf;
    bytePut64(&cursor, UV__DISK_FORMAT);
    bytePut64(&cursor, metadata->version);
    bytePut64(&cursor, metadata->term);
    bytePut64(&cursor, metadata->voted_for);
}

/* Decode the content of a metadata file. */
static int decode(const void *buf, struct uvMetadata *metadata)
{
    const void *cursor = buf;
    unsigned format;
    format = byteGet64(&cursor);
    if (format != UV__DISK_FORMAT) {
        return RAFT_MALFORMED;
    }
    metadata->version = byteGet64(&cursor);
    metadata->term = byteGet64(&cursor);
    metadata->voted_for = byteGet64(&cursor);
    return 0;
}

/* Render the filename of the metadata file with index @n. */
static void filenameOf(const unsigned short n, char *filename)
{
    sprintf(filename, METADATA_FILENAME_PREFIX "%d", n);
}

/* Read the n'th metadata file (with n equal to 1 or 2) and decode the content
 * of the file, populating the given metadata buffer accordingly. */
static int loadFile(struct uv *uv,
                    const unsigned short n,
                    struct uvMetadata *metadata)
{
    char filename[METADATA_FILENAME_SIZE]; /* Filename of the metadata file */
    uint8_t buf[METADATA_CONTENT_SIZE];    /* Content of metadata file */
    int fd;
    char errmsg[2048];
    int rv;

    assert(n == 1 || n == 2);

    /* Render the metadata path */
    filenameOf(n, filename);

    memset(metadata, 0, sizeof *metadata);

    /* Open the metadata file, if it exists. */
    rv = uvOpenFile(uv->dir, filename, O_RDONLY, &fd, errmsg);
    if (rv != 0) {
        if (rv != UV__NOENT) {
            uvErrorf(uv, "open %s: %s", filename, errmsg);
            return RAFT_IOERR;
        }
        /* The file does not exist, just return. */
        return 0;
    }

    /* Read the content of the metadata file. */
    rv = uvReadFully(fd, buf, sizeof buf, errmsg);
    if (rv != 0) {
        if (rv != UV__NODATA) {
            uvErrorf(uv, "read %s: %s", filename, errmsg);
            return RAFT_IOERR;
        }
        /* Assume that the server crashed while writing this metadata file, and
         * pretend it has not been written at all. */
        uvWarnf(uv, "read %s: ignore incomplete data", filename);
        close(fd);
        return 0;
    };

    close(fd);

    /* Decode the content of the metadata file. */
    rv = decode(buf, metadata);
    if (rv != 0) {
        assert(rv == RAFT_MALFORMED);
        uvErrorf(uv, "load %s: bad format version", filename);
        return rv;
    }

    /* Sanity checks that values make sense */
    if (metadata->version == 0) {
        uvErrorf(uv, "load %s: version is set to zero", filename);
        return RAFT_CORRUPT;
    }

    return 0;
}

/* Update both metadata files using the given one as seed, so they are created
 * if they didn't exist. */
static int ensure(struct uv *uv, struct uvMetadata *metadata)
{
    int i;
    char errmsg[2048];
    int rv;

    /* Update both metadata files, so they are created if they didn't
     * exist. Also sync the data directory so the entries get created. */
    for (i = 0; i < 2; i++) {
        metadata->version++;
        rv = uvMetadataStore(uv, metadata);
        if (rv != 0) {
            return rv;
        }
    }

    /* Also sync the data directory so the entries get created. */
    rv = uvSyncDir(uv->dir, errmsg);
    if (rv != 0) {
        uvErrorf(uv, "sync %s: %s", uv->dir, errmsg);
        return RAFT_IOERR;
    }

    return 0;
}

/* Return the metadata file index associated with the given version. */
static int indexOf(int version)
{
    return version % 2 == 1 ? 1 : 2;
}

#define logMetadata(PREFIX, M)                                                 \
    uvDebugf(uv, "metadata" #PREFIX ": version %lld, term %lld, voted for %d", \
             (M)->version, (M)->term, (M)->voted_for);

int uvMetadataLoad(struct uv *uv, struct uvMetadata *metadata)
{
    struct uvMetadata metadata1;
    struct uvMetadata metadata2;
    int rv;

    /* Read the two metadata files (if available). */
    rv = loadFile(uv, 1, &metadata1);
    if (rv != 0) {
        return rv;
    }
    logMetadata(1, &metadata1);

    rv = loadFile(uv, 2, &metadata2);
    if (rv != 0) {
        return rv;
    }
    logMetadata(2, &metadata2);

    /* Check the versions. */
    if (metadata1.version == 0 && metadata2.version == 0) {
        /* Neither metadata file exists: have a brand new server. */
        metadata->version = 0;
        metadata->term = 0;
        metadata->voted_for = 0;
    } else if (metadata1.version == metadata2.version) {
        /* The two metadata files can't have the same version. */
        uvErrorf(uv, "metadata1 and metadata2 are both at version %d",
                 metadata1.version);
        return RAFT_CORRUPT;
    } else {
        /* Pick the metadata with the grater version. */
        if (metadata1.version > metadata2.version) {
            *metadata = metadata1;
        } else {
            *metadata = metadata2;
        }
    }

    /* Update the metadata files, so they are created if they did not exist. */
    rv = ensure(uv, metadata);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int uvMetadataStore(struct uv *uv, const struct uvMetadata *metadata)
{
    char filename[METADATA_FILENAME_SIZE]; /* Filename of the metadata file */
    uint8_t buf[METADATA_CONTENT_SIZE];    /* Content of metadata file */
    const int flags = O_WRONLY | O_CREAT | O_SYNC | O_TRUNC;
    unsigned short n;
    int fd;
    char errmsg[2048];
    int rv;

    assert(metadata->version > 0);

    /* Encode the given metadata. */
    encode(metadata, buf);

    /* Render the metadata file name. */
    n = indexOf(metadata->version);
    filenameOf(n, filename);

    /* Write the metadata file, creating it if it does not exist. */
    rv = uvOpenFile(uv->dir, filename, flags, &fd, errmsg);
    if (rv != 0) {
        uvErrorf(uv, "open %s: %s", filename, errmsg);
        return RAFT_IOERR;
    }

    rv = uvWriteFully(fd, buf, sizeof buf, errmsg);
    close(fd);
    if (rv != 0) {
        uvErrorf(uv, "write %s: %s", filename, errmsg);
        return RAFT_IOERR;
    }

    return 0;
}
