#include "assert.h"
#include "byte.h"
#include "uv.h"
#include "uv_encoding.h"
#include "uv_error.h"

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
static int decode(const void *buf,
                  struct uvMetadata *metadata,
                  struct ErrMsg *errmsg)
{
    const void *cursor = buf;
    unsigned format;
    format = byteGet64(&cursor);
    if (format != UV__DISK_FORMAT) {
        ErrMsgPrintf(errmsg, "bad format version %d", format);
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
    char filename[METADATA_FILENAME_SIZE];  /* Filename of the metadata file */
    uint8_t content[METADATA_CONTENT_SIZE]; /* Content of metadata file */
    struct raft_buffer buf;
    bool exists;
    int rv;

    assert(n == 1 || n == 2);

    /* Render the metadata path */
    filenameOf(n, filename);

    rv = UvFsFileExists(uv->dir, filename, &exists, &uv->errmsg);
    if (rv != 0) {
        ErrMsgWrapf(&uv->errmsg, "check if %s exists", filename);
        return RAFT_IOERR;
    }

    memset(metadata, 0, sizeof *metadata);

    /* If the file does not exist, just return. */
    if (!exists) {
        return 0;
    }

    /* Read the content of the metadata file. */
    buf.base = content;
    buf.len = sizeof content;

    rv = UvFsReadFileInto(uv->dir, filename, &buf, &uv->errmsg);
    if (rv != 0) {
        if (rv != UV__NODATA) {
            ErrMsgWrapf(&uv->errmsg, "load content of %s", filename);
            rv = RAFT_IOERR;
        } else {
            /* Assume that the server crashed while writing this metadata file,
             * and pretend it has not been written at all. */
            UvFsRemoveFile(uv->dir, filename, &uv->errmsg);
            uvWarnf(uv, "read %s: ignore incomplete data", filename);
            rv = 0;
        }
        return rv;
    };

    /* Decode the content of the metadata file. */
    rv = decode(content, metadata, &uv->errmsg);
    if (rv != 0) {
        ErrMsgWrapf(&uv->errmsg, "load %s", filename);
        return rv;
    }

    /* Sanity checks that values make sense */
    if (metadata->version == 0) {
        ErrMsgPrintf(&uv->errmsg, "load %s: version is set to zero", filename);
        return RAFT_CORRUPT;
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
        ErrMsgPrintf(&uv->errmsg,
                     "metadata1 and metadata2 are both at version %d",
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

    return 0;
}

int uvMetadataStore(struct uv *uv, const struct uvMetadata *metadata)
{
    char filename[METADATA_FILENAME_SIZE];  /* Filename of the metadata file */
    uint8_t content[METADATA_CONTENT_SIZE]; /* Content of metadata file */
    struct raft_buffer buf;
    unsigned short n;
    int rv;

    assert(metadata->version > 0);

    /* Encode the given metadata. */
    encode(metadata, content);

    /* Render the metadata file name. */
    n = indexOf(metadata->version);
    filenameOf(n, filename);

    /* Write the metadata file, creating it if it does not exist. */
    buf.base = content;
    buf.len = sizeof content;
    rv = UvFsMakeOrOverwriteFile(uv->dir, filename, &buf, &uv->errmsg);
    if (rv != 0) {
        ErrMsgWrapf(&uv->errmsg, "persist %s", filename);
        return rv;
    }

    return 0;
}
