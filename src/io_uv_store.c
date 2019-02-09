#include <string.h>
#include <unistd.h>

#include "assert.h"
#include "binary.h"
#include "checksum.h"
#include "io_uv_encoding.h"
#include "io_uv_store.h"
#include "uv_fs.h"

/**
 * Current on-disk format version.
 */
#define RAFT_IO_UV_STORE__FORMAT 1

/**
 * Template string for open segment filenames.
 *
 * First param: incrementing counter.
 */
#define RAFT_IO_UV_SEGMENT__OPEN_TEMPLATE "open-%llu"

/**
 * Template string for closed segment filenames.
 *
 * First param: start index, inclusive.
 * Second param: end index, inclusive.
 */
#define RAFT_IO_UV_SEGMENT__CLOSED_TEMPLATE "%020llu-%020llu"

/**
 * Maximum length of a segment filename.
 *
 * This is enough to hold both a closed and a open segment filename.
 */
#define RAFT_IO_UV_SEGMENT__MAX_FILENAME_LEN 42

/**
 * Codes for the writer state.
 */
enum {
    RAFT_IO_UV_STORE__WRITER_IDLE,    /* Not doing anything */
    RAFT_IO_UV_STORE__WRITER_BLOCKED, /* Waiting for a segment to be ready */
    RAFT_IO_UV_STORE__WRITER_WRITING  /* Writing to disk */
};

/**
 * Hold information about a single segment file.
 */
struct raft_io_uv_segment
{
    bool is_open; /* Whether the segment is open */
    union {
        struct
        {
            raft_index first_index; /* First index in a closed segment */
            raft_index end_index;   /* Last index in a closed segment */
        };
        struct
        {
            unsigned long long counter; /* Open segment counter */
        };
    };

    /* Filename of the segment */
    char filename[RAFT_IO_UV_SEGMENT__MAX_FILENAME_LEN];
};

/**
 * Read exactly @n bytes from the given file descriptor.
 */
static int raft_io_uv__read_n(struct raft_logger *logger,
                              const int fd,
                              void *buf,
                              size_t n)
{
    int rv;

    rv = read(fd, buf, n);

    if (rv == -1) {
        raft_errorf(logger, "read: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    assert(rv >= 0);

    if ((size_t)rv < n) {
        raft_errorf(logger, "read: got %d bytes instead of %ld", rv, n);
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Check if the content of the segment file associated with the given file
 * descriptor contains all zeros from the current offset onward.
 */
static int raft_io_uv__is_all_zeros(struct raft_logger *logger,
                                    const int fd,
                                    bool *flag)
{
    off_t size;
    off_t offset;
    uint8_t *data;
    size_t i;
    int rv;

    /* Save the current offset. */
    offset = lseek(fd, 0, SEEK_CUR);

    /* Figure the size of the rest of the file. */
    size = lseek(fd, 0, SEEK_END);
    if (size == -1) {
        raft_errorf(logger, "lseek: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }
    size -= offset;

    /* Reposition the file descriptor offset to the original offset. */
    offset = lseek(fd, offset, SEEK_SET);
    if (offset == -1) {
        raft_errorf(logger, "lseek: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    data = raft_malloc(size);
    if (data == NULL) {
        return RAFT_ERR_NOMEM;
    }

    rv = raft_io_uv__read_n(logger, fd, data, size);
    if (rv != 0) {
        return rv;
    }

    for (i = 0; i < (size_t)size; i++) {
        if (data[i] != 0) {
            *flag = false;
            goto done;
        }
    }

    *flag = true;

done:
    raft_free(data);

    return 0;
}

/**
 * Check whether the given file is empty.
 */
static int raft_io_uv__is_empty(struct raft_logger *logger,
                                const char *path,
                                bool *empty)
{
    struct stat st;
    int rv;

    rv = stat(path, &st);
    if (rv == -1) {
        raft_errorf(logger, "stat '%s': %s", path, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    *empty = st.st_size == 0 ? true : false;

    return 0;
}

/**
 * Check if the given file descriptor has reached the end of the file.
 */
static bool raft_io_uv__is_at_eof(const int fd)
{
    off_t offset; /* Current position */
    off_t size;   /* File size */

    offset = lseek(fd, 0, SEEK_CUR);
    size = lseek(fd, 0, SEEK_END);

    lseek(fd, offset, SEEK_SET);

    return offset == size;
}

/**
 * Sync the given directory.
 */
static int raft_io_uv__sync_dir(struct raft_logger *logger, const char *dir)
{
    int fd;
    int rv;

    fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        raft_errorf(logger, "open '%s': %s", dir, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    rv = fsync(fd);

    close(fd);

    if (rv == -1) {
        raft_errorf(logger, "sync '%s': %s", dir, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Truncate the given file to the given @offset and sync it to disk.
 */
static int raft_io_uv__truncate(struct raft_logger *logger,
                                const int fd,
                                const off_t offset)
{
    int rv;
    rv = ftruncate(fd, offset);
    if (rv == -1) {
        raft_errorf(logger, "ftruncate: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }
    rv = fsync(fd);
    if (rv == -1) {
        raft_errorf(logger, "fsync: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Rename a file and sync its dir.
 */
static int raft_io_uv__rename(struct raft_logger *logger,
                              const char *dir,
                              const char *filename1,
                              const char *filename2)
{
    char path1[RAFT_UV_FS_MAX_PATH_LEN];
    char path2[RAFT_UV_FS_MAX_PATH_LEN];
    int rv;

    raft_uv_fs__join(dir, filename1, path1);
    raft_uv_fs__join(dir, filename2, path2);

    /* TODO: double check that filename2 does not exist. */
    rv = rename(path1, path2);
    if (rv == -1) {
        raft_errorf(logger, "rename '%s' to '%s': %s", path1, path2,
                    uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    rv = raft_io_uv__sync_dir(logger, dir);
    if (rv == -1) {
        return rv;
    }

    return 0;
}

/**
 * Remove the file at the given path.
 */
static int raft_io_uv__remove(struct raft_logger *logger,
                              const char *dir,
                              const char *filename)
{
    char path[RAFT_UV_FS_MAX_PATH_LEN]; /* Full path of segment file */
    int rv;

    raft_uv_fs__join(dir, filename, path);

    rv = unlink(path);
    if (rv != 0) {
        raft_errorf(logger, "unlink '%s': %s", path, uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    rv = raft_io_uv__sync_dir(logger, dir);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

/**
 * Calculate how many bytes are needed to store the given batch of entries.
 */
static size_t raft_io_uv__sizeof_entries(const struct raft_entry *entries,
                                         const unsigned n)
{
    size_t size = 0;
    unsigned i;

    assert(entries != NULL);
    assert(n > 0);

    size += sizeof(uint64_t);                   /* Checksums */
    size += raft_io_uv_sizeof__batch_header(n); /* Batch header */
    for (i = 0; i < n; i++) {                   /* Entry data */
        size_t len = entries[i].buf.len;
        size += len;
        if (len % 8 != 0) {
            /* Add padding */
            size += 8 - (len % 8);
        }
    }

    return size;
}

/**
 * Append to @entries2 all entries in @entries1.
 */
static int raft_io_uv__extend_entries(const struct raft_entry *entries1,
                                      const size_t n_entries1,
                                      struct raft_entry **entries2,
                                      size_t *n_entries2)
{
    struct raft_entry *entries; /* To re-allocate the given entries */
    size_t i;

    entries =
        raft_realloc(*entries2, (*n_entries2 + n_entries1) * sizeof *entries);
    if (entries == NULL) {
        return RAFT_ERR_NOMEM;
    }

    for (i = 0; i < n_entries1; i++) {
        entries[*n_entries2 + i] = entries1[i];
    }

    *entries2 = entries;
    *n_entries2 += n_entries1;

    return 0;
}

/**
 * Encode the content of a metadata file.
 */
static void raft_io_uv_metadata__encode(
    const struct raft_io_uv_metadata *metadata,
    void *buf)
{
    void *cursor = buf;

    raft__put64(&cursor, RAFT_IO_UV_STORE__FORMAT);
    raft__put64(&cursor, metadata->version);
    raft__put64(&cursor, metadata->term);
    raft__put64(&cursor, metadata->voted_for);
    raft__put64(&cursor, metadata->start_index);
}

/**
 * Decode the content of a metadata file.
 */
static int raft_io_uv_metadata__decode(const void *buf,
                                       struct raft_io_uv_metadata *metadata)
{
    const void *cursor = buf;
    unsigned format;

    format = raft__get64(&cursor);

    if (format != RAFT_IO_UV_STORE__FORMAT) {
        return RAFT_ERR_IO_CORRUPT;
    }

    metadata->version = raft__get64(&cursor);
    metadata->term = raft__get64(&cursor);
    metadata->voted_for = raft__get64(&cursor);
    metadata->start_index = raft__get64(&cursor);

    return 0;
}

/**
 * Render the file system path of the metadata file with index @n.
 */
static void raft_io_uv_metadata__path(const char *dir,
                                      const unsigned short n,
                                      char *path)
{
    char filename[strlen("metadataN") + 1]; /* Pattern of metadata filename */

    sprintf(filename, "metadata%d", n);
    raft_uv_fs__join(dir, filename, path);
}

/**
 * Read the @n'th metadata file (with @n equal to 1 or 2) and decode the content
 * of the file, populating the given metadata buffer accordingly.
 */
static int raft_io_uv_metadata__load(struct raft_logger *logger,
                                     const char *dir,
                                     const unsigned short n,
                                     struct raft_io_uv_metadata *metadata)
{
    raft_uv_path path;                     /* Full path of metadata file */
    uint8_t buf[RAFT_IO_UV_METADATA_SIZE]; /* Content of metadata file */
    int fd;
    int rv;

    assert(n == 1 || n == 2);

    /* Render the metadata path */
    raft_io_uv_metadata__path(dir, n, path);

    /* Open the metadata file, if it exists. */
    fd = open(path, O_RDONLY);
    if (fd == -1) {
        if (errno != ENOENT) {
            raft_errorf(logger, "open '%s': %s", path, uv_strerror(-errno));
            return RAFT_ERR_IO;
        }

        /* The file does not exist, just return. */
        metadata->version = 0;

        return 0;
    }

    /* Read the content of the metadata file. */
    rv = read(fd, buf, sizeof buf);
    if (rv == -1) {
        raft_errorf(logger, "read '%s': %s", path, uv_strerror(-errno));
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
    rv = raft_io_uv_metadata__decode(buf, metadata);
    if (rv != 0) {
        raft_errorf(logger, "decode '%s': %s", path, raft_strerror(rv));
        return RAFT_ERR_IO;
    }

    /* Sanity checks that values make sense */
    if (metadata->version == 0) {
        raft_errorf(logger, "metadata '%s': version is set to zero", path);
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Return the metadata file index associated with the given version.
 */
static int raft_io_uv_metadata__n(int version)
{
    return version % 2 == 1 ? 1 : 2;
}

/**
 * Write the given metadata to disk, writing the appropriate metadata file
 * according to the metadata version (if the version is odd, write metadata1,
 * otherwise write metadata2).
 */
static int raft_io_uv_metadata__store(
    struct raft_logger *logger,
    const char *dir,
    const struct raft_io_uv_metadata *metadata)
{
    raft_uv_path path;                     /* Full path of metadata file */
    uint8_t buf[RAFT_IO_UV_METADATA_SIZE]; /* Content of metadata file */
    unsigned short n;
    int fd;
    int rv;

    assert(metadata->version > 0);
    assert(metadata->start_index > 0);

    /* Encode the given metadata. */
    raft_io_uv_metadata__encode(metadata, buf);

    /* Render the metadata file name. */
    n = raft_io_uv_metadata__n(metadata->version);
    raft_io_uv_metadata__path(dir, n, path);

    /* Write the metadata file, creating it if it does not exist. */
    fd = open(path, O_WRONLY | O_CREAT | O_SYNC | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        raft_errorf(logger, "open '%s': %s", path, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    do {
        rv = write(fd, buf, sizeof buf);
    } while (rv == -1 && errno == EINTR);

    close(fd);

    if (rv == -1) {
        raft_errorf(logger, "write '%s': %s", path, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    assert(rv >= 0);

    if ((size_t)rv < sizeof buf) {
        raft_errorf(logger, "write '%s': only %d bytes written", path, rv);
        return RAFT_ERR_IO;
    };

    return 0;
}

/**
 * Filenames to ignore when listing segment files.
 */
static const char *raft_io_uv_segment__ignored_filenames[] = {
    ".", "..", "metadata1", "metadata2", NULL};

/**
 * Return true if this is a segment filename.
 */
static bool raft_io_uv_segment__is_valid_filename(const char *filename)
{
    const char **cursor = raft_io_uv_segment__ignored_filenames;
    bool result = true;

    while (*cursor != NULL) {
        if (strcmp(filename, *cursor) == 0) {
            result = false;
            break;
        }
        cursor++;
    }

    return result;
}

/**
 * Try to match the filename of a closed segment (xxx-yyy), and return its first
 * and end index in case of a match.
 */
static bool raft_io_uv_segment__match_closed_filename(const char *filename,
                                                      raft_index *first_index,
                                                      raft_index *end_index)
{
    unsigned consumed;
    int matched;

    matched = sscanf(filename, RAFT_IO_UV_SEGMENT__CLOSED_TEMPLATE "%n",
                     first_index, end_index, &consumed);

    return matched == 2 && consumed == strlen(filename);
}

/**
 * Try to match the filename of an open segment (open-xxx), and return its
 * counter in case of a match.
 */
static bool raft_io_uv_segment__match_open_filename(const char *filename,
                                                    unsigned long long *counter)
{
    unsigned consumed;
    int matched;

    matched = sscanf(filename, RAFT_IO_UV_SEGMENT__OPEN_TEMPLATE "%n", counter,
                     &consumed);
    return matched == 1 && consumed == strlen(filename);
}

/**
 * Render the filename of a closed segment.
 */
static void raft_io_uv_segment__make_closed_filename(
    const raft_index first_index,
    const raft_index end_index,
    char *filename)
{
    sprintf(filename, RAFT_IO_UV_SEGMENT__CLOSED_TEMPLATE, first_index,
            end_index);
}

/**
 * Render the filename of an open segment.
 */
static void raft_io_uv_segment__make_open_filename(
    const unsigned long long counter,
    char *filename)
{
    sprintf(filename, RAFT_IO_UV_SEGMENT__OPEN_TEMPLATE, counter);
}

/**
 * List all files in the data directory and collect metadata about segment
 * files. The segments are ordered by filename.
 */
static int raft_io_uv_segment__list(struct raft_logger *logger,
                                    const char *dir,
                                    struct raft_io_uv_segment *segments[],
                                    size_t *n)
{
    struct dirent **entries;
    struct raft_io_uv_segment *tmp_segments;
    int n_entries;
    int i;
    int rv = 0;

    n_entries = scandir(dir, &entries, NULL, alphasort);
    if (n_entries < 0) {
        raft_errorf(logger, "scan '%s': %s", dir, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    *segments = NULL;
    *n = 0;

    for (i = 0; i < n_entries; i++) {
        struct dirent *entry = entries[i];
        bool ignore = !raft_io_uv_segment__is_valid_filename(entry->d_name);
        bool matched;
        struct raft_io_uv_segment segment;

        /* If an error occurred while processing a preceeding entry or if we
         * know that this is not a segment filename, just free it and skip to
         * the next one. */
        if (rv != 0 || ignore) {
            goto next;
        }

        /* Check if it's a closed segment filename */
        matched = raft_io_uv_segment__match_closed_filename(
            entry->d_name, &segment.first_index, &segment.end_index);

        if (matched) {
            segment.is_open = false;
            goto append;
        }

        /* Check if it's an open segment filename */
        matched = raft_io_uv_segment__match_open_filename(entry->d_name,
                                                          &segment.counter);

        if (matched) {
            segment.is_open = true;
            goto append;
        }

        /* This is neither a closed or an open segment */
        goto next;

    append:
        (*n)++;
        tmp_segments = raft_realloc(*segments, (*n) * sizeof **segments);

        if (tmp_segments == NULL) {
            rv = RAFT_ERR_NOMEM;
            goto next;
        }

        assert(strlen(entry->d_name) < sizeof segment.filename);
        strcpy(segment.filename, entry->d_name);

        *segments = tmp_segments;
        (*segments)[(*n) - 1] = segment;

    next:
        free(entries[i]);
    }
    free(entries);

    if (rv != 0 && *segments != NULL) {
        raft_free(*segments);
    }

    return rv;
}

/**
 * Open a segment file and read its format version.
 */
static int raft_io_uv_segment__open(struct raft_logger *logger,
                                    const char *path,
                                    const int flags,
                                    int *fd,
                                    uint64_t *format)
{
    int rv;

    *fd = open(path, flags);
    if (*fd == -1) {
        raft_errorf(logger, "open '%s': %s", path, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    rv = raft_io_uv__read_n(logger, *fd, format, sizeof *format);
    if (rv != 0) {
        close(*fd);
        return rv;
    }
    *format = raft__flip64(*format);

    return 0;
}

/**
 * Load a single batch of entries from a segment.
 *
 * Set @last to #true if the loaded batch is the last one.
 */
static int raft_io_uv_segment__load_batch(struct raft_logger *logger,
                                          const int fd,
                                          struct raft_entry **entries,
                                          unsigned *n_entries,
                                          bool *last)
{
    uint64_t preamble[2];      /* CRC32 checksums and number of raft entries */
    unsigned n;                /* Number of entries in the batch */
    unsigned i;                /* Iterate through the entries */
    struct raft_buffer header; /* Batch header */
    struct raft_buffer data;   /* Batch data */
    unsigned crc1;             /* Target checksum */
    unsigned crc2;             /* Actual checksum */
    int rv;

    /* Read the preamble, consisting of the checksums for the batch header and
     * data buffers and the first 8 bytes of the header buffer, which contains
     * the number of entries in the batch. */
    rv = raft_io_uv__read_n(logger, fd, preamble, sizeof preamble);
    if (rv != 0) {
        return rv;
    }

    n = raft__flip64(preamble[1]);

    if (n == 0) {
        raft_errorf(logger, "batch has zero entries");
        rv = RAFT_ERR_IO_CORRUPT;
        goto err;
    }

    /* Read the batch header, excluding the first 8 bytes containing the number
     * of entries, which we have already read. */
    header.len = raft_io_uv_sizeof__batch_header(n);
    header.base = raft_malloc(header.len);
    if (header.base == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }
    *(uint64_t *)header.base = preamble[1];

    rv = raft_io_uv__read_n(logger, fd, header.base + sizeof(uint64_t),
                            header.len - sizeof(uint64_t));
    if (rv != 0) {
        goto err_after_header_alloc;
    }

    /* Check batch header integrity. */
    crc1 = raft__flip32(*(uint32_t *)preamble);
    crc2 = raft__crc32(header.base, header.len, 0);
    if (crc1 != crc2) {
        raft_errorf(logger, "corrupted batch header");
        rv = RAFT_ERR_IO_CORRUPT;
        goto err_after_header_alloc;
    }

    /* Decode the batch header, allocating the entries array. */
    rv = raft_io_uv_decode__batch_header(header.base, entries, n_entries);
    if (rv != 0) {
        goto err_after_header_alloc;
    }

    /* Calculate the total size of the batch data */
    data.len = 0;
    for (i = 0; i < n; i++) {
        data.len += (*entries)[i].buf.len;
    }

    /* Read the batch data */
    data.base = raft_malloc(data.len);
    if (data.base == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err_after_header_decode;
    }
    rv = raft_io_uv__read_n(logger, fd, data.base, data.len);
    if (rv != 0) {
        goto err_after_data_alloc;
    }

    /* Check batch data integrity. */
    crc1 = raft__flip32(*((uint32_t *)preamble + 1));
    crc2 = raft__crc32(data.base, data.len, 0);
    if (crc1 != crc2) {
        raft_errorf(logger, "corrupted batch data");
        rv = RAFT_ERR_IO_CORRUPT;
        goto err_after_data_alloc;
    }

    raft_io_uv_decode__entries_batch(&data, *entries, *n_entries);

    raft_free(header.base);

    *last = raft_io_uv__is_at_eof(fd);

    return 0;

err_after_data_alloc:
    raft_free(data.base);

err_after_header_decode:
    raft_free(*entries);

err_after_header_alloc:
    raft_free(header.base);

err:
    assert(rv != 0);

    return rv;
}

/**
 * Load the entries stored in a closed segment.
 */
static int raft_io_uv_segment__load_closed(
    struct raft_logger *logger,
    const char *dir,
    const raft_index start_index,
    const struct raft_io_uv_segment *segment,
    struct raft_entry **entries,
    size_t *n_entries,
    raft_index *next_index)
{
    raft_uv_path path;              /* Full path of segment file */
    bool empty;                     /* Whether the file is empty */
    int fd;                         /* Segment file descriptor */
    uint64_t format;                /* Format version */
    struct raft_entry *tmp_entries; /* Entries in current batch */
    unsigned tmp_n_entries;         /* Number of entries in current batch */
    bool last = false;              /* Whether the last batch was reached */
    int i;
    int rv;

    raft_uv_fs__join(dir, segment->filename, path);

    /* If the segment is completely empty, just bail out. */
    rv = raft_io_uv__is_empty(logger, path, &empty);
    if (rv != 0) {
        goto err;
    }
    if (empty) {
        raft_errorf(logger, "segment '%s': file is empty", path);
        rv = RAFT_ERR_IO_CORRUPT;
        goto err;
    }

    /* Open the segment file. */
    rv = raft_io_uv_segment__open(logger, path, O_RDONLY, &fd, &format);
    if (rv != 0) {
        goto err;
    }

    /* If the entries in the segment are no longer needed, just remove it. */
    if (segment->end_index < start_index) {
        rv = raft_io_uv__remove(logger, dir, segment->filename);
        if (rv != 0) {
            goto err_after_open;
        }
        goto done;
    }

    /* Check that start index encoded in the name of the segment match what we
     * expect. */
    if (segment->first_index != *next_index) {
        raft_errorf(logger, "segment '%s': expected first index to be %lld",
                    path, *next_index);
        rv = RAFT_ERR_IO_CORRUPT;
        goto err_after_open;
    }

    if (format != RAFT_IO_UV_STORE__FORMAT) {
        raft_errorf(logger, "segment '%s': unexpected format version: %lu",
                    path, format);
        rv = RAFT_ERR_IO;
        goto err_after_open;
    }

    /* Load all batches in the segment. */
    for (i = 1; !last; i++) {
        rv = raft_io_uv_segment__load_batch(logger, fd, &tmp_entries,
                                            &tmp_n_entries, &last);
        if (rv != 0) {
            goto err_after_open;
        }

        rv = raft_io_uv__extend_entries(tmp_entries, tmp_n_entries, entries,
                                        n_entries);
        if (rv != 0) {
            goto err_after_batch_load;
        }

        raft_free(tmp_entries);

        *next_index += tmp_n_entries;
    }

    assert(i > 1); /* At least one batch was loaded. */

done:
    close(fd);

    return 0;

err_after_batch_load:
    raft_free(tmp_entries[0].batch);
    raft_free(tmp_entries);

err_after_open:
    close(fd);

err:
    assert(rv != 0);

    return rv;
}

/**
 * Load the entries stored in an open segment. If there are no entries at all,
 * remove the open segment, mark it as closed (by renaming it).
 */
static int raft_io_uv_segment__load_open(struct raft_logger *logger,
                                         const char *dir,
                                         struct raft_io_uv_segment *segment,
                                         struct raft_entry **entries,
                                         size_t *n_entries,
                                         raft_index *next_index)
{
    raft_uv_path path;              /* Full path of segment file */
    raft_index first_index;         /* Index of first entry in segment */
    bool all_zeros;                 /* Whether the file is zero'ed */
    bool empty;                     /* Whether the segment file is empty */
    bool remove = false;            /* Whether to remove this segment */
    bool last = false;              /* Whether the last batch was reached */
    int fd;                         /* Segment file descriptor */
    uint64_t format;                /* Format version */
    size_t n_batches = 0;           /* Number of loaded batches */
    struct raft_entry *tmp_entries; /* Entries in current batch */
    unsigned tmp_n_entries;         /* Number of entries in current batch */
    int i;
    int rv;

    first_index = *next_index;

    raft_uv_fs__join(dir, segment->filename, path);

    rv = raft_io_uv__is_empty(logger, path, &empty);
    if (rv != 0) {
        goto err;
    }

    if (empty) {
        /* Empty segment, let's discard it. */
        remove = true;
        goto done;
    }

    rv = raft_io_uv_segment__open(logger, path, O_RDWR, &fd, &format);
    if (rv != 0) {
        goto err;
    }

    /* Check that the format is the expected one, or perhaps 0, indicating that
     * the segment was allocated but never written. */
    if (format != RAFT_IO_UV_STORE__FORMAT) {
        if (format == 0) {
            rv = raft_io_uv__is_all_zeros(logger, fd, &all_zeros);
            if (rv != 0) {
                goto err_after_open;
            }

            if (all_zeros) {
                /* This is equivalent to the empty case, let's remove the
                 * segment. */
                remove = true;
                goto done;
            }
        }

        raft_errorf(logger, "segment '%s': unexpected format version: %lu",
                    path, format);
        rv = RAFT_ERR_IO;
        goto err_after_open;
    }

    /* Load all batches in the segment. */
    for (i = 1; !last; i++) {
        /* Save the current file descriptor offset, in case we need to truncate
         * the file to exclude this batch because it's incomplete. */
        off_t offset = lseek(fd, 0, SEEK_CUR);

        if (offset == -1) {
            raft_errorf(logger, "segment '%s': batch %d: save offset: %s", path,
                        i, uv_strerror(-errno));
            return RAFT_ERR_IO;
        }

        rv = raft_io_uv_segment__load_batch(logger, fd, &tmp_entries,
                                            &tmp_n_entries, &last);
        if (rv != 0) {
            int rv2;

            /* If this isn't a decoding error, just bail out. */
            if (rv != RAFT_ERR_IO_CORRUPT) {
                goto err_after_open;
            }

            /* If this is a decoding error, and not an OS error, check if the
             * rest of the file is filled with zeros. In that case we assume
             * that the server shutdown uncleanly and we just truncate this
             * incomplete data. */
            lseek(fd, offset, SEEK_SET);

            rv2 = raft_io_uv__is_all_zeros(logger, fd, &all_zeros);
            if (rv2 != 0) {
                rv = rv2;
                goto err_after_open;
            }

            if (!all_zeros) {
                /* TODO: log a warning here, stating that the segment had a
                 * non-zero partial batch, and reporting the decoding error. */
            }

            rv = raft_io_uv__truncate(logger, fd, offset);
            if (rv != 0) {
                goto err_after_open;
            }

            break;
        }

        rv = raft_io_uv__extend_entries(tmp_entries, tmp_n_entries, entries,
                                        n_entries);
        if (rv != 0) {
            goto err_after_batch_load;
        }

        raft_free(tmp_entries);

        n_batches++;
        *next_index += tmp_n_entries;
    }

    rv = close(fd);
    assert(rv == 0);

    if (n_batches == 0) {
        remove = true;
    }

done:
    /* If the segment has no valid entries in it, we remove it. Otherwise we
     * rename it and keep it. */
    if (remove) {
        rv = raft_io_uv__remove(logger, dir, segment->filename);
        if (rv != 0) {
            goto err_after_open;
        }
    } else {
        char filename[RAFT_IO_UV_SEGMENT__MAX_FILENAME_LEN];
        raft_index end_index = *next_index - 1;

        /* At least one entry was loaded */
        assert(end_index >= first_index);

        raft_io_uv_segment__make_closed_filename(first_index, end_index,
                                                 filename);
        rv = raft_io_uv__rename(logger, dir, segment->filename, filename);
        if (rv != 0) {
            goto err_after_open;
        }

        segment->is_open = false;
        segment->first_index = first_index;
        segment->end_index = end_index;
        strcpy(segment->filename, filename);
    }

    return 0;

err_after_batch_load:
    raft_free(tmp_entries[0].batch);
    raft_free(tmp_entries);

err_after_open:
    close(fd);

err:
    assert(rv != 0);

    return rv;
}

/**
 * Load raft entries from the given segments.
 */
static int raft_io_uv_segment__load_all(struct raft_logger *logger,
                                        const char *dir,
                                        const raft_index start_index,
                                        struct raft_io_uv_segment *segments,
                                        size_t n_segments,
                                        struct raft_entry **entries,
                                        size_t *n_entries)
{
    raft_index next_index; /* Index of the next entry to load from disk */
    size_t i;
    int rv;

    assert(start_index >= 1);
    assert(n_segments > 0);
    assert(*entries == NULL);
    assert(*n_entries == 0);

    next_index = start_index;

    for (i = 0; i < n_segments; i++) {
        struct raft_io_uv_segment *segment = &segments[i];

        if (segment->is_open) {
            rv = raft_io_uv_segment__load_open(logger, dir, segment, entries,
                                               n_entries, &next_index);
            if (rv != 0) {
                goto err;
            }
        } else {
            rv = raft_io_uv_segment__load_closed(logger, dir, start_index,
                                                 segment, entries, n_entries,
                                                 &next_index);
            if (rv != 0) {
                goto err;
            }
        }
    }

    return 0;

err:
    assert(rv != 0);

    /* Free any batch that we might have allocated and the entries array as
     * well. */
    if (*entries != NULL) {
        void *batch = NULL;

        for (i = 0; i < *n_entries; i++) {
            struct raft_entry *entry = &(*entries)[i];

            if (entry->batch != batch) {
                batch = entry->batch;
                raft_free(batch);
            }
        }

        raft_free(*entries);
    }

    return rv;
}

/**
 * Reset the given prepared open segment metadata, assigning it the given
 * counter.
 */
static void raft_io_uv_prepared__reset(struct raft_io_uv_prepared *p,
                                       const char *dir,
                                       const unsigned long long counter,
                                       struct raft_io_uv_store *store)
{
    char filename[RAFT_IO_UV_SEGMENT__MAX_FILENAME_LEN];

    p->state = RAFT_IO_UV_STORE__PREPARED_PENDING;
    p->counter = counter;
    p->req.data = store;
    p->next_block = 0;
    p->block_size = store->block_size;
    p->used = 0;
    p->first_index = 0;
    p->end_index = 0;
    raft_io_uv_segment__make_open_filename(counter, filename);

    raft_uv_fs__join(dir, filename, p->path);
}

/**
 * Perform an asynchronous write against the given prepared open segment.
 */
static int raft_io_uv_prepared__write(struct raft_io_uv_prepared *p,
                                      struct raft_logger *logger,
                                      const uv_buf_t bufs[],
                                      unsigned n,
                                      raft_uv_fs_cb cb)
{
    int rv;
    size_t offset = p->next_block * p->block_size;

    rv = raft_uv_fs__write(&p->file, &p->req, bufs, n, offset, cb);
    if (rv != 0) {
        raft_errorf(logger, "write segment %lld: %s", p->counter,
                    uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Initialize the given block buffers.
 */
static void raft_io_uv_blocks__init(struct raft_io_uv_blocks *b,
                                    const size_t block_size)
{
    b->block_size = block_size;
    b->bufs = NULL;
    b->n_bufs = 0;
    b->offset = 0;
}

/**
 * Release all memory associated with the given block buffers.
 */
static void raft_io_uv_blocks__close(struct raft_io_uv_blocks *b)
{
    unsigned i;

    for (i = 0; i < b->n_bufs; i++) {
        if (b->bufs[i].base != NULL) {
            free(b->bufs[i].base);
        }
    }

    if (b->bufs != NULL) {
        free(b->bufs);
    }
}

/**
 * Make sure there that there are least @n block buffers.
 */
static int raft_io_uv_blocks__ensure_n(struct raft_io_uv_blocks *b,
                                       const unsigned n)
{
    uv_buf_t *bufs;
    unsigned i;
    int rv = 0;

    if (b->n_bufs >= n) {
        return 0;
    }

    bufs = raft_realloc(b->bufs, n * sizeof *b->bufs);
    if (bufs == NULL) {
        return RAFT_ERR_NOMEM;
    }

    for (i = b->n_bufs; i < n; i++) {
        uv_buf_t *buf = &bufs[i];

        buf->base = aligned_alloc(b->block_size, b->block_size);

        /* Don't break the loop, so we initialize all buffers (possibly to
         * NULL) */
        if (buf->base == NULL) {
            rv = RAFT_ERR_NOMEM;
        }
        buf->len = b->block_size;
    }

    b->bufs = bufs;
    b->n_bufs = n;

    return rv;
}

/**
 * Make sure that at least @size bytes are available in the block buffers,
 * considering that b->offset bytes are used.
 */
static int raft_io_uv_blocks__ensure_size(struct raft_io_uv_blocks *b,
                                          const size_t size)
{
    size_t needed = size; /* Number of bytes needed */
    unsigned n;

    /* If the offset parameter is non-zero, it means that there is at least one
     * block buffer and that it has some spare capacity. Let's account for
     * that. */
    if (b->offset > 0) {
        assert(b->n_bufs >= 1);
        needed += b->offset;
    }

    /* Calculate how many blocks we need. */
    n = (needed / b->block_size) + 1;

    /* Possibly allocate what we need */
    return raft_io_uv_blocks__ensure_n(b, n);
}

/**
 * Initialize the buffer of the first block of a new open segment, writing the
 * format version.
 */
static void raft_io_uv_blocks__put_format(uv_buf_t *buf)
{
    void *cursor = buf->base;

    memset(buf->base, 0, buf->len);

    raft__put64(&cursor, RAFT_IO_UV_STORE__FORMAT);
}

/**
 * Reset the block buffers so they can start to be used for writing a new
 * segment.
 *
 * The first block will be filled with the format version.
 */
static int raft_io_uv_blocks__reset(struct raft_io_uv_blocks *b)
{
    int rv;

    rv = raft_io_uv_blocks__ensure_n(b, 1);
    if (rv != 0) {
        return rv;
    }

    raft_io_uv_blocks__put_format(&b->bufs[0]);

    b->offset = sizeof(uint64_t); /* Format version. */

    return 0;
}

/**
 * Return a pointer to the next byte to be written.
 */
static void *raft_io_uv_blocks__cursor(struct raft_io_uv_blocks *b)
{
    unsigned block = b->offset / b->block_size; /* Block number to write */
    size_t k = b->offset % b->block_size;       /* Relative position */
    void *cursor = b->bufs[block].base + k;

    return cursor;
}

/**
 * Write a byte into the blocks.
 */
static void *raft_io_uv_blocks__put8(struct raft_io_uv_blocks *b, uint8_t value)
{
    void *cursor = raft_io_uv_blocks__cursor(b);

    *(uint8_t *)cursor = value;

    b->offset += sizeof value;

    return cursor;
}

/**
 * Write a 32-bit integer into the blocks.
 */
static void *raft_io_uv_blocks__put32(struct raft_io_uv_blocks *b,
                                      uint32_t value)
{
    void *cursor = raft_io_uv_blocks__cursor(b);

    *(uint32_t *)cursor = raft__flip32(value);

    b->offset += sizeof value;

    return cursor;
}

/**
 * Write a 64-bit integer into the blocks.
 */
static void *raft_io_uv_blocks__put64(struct raft_io_uv_blocks *b,
                                      uint64_t value)
{
    void *cursor = raft_io_uv_blocks__cursor(b);

    *(uint64_t *)cursor = raft__flip64(value);

    b->offset += sizeof value;

    return cursor;
}

/**
 * Write the entries batch header into the blocks.
 */
static void raft_io_uv_blocks__put_header(struct raft_io_uv_blocks *b,
                                          const struct raft_entry *entries,
                                          const unsigned n,
                                          unsigned *crc)
{
    unsigned i;
    void *data;

    *crc = 0;

    data = raft_io_uv_blocks__put64(b, n);
    *crc = raft__crc32(data, sizeof(uint64_t), *crc);

    for (i = 0; i < n; i++) {
        const struct raft_entry *entry = &entries[i];

        data = raft_io_uv_blocks__put64(b, entry->term);
        *crc = raft__crc32(data, sizeof(uint64_t), *crc);

        data = raft_io_uv_blocks__put8(b, entry->type);
        *crc = raft__crc32(data, sizeof(uint8_t), *crc);

        data = raft_io_uv_blocks__put8(b, 0);
        *crc = raft__crc32(data, sizeof(uint8_t), *crc);

        data = raft_io_uv_blocks__put8(b, 0);
        *crc = raft__crc32(data, sizeof(uint8_t), *crc);

        data = raft_io_uv_blocks__put8(b, 0);
        *crc = raft__crc32(data, sizeof(uint8_t), *crc);

        data = raft_io_uv_blocks__put32(b, entry->buf.len);
        *crc = raft__crc32(data, sizeof(uint32_t), *crc);
    }
}

/**
 * Write the entries data into the blocks.
 */
static void raft_io_uv_blocks__put_data(struct raft_io_uv_blocks *b,
                                        const struct raft_entry *entries,
                                        const unsigned n,
                                        unsigned *crc)
{
    unsigned i;

    *crc = 0;

    for (i = 0; i < n; i++) {
        const struct raft_entry *entry = &entries[i];
        uint8_t *bytes = entry->buf.base;
        unsigned j;
        void *data;

        /* TODO: we shouldn't copy the data byte by byte */
        for (j = 0; j < entry->buf.len; j++) {
            data = raft_io_uv_blocks__put8(b, bytes[j]);
            *crc = raft__crc32(data, sizeof(uint8_t), *crc);
        }

        if (entry->buf.len % 8 != 0) {
            /* Add padding */
            for (j = 0; j < 8 - (entry->buf.len % 8); j++) {
                data = raft_io_uv_blocks__put8(b, 0);
                *crc = raft__crc32(data, sizeof(uint8_t), *crc);
            }
        }
    }
}

/**
 * Write the given batch of entries into the blocks.
 *
 * The function will start writing from the current offset, and will encode the
 * batch according to the current format version.
 */
static int raft_io_uv_blocks__append_entries(struct raft_io_uv_blocks *b,
                                             const struct raft_entry *entries,
                                             const unsigned n)
{
    size_t size = raft_io_uv__sizeof_entries(entries, n);
    unsigned crc1; /* Header checksum */
    unsigned crc2; /* Data checksum */
    void *crc1_p;  /* Pointer to header checksum slot */
    void *crc2_p;  /* Pointer to data checksum slot */
    int rv;

    rv = raft_io_uv_blocks__ensure_size(b, size);
    if (rv != 0) {
        return rv;
    }

    /* Placeholder of the checksums */
    crc1_p = raft_io_uv_blocks__put32(b, 0);
    crc2_p = raft_io_uv_blocks__put32(b, 0);

    /* Batch header and data */
    raft_io_uv_blocks__put_header(b, entries, n, &crc1);
    raft_io_uv_blocks__put_data(b, entries, n, &crc2);

    /* Fill the checksums placehoders */
    *(uint32_t *)crc1_p = raft__flip32(crc1);
    *(uint32_t *)crc2_p = raft__flip32(crc2);

    return 0;
}

/**
 * Return the number of block buffers that have been written.
 */
static unsigned raft_io_uv_blocks__n_written(struct raft_io_uv_blocks *b)
{
    unsigned blocks = b->offset / b->block_size;

    if (b->offset % b->block_size != 0) {
        blocks++;
    }

    return blocks;
}

/**
 * Push the append callback to the given list.
 */
static int raft_io_uv_append_cb__push(const struct raft_io_uv_append_cb *cb,
                                      struct raft_io_uv_append_cb **cbs,
                                      size_t *n)
{
    struct raft_io_uv_append_cb *cbs_tmp;

    cbs_tmp = raft_realloc(*cbs, (*n + 1) * sizeof **cbs);
    if (cbs_tmp == NULL) {
        return RAFT_ERR_NOMEM;
    }

    *cbs = cbs_tmp;
    *n += 1;

    (*cbs)[(*n) - 1] = *cb;

    return 0;
}

/**
 * Invoke all the given append callbacks, passing them the given status.
 */
static void raft_io_uv_append_cb__invoke(const struct raft_io_uv_append_cb *cbs,
                                         const size_t n,
                                         int status)
{
    size_t i;

    for (i = 0; i < n; i++) {
        cbs[i].f(cbs[i].data, status);
    }
}

/**
 * Check that the given directory exists, and try to create it if it doesn't.
 */
static int raft_io_uv_store__ensure_dir(struct raft_logger *logger,
                                        const char *dir)
{
    struct stat sb;
    int rv;

    /* Ensure that the given path doesn't exceed our static buffer limit */
    if (strlen(dir) > RAFT_UV_FS_MAX_DIR_LEN) {
        return RAFT_ERR_IO_NAMETOOLONG;
    }

    /* Make sure we have a directory we can write into. */
    rv = stat(dir, &sb);
    if (rv != 0) {
        if (errno == ENOENT) {
            rv = mkdir(dir, 0700);
            if (rv != 0) {
                raft_errorf(logger, "create data directory '%s': %s", dir,
                            uv_strerror(-errno));
                return RAFT_ERR_IO;
            }
        } else {
            raft_errorf(logger, "access data directory '%s': %s", dir,
                        uv_strerror(-errno));
            return RAFT_ERR_IO;
        }
    } else if ((sb.st_mode & S_IFMT) != S_IFDIR) {
        raft_errorf(logger, "path '%s' is not a directory", dir);
        return RAFT_ERR_IO;
    }

    return 0;
}

int raft_io_uv_store__init(struct raft_io_uv_store *s,
                           struct raft_logger *logger,
                           struct uv_loop_s *loop,
                           const char *dir)
{
    int i;
    int rv;

    s->logger = logger;

    /* Make a copy of the directory string, stripping any trailing slash */
    s->dir = raft_malloc(strlen(dir) + 1);
    if (s->dir == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }
    strcpy(s->dir, dir);

    if (s->dir[strlen(s->dir) - 1] == '/') {
        s->dir[strlen(s->dir) - 1] = 0;
    }

    /* Ensure that we have a valid data directory */
    rv = raft_io_uv_store__ensure_dir(logger, s->dir);
    if (rv != 0) {
        goto err_after_dir_alloc;
    }

    /* Detect the file system block size */
    rv = raft_uv_fs__block_size(s->dir, &s->block_size);
    if (rv != 0) {
        raft_errorf(logger, "detect block size: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_dir_alloc;
    }

    /* This is should be changed only by unit tests */
    s->max_segment_size = RAFT_IO_UV_MAX_SEGMENT_SIZE;

    s->loop = loop;

    memset(&s->metadata, 0, sizeof s->metadata);

    s->preparer.segment = NULL;
    s->preparer.buf.base = NULL;
    s->preparer.buf.len = 0;

    s->writer.state = RAFT_IO_UV_STORE__WRITER_IDLE;

    raft_io_uv_blocks__init(&s->writer.blocks, s->block_size);

    s->writer.segment = NULL;
    s->writer.next_index = 0;
    s->writer.submitted = false;

    s->writer.cbs = NULL;
    s->writer.n_cbs = 0;

    for (i = 0; i < RAFT_IO_UV_STORE__N_PREPARED; i++) {
        raft_io_uv_prepared__reset(&s->pool[i], s->dir, i + 1, s);
    }

    s->queue.entries = NULL;
    s->queue.n_entries = 0;
    s->queue.cbs = NULL;
    s->queue.n_cbs = 0;

    s->closer.segment = NULL;
    s->closer.work.data = s;
    s->closer.status = 0;

    s->stop.p = NULL;
    s->stop.cb = NULL;

    s->aborted = false;

    return 0;

err_after_dir_alloc:
    raft_free(s->dir);

err:
    assert(rv != 0);
    return rv;
}

int raft_io_uv_store__start(struct raft_io_uv_store *s)
{
    /* A the moment this is a no-op. */
    (void)s;

    return 0;
}

/**
 * Update both metadata files, so they are created if they didn't exist.
 */
static int raft_io_uv_store__ensure_metadata(struct raft_io_uv_store *s)
{
    int i;
    int rv;

    /* Update both metadata files, so they are created if they didn't
     * exist. Also sync the data directory so the entries get created. */
    for (i = 0; i < 2; i++) {
        s->metadata.version++;
        rv = raft_io_uv_metadata__store(s->logger, s->dir, &s->metadata);
        if (rv != 0) {
            return rv;
        }
    }

    /* Also sync the data directory so the entries get created. */
    rv = raft_io_uv__sync_dir(s->logger, s->dir);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static int raft_io_uv_store__load_metadata(struct raft_io_uv_store *s)
{
    struct raft_io_uv_metadata metadata1;
    struct raft_io_uv_metadata metadata2;
    int rv;

    /* This API is supposed to be invoked just once. */
    assert(s->metadata.version == 0);

    /* Read the two metadata files (if available). */
    rv = raft_io_uv_metadata__load(s->logger, s->dir, 1, &metadata1);
    if (rv != 0) {
        return rv;
    }

    rv = raft_io_uv_metadata__load(s->logger, s->dir, 2, &metadata2);
    if (rv != 0) {
        return rv;
    }

    /* Check the versions. */
    if (metadata1.version == 0 && metadata2.version == 0) {
        /* Neither metadata file exists: have a brand new server. */
        s->metadata.start_index = 1;
    } else if (metadata1.version == metadata2.version) {
        /* The two metadata files can't have the same version. */
        raft_errorf(s->logger, "metadata1 and metadata2 are both at version %d",
                    metadata1.version);
        return RAFT_ERR_IO_CORRUPT;
    } else {
        /* Pick the metadata with the grater version. */
        if (metadata1.version > metadata2.version) {
            s->metadata = metadata1;
        } else {
            s->metadata = metadata2;
        }
    }

    /* Update the metadata files, so they are created if they did not exist. */
    rv = raft_io_uv_store__ensure_metadata(s);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_io_uv_store__load(struct raft_io_uv_store *s,
                           raft_term *term,
                           unsigned *voted_for,
                           raft_index *start_index,
                           struct raft_entry **entries,
                           size_t *n)
{
    struct raft_io_uv_segment *segments;
    size_t n_segments;
    int rv;

    if (s->aborted) {
        return RAFT_ERR_IO_ABORTED;
    }

    /* If haven't loaded the metadata yet, do it now. */
    if (s->metadata.version == 0) {
        rv = raft_io_uv_store__load_metadata(s);
        if (rv != 0) {
            goto err;
        }
    }

    *term = s->metadata.term;
    *voted_for = s->metadata.voted_for;
    *start_index = s->metadata.start_index;

    /* List available segments. */
    rv = raft_io_uv_segment__list(s->logger, s->dir, &segments, &n_segments);
    if (rv != 0) {
        goto err;
    }

    *entries = NULL;
    *n = 0;

    /* Read data from segments, closing any open segments. */
    if (segments != NULL) {
        rv = raft_io_uv_segment__load_all(s->logger, s->dir,
                                          s->metadata.start_index, segments,
                                          n_segments, entries, n);
        raft_free(segments);

        if (rv != 0) {
            goto err;
        }
    }

    /* Save the index of the next entry that will be appended. */
    s->writer.last_index = s->metadata.start_index + *n - 1;
    s->writer.next_index = s->writer.last_index + 1;

    return 0;

err:
    assert(rv != 0);

    *term = 0;
    *voted_for = 0;
    *start_index = 0;
    *entries = NULL;
    *n = 0;

    s->aborted = true;

    return rv;
}

/**
 * Write the first block of the first closed segment.
 */
static int raft_io_uv_store__write_closed_1_1(struct raft_io_uv_store *s,
                                              const int fd,
                                              const struct raft_buffer *conf)
{
    void *buf;
    size_t len;
    size_t cap;
    void *cursor;
    void *header;
    unsigned crc1; /* Header checksum */
    unsigned crc2; /* Data checksum */
    void *crc1_p;  /* Pointer to header checksum slot */
    void *crc2_p;  /* Pointer to data checksum slot */
    int rv;

    /* Make sure that the given encoded configuration fits in the first block */
    cap = s->block_size - (sizeof(uint64_t) /* Format version */ +
                           sizeof(uint64_t) /* Checksums */ +
                           raft_io_uv_sizeof__batch_header(1));
    if (conf->len > cap) {
        return RAFT_ERR_IO_TOOBIG;
    }

    len = sizeof(uint64_t) * 2 + raft_io_uv_sizeof__batch_header(1) + conf->len;
    buf = malloc(len);
    if (buf == NULL) {
        return RAFT_ERR_NOMEM;
    }
    memset(buf, 0, len);

    cursor = buf;

    raft__put64(&cursor, RAFT_IO_UV_STORE__FORMAT); /* Format version */

    crc1_p = cursor;
    raft__put32(&cursor, 0);

    crc2_p = cursor;
    raft__put32(&cursor, 0);

    header = cursor;
    raft__put64(&cursor, 1);                     /* Number of entries */
    raft__put64(&cursor, 1);                     /* Entry term */
    raft__put8(&cursor, RAFT_LOG_CONFIGURATION); /* Entry type */
    raft__put8(&cursor, 0);                      /* Unused */
    raft__put8(&cursor, 0);                      /* Unused */
    raft__put8(&cursor, 0);                      /* Unused */
    raft__put32(&cursor, conf->len);             /* Size of entry data */

    memcpy(cursor, conf->base, conf->len);

    crc1 = raft__crc32(header, raft_io_uv_sizeof__batch_header(1), 0);
    raft__put32(&crc1_p, crc1);

    crc2 = raft__crc32(conf->base, conf->len, 0);
    raft__put32(&crc2_p, crc2);

    rv = write(fd, buf, len);
    if (rv == -1) {
        free(buf);
        raft_errorf(s->logger, "write segment 1: %s", strerror(errno));
        return RAFT_ERR_IO;
    }
    if (rv != (int)len) {
        free(buf);
        raft_errorf(s->logger, "write segment 1: only %d bytes written", rv);
        return RAFT_ERR_IO;
    }

    free(buf);

    rv = fsync(fd);
    if (rv == -1) {
        raft_errorf(s->logger, "fsync segment 1: %s", strerror(errno));
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Open and allocate the first closed segment, containing just one entry, and
 * return its file descriptor.
 */
static int raft_io_uv_store__create_closed_1_1(struct raft_io_uv_store *s,
                                               const struct raft_buffer *conf)
{
    raft_uv_path path;
    char filename[RAFT_IO_UV_SEGMENT__MAX_FILENAME_LEN];
    int fd;
    int rv;

    /* Render the path */
    raft_io_uv_segment__make_closed_filename(1, 1, filename);
    raft_uv_fs__join(s->dir, filename, path);

    /* Open the file. */
    fd = open(path, O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        raft_errorf(s->logger, "open '%s': %s", path, strerror(errno));
        return RAFT_ERR_IO;
    }

    /* Write the content */
    rv = raft_io_uv_store__write_closed_1_1(s, fd, conf);
    if (rv != 0) {
        return rv;
    }

    close(fd);

    rv = raft_io_uv__sync_dir(s->logger, s->dir);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static bool raft_io_uv_store__writer_is_idle(struct raft_io_uv_store *s);

int raft_io_uv_store__bootstrap(struct raft_io_uv_store *s,
                                const struct raft_configuration *configuration)
{
    struct raft_buffer buf;
    int rv;

    assert(raft_io_uv_store__writer_is_idle(s));

    /* If haven't loaded the metadata yet, do it now. */
    if (s->metadata.version == 0) {
        rv = raft_io_uv_store__load_metadata(s);
        if (rv != 0) {
            goto err;
        }
    }

    /* We shouldn't have written anything else yet. */
    if (s->metadata.term != 0) {
        rv = RAFT_ERR_IO_NOTEMPTY;
        goto err;
    }

    /* Encode the given configuration. */
    rv = raft_configuration_encode(configuration, &buf);
    if (rv != 0) {
        goto err;
    }

    /* Write the term */
    rv = raft_io_uv_store__term(s, 1);
    if (rv != 0) {
        goto err_after_configuration_encode;
    }

    /* Create the first closed segment file, containing just one entry. */
    rv = raft_io_uv_store__create_closed_1_1(s, &buf);
    if (rv != 0) {
        goto err_after_configuration_encode;
    }

    raft_free(buf.base);

    return 0;

err_after_configuration_encode:
    raft_free(buf.base);

err:
    assert(rv != 0);
    return rv;
}

int raft_io_uv_store__term(struct raft_io_uv_store *s, const raft_term term)
{
    int rv;

    if (s->aborted) {
        return RAFT_ERR_IO_ABORTED;
    }

    assert(s->metadata.version > 0);

    s->metadata.version++;
    s->metadata.term = term;
    s->metadata.voted_for = 0;

    rv = raft_io_uv_metadata__store(s->logger, s->dir, &s->metadata);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
    assert(rv != 0);

    s->aborted = true;

    return rv;
}

int raft_io_uv_store__vote(struct raft_io_uv_store *s, const unsigned server_id)
{
    int rv;

    if (s->aborted) {
        return RAFT_ERR_IO_ABORTED;
    }

    assert(s->metadata.version > 0);

    s->metadata.version++;
    s->metadata.voted_for = server_id;

    rv = raft_io_uv_metadata__store(s->logger, s->dir, &s->metadata);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
    assert(rv != 0);

    s->aborted = true;

    return rv;
}

/**
 * Return the value of the highest counter in the pool.
 */
static unsigned long long raft_io_uv_store__pool_counter(
    struct raft_io_uv_store *s)
{
    unsigned i;
    unsigned long long counter = 0;

    for (i = 0; i < RAFT_IO_UV_STORE__N_PREPARED; i++) {
        struct raft_io_uv_prepared *prepared;

        prepared = &s->pool[i];

        if (prepared->counter > counter) {
            counter = prepared->counter;
        }
    }

    return counter;
}

/**
 * Return the index of the next open segment in the pool which is the given
 * state and has the lowest counter.
 *
 * Return #RAFT_IO_UV_STORE__N_PREPARED if no pool segment is in the given
 * state.
 */
static unsigned raft_io_uv_store__pool_index(struct raft_io_uv_store *s,
                                             int state)
{
    unsigned i;
    unsigned j = RAFT_IO_UV_STORE__N_PREPARED;
    unsigned long long counter = ~0ULL; /* Max possible value */
    struct raft_io_uv_prepared *prepared;

    /* Find the non-ready segment with the lowest counter */
    for (i = 0; i < RAFT_IO_UV_STORE__N_PREPARED; i++) {
        prepared = &s->pool[i];

        if (prepared->state == state && prepared->counter < counter) {
            counter = prepared->counter;
            j = i;
        }
    }

    return j;
}

/**
 * Return the open segment in pool which is in the given state and has the
 * lowest counter, or #NULL if no pool segment is in the given state.
 */
static struct raft_io_uv_prepared *raft_io_uv_store__pool_get(
    struct raft_io_uv_store *s,
    int state)
{
    unsigned i;
    struct raft_io_uv_prepared *prepared;

    i = raft_io_uv_store__pool_index(s, state);

    if (i == RAFT_IO_UV_STORE__N_PREPARED) {
        return NULL;
    }

    assert(i < RAFT_IO_UV_STORE__N_PREPARED);

    prepared = &s->pool[i];

    assert(prepared->state == state);

    return prepared;
}

/**
 * Return the pending open segment with the lowest counter, or #NULL.
 */
static struct raft_io_uv_prepared *raft_io_uv_store__pool_get_pending(
    struct raft_io_uv_store *s)
{
    int state = RAFT_IO_UV_STORE__PREPARED_PENDING;

    return raft_io_uv_store__pool_get(s, state);
}

/**
 * Return the ready open segment with the lowest counter, or #NULL.
 */
static struct raft_io_uv_prepared *raft_io_uv_store__pool_get_ready(
    struct raft_io_uv_store *s)
{
    int state = RAFT_IO_UV_STORE__PREPARED_READY;

    return raft_io_uv_store__pool_get(s, state);
}

/**
 * Return the closing open segment with the lowest counter, or #NULL.
 */
static struct raft_io_uv_prepared *raft_io_uv_store__pool_get_closing(
    struct raft_io_uv_store *s)
{
    int state = RAFT_IO_UV_STORE__PREPARED_CLOSING;

    return raft_io_uv_store__pool_get(s, state);
}

/**
 * Return true if there's currently no write request being processed.
 */
static bool raft_io_uv_store__writer_is_idle(struct raft_io_uv_store *s)
{
    return s->writer.state == RAFT_IO_UV_STORE__WRITER_IDLE;
}

/**
 * Return true if the writer is not idle.
 */
static bool raft_io_uv_store__writer_is_active(struct raft_io_uv_store *s)
{
    return s->writer.state != RAFT_IO_UV_STORE__WRITER_IDLE;
}

/**
 * Return true if the writer is waiting for a prepared segment to become ready.
 */
static bool raft_io_uv_store__writer_is_blocked(struct raft_io_uv_store *s)
{
    if (s->writer.state == RAFT_IO_UV_STORE__WRITER_BLOCKED) {
        /* We must be waiting for a ready segment. */
        assert(s->writer.segment == NULL);
        return true;
    }

    return false;
}

/**
 * Return true if the writer is actively writing a request.
 */
static bool raft_io_uv_store__writer_is_writing(struct raft_io_uv_store *s)
{
    return s->writer.state == RAFT_IO_UV_STORE__WRITER_WRITING;
}

/**
 * Return true if there's currently a segment being closed.
 */
static bool raft_io_uv_store__closer_is_active(struct raft_io_uv_store *s)
{
    return s->closer.segment != NULL;
}

/**
 * Return true if there's currently a segment being prepared.
 */
static bool raft_io_uv_store__preparer_is_active(struct raft_io_uv_store *s)
{
    return s->preparer.segment != NULL;
}

/**
 * Return true if we've been requested to stop.
 */
static bool raft_io_uv_store__is_stopping(struct raft_io_uv_store *s)
{
    return s->stop.cb != NULL;
}

/**
 * This function is called whenever the preparer, the closer or the writer hit
 * an error, or whenever a stop request was issued with @raft_io_uv_store__stop.
 *
 * If no pending activity is in progress the stop callback will be invoked.
 */
static void raft_io_uv_store__aborted(struct raft_io_uv_store *s)
{
    unsigned i;
    void *data = s->stop.p;
    void (*cb)(void *data) = s->stop.cb;
    int rv;

    assert(s->aborted);

    /* Check if any activity is still in progress */
    if (!raft_io_uv_store__is_stopping(s) ||
        raft_io_uv_store__preparer_is_active(s) ||
        raft_io_uv_store__closer_is_active(s) ||
        raft_io_uv_store__writer_is_active(s)) {
        return;
    }

    assert(cb != NULL);

    /* Close all prepared open segments which are ready. */
    for (i = 0; i < RAFT_IO_UV_STORE__N_PREPARED; i++) {
        struct raft_io_uv_prepared *prepared = &s->pool[i];

        if (prepared->state == RAFT_IO_UV_STORE__PREPARED_READY) {
            rv = raft_uv_fs__close(&prepared->file);
            assert(rv == 0); /* TODO: can this fail? */
        }
    }

    /* For idempotency: further calls to this function will no-op. */
    memset(&s->stop, 0, sizeof s->stop);

    cb(data);
}

/**
 * Run all blocking syscalls involved in closing a segment. This is run a worker
 * thread.
 *
 * An open segment is closed by truncating its length to the number of bytes
 * that were actually written into it and then renaming it.
 */
static void raft_io_uv_store__closer_work_cb(uv_work_t *work)
{
    struct raft_io_uv_store *s = work->data;
    char path[RAFT_UV_FS_MAX_PATH_LEN];
    char filename1[RAFT_IO_UV_SEGMENT__MAX_FILENAME_LEN];
    char filename2[RAFT_IO_UV_SEGMENT__MAX_FILENAME_LEN];
    int fd;
    int rv;

    assert(raft_io_uv_store__closer_is_active(s));

    raft_io_uv_segment__make_open_filename(s->closer.segment->counter,
                                           filename1);
    raft_io_uv_segment__make_closed_filename(s->closer.segment->first_index,
                                             s->closer.segment->end_index,
                                             filename2);

    /* Truncate and rename the segment */
    raft_uv_fs__join(s->dir, filename1, path);

    fd = open(path, O_RDWR);
    if (fd == -1) {
        raft_errorf(s->logger, "open '%s': %s", path, uv_strerror(-errno));
        rv = RAFT_ERR_IO;
        goto abort;
    }

    rv = raft_io_uv__truncate(s->logger, fd, s->closer.segment->used);
    if (rv != 0) {
        goto abort;
    }

    rv = raft_io_uv__rename(s->logger, s->dir, filename1, filename2);
    if (rv != 0) {
        goto abort;
    }

    return;

abort:
    assert(rv != 0);

    s->closer.status = rv;
}

/* Forward declaration */
static void raft_io_uv_store__writer_finish(struct raft_io_uv_store *s);

/* Forward declaration */
static int raft_io_uv_store__preparer_start(struct raft_io_uv_store *s);

/**
 * Invoked after the work performed in threadpool has completed. This is run in
 * the main thread.
 */
static void raft_io_uv_store__closer_after_work_cb(uv_work_t *work, int status)
{
    struct raft_io_uv_store *s = work->data;
    bool succeeded = s->closer.status == 0;
    unsigned long long counter = raft_io_uv_store__pool_counter(s) + 1;
    int rv;

    assert(raft_io_uv_store__closer_is_active(s));

    assert(status == 0); /* We don't cancel worker requests */

    /* If in the meantime we have been aborted, let's bail out. */
    if (s->aborted) {
        rv = RAFT_ERR_IO_ABORTED;
        goto abort;
    }

    /* If the closing logic failed, let's bail out. */
    if (!succeeded) {
        rv = s->closer.status;
        goto abort;
    }

    /* Assign to the segment a new counter and mark it as pending. */
    raft_io_uv_prepared__reset(s->closer.segment, s->dir, counter, s);

    /* We don't have anything else to do for now, */
    s->closer.segment = NULL;

    /* Wakeup the preparer if needed, since we are short of one prepared open
     * segment now. */
    if (!raft_io_uv_store__preparer_is_active(s)) {
        rv = raft_io_uv_store__preparer_start(s);
        if (rv != 0) {
            goto abort;
        }
    }

    return;

abort:
    s->aborted = true;

    s->closer.segment = NULL;

    assert(rv != 0);

    /* If there's a pending write request waiting for a segment to be ready, and
     * no preparer logic is in progress, let's fail the request ourselves, since
     * we're not going to wake up the preparer. */
    if (raft_io_uv_store__writer_is_blocked(s) &&
        !raft_io_uv_store__preparer_is_active(s)) {
        s->writer.status = rv;
        raft_io_uv_store__writer_finish(s);
    }

    /* Possibly invoke the stop callback. */
    raft_io_uv_store__aborted(s);
}

/**
 * Trigger the closer logic in a work thread which will close all closing
 * segment.
 */
static int raft_io_uv_store__closer_start(struct raft_io_uv_store *s)
{
    int rv;

    /* We haven't aborted. */
    assert(!s->aborted);

    /* We're not already active */
    assert(!raft_io_uv_store__closer_is_active(s));

    /* Pick the closing segment with the lowest counter. */
    s->closer.segment = raft_io_uv_store__pool_get_closing(s);

    /* Check that we've been invoked for a reason and there's actually something
     * to do. */
    assert(s->closer.segment != NULL);

    rv = uv_queue_work(s->loop, &s->closer.work,
                       raft_io_uv_store__closer_work_cb,
                       raft_io_uv_store__closer_after_work_cb);
    if (rv != 0) {
        raft_errorf(s->logger, "uv_queue_work: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    return 0;
}

static size_t raft_io_uv_store__queue_size(struct raft_io_uv_store *s);

static int raft_io_uv_store__writer_start(
    struct raft_io_uv_store *s,
    const struct raft_entry *entries,
    const unsigned n,
    const struct raft_io_uv_append_cb *cbs,
    size_t n_cbs);

/**
 * Reset our internal state and invoke the appended entries callback.
 */
static void raft_io_uv_store__writer_finish(struct raft_io_uv_store *s)
{
    int status;

    assert(raft_io_uv_store__writer_is_active(s));

    status = s->writer.status;

    s->writer.state = RAFT_IO_UV_STORE__WRITER_IDLE;
    s->writer.status = 0;

    raft_io_uv_append_cb__invoke(s->writer.cbs, s->writer.n_cbs, status);

    s->writer.n_cbs = 0;

    if (s->stop.p != NULL) {
        raft_io_uv_store__aborted(s);
        return;
    }

    /* If we have queued some more write requests, let's trigger them. */
    if (s->queue.n_entries > 0) {
        int rv;

        assert(s->queue.entries != NULL);
        assert(s->queue.n_cbs > 0);

        rv = raft_io_uv_store__writer_start(s, s->queue.entries,
                                            s->queue.n_entries, s->queue.cbs,
                                            s->queue.n_cbs);
        if (rv != 0) {
            s->aborted = true;
            raft_io_uv_append_cb__invoke(s->queue.cbs, s->queue.n_cbs, status);
        }

        s->queue.n_entries = 0;
        s->queue.n_cbs = 0;
    }
}

/**
 * Mark the segment currently being written as closing.
 */
static void raft_io_uv_store__writer_segment_full(struct raft_io_uv_store *s)
{
    int rv;

    rv = raft_uv_fs__close(&s->writer.segment->file);
    assert(rv == 0); /* TODO: can this fail? */

    s->writer.segment->end_index = s->writer.last_index;
    s->writer.segment->state = RAFT_IO_UV_STORE__PREPARED_CLOSING;
}

/**
 * Called after a segment disk write has been completed.
 */
static void raft_io_uv_store__writer_submit_cb(struct raft_uv_fs *req)
{
    struct raft_io_uv_store *s = req->data;
    size_t offset = s->writer.blocks.offset;
    unsigned blocks = raft_io_uv_blocks__n_written(&s->writer.blocks);

    /* Check that we have written the expected number of bytes */
    if (req->status != (int)(blocks * s->block_size)) {
        s->writer.status = RAFT_ERR_IO;
        if (req->status < 0) {
            raft_errorf(s->logger, "write: %s", uv_strerror(req->status));
        } else {
            raft_errorf(s->logger, "only %d bytes written", req->status);
        }
        goto done;
    }

    s->writer.status = 0;
    s->writer.last_index = s->writer.next_index - 1;
    s->writer.segment->used += offset;

    /* Update the state of the write buffers.
     *
     * We have four cases:
     *
     * - The data fit completely in the leftover space of the first block
     * and there is more space left. In this case we just advance the first
     * block offset.
     *
     * - The data fit completely in the leftover space of the first block and
     *   there is no space left. In this case we advance the current block
     *   counter, reset the first write buffer and set its offset to 0.
     *
     * - The data did not fit completely in the leftover space of the first
     *   block, so we wrote more than one block. The last block we wrote was not
     *   filled completely and has leftover space. In this case we advance the
     *   current block counter and copy the write buffer used for the last block
     *   to the head of the write buffers list, updating its offset.
     *
     * - The data did not fit completely in the leftover space of the first
     *   block, so we wrote more than one block. The last block we wrote was
     *   filled exactly and has no leftover space. In this case we advance the
     *   current block counter, reset the first buffer and set its offset to 0.
     */
    if (offset < s->block_size) {
        /* Nothing to do */
        assert(blocks == 1);
    } else if (offset == s->block_size) {
        assert(blocks == 1);
        s->writer.segment->next_block++;
        s->writer.blocks.offset = 0;
        memset(s->writer.blocks.bufs[0].base, 0, s->block_size);
    } else {
        assert(offset > s->block_size);
        assert(blocks > 1);

        if (offset % s->block_size > 0) {
            s->writer.segment->next_block += blocks - 1;
            s->writer.blocks.offset = offset % s->block_size;
            memcpy(s->writer.blocks.bufs[0].base,
                   s->writer.blocks.bufs[blocks - 1].base, s->block_size);
        } else {
            s->writer.segment->next_block += blocks;
            s->writer.blocks.offset = 0;
            memset(s->writer.blocks.bufs[0].base, 0, s->block_size);
        }
    }

done:
    raft_io_uv_store__writer_finish(s);
}

/**
 * Push the given entries to the writer by encoding the given entries into the
 * block buffers and making a copy of the given callbacks.
 */
static int raft_io_uv_store__writer_push(struct raft_io_uv_store *s,
                                         const struct raft_entry *entries,
                                         const unsigned n,
                                         const struct raft_io_uv_append_cb *cbs,
                                         const unsigned n_cbs)
{
    int rv;
    unsigned i;

    assert(!raft_io_uv_store__writer_is_writing(s));

    /* Encode the given entries into the block buffers. */
    rv = raft_io_uv_blocks__append_entries(&s->writer.blocks, entries, n);
    if (rv != 0) {
        return rv;
    }

    for (i = 0; i < n_cbs; i++) {
        const struct raft_io_uv_append_cb *cb = &cbs[i];

        rv = raft_io_uv_append_cb__push(cb, &s->writer.cbs, &s->writer.n_cbs);
        if (rv != 0) {
            return rv;
        }
    }

    s->writer.next_index += n;

    return 0;
}

/**
 * Submit an I/O write request to persist the entries that were passed to
 * @raft_io_uv_store__entries.
 */
static int raft_io_uv_store__writer_submit(struct raft_io_uv_store *s)
{
    unsigned blocks; /* Number of blocks to write */
    int rv;

    assert(!raft_io_uv_store__writer_is_writing(s));

    s->writer.state = RAFT_IO_UV_STORE__WRITER_WRITING;

    /* We must have a ready prepared segment at this point. */
    assert(s->writer.segment != NULL);
    assert(s->writer.segment->state == RAFT_IO_UV_STORE__PREPARED_READY);

    /* The write request can't be empty and must be aligned */
    assert(s->writer.blocks.offset > 0);
    assert(s->writer.blocks.offset % 8 == 0);

    /* The size can't exceed the remaining capacity of the segment. */
    assert(s->writer.blocks.offset <=
           s->max_segment_size - s->writer.segment->used);

    blocks = raft_io_uv_blocks__n_written(&s->writer.blocks);

    rv = raft_io_uv_prepared__write(s->writer.segment, s->logger,
                                    s->writer.blocks.bufs, blocks,
                                    raft_io_uv_store__writer_submit_cb);
    if (rv != 0) {
        return RAFT_ERR_IO;
        goto fail;
    }

    s->writer.submitted = true;

    return 0;

fail:
    assert(rv != 0);

    return rv;
}

/**
 * Try to start the writer right away, submitting the given entries.
 *
 * If the size of the given entries exceed the available size in the current
 * segment, close the segment and possibly wait for a newly prepared one.
 */
static int raft_io_uv_store__writer_start(
    struct raft_io_uv_store *s,
    const struct raft_entry *entries,
    const unsigned n,
    const struct raft_io_uv_append_cb *cbs,
    size_t n_cbs)
{
    bool overflow;
    size_t size;
    int rv;

    assert(raft_io_uv_store__writer_is_idle(s));
    assert(s->writer.segment != NULL);

    size = raft_io_uv__sizeof_entries(entries, n);
    overflow = size > s->max_segment_size - s->writer.segment->used;

    /* If there's not enough room available in the segment, let's reset the
     * block buffers since we need to start a new segment. */
    if (overflow) {
        rv = raft_io_uv_blocks__reset(&s->writer.blocks);
        if (rv != 0) {
            return rv;
        }
    }

    rv = raft_io_uv_store__writer_push(s, entries, n, cbs, n_cbs);
    if (rv != 0) {
        return rv;
    }

    /* If there's not enough room available in the segment, we need to mark
     * this segment as closing, possibly wake up the closer and/or the
     * preparer. */
    if (overflow) {
        raft_io_uv_store__writer_segment_full(s);

        if (!raft_io_uv_store__closer_is_active(s)) {
            rv = raft_io_uv_store__closer_start(s);
            if (rv != 0) {
                return rv;
            }
        }

        if (!raft_io_uv_store__preparer_is_active(s)) {
            rv = raft_io_uv_store__preparer_start(s);
            if (rv != 0) {
                return rv;
            }
        }

        s->writer.segment = raft_io_uv_store__pool_get_ready(s);

        /* If there's no ready prepared segment available, we need to
         * wait. */
        if (s->writer.segment == NULL) {
            s->writer.state = RAFT_IO_UV_STORE__WRITER_BLOCKED;
            return 0;
        }

        s->writer.segment->first_index = s->writer.last_index + 1;
    }

    rv = raft_io_uv_store__writer_submit(s);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

/**
 * Called when the preparer finishes to prepare a new open segment and wants
 * to wake up the writer, which can now proceed writing to the newly
 * prepared segment.
 */
static int raft_io_uv_store__writer_unblock(struct raft_io_uv_store *s)
{
    int rv;

    assert(raft_io_uv_store__preparer_is_active(s));
    assert(s->preparer.segment->state == RAFT_IO_UV_STORE__PREPARED_READY);
    assert(s->writer.segment == NULL);

    /* Use the newly prepared segment from now on. */
    s->writer.segment = s->preparer.segment;
    s->writer.segment->first_index = s->writer.last_index + 1;

    /* Start writing. */
    rv = raft_io_uv_store__writer_submit(s);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

/**
 * Return the size of the entries currently in the queue.
 */
static size_t raft_io_uv_store__queue_size(struct raft_io_uv_store *s)
{
    size_t size = 0;

    if (s->queue.n_entries > 0) {
        size +=
            raft_io_uv__sizeof_entries(s->queue.entries, s->queue.n_entries);
    }

    return size;
}

/**
 * Push the given append request to the queue of pending ones that are waiting
 * for the current write to complete.
 */
static int raft_io_uv_store__queue_push(struct raft_io_uv_store *s,
                                        const struct raft_entry *entries,
                                        const unsigned n,
                                        const struct raft_io_uv_append_cb *cb)
{
    int rv;

    assert(!raft_io_uv_store__writer_is_idle(s));

    rv = raft_io_uv__extend_entries(entries, n, &s->queue.entries,
                                    &s->queue.n_entries);
    if (rv != 0) {
        return rv;
    }

    rv = raft_io_uv_append_cb__push(cb, &s->queue.cbs, &s->queue.n_cbs);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

/**
 * Return true if there are currently pending segments waiting to be
 * prepared.
 */
static bool raft_io_uv_store__preparer_has_work(struct raft_io_uv_store *s)
{
    unsigned i;

    i = raft_io_uv_store__pool_index(s, RAFT_IO_UV_STORE__PREPARED_PENDING);
    return i < RAFT_IO_UV_STORE__N_PREPARED;
}

/**
 * Callback invoked after completing writing the format version of a new
 * prepared open segment.
 */
static void raft_io_uv_store__preparer_format_cb(struct raft_uv_fs *req)
{
    struct raft_io_uv_store *s = req->data;
    bool succeeded = req->status == (int)s->block_size;
    int rv;
    int rv2;

    assert(raft_io_uv_store__preparer_is_active(s));

    /* If in the meantime we have been aborted, let's bail out. */
    if (s->aborted) {
        rv = RAFT_ERR_IO_ABORTED;
        goto abort;
    }

    /* If the write failed, let's bail out. */
    if (!succeeded) {
        raft_errorf(s->logger, "format segment '%s': %s",
                    s->preparer.segment->path, uv_strerror(req->status));
        rv = RAFT_ERR_IO;
        goto abort;
    }

    /* Double check that we're pointing at the beginning of the segment and that
     * the segment is marked as pending. */
    assert(s->preparer.segment->next_block == 0);
    assert(s->preparer.segment->state == RAFT_IO_UV_STORE__PREPARED_PENDING);

    /* Mark the prepared segment as ready */
    s->preparer.segment->state = RAFT_IO_UV_STORE__PREPARED_READY;

    /* If there's a pending write request which is waiting for a segment to
     * be ready, let's resume it. */
    if (raft_io_uv_store__writer_is_blocked(s)) {
        rv = raft_io_uv_store__writer_unblock(s);
        if (rv != 0) {
            goto abort;
        }
    }

    /* We're done with this particular segment. */
    s->preparer.segment = NULL;

    /* If the preparation was successful and there are more segments that
     * need to be prepared, let's start the preparer again. */
    if (raft_io_uv_store__preparer_has_work(s)) {
        rv = raft_io_uv_store__preparer_start(s);
        if (rv != 0) {
            goto abort;
        }
    }

    return;

abort:
    s->aborted = true;

    assert(rv != 0);

    rv2 = raft_uv_fs__close(&s->preparer.segment->file);
    assert(rv2 == 0); /* TODO: can this fail? */

    s->preparer.segment->state = RAFT_IO_UV_STORE__PREPARED_PENDING;
    s->preparer.segment = NULL;

    /* If there's a pending write request waiting for a segment to be ready,
     * let's fail it. */
    if (raft_io_uv_store__writer_is_blocked(s)) {
        s->writer.status = rv;
        raft_io_uv_store__writer_finish(s);
    }

    /* If we have been stopped, invoke the callback. */
    raft_io_uv_store__aborted(s);
}

/**
 * Submit a write request for the format version of a newly created prepared
 * segment.
 */
static int raft_io_uv_store__preparer_format(struct raft_io_uv_store *s)
{
    int rv;

    /* If not done already, allocate the buffer to use for writing the
     * format version */
    if (s->preparer.buf.base == NULL) {
        s->preparer.buf.base = aligned_alloc(s->block_size, s->block_size);

        if (s->preparer.buf.base == NULL) {
            return RAFT_ERR_NOMEM;
        }

        s->preparer.buf.len = s->block_size;
        raft_io_uv_blocks__put_format(&s->preparer.buf);
    }

    rv = raft_uv_fs__write(&s->preparer.segment->file,
                           &s->preparer.segment->req, &s->preparer.buf, 1, 0,
                           raft_io_uv_store__preparer_format_cb);
    if (rv != 0) {
        raft_errorf(s->logger, "request formatting of open segment '%s': %s",
                    s->preparer.segment->path, uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Callback invoked when the open segment file being prepared has been
 * created and its space allocated on disk. We now need to write its format
 * version.
 */
static void raft_io_uv_store__preparer_create_cb(struct raft_uv_fs *req)
{
    struct raft_io_uv_store *s = req->data;
    bool succeeded = req->status == 0;
    int rv;
    int rv2;

    assert(raft_io_uv_store__preparer_is_active(s));

    assert(s->preparer.segment->state == RAFT_IO_UV_STORE__PREPARED_PENDING);

    /* If in the meantime we have been aborted, let's bail out. */
    if (s->aborted) {
        rv = RAFT_ERR_IO_ABORTED;
        goto abort;
    }

    /* If the file creation failed, let's bail out. */
    if (!succeeded) {
        rv = RAFT_ERR_IO;
        raft_errorf(s->logger, "create open segment '%s': %s",
                    s->preparer.segment->path, uv_strerror(req->status));
        goto abort;
    }

    /* Request to write the format version of the prepared segment. */
    rv = raft_io_uv_store__preparer_format(s);
    if (rv != 0) {
        goto abort;
    }

    return;

abort:
    assert(rv != 0);

    s->aborted = true;

    /* If we managed to create the file, let's close it. */
    if (succeeded) {
        rv2 = raft_uv_fs__close(&s->preparer.segment->file);
        assert(rv2 == 0); /* TODO: can this fail? */
    }

    s->preparer.segment = NULL;

    /* If there's a pending write request waiting for a segment to be ready,
     * let's fail it. */
    if (raft_io_uv_store__writer_is_blocked(s)) {
        s->writer.status = rv;
        raft_io_uv_store__writer_finish(s);
    }

    /* Possibly invoke the stop callback. */
    raft_io_uv_store__aborted(s);
}

/**
 * Start preparing the pending segment with the lowest counter.
 */
static int raft_io_uv_store__preparer_start(struct raft_io_uv_store *s)
{
    int rv;

    /* We haven't aborted. */
    assert(!s->aborted);

    /* We're not already active */
    assert(!raft_io_uv_store__preparer_is_active(s));

    /* Pick the pending segment with the lowest counter.*/
    s->preparer.segment = raft_io_uv_store__pool_get_pending(s);

    /* If no open segment is pending it means that the closer is still busy
     * closing them, and we have to wait for it to be done with at least one of
     * them. */
    if (s->preparer.segment == NULL) {
        assert(raft_io_uv_store__closer_is_active(s));
        return 0;
    }

    /* Sanity check of block and segment sizes */
    assert(s->block_size > 0);
    assert(s->max_segment_size % s->block_size == 0);

    rv = raft_uv_fs__create(&s->preparer.segment->file,
                            &s->preparer.segment->req, s->loop,
                            s->preparer.segment->path, s->max_segment_size, 1,
                            raft_io_uv_store__preparer_create_cb);
    if (rv != 0) {
        raft_errorf(s->logger, "create '%s': %s", s->preparer.segment->path,
                    uv_strerror(rv));
        goto err;
    }

    return 0;

err:
    assert(rv != 0);

    s->preparer.segment = NULL;

    return rv;
}

/**
 * Return true a batch with the given size would fit in a segment.
 */
static bool raft_io_uv_store__batch_fits_in_segment(struct raft_io_uv_store *s,
                                                    size_t size)
{
    size_t cap = s->max_segment_size - sizeof(uint64_t) /* Format version */;
    return size <= cap;
}

int raft_io_uv_store__append(struct raft_io_uv_store *s,
                             const struct raft_entry *entries,
                             const unsigned n,
                             void *data,
                             void (*f)(void *data, int status))
{
    struct raft_io_uv_append_cb cb;
    size_t size;
    int rv;

    /* We aren't stopping. */
    assert(!raft_io_uv_store__is_stopping(s));

    /* We shouldn't be given an empty list */
    assert(entries != NULL);
    assert(n > 0);

    if (s->aborted) {
        return RAFT_ERR_IO_ABORTED;
    }

    cb.data = data;
    cb.f = f;

    /* TODO: at the moment we don't allow a single batch to exceed the size of a
     * segment. */
    size = raft_io_uv__sizeof_entries(entries, n);

    if (!raft_io_uv_store__batch_fits_in_segment(s, size)) {
        return RAFT_ERR_IO_TOOBIG;
    }

    /* If the queue already not empty, let's keep pushing to it. */
    if (s->queue.n_entries > 0) {
        goto queue;
    }

    /* If there's currently no open segment ready to be written, we need to
     * wait. */
    if (s->writer.segment == NULL) {
        assert(raft_io_uv_store__writer_is_idle(s) ||
               raft_io_uv_store__writer_is_blocked(s));

        /* We have two cases:
         *
         * (1) If we are idle, we need to initialize the block buffers, since
         *     once a segment is ready we'll be writing to block 1 and
         *     beyond. We also need to kick the preparer if it's not running
         *     already.
         *
         * (2) If we are blocked, case (1) must have already happened, so we
         *     need to add these entries to the block buffers and save the given
         *     callback too.
         */
        if (raft_io_uv_store__writer_is_idle(s)) {
            rv = raft_io_uv_blocks__reset(&s->writer.blocks);
            if (rv != 0) {
                goto err;
            }

            if (!raft_io_uv_store__preparer_is_active(s)) {
                rv = raft_io_uv_store__preparer_start(s);
                if (rv != 0) {
                    goto err;
                }
            }

            s->writer.state = RAFT_IO_UV_STORE__WRITER_BLOCKED;
        } else {
            size_t combined_size;

            assert(raft_io_uv_store__writer_is_blocked(s));
            assert(raft_io_uv_store__preparer_is_active(s));

            /* If after adding these entries there wouldn't be enough room left
             * in the segment, we need to queue this request. */
            combined_size = s->writer.blocks.offset + size;
            if (!raft_io_uv_store__batch_fits_in_segment(s, combined_size)) {
                goto queue;
            }
        }

        rv = raft_io_uv_store__writer_push(s, entries, n, &cb, 1);
        if (rv != 0) {
            goto err;
        }

        return 0;
    }

    /* We are either idle or writing, since if we were blocked the segment would
     * be NULL and we would have taken the if branch above. */
    assert(raft_io_uv_store__writer_is_idle(s) ||
           raft_io_uv_store__writer_is_writing(s));

    assert(s->writer.segment != NULL);

    /* If no other write is in progress let's try to proceed immediately. */
    if (raft_io_uv_store__writer_is_idle(s)) {
        rv = raft_io_uv_store__writer_start(s, entries, n, &cb, 1);
        if (rv != 0) {
            goto err;
        }

        return 0;
    }

queue:
    /* If we get here it means that we're either writing, or blocked and without
     * enough spare space in the segment to push these entries. Let's queue them
     * up. */
    assert(raft_io_uv_store__writer_is_active(s));

    /* Check that pushing this batch to the queue would not make the queue too
     * big.
     *
     * TODO: allow any batch size.
     */
    size += raft_io_uv_store__queue_size(s);

    if (!raft_io_uv_store__batch_fits_in_segment(s, size)) {
        return RAFT_ERR_IO_TOOBIG;
    }

    rv = raft_io_uv_store__queue_push(s, entries, n, &cb);
    if (rv != 0) {
        return rv;
    }

    return 0;

err:
    assert(rv != 0);

    s->aborted = true;

    return rv;
}

void raft_io_uv_store__stop(struct raft_io_uv_store *s,
                            void *p,
                            void (*cb)(void *p))
{
    assert(s->stop.p == NULL);
    assert(s->stop.cb == NULL);

    s->stop.p = p;
    s->stop.cb = cb;

    /* We don't accept further requests from now on. */
    s->aborted = true;

    /* If we're not preparing or closing segments and we have no pending
     * write, this will invoke the stop callback immediately. Otherwise it
     * will be invoked when the dust settles. */
    raft_io_uv_store__aborted(s);
}

void raft_io_uv_store__close(struct raft_io_uv_store *s)
{
    /* We shall not be called if there are pending operations */
    assert(!raft_io_uv_store__preparer_is_active(s));
    assert(!raft_io_uv_store__writer_is_active(s));
    assert(!raft_io_uv_store__closer_is_active(s));

    /* Free all block buffers we've allocated. */
    raft_io_uv_blocks__close(&s->writer.blocks);

    /* Free the callbacks buffers .*/
    if (s->writer.cbs != NULL) {
        raft_free(s->writer.cbs);
    }
    if (s->queue.cbs != NULL) {
        raft_free(s->queue.cbs);
        raft_free(s->queue.entries);
    }

    /* Free the format version buffer */
    if (s->preparer.buf.base != NULL) {
        free(s->preparer.buf.base);
    }

    raft_free(s->dir);
}
