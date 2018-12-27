#include <assert.h>
#include <string.h>
#include <unistd.h>

#include "binary.h"
#include "checksum.h"
#include "io_uv_store.h"
#include "uv_fs.h"

/**
 * Current on-disk format.
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
 * This is enough to hold the #RAFT_IO_UV_SEGMENT__CLOSED_TEMPLATE.
 */
#define RAFT_IO_UV_SEGMENT__MAX_FILENAME_LEN 42

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
 * Build the full path for the given filename, including the data directory.
 */
static void raft_io_uv__path(const char *dir, const char *filename, char *path)
{
    assert(filename != NULL);
    assert(strlen(filename) < RAFT_UV_FS_MAX_FILENAME_LEN);

    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);
}

/**
 * Read exactly @n bytes from the given file descriptor.
 */
static int raft_io_uv__read_n(const int fd, void *buf, size_t n, char *errmsg)
{
    int rv;

    rv = read(fd, buf, n);

    if (rv == -1) {
        raft_errorf(errmsg, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    assert(rv >= 0);

    if ((size_t)rv < n) {
        raft_errorf(errmsg, "got %d bytes instead of %ld", rv, n);
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Sync the given directory.
 */
static int raft_io_uv__sync_dir(const char *dir, char *errmsg)
{
    int fd;
    int rv;

    fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        raft_errorf(errmsg, "open data directory: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    rv = fsync(fd);

    close(fd);

    if (rv == -1) {
        raft_errorf(errmsg, "sync data directory: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Decode the content of a metadata file.
 */
static int raft_io_uv_metadata__decode(const void *buf,
                                       struct raft_io_uv_metadata *metadata,
                                       char *errmsg)
{
    const void *cursor = buf;
    unsigned format;

    format = raft__get64(&cursor);

    if (format != RAFT_IO_UV_STORE__FORMAT) {
        raft_errorf(errmsg, "unknown format %d", format);
        return RAFT_ERR_IO;
    }

    metadata->version = raft__get64(&cursor);
    metadata->term = raft__get64(&cursor);
    metadata->voted_for = raft__get64(&cursor);
    metadata->start_index = raft__get64(&cursor);

    return 0;
}

/**
 * Read the @n'th metadata file (with @n equal to 1 or 2) and decode the content
 * of the file, populating the given metadata buffer accordingly.
 */
static int raft_io_uv_metadata__load(const char *dir,
                                     const unsigned short n,
                                     struct raft_io_uv_metadata *metadata,
                                     char *errmsg)
{
    char path[RAFT_UV_FS_MAX_PATH_LEN];     /* Full path of metadata file */
    char filename[strlen("metadataN") + 1]; /* Pattern of metadata filename */
    uint8_t buf[RAFT_IO_UV_METADATA_SIZE];  /* Content of metadata file */
    int fd;
    int rv;

    assert(n == 1 || n == 2);

    /* Render the metadata path */
    sprintf(filename, "metadata%d", n);
    raft_io_uv__path(dir, filename, path);

    /* Open the metadata file, if it exists. */
    fd = open(path, O_RDONLY);
    if (fd == -1) {
        if (errno != ENOENT) {
            raft_errorf(errmsg, "open: %s", uv_strerror(-errno));
            return RAFT_ERR_IO;
        }

        /* The file does not exist, just return. */
        metadata->version = 0;

        return 0;
    }

    /* Read the content of the metadata file. */
    rv = read(fd, buf, sizeof buf);
    if (rv == -1) {
        raft_errorf(errmsg, "read: %s", uv_strerror(-errno));
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
    rv = raft_io_uv_metadata__decode(buf, metadata, errmsg);
    if (rv != 0) {
        return RAFT_ERR_IO;
    }

    /* Sanity checks that values make sense */
    if (metadata->version == 0) {
        raft_errorf(errmsg, "version is set to zero");
        return RAFT_ERR_IO;
    }

    if (metadata->term == 0) {
        raft_errorf(errmsg, "term is set to zero");
        return RAFT_ERR_IO;
    }

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
    const char *dir,
    const struct raft_io_uv_metadata *metadata,
    char *errmsg)
{
    char path[RAFT_UV_FS_MAX_PATH_LEN];     /* Full path of metadata file */
    char filename[strlen("metadataN") + 1]; /* Pattern of metadata filename */
    uint8_t buf[RAFT_IO_UV_METADATA_SIZE];  /* Content of metadata file */
    int fd;
    int rv;

    assert(metadata->version > 0);
    assert(metadata->start_index > 0);

    /* Encode the given metadata. */
    raft_io_uv_metadata__encode(metadata, buf);

    /* Render the metadata file name. */
    sprintf(filename, "metadata%d", raft_io_uv_metadata__n(metadata->version));
    raft_io_uv__path(dir, filename, path);

    /* Write the metadata file, creating it if it does not exist. */
    fd = open(path, O_WRONLY | O_CREAT | O_SYNC | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        raft_errorf(errmsg, "open: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    do {
        rv = write(fd, buf, sizeof buf);
    } while (rv == -1 && errno == EINTR);

    close(fd);

    if (rv == -1) {
        raft_errorf(errmsg, "write: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    assert(rv >= 0);

    if ((size_t)rv < sizeof buf) {
        raft_errorf(errmsg, "only %d bytes written", rv);
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
 * Try to match the filename of a closed segment (xxx-yyy).
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
 * Try to match the filename of an open segment (open-xxx).
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
 * Rename a segment and sync the data dir.
 */
static int raft_io_uv_segment__rename(const char *dir,
                                      const char *filename1,
                                      const char *filename2,
                                      char *errmsg)
{
    char path1[RAFT_UV_FS_MAX_PATH_LEN];
    char path2[RAFT_UV_FS_MAX_PATH_LEN];
    int rv;

    raft_io_uv__path(dir, filename1, path1);
    raft_io_uv__path(dir, filename2, path2);

    /* TODO: double check that filename2 does not exist. */
    rv = rename(path1, path2);
    if (rv == -1) {
        raft_errorf(errmsg, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    rv = raft_io_uv__sync_dir(dir, errmsg);
    if (rv == -1) {
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * List all files in the data directory and collect metadata about segment
 * files. The segments are ordered by filename.
 */
static int raft_io_uv_segment__list(const char *dir,
                                    struct raft_io_uv_segment *segments[],
                                    size_t *n,
                                    char *errmsg)
{
    struct dirent **entries;
    struct raft_io_uv_segment *tmp_segments;
    int n_entries;
    int i;
    int rv = 0;

    n_entries = scandir(dir, &entries, NULL, alphasort);
    if (n_entries < 0) {
        raft_errorf(errmsg, "list data directory: %s", uv_strerror(-errno));
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
            raft_errorf(errmsg, "can't allocate segments info array");
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
 * Check whether the given segment file is empty.
 */
static int raft_io_uv_segment__is_empty(const char *path,
                                        bool *empty,
                                        char *errmsg)
{
    struct stat st;
    int rv;

    rv = stat(path, &st);
    if (rv == -1) {
        raft_errorf(errmsg, "stat: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    *empty = st.st_size == 0 ? true : false;

    return 0;
}

/**
 * Open a segment file and read its format version.
 */
static int raft_io_uv_segment__open(const char *path,
                                    const int flags,
                                    int *fd,
                                    uint64_t *format,
                                    char *errmsg)
{
    int rv;

    *fd = open(path, flags);
    if (*fd == -1) {
        raft_errorf(errmsg, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    rv = raft_io_uv__read_n(*fd, format, sizeof *format, errmsg);
    if (rv != 0) {
        close(*fd);
        raft_wrapf(errmsg, "read format");
        return rv;
    }
    *format = raft__flip64(*format);

    return 0;
}

/**
 * Check if the content of the segment file associated with the given file
 * descriptor contains all zeros from the current offset onward.
 */
static int raft_io_uv_segment__is_all_zeros(const int fd,
                                            bool *flag,
                                            char *errmsg)
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
        raft_errorf(errmsg, "lseek: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }
    size -= offset;

    /* Reposition the file descriptor offset to the original offset. */
    offset = lseek(fd, offset, SEEK_SET);
    if (offset == -1) {
        raft_errorf(errmsg, "lseek: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    data = raft_malloc(size);
    if (data == NULL) {
        raft_errorf(errmsg, "can't allocate read buffer");
        return RAFT_ERR_NOMEM;
    }

    rv = raft_io_uv__read_n(fd, data, size, errmsg);
    if (rv != 0) {
        raft_wrapf(errmsg, "read file content");
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
 * Check if the given file descriptor has reached the end of the file.
 */
static bool raft_io_uv_segment__is_eof(const int fd)
{
    off_t offset; /* Current position */
    off_t size;   /* File size */

    offset = lseek(fd, 0, SEEK_CUR);
    size = lseek(fd, 0, SEEK_END);

    lseek(fd, offset, SEEK_SET);

    return offset == size;
}

/**
 * Load a single batch of entries from a segment.
 *
 * Set @last to #true if the loaded batch is the last one.
 */
static int raft_io_uv_segment__load_batch(const int fd,
                                          struct raft_entry **entries,
                                          unsigned *n_entries,
                                          bool *last,
                                          char *errmsg)
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
    rv = raft_io_uv__read_n(fd, preamble, sizeof preamble, errmsg);
    if (rv != 0) {
        raft_wrapf(errmsg, "read preamble");
        return rv;
    }

    n = raft__flip64(preamble[1]);

    if (n == 0) {
        raft_errorf(errmsg, "zero entries");
        rv = RAFT_ERR_IO_CORRUPT;
        goto err;
    }

    /* Read the batch header, excluding the first 8 bytes containing the number
     * of entries, which we have already read. */
    header.len = raft_batch_header_size(n);
    header.base = raft_malloc(header.len);
    if (header.base == NULL) {
        raft_errorf(errmsg, "can't allocate header buffer");
        rv = RAFT_ERR_NOMEM;
        goto err;
    }
    *(uint64_t *)header.base = preamble[1];

    rv = raft_io_uv__read_n(fd, header.base + sizeof(uint64_t),
                            header.len - sizeof(uint64_t), errmsg);
    if (rv != 0) {
        raft_wrapf(errmsg, "read header");
        goto err_after_header_alloc;
    }

    /* Check batch header integrity. */
    crc1 = raft__flip32(*(uint32_t *)preamble);
    crc2 = raft__crc32(header.base, header.len);
    if (crc1 != crc2) {
        raft_errorf(errmsg, "corrupted header");
        rv = RAFT_ERR_IO_CORRUPT;
        goto err_after_header_alloc;
    }

    /* Decode the batch header, allocating the entries array. */
    rv = raft_decode_batch_header(header.base, entries, n_entries);
    if (rv != 0) {
        raft_errorf(errmsg, "decode header");
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
        raft_errorf(errmsg, "can't allocate data buffer");
        rv = RAFT_ERR_NOMEM;
        goto err_after_header_decode;
    }
    rv = raft_io_uv__read_n(fd, data.base, data.len, errmsg);
    if (rv != 0) {
        raft_wrapf(errmsg, "read data");
        goto err_after_data_alloc;
    }

    /* Check batch data integrity. */
    crc1 = raft__flip32(*((uint32_t *)preamble + 1));
    crc2 = raft__crc32(data.base, data.len);
    if (crc1 != crc2) {
        raft_errorf(errmsg, "corrupted data");
        rv = RAFT_ERR_IO_CORRUPT;
        goto err_after_data_alloc;
    }

    rv = raft_decode_entries_batch(&data, *entries, *n_entries);
    assert(rv == 0); /* At the moment this can't fail */

    raft_free(header.base);

    *last = raft_io_uv_segment__is_eof(fd);

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
 * Truncate the given segment to the given @offset and sync it to disk.
 */
static int raft_io_uv_segment__truncate(const int fd,
                                        const off_t offset,
                                        char *errmsg)
{
    int rv;
    rv = ftruncate(fd, offset);
    if (rv == -1) {
        raft_errorf(errmsg, "truncate: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }
    rv = fsync(fd);
    if (rv == -1) {
        raft_errorf(errmsg, "sync: %s", uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Remove the segment at the given path.
 */
static int raft_io_uv_segment__remove(const char *dir,
                                      const char *filename,
                                      char *errmsg)
{
    char path[RAFT_UV_FS_MAX_PATH_LEN]; /* Full path of segment file */
    int rv;

    raft_io_uv__path(dir, filename, path);

    rv = unlink(path);
    if (rv != 0) {
        raft_wrapf(errmsg, "remove");
        return RAFT_ERR_IO;
    }

    rv = raft_io_uv__sync_dir(dir, errmsg);
    if (rv != 0) {
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Append the given entries to the given list.
 */
static int raft_io_uv_segment__append_entries(
    const struct raft_entry *tmp_entries,
    const size_t tmp_n_entries,
    struct raft_entry **entries,
    size_t *n_entries,
    char *errmsg)
{
    struct raft_entry *all_entries; /* To re-allocate the given entries */
    size_t i;

    all_entries =
        raft_realloc(*entries, (*n_entries + tmp_n_entries) * sizeof **entries);
    if (all_entries == NULL) {
        raft_errorf(errmsg, "can't reallocate entries");
        return RAFT_ERR_NOMEM;
    }

    for (i = 0; i < tmp_n_entries; i++) {
        all_entries[*n_entries + i] = tmp_entries[i];
    }

    *entries = all_entries;
    *n_entries += tmp_n_entries;

    return 0;
}

/**
 * Load the entries stored in a closed segment.
 */
static int raft_io_uv_segment__load_closed(
    const char *dir,
    const raft_index start_index,
    const struct raft_io_uv_segment *segment,
    struct raft_entry **entries,
    size_t *n_entries,
    raft_index *next_index,
    char *errmsg)
{
    char path[RAFT_UV_FS_MAX_PATH_LEN]; /* Full path of segment file */
    bool empty;                         /* Whether the file is empty */
    int fd;                             /* Segment file descriptor */
    uint64_t format;                    /* Format version */
    struct raft_entry *tmp_entries;     /* Entries in current batch */
    unsigned tmp_n_entries;             /* Number of entries in current batch */
    bool last = false;                  /* Whether the last batch was reached */
    int i;
    int rv;

    raft_io_uv__path(dir, segment->filename, path);

    /* If the segment is completely empty, just bail out. */
    rv = raft_io_uv_segment__is_empty(path, &empty, errmsg);
    if (rv != 0) {
        raft_wrapf(errmsg, "check if file is empty");
        goto err;
    }
    if (empty) {
        raft_errorf(errmsg, "file is empty");
        rv = RAFT_ERR_IO_CORRUPT;
        goto err;
    }

    /* Open the segment file. */
    rv = raft_io_uv_segment__open(path, O_RDONLY, &fd, &format, errmsg);
    if (rv != 0) {
        goto err;
    }

    /* If the entries in the segment are no longer needed, just remove it. */
    if (segment->end_index < start_index) {
        rv = raft_io_uv_segment__remove(dir, segment->filename, errmsg);
        if (rv != 0) {
            goto err_after_open;
        }
        goto done;
    }

    /* Check that start index encoded in the name of the segment match what we
     * expect. */
    if (segment->first_index != *next_index) {
        raft_errorf(errmsg, "expected first index to be %lld", *next_index);
        rv = RAFT_ERR_IO_CORRUPT;
        goto err_after_open;
    }

    if (format != RAFT_IO_UV_STORE__FORMAT) {
        raft_errorf(errmsg, "unexpected format version: %lu", format);
        rv = RAFT_ERR_IO;
        goto err_after_open;
    }

    /* Load all batches in the segment. */
    for (i = 1; !last; i++) {
        rv = raft_io_uv_segment__load_batch(fd, &tmp_entries, &tmp_n_entries,
                                            &last, errmsg);
        if (rv != 0) {
            raft_wrapf(errmsg, "batch %d", i);
            goto err_after_open;
        }

        rv = raft_io_uv_segment__append_entries(tmp_entries, tmp_n_entries,
                                                entries, n_entries, errmsg);
        if (rv != 0) {
            raft_wrapf(errmsg, "append entries");
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
static int raft_io_uv_segment__load_open(const char *dir,
                                         struct raft_io_uv_segment *segment,
                                         struct raft_entry **entries,
                                         size_t *n_entries,
                                         raft_index *next_index,
                                         char *errmsg)
{
    char path[RAFT_UV_FS_MAX_PATH_LEN]; /* Full path of segment file */
    raft_index first_index;             /* Index of first entry in segment */
    bool all_zeros;                     /* Whether the file is zero'ed */
    bool empty;                         /* Whether the segment file is empty */
    bool remove = false;                /* Whether to remove this segment */
    bool last = false;                  /* Whether the last batch was reached */
    int fd;                             /* Segment file descriptor */
    uint64_t format;                    /* Format version */
    size_t n_batches = 0;               /* Number of loaded batches */
    struct raft_entry *tmp_entries;     /* Entries in current batch */
    unsigned tmp_n_entries;             /* Number of entries in current batch */
    int i;
    int rv;

    first_index = *next_index;

    raft_io_uv__path(dir, segment->filename, path);

    rv = raft_io_uv_segment__is_empty(path, &empty, errmsg);
    if (rv != 0) {
        raft_wrapf(errmsg, "check if file is empty");
        goto err;
    }

    if (empty) {
        /* Empty segment, let's discard it. */
        remove = true;
        goto done;
    }

    rv = raft_io_uv_segment__open(path, O_RDWR, &fd, &format, errmsg);
    if (rv != 0) {
        goto err;
    }

    /* Check that the format is the expected one, or perhaps 0, indicating that
     * the segment was allocated but never written. */
    if (format != RAFT_IO_UV_STORE__FORMAT) {
        if (format == 0) {
            rv = raft_io_uv_segment__is_all_zeros(fd, &all_zeros, errmsg);
            if (rv != 0) {
                raft_wrapf(errmsg, "check if segment is all zeros");
                goto err_after_open;
            }

            if (all_zeros) {
                /* This is equivalent to the empty case, let's remove the
                 * segment. */
                remove = true;
                goto done;
            }
        }

        raft_errorf(errmsg, "unexpected format version: %lu", format);
        rv = RAFT_ERR_IO;
        goto err_after_open;
    }

    /* Load all batches in the segment. */
    for (i = 1; !last; i++) {
        /* Save the current file descriptor offset, in case we need to truncate
         * the file to exclude this batch because it's incomplete. */
        off_t offset = lseek(fd, 0, SEEK_CUR);

        if (offset == -1) {
            raft_errorf(errmsg, "batch %d: save offset: %s", i,
                        uv_strerror(-errno));
            return RAFT_ERR_IO;
        }

        rv = raft_io_uv_segment__load_batch(fd, &tmp_entries, &tmp_n_entries,
                                            &last, errmsg);
        if (rv != 0) {
            int rv2;

            /* If this isn't a decoding error, just bail out. */
            if (rv != RAFT_ERR_IO_CORRUPT) {
                raft_wrapf(errmsg, "batch %d", i);
                goto err_after_open;
            }

            /* If this is a decoding error, and not an OS error, check if the
             * rest of the file is filled with zeros. In that case we assume
             * that the server shutdown uncleanly and we just truncate this
             * incomplete data. */
            lseek(fd, offset, SEEK_SET);

            rv2 = raft_io_uv_segment__is_all_zeros(fd, &all_zeros, errmsg);
            if (rv2 != 0) {
                rv = rv2;
                raft_wrapf(errmsg, "check if segment is all zeros");
                goto err_after_open;
            }

            if (!all_zeros) {
                /* TODO: log a warning here, stating that the segment had a
                 * non-zero partial batch, and reporting the decoding error. */
            }

            rv = raft_io_uv_segment__truncate(fd, offset, errmsg);
            if (rv != 0) {
                goto err_after_open;
            }

            break;
        }

        rv = raft_io_uv_segment__append_entries(tmp_entries, tmp_n_entries,
                                                entries, n_entries, errmsg);
        if (rv != 0) {
            raft_wrapf(errmsg, "append entries");
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
        rv = raft_io_uv_segment__remove(dir, segment->filename, errmsg);
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
        rv = raft_io_uv_segment__rename(dir, segment->filename, filename,
                                        errmsg);
        if (rv != 0) {
            raft_wrapf(errmsg, "rename");
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
static int raft_io_uv_segment__load_all(const char *dir,
                                        const raft_index start_index,
                                        struct raft_io_uv_segment *segments,
                                        size_t n_segments,
                                        struct raft_entry **entries,
                                        size_t *n_entries,
                                        char *errmsg)
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
            rv = raft_io_uv_segment__load_open(dir, segment, entries, n_entries,
                                               &next_index, errmsg);
            if (rv != 0) {
                raft_wrapf(errmsg, "open segment %d", segment->counter);
                goto err;
            }
        } else {
            rv = raft_io_uv_segment__load_closed(dir, start_index, segment,
                                                 entries, n_entries,
                                                 &next_index, errmsg);
            if (rv != 0) {
                raft_wrapf(errmsg, "closed segment %llu-%llu",
                           segment->first_index, segment->end_index);
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
 * Check that the given directory exists, and try to create it if it doesn't.
 */
static int raft_io_uv_store__ensure_dir(const char *dir, char *errmsg)
{
    struct stat sb;
    int rv;

    /* Ensure that the given path doesn't exceed our static buffer limit */
    if (strlen(dir) > RAFT_UV_FS_MAX_DIR_LEN) {
        raft_errorf(errmsg, "data directory exceeds %d characters",
                    RAFT_UV_FS_MAX_DIR_LEN);
        return RAFT_ERR_IO;
    }

    /* Make sure we have a directory we can write into. */
    rv = stat(dir, &sb);
    if (rv != 0) {
        if (errno == ENOENT) {
            rv = mkdir(dir, 0700);
            if (rv != 0) {
                raft_errorf(errmsg, "create data directory '%s': %s", dir,
                            uv_strerror(-errno));
                return RAFT_ERR_IO;
            }
        } else {
            raft_errorf(errmsg, "access data directory '%s': %s", dir,
                        uv_strerror(-errno));
            return RAFT_ERR_IO;
        }
    } else if ((sb.st_mode & S_IFMT) != S_IFDIR) {
        raft_errorf(errmsg, "path '%s' is not a directory", dir);
        return RAFT_ERR_IO;
    }

    return 0;
}

int raft_io_uv_store__init(struct raft_io_uv_store *s,
                           const char *dir,
                           char *errmsg)
{
    int rv;

    /* Ensure that we have a valid data directory */
    rv = raft_io_uv_store__ensure_dir(dir, errmsg);
    if (rv != 0) {
        return rv;
    }

    /* Make a copy of the directory string, stripping any trailing slash */
    s->dir = raft_malloc(strlen(dir) + 1);
    if (s->dir == NULL) {
        raft_errorf(errmsg, "can't copy data directory path");
        return RAFT_ERR_NOMEM;
    }
    strcpy(s->dir, dir);

    if (s->dir[strlen(s->dir) - 1] == '/') {
        s->dir[strlen(s->dir) - 1] = 0;
    }

    /* Detect the file system block size */
    rv = raft_uv_fs__block_size(s->dir, &s->block_size);
    if (rv != 0) {
        raft_free(s->dir);
        raft_errorf(errmsg, "detect block size: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    return 0;
}

void raft_io_uv_store__close(struct raft_io_uv_store *s)
{
    raft_free(s->dir);
}

/**
 * Update both metadata files, so they are created if they didn't exist.
 */
static int raft_io_uv_store__ensure_metadata(struct raft_io_uv_store *s,
                                             char *errmsg)
{
    int i;
    int rv;

    /* Update both metadata files, so they are created if they didn't
     * exist. Also sync the data directory so the entries get created. */
    for (i = 0; i < 2; i++) {
        s->metadata.version++;
        rv = raft_io_uv_metadata__store(s->dir, &s->metadata, errmsg);
        if (rv != 0) {
            raft_wrapf(errmsg, "store metadata file %d", i);
            return rv;
        }
    }

    /* Also sync the data directory so the entries get created. */
    rv = raft_io_uv__sync_dir(s->dir, errmsg);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_io_uv_store__load(struct raft_io_uv_store *s,
                           struct raft_io_request *request,
                           char *errmsg)
{
    struct raft_io_uv_metadata metadata1;
    struct raft_io_uv_metadata metadata2;
    struct raft_io_uv_segment *segments;
    struct raft_entry *entries;
    size_t n_segments;
    size_t n_entries;
    int rv;

    /* The RAFT_IO_READ_STATE request is supposed to be invoked just once, right
     * after backend initialization */
    assert(s->metadata.version == 0);

    /* Read the two metadata files (if available). */
    rv = raft_io_uv_metadata__load(s->dir, 1, &metadata1, errmsg);
    if (rv != 0) {
        raft_wrapf(errmsg, "load metadata1");
        return rv;
    }

    rv = raft_io_uv_metadata__load(s->dir, 2, &metadata2, errmsg);
    if (rv != 0) {
        raft_wrapf(errmsg, "load metadata2");
        return rv;
    }

    /* Check the versions. */
    if (metadata1.version == 0 && metadata2.version == 0) {
        /* Neither metadata file exists: have a brand new server. */
        s->metadata.start_index = 1;
    } else if (metadata1.version == metadata2.version) {
        /* The two metadata files can't have the same version. */
        raft_errorf(errmsg, "metadata1 and metadata2 are both at version %d",
                    metadata1.version);
        return RAFT_ERR_IO;
    } else {
        /* Pick the metadata with the grater version. */
        if (metadata1.version > metadata2.version) {
            s->metadata = metadata1;
        } else {
            s->metadata = metadata2;
        }
    }

    /* Update the metadata files, so they are created if they did not exist. */
    rv = raft_io_uv_store__ensure_metadata(s, errmsg);
    if (rv != 0) {
        return rv;
    }

    /* List available segments. */
    rv = raft_io_uv_segment__list(s->dir, &segments, &n_segments, errmsg);
    if (rv != 0) {
        raft_wrapf(errmsg, "list segment files");
        return rv;
    }

    entries = NULL;
    n_entries = 0;

    /* Read data from segments, closing any open segments. */
    if (segments != NULL) {
        rv = raft_io_uv_segment__load_all(s->dir, s->metadata.start_index,
                                          segments, n_segments, &entries,
                                          &n_entries, errmsg);
        raft_free(segments);

        if (rv != 0) {
            return rv;
        }
    }

    /* Fill the result. */
    request->result.read_state.term = s->metadata.term;
    request->result.read_state.voted_for = s->metadata.voted_for;
    request->result.read_state.first_index = s->metadata.start_index;
    request->result.read_state.entries = entries;
    request->result.read_state.n = n_entries;

    return 0;
}

int raft_io_uv_store__term(struct raft_io_uv_store *s,
                           struct raft_io_request *request,
                           char *errmsg)
{
    int rv;

    assert(s->metadata.version > 0);

    s->metadata.version++;
    s->metadata.term = request->args.write_term.term;
    s->metadata.voted_for = 0;

    rv = raft_io_uv_metadata__store(s->dir, &s->metadata, errmsg);
    if (rv != 0) {
        int n = raft_io_uv_metadata__n(s->metadata.version);
        raft_wrapf(errmsg, "store metadata file %d", n);
        return rv;
    }

    return 0;
}

int raft_io_uv_store__vote(struct raft_io_uv_store *s,
                           struct raft_io_request *request,
                           char *errmsg)
{
    int rv;

    assert(s->metadata.version > 0);

    s->metadata.version++;
    s->metadata.voted_for = request->args.write_vote.server_id;

    rv = raft_io_uv_metadata__store(s->dir, &s->metadata, errmsg);
    if (rv != 0) {
        int n = raft_io_uv_metadata__n(s->metadata.version);
        raft_wrapf(errmsg, "store metadata file %d", n);
        return rv;
    }

    return 0;
}

int raft_io_uv_store__entries(struct raft_io_uv_store *s,
                              const unsigned request_id,
                              char *errmsg)
{
    (void)s;
    (void)request_id;
    (void)errmsg;

    return 0;
}
