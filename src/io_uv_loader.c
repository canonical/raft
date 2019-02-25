#include <string.h>
#include <unistd.h>

#include "assert.h"
#include "binary.h"
#include "checksum.h"
#include "io_uv_encoding.h"
#include "io_uv_loader.h"

/* Current on-disk format version. */
#define RAFT_IO_UV_STORE__FORMAT 1

/* Template string for snapshot filenames: snapshot term, snapshot index,
 * creation timestamp (milliseconds since epoch). */
#define RAFT__IO_UV_LOADER_SNAPSHOT_TEMPLATE "snapshot-%020llu-%020llu-%020llu"

/* Template string for snapshot metadata filenames: snapshot term,  snapshot
 * index, creation timestamp (milliseconds since epoch). */
#define RAFT__IO_UV_LOADER_SNAPSHOT_META_TEMPLATE \
    RAFT__IO_UV_LOADER_SNAPSHOT_TEMPLATE ".meta"

/* Template string for open segment filenames: incrementing counter. */
#define RAFT__IO_UV_LOADER_OPEN_SEGMENT_TEMPLATE "open-%llu"

/* Template string for closed segment filenames: start index (inclusive), end
 * index (inclusive). */
#define RAFT__IO_UV_LOADER_CLOSED_SEGMENT_TEMPLATE "%020llu-%020llu"

/* Return true if the given filename should be ignored. */
static bool raft__io_uv_loader_is_ignore_filename(const char *filename);

/* Try to match the filename of a snapshot metadata file
 * (snapshot-xxx-yyy-zzz.meta), and return its term and index in case of a
 * match. */
static bool raft__io_uv_loader_match_snapshot_meta(const char *filename,
                                                   raft_term *term,
                                                   raft_index *index);

/* Try to match the filename of a closed segment (xxx-yyy), and return its first
 * and end index in case of a match. */
static bool raft__io_uv_loader_match_closed_segment(const char *filename,
                                                    raft_index *first_index,
                                                    raft_index *end_index);

/* Try to match the filename of an open segment (open-xxx), and return its
 * counter in case of a match. */
static bool raft__io_uv_loader_match_open_segment(const char *filename,
                                                  unsigned long long *counter);

/* Append a new item to the given snapshot metadata list if the filename
 * matches. */
static int raft__io_uv_loader_maybe_append_snapshot(
    const char *filename,
    struct raft__io_uv_loader_snapshot *snapshots[],
    size_t *n,
    bool *appended);

/* Append a new item to the given segment list if the filename matches an open
 * or closed filename. */
static int raft__io_uv_loader_maybe_append_segment(
    const char *filename,
    struct raft__io_uv_loader_segment *segments[],
    size_t *n,
    bool *appended);

/* Load raft entries from the given segments. */
static int raft__io_uv_loader_load_from_list(
    struct raft__io_uv_loader *l,
    const raft_index start_index,
    struct raft__io_uv_loader_segment *segments,
    size_t n_segments,
    struct raft_entry **entries,
    size_t *n_entries);

/* Open a segment file and read its format version. */
static int raft__io_uv_loader_segment_open(struct raft_logger *logger,
                                           const char *path,
                                           const int flags,
                                           int *fd,
                                           uint64_t *format);

/* Load the entries stored in an open segment. If there are no entries at all,
 * remove the open segment, mark it as closed (by renaming it). */
static int raft__io_uv_loader_segment_load_open(
    struct raft_logger *logger,
    const char *dir,
    struct raft__io_uv_loader_segment *segment,
    struct raft_entry **entries,
    size_t *n_entries,
    raft_index *next_index);

/* Load a single batch of entries from a segment.
 *
 * Set @last to #true if the loaded batch is the last one. */
static int raft__io_uv_loader_segment_load_batch(struct raft_logger *logger,
                                                 const int fd,
                                                 struct raft_entry **entries,
                                                 unsigned *n_entries,
                                                 bool *last);

/* Render the filename of a closed segment. */
static void raft__io_uv_loader_segment_make_closed(const raft_index first_index,
                                                   const raft_index end_index,
                                                   char *filename);

/* Append to @entries2 all entries in @entries1. */
static int raft__io_uv_loader_extend_entries(const struct raft_entry *entries1,
                                             const size_t n_entries1,
                                             struct raft_entry **entries2,
                                             size_t *n_entries2);

void raft__io_uv_loader_init(struct raft__io_uv_loader *l,
                             struct raft_logger *logger,
                             const char *dir)
{
    l->logger = logger;
    l->dir = dir;
}

int raft__io_uv_loader_list(struct raft__io_uv_loader *l,
                            struct raft__io_uv_loader_snapshot *snapshots[],
                            size_t *n_snapshots,
                            struct raft__io_uv_loader_segment *segments[],
                            size_t *n_segments)
{
    struct dirent **dirents;
    int n_dirents;
    int i;
    int rv = 0;

    n_dirents = scandir(l->dir, &dirents, NULL, alphasort);
    if (n_dirents < 0) {
        raft_errorf(l->logger, "scan %s: %s", l->dir, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    *snapshots = NULL;
    *n_snapshots = 0;

    *segments = NULL;
    *n_segments = 0;

    for (i = 0; i < n_dirents; i++) {
        struct dirent *entry = dirents[i];
        bool ignore = raft__io_uv_loader_is_ignore_filename(entry->d_name);
        bool appended;

        /* If an error occurred while processing a preceeding entry or if we
         * know that this is not a segment filename, just free it and skip to
         * the next one. */
        if (rv != 0 || ignore) {
            goto next;
        }

        /* Append to the snapshot list if it's a snapshot metadata filename */
        rv = raft__io_uv_loader_maybe_append_snapshot(entry->d_name, snapshots,
                                                      n_snapshots, &appended);
        if (appended || rv != 0) {
            goto next;
        }

        rv = raft__io_uv_loader_maybe_append_segment(entry->d_name, segments,
                                                     n_segments, &appended);
        if (appended || rv != 0) {
            goto next;
        }

    next:
        free(dirents[i]);
    }
    free(dirents);

    if (rv != 0 && *segments != NULL) {
        raft_free(*segments);
    }

    return rv;
}

int raft__io_uv_loader_load_closed(struct raft__io_uv_loader *l,
                                   struct raft__io_uv_loader_segment *segment,
                                   struct raft_entry *entries[],
                                   size_t *n)
{
    raft__io_uv_fs_path path;       /* Full path of segment file */
    bool empty;                     /* Whether the file is empty */
    int fd;                         /* Segment file descriptor */
    uint64_t format;                /* Format version */
    bool last;                      /* Whether the last batch was reached */
    struct raft_entry *tmp_entries; /* Entries in current batch */
    unsigned tmp_n;                 /* Number of entries in current batch */
    int i;
    int rv;

    raft__io_uv_fs_join(l->dir, segment->filename, path);

    /* If the segment is completely empty, just bail out. */
    rv = raft__io_uv_fs_is_empty(path, &empty);
    if (rv != 0) {
        goto err;
    }
    if (empty) {
        raft_errorf(l->logger, "segment %s: file is empty", path);
        rv = RAFT_ERR_IO_CORRUPT;
        goto err;
    }

    /* Open the segment file. */
    rv = raft__io_uv_loader_segment_open(l->logger, path, O_RDONLY, &fd,
                                         &format);
    if (rv != 0) {
        goto err;
    }

    if (format != RAFT_IO_UV_STORE__FORMAT) {
        raft_errorf(l->logger, "segment %s: unexpected format version: %lu",
                    path, format);
        rv = RAFT_ERR_IO;
        goto err_after_open;
    }

    /* Load all batches in the segment. */
    *entries = NULL;
    *n = 0;

    last = false;
    for (i = 1; !last; i++) {
        rv = raft__io_uv_loader_segment_load_batch(l->logger, fd, &tmp_entries,
                                                   &tmp_n, &last);
        if (rv != 0) {
            goto err_after_open;
        }

        rv = raft__io_uv_loader_extend_entries(tmp_entries, tmp_n, entries, n);
        if (rv != 0) {
            goto err_after_batch_load;
        }

        raft_free(tmp_entries);
    }

    assert(i > 1); /* At least one batch was loaded. */

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

int raft__io_uv_loader_load_all(struct raft__io_uv_loader *l,
                                raft_index start_index,
                                struct raft_entry *entries[],
                                size_t *n)
{
    struct raft__io_uv_loader_snapshot *snapshots;
    struct raft__io_uv_loader_segment *segments;
    size_t n_snapshots;
    size_t n_segments;
    int rv;

    /* List available segments. */
    rv = raft__io_uv_loader_list(l, &snapshots, &n_snapshots, &segments,
                                 &n_segments);
    if (rv != 0) {
        goto err;
    }

    *entries = NULL;
    *n = 0;

    /* Read data from segments, closing any open segments. */
    if (segments != NULL) {
        rv = raft__io_uv_loader_load_from_list(l, start_index, segments,
                                               n_segments, entries, n);
        raft_free(segments);

        if (rv != 0) {
            goto err;
        }
    }

    return 0;

err:
    assert(rv != 0);

    *n = 0;

    return rv;
}

static const char *raft__io_uv_loader_is_ignore_filenamed_filenames[] = {
    ".", "..", "metadata1", "metadata2", NULL};

/**
 * Return true if this is a segment filename.
 */
static bool raft__io_uv_loader_is_ignore_filename(const char *filename)
{
    const char **cursor = raft__io_uv_loader_is_ignore_filenamed_filenames;
    bool result = false;

    while (*cursor != NULL) {
        if (strcmp(filename, *cursor) == 0) {
            result = true;
            break;
        }
        cursor++;
    }

    return result;
}

static bool raft__io_uv_loader_match_snapshot_meta(const char *filename,
                                                   raft_term *term,
                                                   raft_index *index)
{
    unsigned consumed;
    int matched;
    unsigned long long timestamp;

    matched = sscanf(filename, RAFT__IO_UV_LOADER_SNAPSHOT_META_TEMPLATE "%n",
                     term, index, &timestamp, &consumed);

    return matched == 3 && consumed == strlen(filename);
}

static bool raft__io_uv_loader_match_closed_segment(const char *filename,
                                                    raft_index *first_index,
                                                    raft_index *end_index)
{
    unsigned consumed;
    int matched;

    matched = sscanf(filename, RAFT__IO_UV_LOADER_CLOSED_SEGMENT_TEMPLATE "%n",
                     first_index, end_index, &consumed);

    return matched == 2 && consumed == strlen(filename);
}

static bool raft__io_uv_loader_match_open_segment(const char *filename,
                                                  unsigned long long *counter)
{
    unsigned consumed;
    int matched;

    matched = sscanf(filename, RAFT__IO_UV_LOADER_OPEN_SEGMENT_TEMPLATE "%n",
                     counter, &consumed);
    return matched == 1 && consumed == strlen(filename);
}

static int raft__io_uv_loader_maybe_append_snapshot(
    const char *filename,
    struct raft__io_uv_loader_snapshot *snapshots[],
    size_t *n,
    bool *appended)
{
    struct raft__io_uv_loader_snapshot snapshot;
    struct raft__io_uv_loader_snapshot *tmp_snapshots;
    bool matched;

    /* Check if it's a snapshot metadata filename */
    matched = raft__io_uv_loader_match_snapshot_meta(filename, &snapshot.term,
                                                     &snapshot.index);
    if (!matched) {
        *appended = false;
        return 0;
    }

    (*n)++;
    tmp_snapshots = raft_realloc(*snapshots, (*n) * sizeof **snapshots);
    if (tmp_snapshots == NULL) {
        return RAFT_ERR_NOMEM;
    }

    assert(strlen(filename) < sizeof snapshot.filename);
    strcpy(snapshot.filename, filename);

    *snapshots = tmp_snapshots;
    (*snapshots)[(*n) - 1] = snapshot;

    *appended = true;

    return 0;
}

/* Append a new item to the given segment list if the filename matches an open
 * or closed filename. */
static int raft__io_uv_loader_maybe_append_segment(
    const char *filename,
    struct raft__io_uv_loader_segment *segments[],
    size_t *n,
    bool *appended)
{
    struct raft__io_uv_loader_segment segment;
    struct raft__io_uv_loader_segment *tmp_segments;
    bool matched;

    /* Check if it's a closed segment filename */
    matched = raft__io_uv_loader_match_closed_segment(
        filename, &segment.first_index, &segment.end_index);
    if (matched) {
        segment.is_open = false;
        goto append;
    }

    /* Check if it's an open segment filename */
    matched = raft__io_uv_loader_match_open_segment(filename, &segment.counter);

    if (matched) {
        segment.is_open = true;
        goto append;
    }

    /* This is neither a closed or an open segment */
    *appended = false;
    return 0;

append:
    (*n)++;
    tmp_segments = raft_realloc(*segments, (*n) * sizeof **segments);

    if (tmp_segments == NULL) {
        return RAFT_ERR_NOMEM;
    }

    assert(strlen(filename) < sizeof segment.filename);
    strcpy(segment.filename, filename);

    *segments = tmp_segments;
    (*segments)[(*n) - 1] = segment;

    return 0;
}

static int raft__io_uv_loader_load_from_list(
    struct raft__io_uv_loader *l,
    const raft_index start_index,
    struct raft__io_uv_loader_segment *segments,
    size_t n_segments,
    struct raft_entry **entries,
    size_t *n_entries)
{
    raft_index next_index; /* Index of the next entry to load from disk */
    struct raft_entry *tmp_entries; /* Entries in current segment */
    size_t tmp_n;                   /* Number of entries in current segment */
    size_t i;
    int rv;

    assert(start_index >= 1);
    assert(n_segments > 0);
    assert(*entries == NULL);
    assert(*n_entries == 0);

    next_index = start_index;

    for (i = 0; i < n_segments; i++) {
        struct raft__io_uv_loader_segment *segment = &segments[i];

        if (segment->is_open) {
            rv = raft__io_uv_loader_segment_load_open(
                l->logger, l->dir, segment, entries, n_entries, &next_index);
            if (rv != 0) {
                goto err;
            }
        } else {
            /* If the entries in the segment are no longer needed, just remove
             * it. */
            if (segment->end_index < start_index) {
                rv = raft__io_uv_fs_unlink(l->dir, segment->filename);
                if (rv != 0) {
                    goto err;
                }
                continue;
            }

            /* Check that start index encoded in the name of the segment matches
             * what we expect. */
            if (segment->first_index != next_index) {
                raft_errorf(l->logger,
                            "segment %s: expected first index to be %lld",
                            segment->filename, next_index);
                rv = RAFT_ERR_IO_CORRUPT;
                goto err;
            }

            rv = raft__io_uv_loader_load_closed(l, segment, &tmp_entries,
                                                &tmp_n);
            if (rv != 0) {
                goto err;
            }

            rv = raft__io_uv_loader_extend_entries(tmp_entries, tmp_n, entries,
                                                   n_entries);
            if (rv != 0) {
                /* TODO: release memory of entries in tmp_entries */
                goto err;
            }

            raft_free(tmp_entries);

            next_index += tmp_n;
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

static int raft__io_uv_loader_segment_load_open(
    struct raft_logger *logger,
    const char *dir,
    struct raft__io_uv_loader_segment *segment,
    struct raft_entry **entries,
    size_t *n_entries,
    raft_index *next_index)
{
    raft__io_uv_fs_path path;       /* Full path of segment file */
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

    raft__io_uv_fs_join(dir, segment->filename, path);

    rv = raft__io_uv_fs_is_empty(path, &empty);
    if (rv != 0) {
        goto err;
    }

    if (empty) {
        /* Empty segment, let's discard it. */
        remove = true;
        goto done;
    }

    rv = raft__io_uv_loader_segment_open(logger, path, O_RDWR, &fd, &format);
    if (rv != 0) {
        goto err;
    }

    /* Check that the format is the expected one, or perhaps 0, indicating that
     * the segment was allocated but never written. */
    if (format != RAFT_IO_UV_STORE__FORMAT) {
        if (format == 0) {
            rv = raft__io_uv_fs_is_all_zeros(fd, &all_zeros);
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

        raft_errorf(logger, "segment %s: unexpected format version: %lu", path,
                    format);
        rv = RAFT_ERR_IO;
        goto err_after_open;
    }

    /* Load all batches in the segment. */
    for (i = 1; !last; i++) {
        /* Save the current file descriptor offset, in case we need to truncate
         * the file to exclude this batch because it's incomplete. */
        off_t offset = lseek(fd, 0, SEEK_CUR);

        if (offset == -1) {
            raft_errorf(logger, "segment %s: batch %d: save offset: %s", path,
                        i, uv_strerror(-errno));
            return RAFT_ERR_IO;
        }

        rv = raft__io_uv_loader_segment_load_batch(logger, fd, &tmp_entries,
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

            rv2 = raft__io_uv_fs_is_all_zeros(fd, &all_zeros);
            if (rv2 != 0) {
                rv = rv2;
                goto err_after_open;
            }

            if (!all_zeros) {
                /* TODO: log a warning here, stating that the segment had a
                 * non-zero partial batch, and reporting the decoding error. */
            }

            rv = ftruncate(fd, offset);
            if (rv == -1) {
                rv = RAFT_ERR_IO;
                goto err_after_open;
            }

            break;
        }

        rv = raft__io_uv_loader_extend_entries(tmp_entries, tmp_n_entries,
                                               entries, n_entries);
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
        rv = raft__io_uv_fs_unlink(dir, segment->filename);
        if (rv != 0) {
            goto err_after_open;
        }
    } else {
        raft__io_uv_fs_filename filename;
        raft_index end_index = *next_index - 1;

        /* At least one entry was loaded */
        assert(end_index >= first_index);

        raft__io_uv_loader_segment_make_closed(first_index, end_index,
                                               filename);
        rv = raft__io_uv_fs_rename(dir, segment->filename, filename);
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

static int raft__io_uv_loader_segment_load_batch(struct raft_logger *logger,
                                                 const int fd,
                                                 struct raft_entry **entries,
                                                 unsigned *n_entries,
                                                 bool *last)
{
    uint64_t preamble[2];      /* CRC32 checksums and number of raft entries */
    unsigned n;                /* Number of entries in the batch */
    unsigned max_n;            /* Maximum number of entries we expect */
    unsigned i;                /* Iterate through the entries */
    struct raft_buffer header; /* Batch header */
    struct raft_buffer data;   /* Batch data */
    unsigned crc1;             /* Target checksum */
    unsigned crc2;             /* Actual checksum */
    int rv;

    /* Read the preamble, consisting of the checksums for the batch header and
     * data buffers and the first 8 bytes of the header buffer, which contains
     * the number of entries in the batch. */
    off_t pos = lseek(fd, 0, SEEK_CUR);
    rv = raft__io_uv_fs_read_n(fd, preamble, sizeof preamble);
    if (rv != 0) {
        return RAFT_ERR_IO;
    }

    n = raft__flip64(preamble[1]);

    if (n == 0) {
        raft_errorf(logger, "batch has zero entries");
        rv = RAFT_ERR_IO_CORRUPT;
        goto err;
    }

    /* Very optimistic upper bound of the number of entries we should
     * expect. This is mainly a protection against allocating too much
     * memory. Each entry will consume at least 4 words (for term, type, size
     * and payload). */
    max_n = RAFT_IO_UV_MAX_SEGMENT_SIZE / (sizeof(uint64_t) * 4);

    if (n > max_n) {
        raft_errorf(logger, "batch has %u entries (preamble at %d)", n, pos);
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

    rv = raft__io_uv_fs_read_n(fd, header.base + sizeof(uint64_t),
                               header.len - sizeof(uint64_t));
    if (rv != 0) {
        rv = RAFT_ERR_IO;
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
    rv = raft__io_uv_fs_read_n(fd, data.base, data.len);
    if (rv != 0) {
        rv = RAFT_ERR_IO;
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

    *last = raft__io_uv_fs_is_at_eof(fd);

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

static void raft__io_uv_loader_segment_make_closed(const raft_index first_index,
                                                   const raft_index end_index,
                                                   char *filename)
{
    sprintf(filename, RAFT__IO_UV_LOADER_CLOSED_SEGMENT_TEMPLATE, first_index,
            end_index);
}

static int raft__io_uv_loader_segment_open(struct raft_logger *logger,
                                           const char *path,
                                           const int flags,
                                           int *fd,
                                           uint64_t *format)
{
    int rv;

    *fd = open(path, flags);
    if (*fd == -1) {
        raft_errorf(logger, "open %s: %s", path, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    rv = raft__io_uv_fs_read_n(*fd, format, sizeof *format);
    if (rv != 0) {
        close(*fd);
        return RAFT_ERR_IO;
    }
    *format = raft__flip64(*format);

    return 0;
}

/**
 * Append to @entries2 all entries in @entries1.
 */
static int raft__io_uv_loader_extend_entries(const struct raft_entry *entries1,
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
