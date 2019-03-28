#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../include/raft/io_uv.h"

#include "assert.h"
#include "byte.h"
#include "configuration.h"
#include "io_uv.h"
#include "io_uv_encoding.h"
#include "io_uv_load.h"
#include "logging.h"

/* Template string for snapshot filenames: snapshot term, snapshot index,
 * creation timestamp (milliseconds since epoch). */
#define SNAPSHOT_TEMPLATE "snapshot-%llu-%llu-%llu"

/* Template string for snapshot metadata filenames: snapshot term,  snapshot
 * index, creation timestamp (milliseconds since epoch). */
#define SNAPSHOT_META_TEMPLATE SNAPSHOT_TEMPLATE ".meta"

/* Arbitrary maximum configuration size. Should be practically be enough */
#define SNAPSHOT_META_MAX_CONFIGURATION_SIZE 1024 * 1024

/* Template string for open segment filenames: incrementing counter. */
#define OPEN_SEGMENT_TEMPLATE "open-%llu"

/* Template string for closed segment filenames: start index (inclusive), end
 * index (inclusive). */
#define CLOSED_SEGMENT_TEMPLATE "%llu-%llu"

/* Return true if the given filename should be ignored. */
static bool is_ignore_filename(const char *filename);

/* Try to match the filename of a snapshot metadata file
 * (snapshot-xxx-yyy-zzz.meta), and return its term and index in case of a
 * match. */
static bool match_snapshot_meta(const char *filename,
                                raft_term *term,
                                raft_index *index,
                                unsigned long long *timestamp);

/* Try to match the filename of a closed segment (xxx-yyy), and return its first
 * and end index in case of a match. */
static bool match_closed_segment(const char *filename,
                                 raft_index *first_index,
                                 raft_index *end_index);

/* Try to match the filename of an open segment (open-xxx), and return its
 * counter in case of a match. */
static bool match_open_segment(const char *filename,
                               unsigned long long *counter);

/* Append a new item to the given snapshot metadata list if the filename
 * matches. */
static int maybe_append_snapshot_meta(const char *dir,
                                      const char *filename,
                                      struct io_uv__snapshot_meta *snapshots[],
                                      size_t *n,
                                      bool *appended);

/* Append a new item to the given segment list if the filename matches an open
 * or closed filename. */
static int maybe_append_segment_meta(const char *filename,
                                     struct io_uv__segment_meta *segments[],
                                     size_t *n,
                                     bool *appended);

/* Parse the metadata file of a snapshot and populate the given snapshot object
 * accordingly. */
static int load_snapshot_meta(struct io_uv *uv,
                              struct io_uv__snapshot_meta *meta,
                              struct raft_snapshot *snapshot);

/* Load the snapshot data file. */
static int load_snapshot_data(struct io_uv *uv,
                              struct io_uv__snapshot_meta *meta,
                              struct raft_snapshot *snapshot);

/* Render the filename of the data file of a snapshot */
static void snapshot_data_filename(struct io_uv__snapshot_meta *meta,
                                   io_uv__filename filename);

/* Compare two snapshots to decide which one is more recent. */
static int compare_snapshots(const void *item1, const void *item2);

/* Compare two segments to decide which one is more recent. */
static int compare_segments(const void *item1, const void *item2);

/* Load raft entries from the given segments. */
static int load_entries_from_segments(struct io_uv *uv,
                                      const raft_index start_index,
                                      struct io_uv__segment_meta *segments,
                                      size_t n_segments,
                                      struct raft_entry **entries,
                                      size_t *n_entries);

/* Open a segment file and read its format version. */
static int open_segment_meta(struct raft_io *io,
                             const char *dir,
                             const char *filename,
                             const int flags,
                             int *fd,
                             uint64_t *format);

/* Load the entries stored in an open segment. If there are no entries at all,
 * remove the open segment, mark it as closed (by renaming it). */
static int load_entries_from_open_segment(struct raft_io *io,
                                          const char *dir,
                                          struct io_uv__segment_meta *segment,
                                          struct raft_entry **entries,
                                          size_t *n_entries,
                                          raft_index *next_index);

/* Load a single batch of entries from a segment.
 *
 * Set @last to #true if the loaded batch is the last one. */
static int load_entries_batch_from_segment(struct raft_io *io,
                                           const int fd,
                                           struct raft_entry **entries,
                                           unsigned *n_entries,
                                           bool *last);

/* Render the filename of a closed segment. */
static void closed_segment_filename(const raft_index first_index,
                                    const raft_index end_index,
                                    char *filename);

/* Append to @entries2 all entries in @entries1. */
static int extend_entries(const struct raft_entry *entries1,
                          const size_t n_entries1,
                          struct raft_entry **entries2,
                          size_t *n_entries2);

int io_uv__load_list(struct io_uv *uv,
                     struct io_uv__snapshot_meta *snapshots[],
                     size_t *n_snapshots,
                     struct io_uv__segment_meta *segments[],
                     size_t *n_segments)
{
    struct dirent **dirents;
    int n_dirents;
    int i;
    int rv = 0;

    n_dirents = scandir(uv->dir, &dirents, NULL, alphasort);
    if (n_dirents < 0) {
        errorf(uv->io, "scan %s: %s", uv->dir, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    *snapshots = NULL;
    *n_snapshots = 0;

    *segments = NULL;
    *n_segments = 0;

    for (i = 0; i < n_dirents; i++) {
        struct dirent *entry = dirents[i];
        bool ignore = is_ignore_filename(entry->d_name);
        bool appended;

        /* If an error occurred while processing a preceeding entry or if we
         * know that this is not a segment filename, just free it and skip to
         * the next one. */
        if (rv != 0 || ignore) {
            goto next;
        }

        /* Append to the snapshot list if it's a snapshot metadata filename */
        rv = maybe_append_snapshot_meta(uv->dir, entry->d_name, snapshots,
                                        n_snapshots, &appended);
        if (appended || rv != 0) {
            goto next;
        }

        rv = maybe_append_segment_meta(entry->d_name, segments, n_segments,
                                       &appended);
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

    if (*snapshots != NULL) {
        qsort(*snapshots, *n_snapshots, sizeof **snapshots, compare_snapshots);
    }

    if (*segments != NULL) {
        qsort(*segments, *n_segments, sizeof **segments, compare_segments);
    }

    return rv;
}

int io_uv__load_snapshot(struct io_uv *uv,
                         struct io_uv__snapshot_meta *meta,
                         struct raft_snapshot *snapshot)
{
    int rv;

    rv = load_snapshot_meta(uv, meta, snapshot);
    if (rv != 0) {
        return rv;
    }

    rv = load_snapshot_data(uv, meta, snapshot);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int io_uv__load_closed(struct io_uv *uv,
                       struct io_uv__segment_meta *segment,
                       struct raft_entry *entries[],
                       size_t *n)
{
    bool empty;                     /* Whether the file is empty */
    int fd;                         /* Segment file descriptor */
    uint64_t format;                /* Format version */
    bool last;                      /* Whether the last batch was reached */
    struct raft_entry *tmp_entries; /* Entries in current batch */
    unsigned tmp_n;                 /* Number of entries in current batch */
    int i;
    int rv;

    /* If the segment is completely empty, just bail out. */
    rv = raft__io_uv_fs_is_empty(uv->dir, segment->filename, &empty);
    if (rv != 0) {
        goto err;
    }
    if (empty) {
        errorf(uv->io, "segment %s: file is empty", segment->filename);
        rv = RAFT_ERR_IO_CORRUPT;
        goto err;
    }

    /* Open the segment file. */
    rv = open_segment_meta(uv->io, uv->dir, segment->filename, O_RDONLY, &fd,
                           &format);
    if (rv != 0) {
        goto err;
    }

    if (format != IO_UV__DISK_FORMAT) {
        errorf(uv->io, "segment %s: unexpected format version: %lu",
               segment->filename, format);
        rv = RAFT_ERR_IO;
        goto err_after_open;
    }

    /* Load all batches in the segment. */
    *entries = NULL;
    *n = 0;

    last = false;
    for (i = 1; !last; i++) {
        rv = load_entries_batch_from_segment(uv->io, fd, &tmp_entries, &tmp_n,
                                             &last);
        if (rv != 0) {
            goto err_after_open;
        }

        rv = extend_entries(tmp_entries, tmp_n, entries, n);
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

int io_uv__load_all(struct io_uv *uv,
                    struct raft_snapshot **snapshot,
                    raft_index *start_index,
                    struct raft_entry *entries[],
                    size_t *n)
{
    struct io_uv__snapshot_meta *snapshots;
    struct io_uv__segment_meta *segments;
    size_t n_snapshots;
    size_t n_segments;
    int rv;

    *snapshot = NULL;
    *entries = NULL;
    *n = 0;
    *start_index = 1;

    /* List available snapshots and segments. */
    rv = io_uv__load_list(uv, &snapshots, &n_snapshots, &segments, &n_segments);
    if (rv != 0) {
        goto err;
    }

    /* Load the most recent snapshot, if any. */
    if (snapshots != NULL) {
        raft_index last_index;

        *snapshot = raft_malloc(sizeof **snapshot);
        if (*snapshot == NULL) {
            rv = RAFT_ENOMEM;
            goto err;
        }
        rv = io_uv__load_snapshot(uv, &snapshots[n_snapshots - 1], *snapshot);
        if (rv != 0) {
            goto err;
        }
        raft_free(snapshots);
        snapshots = NULL;

        last_index = (*snapshot)->index;
        /* Update the start index. If there are closed segments on disk and the
         * first index of the first closed segment is lower than the snapshot's
         * last index, let's try to retain n_trailing entries if available. */
        if (segments != NULL && !segments[0].is_open &&
            segments[0].first_index <= last_index) {
            *start_index = segments[0].first_index;
        } else {
            *start_index = (*snapshot)->index + 1;
        }
    }

    /* Read data from segments, closing any open segments. */
    if (segments != NULL) {
        rv = load_entries_from_segments(uv, *start_index, segments, n_segments,
                                        entries, n);
        if (rv != 0) {
            goto err;
        }
        raft_free(segments);
        segments = NULL;
    }

    return 0;

err:
    if (snapshots != NULL) {
        raft_free(snapshots);
    }
    if (segments != NULL) {
        raft_free(segments);
    }
    if (*snapshot != NULL) {
        raft_free(*snapshot);
    }

    assert(rv != 0);

    *n = 0;

    return rv;
}

static const char *is_ignore_filenamed_filenames[] = {".", "..", "metadata1",
                                                      "metadata2", NULL};

/**
 * Return true if this is a segment filename.
 */
static bool is_ignore_filename(const char *filename)
{
    const char **cursor = is_ignore_filenamed_filenames;
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

static bool match_snapshot_meta(const char *filename,
                                raft_term *term,
                                raft_index *index,
                                unsigned long long *timestamp)
{
    unsigned consumed;
    int matched;

    matched = sscanf(filename, SNAPSHOT_META_TEMPLATE "%n", term, index,
                     timestamp, &consumed);

    return matched == 3 && consumed == strlen(filename);
}

static bool match_closed_segment(const char *filename,
                                 raft_index *first_index,
                                 raft_index *end_index)
{
    unsigned consumed;
    int matched;

    matched = sscanf(filename, CLOSED_SEGMENT_TEMPLATE "%n", first_index,
                     end_index, &consumed);

    return matched == 2 && consumed == strlen(filename);
}

static bool match_open_segment(const char *filename,
                               unsigned long long *counter)
{
    unsigned consumed;
    int matched;

    matched = sscanf(filename, OPEN_SEGMENT_TEMPLATE "%n", counter, &consumed);
    return matched == 1 && consumed == strlen(filename);
}

static int maybe_append_snapshot_meta(const char *dir,
                                      const char *filename,
                                      struct io_uv__snapshot_meta *snapshots[],
                                      size_t *n,
                                      bool *appended)
{
    struct io_uv__snapshot_meta snapshot;
    struct io_uv__snapshot_meta *tmp_snapshots;
    bool matched;
    struct stat sb;
    io_uv__filename snapshot_filename;
    int rv;

    /* Check if it's a snapshot metadata filename */
    matched = match_snapshot_meta(filename, &snapshot.term, &snapshot.index,
                                  &snapshot.timestamp);
    if (!matched) {
        *appended = false;
        return 0;
    }

    assert(strlen(filename) < sizeof snapshot.filename);
    strcpy(snapshot.filename, filename);

    /* Check if there's actually a snapshot file for this snapshot metadata. If
     * there's none, it means that we aborted before finishing the snapshot, so
     * let's remove the metadata file. */
    snapshot_data_filename(&snapshot, snapshot_filename);
    rv = raft__io_uv_fs_stat(dir, snapshot_filename, &sb);
    if (rv == -1) {
        if (errno == ENOENT) {
            unlink(filename);
            *appended = false;
            return 0;
        }
        return RAFT_ERR_IO;
    }

    (*n)++;
    tmp_snapshots = raft_realloc(*snapshots, (*n) * sizeof **snapshots);
    if (tmp_snapshots == NULL) {
        return RAFT_ENOMEM;
    }

    *snapshots = tmp_snapshots;
    (*snapshots)[(*n) - 1] = snapshot;

    *appended = true;

    return 0;
}

/* Append a new item to the given segment list if the filename matches an open
 * or closed filename. */
static int maybe_append_segment_meta(const char *filename,
                                     struct io_uv__segment_meta *segments[],
                                     size_t *n,
                                     bool *appended)
{
    struct io_uv__segment_meta segment;
    struct io_uv__segment_meta *tmp_segments;
    bool matched;

    /* Check if it's a closed segment filename */
    matched = match_closed_segment(filename, &segment.first_index,
                                   &segment.end_index);
    if (matched) {
        segment.is_open = false;
        goto append;
    }

    /* Check if it's an open segment filename */
    matched = match_open_segment(filename, &segment.counter);

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
        return RAFT_ENOMEM;
    }

    assert(strlen(filename) < sizeof segment.filename);
    strcpy(segment.filename, filename);

    *segments = tmp_segments;
    (*segments)[(*n) - 1] = segment;

    return 0;
}

static int load_snapshot_meta(struct io_uv *uv,
                              struct io_uv__snapshot_meta *meta,
                              struct raft_snapshot *snapshot)
{
    uint64_t header[1 + /* Format version */
                    1 + /* CRC checksum */
                    1 + /* Configuration index */
                    1 /* Configuration length */];
    struct raft_buffer buf;
    unsigned format;
    unsigned crc1;
    unsigned crc2;
    int fd;
    int rv;

    snapshot->term = meta->term;
    snapshot->index = meta->index;

    fd = raft__io_uv_fs_open(uv->dir, meta->filename, O_RDONLY);
    if (fd == -1) {
        errorf(uv->io, "open %s: %s", meta->filename, uv_strerror(-errno));
        rv = RAFT_ERR_IO;
        goto err;
    }

    rv = raft__io_uv_fs_read_n(fd, header, sizeof header);
    if (rv != 0) {
        errorf(uv->io, "read %s: %s", meta->filename, uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_open;
    }

    format = byte__flip64(header[0]);
    if (format != IO_UV__DISK_FORMAT) {
        errorf(uv->io, "read %s: unsupported format %lu", meta->filename,
               format);
        rv = RAFT_ERR_IO_CORRUPT;
        goto err_after_open;
    }

    crc1 = byte__flip64(header[1]);

    snapshot->configuration_index = byte__flip64(header[2]);
    buf.len = byte__flip64(header[3]);
    if (buf.len > SNAPSHOT_META_MAX_CONFIGURATION_SIZE) {
        errorf(uv->io, "read %s: configuration data too big (%ld)",
               meta->filename, buf.len);
        rv = RAFT_ERR_IO_CORRUPT;
        goto err_after_open;
    }
    if (buf.len == 0) {
        errorf(uv->io, "read %s: no configuration data", meta->filename,
               buf.len);
        rv = RAFT_ERR_IO_CORRUPT;
        goto err_after_open;
    }
    buf.base = raft_malloc(buf.len);
    if (buf.base == NULL) {
        rv = RAFT_ENOMEM;
        goto err_after_open;
    }

    rv = raft__io_uv_fs_read_n(fd, buf.base, buf.len);
    if (rv != 0) {
        goto err_after_buf_malloc;
    }

    crc2 = byte__crc32(header + 2, sizeof header - sizeof(uint64_t) * 2, 0);
    crc2 = byte__crc32(buf.base, buf.len, crc2);

    if (crc1 != crc2) {
        errorf(uv->io, "read %s: corrupted data", meta->filename);
        rv = RAFT_ERR_IO_CORRUPT;
        goto err_after_open;
    }

    raft_configuration_init(&snapshot->configuration);
    rv = configuration__decode(&buf, &snapshot->configuration);
    if (rv != 0) {
        goto err_after_buf_malloc;
    }

    raft_free(buf.base);
    close(fd);

    return 0;

err_after_buf_malloc:
    raft_free(buf.base);

err_after_open:
    close(fd);

err:
    assert(rv != 0);
    return rv;
}

static int load_snapshot_data(struct io_uv *uv,
                              struct io_uv__snapshot_meta *meta,
                              struct raft_snapshot *snapshot)
{
    struct stat sb;
    io_uv__filename filename;
    struct raft_buffer buf;
    int fd;
    int rv;

    snapshot_data_filename(meta, filename);

    rv = raft__io_uv_fs_stat(uv->dir, filename, &sb);
    if (rv != 0) {
        errorf(uv->io, "stat %s: %s", filename, uv_strerror(-errno));
        rv = RAFT_ERR_IO;
        goto err;
    }

    fd = raft__io_uv_fs_open(uv->dir, filename, O_RDONLY);
    if (fd == -1) {
        errorf(uv->io, "open %s: %s", filename, uv_strerror(-errno));
        rv = RAFT_ERR_IO;
        goto err;
    }

    buf.len = sb.st_size;
    buf.base = raft_malloc(buf.len);
    if (buf.base == NULL) {
        rv = RAFT_ENOMEM;
        goto err_after_open;
    }

    rv = raft__io_uv_fs_read_n(fd, buf.base, buf.len);
    if (rv != 0) {
        goto err_after_buf_alloc;
    }

    snapshot->bufs = raft_malloc(sizeof *snapshot->bufs);
    snapshot->n_bufs = 1;
    if (snapshot->bufs == NULL) {
        rv = RAFT_ENOMEM;
        goto err_after_buf_alloc;
    }

    snapshot->bufs[0] = buf;

    close(fd);

    return 0;

err_after_buf_alloc:
    raft_free(buf.base);

err_after_open:
    close(fd);

err:
    assert(rv != 0);
    return rv;
}

static void snapshot_data_filename(struct io_uv__snapshot_meta *meta,
                                   io_uv__filename filename)
{
    size_t len = strlen(meta->filename) - strlen(".meta");
    strncpy(filename, meta->filename, len);
    filename[len] = 0;
}

static int compare_snapshots(const void *p1, const void *p2)
{
    struct io_uv__snapshot_meta *s1 = (struct io_uv__snapshot_meta *)p1;
    struct io_uv__snapshot_meta *s2 = (struct io_uv__snapshot_meta *)p2;

    /* If terms are different, the snaphot with the highest term is the most
     * recent. */
    if (s1->term != s2->term) {
        return s1->term < s2->term ? -1 : 1;
    }

    /* If the term are identical and the index differ, the snapshot with the
     * highest index is the most recent */
    if (s1->index != s2->index) {
        return s1->index < s2->index ? -1 : 1;
    }

    /* If term and index are identical, compare the timestamp. */
    return s1->timestamp < s2->timestamp ? -1 : 1;
}

static int compare_segments(const void *p1, const void *p2)
{
    struct io_uv__segment_meta *s1 = (struct io_uv__segment_meta *)p1;
    struct io_uv__segment_meta *s2 = (struct io_uv__segment_meta *)p2;

    /* Closed segments are less recent than open segments. */
    if (s1->is_open && !s2->is_open) {
        return 1;
    }
    if (!s1->is_open && s2->is_open) {
        return -1;
    }

    /* If the segments are open, compare the counter. */
    if (s1->is_open) {
        assert(s2->is_open);
        assert(s1->counter != s2->counter);
        return s1->counter < s2->counter ? -1 : 1;
    }

    /* If the segments are closed, compare the first index. The index ranges
     * must be disjoint. */
    if (s2->first_index > s1->end_index) {
        return -1;
    }

    return 1;
}

static int load_entries_from_segments(struct io_uv *uv,
                                      const raft_index start_index,
                                      struct io_uv__segment_meta *segments,
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
        struct io_uv__segment_meta *segment = &segments[i];

        if (segment->is_open) {
            rv = load_entries_from_open_segment(
                uv->io, uv->dir, segment, entries, n_entries, &next_index);
            if (rv != 0) {
                goto err;
            }
        } else {
            unsigned prefix = 0; /* N of prefix entries ignored */
            unsigned j;
            void *batch;

            /* If the entries in the segment are no longer needed, just remove
             * it. */
            if (segment->end_index < start_index) {
                rv = raft__io_uv_fs_unlink(uv->dir, segment->filename);
                if (rv != 0) {
                    goto err;
                }
                continue;
            }

            /* Check that start index encoded in the name of the segment matches
             * what we expect. */
            if (segment->first_index > next_index) {
                errorf(uv->io, "segment %s: expected first index to be %lld",
                       segment->filename, next_index);
                rv = RAFT_ERR_IO_CORRUPT;
                goto err;
            }

            /* If the first index of the segment is lower than the next
             * expected, it must mean that it overlaps with the last snapshot we
             * loaded, and it must be the first segment we're loading. */
            if (segment->first_index < next_index) {
                if (*n_entries != 0) {
                    errorf(
                        uv->io,
                        "segment %s: expected first segment at %lld or lower",
                        segment->filename, next_index);
                    rv = RAFT_ERR_IO_CORRUPT;
                    goto err;
                }
                prefix = next_index - segment->first_index;
            }

            rv = io_uv__load_closed(uv, segment, &tmp_entries, &tmp_n);
            if (rv != 0) {
                goto err;
            }

            if (tmp_n - prefix > 0) {
                rv = extend_entries(tmp_entries + prefix, tmp_n - prefix,
                                    entries, n_entries);
                if (rv != 0) {
                    /* TODO: release memory of entries in tmp_entries */
                    goto err;
                }
                if (prefix > 0) {
                    batch = tmp_entries[prefix].batch;
                    for (j = prefix; j > 0; j--) {
                        struct raft_entry *entry = &tmp_entries[j - 1];
                        if (entry->batch != batch) {
                            raft_free(entry->batch);
                            batch = entry->batch;
                        }
                    }
                }
            } else {
                batch = NULL;
                for (j = 0; j < tmp_n; j++) {
                    struct raft_entry *entry = &tmp_entries[j];
                    if (entry->batch != batch) {
                        raft_free(entry->batch);
                        batch = entry->batch;
                    }
                }
            }

            raft_free(tmp_entries);

            next_index += tmp_n - prefix;
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

static int load_entries_from_open_segment(struct raft_io *io,
                                          const char *dir,
                                          struct io_uv__segment_meta *segment,
                                          struct raft_entry **entries,
                                          size_t *n_entries,
                                          raft_index *next_index)
{
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

    rv = raft__io_uv_fs_is_empty(dir, segment->filename, &empty);
    if (rv != 0) {
        goto err;
    }

    if (empty) {
        /* Empty segment, let's discard it. */
        remove = true;
        goto done;
    }

    rv = open_segment_meta(io, dir, segment->filename, O_RDWR, &fd, &format);
    if (rv != 0) {
        goto err;
    }

    /* Check that the format is the expected one, or perhaps 0, indicating that
     * the segment was allocated but never written. */
    if (format != IO_UV__DISK_FORMAT) {
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

        errorf(io, "segment %s: unexpected format version: %lu",
               segment->filename, format);
        rv = RAFT_ERR_IO;
        goto err_after_open;
    }

    /* Load all batches in the segment. */
    for (i = 1; !last; i++) {
        /* Save the current file descriptor offset, in case we need to truncate
         * the file to exclude this batch because it's incomplete. */
        off_t offset = lseek(fd, 0, SEEK_CUR);

        if (offset == -1) {
            errorf(io, "segment %s: batch %d: save offset: %s",
                   segment->filename, i, uv_strerror(-errno));
            return RAFT_ERR_IO;
        }

        rv = load_entries_batch_from_segment(io, fd, &tmp_entries,
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

        rv = extend_entries(tmp_entries, tmp_n_entries, entries, n_entries);
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
        io_uv__filename filename;
        raft_index end_index = *next_index - 1;

        /* At least one entry was loaded */
        assert(end_index >= first_index);

        closed_segment_filename(first_index, end_index, filename);
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

static int load_entries_batch_from_segment(struct raft_io *io,
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

    n = byte__flip64(preamble[1]);

    if (n == 0) {
        errorf(io, "batch has zero entries");
        rv = RAFT_ERR_IO_CORRUPT;
        goto err;
    }

    /* Very optimistic upper bound of the number of entries we should
     * expect. This is mainly a protection against allocating too much
     * memory. Each entry will consume at least 4 words (for term, type, size
     * and payload). */
    max_n = RAFT_IO_UV_MAX_SEGMENT_SIZE / (sizeof(uint64_t) * 4);

    if (n > max_n) {
        errorf(io, "batch has %u entries (preamble at %d)", n, pos);
        rv = RAFT_ERR_IO_CORRUPT;
        goto err;
    }

    /* Read the batch header, excluding the first 8 bytes containing the number
     * of entries, which we have already read. */
    header.len = io_uv__sizeof_batch_header(n);
    header.base = raft_malloc(header.len);
    if (header.base == NULL) {
        rv = RAFT_ENOMEM;
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
    crc1 = byte__flip32(*(uint64_t *)preamble);
    crc2 = byte__crc32(header.base, header.len, 0);
    if (crc1 != crc2) {
        errorf(io, "corrupted batch header");
        rv = RAFT_ERR_IO_CORRUPT;
        goto err_after_header_alloc;
    }

    /* Decode the batch header, allocating the entries array. */
    rv = io_uv__decode_batch_header(header.base, entries, n_entries);
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
        rv = RAFT_ENOMEM;
        goto err_after_header_decode;
    }
    rv = raft__io_uv_fs_read_n(fd, data.base, data.len);
    if (rv != 0) {
        rv = RAFT_ERR_IO;
        goto err_after_data_alloc;
    }

    /* Check batch data integrity. */
    crc1 = byte__flip32(*((uint32_t *)preamble + 1));
    crc2 = byte__crc32(data.base, data.len, 0);
    if (crc1 != crc2) {
        errorf(io, "corrupted batch data");
        rv = RAFT_ERR_IO_CORRUPT;
        goto err_after_data_alloc;
    }

    io_uv__decode_entries_batch(&data, *entries, *n_entries);

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

static void closed_segment_filename(const raft_index first_index,
                                    const raft_index end_index,
                                    char *filename)
{
    sprintf(filename, CLOSED_SEGMENT_TEMPLATE, first_index, end_index);
}

static int open_segment_meta(struct raft_io *io,
                             const char *dir,
                             const char *filename,
                             const int flags,
                             int *fd,
                             uint64_t *format)
{
    int rv;

    *fd = raft__io_uv_fs_open(dir, filename, flags);
    if (*fd == -1) {
        errorf(io, "open %s: %s", filename, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    rv = raft__io_uv_fs_read_n(*fd, format, sizeof *format);
    if (rv != 0) {
        close(*fd);
        return RAFT_ERR_IO;
    }
    *format = byte__flip64(*format);

    return 0;
}

/**
 * Append to @entries2 all entries in @entries1.
 */
static int extend_entries(const struct raft_entry *entries1,
                          const size_t n_entries1,
                          struct raft_entry **entries2,
                          size_t *n_entries2)
{
    struct raft_entry *entries; /* To re-allocate the given entries */
    size_t i;

    entries =
        raft_realloc(*entries2, (*n_entries2 + n_entries1) * sizeof *entries);
    if (entries == NULL) {
        return RAFT_ENOMEM;
    }

    for (i = 0; i < n_entries1; i++) {
        entries[*n_entries2 + i] = entries1[i];
    }

    *entries2 = entries;
    *n_entries2 += n_entries1;

    return 0;
}
