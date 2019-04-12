#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "array.h"
#include "assert.h"
#include "byte.h"
#include "io_uv_encoding.h"
#include "uv.h"

/* Template string for closed segment filenames: start index (inclusive), end
 * index (inclusive). */
#define CLOSED_TEMPLATE "%llu-%llu"

/* Template string for open segment filenames: incrementing counter. */
#define OPEN_TEMPLATE "open-%llu"

/* Check if the given filename matches the one of a closed segment (xxx-yyy), or
 * of an open segment (open-xxx), and fill the given info structure if so.
 *
 * Return true if the filename matched, false otherwise. */
static bool infoMatch(const char *filename, struct uvSegmentInfo *info)
{
    unsigned consumed;
    int matched;
    size_t filename_len = strnlen(filename, OS_MAX_FILENAME_LEN + 1);

    if (filename_len > OS_MAX_FILENAME_LEN) {
        return false;
    }

    matched = sscanf(filename, CLOSED_TEMPLATE "%n", &info->first_index,
                     &info->end_index, &consumed);
    if (matched == 2 && consumed == filename_len) {
        info->is_open = false;
        goto match;
    }

    matched = sscanf(filename, OPEN_TEMPLATE "%n", &info->counter, &consumed);
    if (matched == 1 && consumed == filename_len) {
        info->is_open = true;
        goto match;
    }

    return false;

match:
    strcpy(info->filename, filename);
    return true;
}

int uvSegmentInfoAppendIfMatch(const char *filename,
                               struct uvSegmentInfo *infos[],
                               size_t *n_infos,
                               bool *appended)
{
    struct uvSegmentInfo info;
    bool matched;
    int rv;

    /* Check if it's a closed or open filename */
    matched = infoMatch(filename, &info);

    /* If fhis is neither a closed or an open segment, return. */
    if (!matched) {
        *appended = false;
        return 0;
    }

    ARRAY__APPEND(struct uvSegmentInfo, info, infos, n_infos, rv);
    if (rv == -1) {
        return RAFT_NOMEM;
    }

    *appended = true;

    return 0;
}

/* Compare two segments to decide which one is more recent. */
static int compare(const void *p1, const void *p2)
{
    struct uvSegmentInfo *s1 = (struct uvSegmentInfo *)p1;
    struct uvSegmentInfo *s2 = (struct uvSegmentInfo *)p2;

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

void uvSegmentSort(struct uvSegmentInfo *infos, size_t n_infos)
{
    qsort(infos, n_infos, sizeof *infos, compare);
}

/* Open a segment file and read its format version. */
static int openSegment(struct uv *uv,
                       const osFilename filename,
                       const int flags,
                       int *fd,
                       uint64_t *format)
{
    int rv;
    rv = osOpen(uv->dir, filename, flags, fd);
    if (rv != 0) {
        uvErrorf(uv, "open %s: %s", filename, osStrError(rv));
        return RAFT_IOERR;
    }
    rv = osReadN(*fd, format, sizeof *format);
    if (rv != 0) {
        uvErrorf(uv, "read %s: %s", filename, osStrError(rv));
        close(*fd);
        return RAFT_IOERR;
    }
    *format = byte__flip64(*format);
    return 0;
}

/* Load a single batch of entries from a segment.
 *
 * Set @last to #true if the loaded batch is the last one. */
static int loadEntriesBatch(struct uv *uv,
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
    off_t offset;              /* Current segment file offset */
    int rv;

    /* Save the current offset, to provide more information when logging. */
    offset = lseek(fd, 0, SEEK_CUR);

    /* Read the preamble, consisting of the checksums for the batch header and
     * data buffers and the first 8 bytes of the header buffer, which contains
     * the number of entries in the batch. */
    rv = osReadN(fd, preamble, sizeof preamble);
    if (rv != 0) {
        uvErrorf(uv, "read: %s", osStrError(rv));
        return RAFT_IOERR;
    }

    n = byte__flip64(preamble[1]);
    if (n == 0) {
        uvErrorf(uv, "batch has zero entries");
        rv = RAFT_CORRUPT;
        goto err;
    }

    /* Very optimistic upper bound of the number of entries we should
     * expect. This is mainly a protection against allocating too much
     * memory. Each entry will consume at least 4 words (for term, type, size
     * and payload). */
    max_n = UV__MAX_SEGMENT_SIZE / (sizeof(uint64_t) * 4);

    if (n > max_n) {
        uvErrorf(uv, "batch has %u entries (preamble at %d)", n, offset);
        rv = RAFT_CORRUPT;
        goto err;
    }

    /* Read the batch header, excluding the first 8 bytes containing the number
     * of entries, which we have already read. */
    header.len = io_uv__sizeof_batch_header(n);
    header.base = raft_malloc(header.len);
    if (header.base == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    *(uint64_t *)header.base = preamble[1];

    rv = osReadN(fd, header.base + sizeof(uint64_t),
                 header.len - sizeof(uint64_t));
    if (rv != 0) {
        uvErrorf(uv, "read: %s", osStrError(rv));
        rv = RAFT_IOERR;
        goto err_after_header_alloc;
    }

    /* Check batch header integrity. */
    crc1 = byte__flip32(*(uint64_t *)preamble);
    crc2 = byte__crc32(header.base, header.len, 0);
    if (crc1 != crc2) {
        uvErrorf(uv, "corrupted batch header");
        rv = RAFT_CORRUPT;
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
        rv = RAFT_NOMEM;
        goto err_after_header_decode;
    }
    rv = osReadN(fd, data.base, data.len);
    if (rv != 0) {
        uvErrorf(uv, "read: %s", osStrError(rv));
        rv = RAFT_IOERR;
        goto err_after_data_alloc;
    }

    /* Check batch data integrity. */
    crc1 = byte__flip32(*((uint32_t *)preamble + 1));
    crc2 = byte__crc32(data.base, data.len, 0);
    if (crc1 != crc2) {
        uvErrorf(uv, "corrupted batch data");
        rv = RAFT_CORRUPT;
        goto err_after_data_alloc;
    }

    io_uv__decode_entries_batch(&data, *entries, *n_entries);

    raft_free(header.base);

    *last = osIsAtEof(fd);

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

/* Append to @entries2 all entries in @entries1. */
static int extendEntries(const struct raft_entry *entries1,
                         const size_t n_entries1,
                         struct raft_entry **entries2,
                         size_t *n_entries2)
{
    struct raft_entry *entries; /* To re-allocate the given entries */
    size_t i;

    entries =
        raft_realloc(*entries2, (*n_entries2 + n_entries1) * sizeof *entries);
    if (entries == NULL) {
        return RAFT_NOMEM;
    }

    for (i = 0; i < n_entries1; i++) {
        entries[*n_entries2 + i] = entries1[i];
    }

    *entries2 = entries;
    *n_entries2 += n_entries1;

    return 0;
}

int uvSegmentLoadClosed(struct uv *uv,
                        struct uvSegmentInfo *info,
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
    rv = osIsEmpty(uv->dir, info->filename, &empty);
    if (rv != 0) {
        uvErrorf(uv, "stat %s: %s", info->filename, osStrError(rv));
        rv = RAFT_IOERR;
        goto err;
    }
    if (empty) {
        uvErrorf(uv, "load %s: file is empty", info->filename);
        rv = RAFT_CORRUPT;
        goto err;
    }

    /* Open the segment file. */
    rv = openSegment(uv, info->filename, O_RDONLY, &fd, &format);
    if (rv != 0) {
        goto err;
    }
    if (format != UV__DISK_FORMAT) {
        uvErrorf(uv, "load %s: unexpected format version: %lu", info->filename,
                 format);
        rv = RAFT_IOERR;
        goto err_after_open;
    }

    /* Load all batches in the segment. */
    *entries = NULL;
    *n = 0;

    last = false;
    for (i = 1; !last; i++) {
        rv = loadEntriesBatch(uv, fd, &tmp_entries, &tmp_n, &last);
        if (rv != 0) {
            goto err_after_open;
        }
        rv = extendEntries(tmp_entries, tmp_n, entries, n);
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

/* Load all entries contained in an open segment. */
static int loadOpen(struct uv *uv,
                    struct uvSegmentInfo *info,
                    struct raft_entry *entries[],
                    size_t *n,
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

    rv = osIsEmpty(uv->dir, info->filename, &empty);
    if (rv != 0) {
        uvErrorf(uv, "stat %s: %s", info->filename, osStrError(rv));
        rv = RAFT_IOERR;
        goto err;
    }

    if (empty) {
        /* Empty segment, let's discard it. */
        remove = true;
        goto done;
    }

    rv = openSegment(uv, info->filename, O_RDWR, &fd, &format);
    if (rv != 0) {
        goto err;
    }

    /* Check that the format is the expected one, or perhaps 0, indicating that
     * the segment was allocated but never written. */
    if (format != UV__DISK_FORMAT) {
        if (format == 0) {
            rv = osHasTrailingZeros(fd, &all_zeros);
            if (rv != 0) {
                uvErrorf(uv, "check %s: %s", info->filename, osStrError(rv));
                rv = RAFT_IOERR;
                goto err_after_open;
            }
            if (all_zeros) {
                /* This is equivalent to the empty case, let's remove the
                 * segment. */
                remove = true;
                goto done;
            }
        }
        uvErrorf(uv, "segment %s: unexpected format version: %lu",
                 info->filename, format);
        rv = RAFT_MALFORMED;
        goto err_after_open;
    }

    /* Load all batches in the segment. */
    for (i = 1; !last; i++) {
        /* Save the current file descriptor offset, in case we need to truncate
         * the file to exclude this batch because it's incomplete. */
        off_t offset = lseek(fd, 0, SEEK_CUR);

        if (offset == -1) {
            uvErrorf(uv, "offset %s: %s", info->filename, i, osStrError(errno));
            return RAFT_IOERR;
        }

        rv = loadEntriesBatch(uv, fd, &tmp_entries, &tmp_n_entries, &last);
        if (rv != 0) {
            int rv2;

            /* If this isn't a decoding error, just bail out. */
            if (rv != RAFT_CORRUPT) {
                goto err_after_open;
            }

            /* If this is a decoding error, and not an OS error, check if the
             * rest of the file is filled with zeros. In that case we assume
             * that the server shutdown uncleanly and we just truncate this
             * incomplete data. */
            lseek(fd, offset, SEEK_SET);

            rv2 = osHasTrailingZeros(fd, &all_zeros);
            if (rv2 != 0) {
                uvErrorf(uv, "check %s: %s", info->filename, i,
                         osStrError(rv2));
                rv = RAFT_IOERR;
                goto err_after_open;
            }

            if (!all_zeros) {
                /* TODO: log a warning here, stating that the segment had a
                 * non-zero partial batch, and reporting the decoding error. */
            }

            rv = ftruncate(fd, offset);
            if (rv == -1) {
                uvErrorf(uv, "truncate %s: %s", info->filename,
                         osStrError(errno));
                rv = RAFT_IOERR;
                goto err_after_open;
            }

            break;
        }

        rv = extendEntries(tmp_entries, tmp_n_entries, entries, n);
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
        rv = osUnlink(uv->dir, info->filename);
        if (rv != 0) {
            uvErrorf(uv, "unlink %s: %s", info->filename, osStrError(rv));
            rv = RAFT_IOERR;
            goto err_after_open;
        }
    } else {
        osFilename filename;
        raft_index end_index = *next_index - 1;

        /* At least one entry was loaded */
        assert(end_index >= first_index);

        sprintf(filename, CLOSED_TEMPLATE, first_index, end_index);
        rv = osRename(uv->dir, info->filename, filename);
        if (rv != 0) {
            uvErrorf(uv, "rename %s: %s", info->filename, osStrError(rv));
            rv = RAFT_IOERR;
            goto err_after_open;
        }

        info->is_open = false;
        info->first_index = first_index;
        info->end_index = end_index;
        strcpy(info->filename, filename);
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

int uvSegmentLoadAll(struct uv *uv,
                     const raft_index start_index,
                     struct uvSegmentInfo *infos,
                     size_t n_infos,
                     struct raft_entry **entries,
                     size_t *n_entries)
{
    raft_index next_index;          /* Next entry to load from disk */
    struct raft_entry *tmp_entries; /* Entries in current segment */
    size_t tmp_n;                   /* Number of entries in current segment */
    size_t i;
    int rv;
    assert(start_index >= 1);
    assert(n_infos > 0);

    *entries = NULL;
    *n_entries = 0;

    next_index = start_index;

    for (i = 0; i < n_infos; i++) {
        struct uvSegmentInfo *info = &infos[i];

        if (info->is_open) {
            rv = loadOpen(uv, info, entries, n_entries, &next_index);
            if (rv != 0) {
                goto err;
            }
        } else {
            unsigned prefix = 0; /* N of prefix entries ignored */
            unsigned j;
            void *batch;

            /* If the entries in the segment are no longer needed, just remove
             * it. */
            if (info->end_index < start_index) {
                rv = osUnlink(uv->dir, info->filename);
                if (rv != 0) {
                    uvErrorf(uv, "unlink %s: %s", info->filename,
                             osStrError(rv));
                    rv = RAFT_IOERR;
                    goto err;
                }
                continue;
            }

            /* Check that start index encoded in the name of the segment matches
             * what we expect. */
            if (info->first_index > next_index) {
                uvErrorf(uv, "load %s: expected first index to be %lld",
                         info->filename, next_index);
                rv = RAFT_CORRUPT;
                goto err;
            }

            /* If the first index of the segment is lower than the next
             * expected, it must mean that it overlaps with the last snapshot we
             * loaded, and it must be the first segment we're loading. */
            if (info->first_index < next_index) {
                if (*n_entries != 0) {
                    uvErrorf(uv,
                             "load %s: expected first segment at %lld or lower",
                             info->filename, next_index);
                    rv = RAFT_CORRUPT;
                    goto err;
                }
                prefix = next_index - info->first_index;
            }

            rv = uvSegmentLoadClosed(uv, info, &tmp_entries, &tmp_n);
            if (rv != 0) {
                goto err;
            }

            if (tmp_n - prefix > 0) {
                rv = extendEntries(tmp_entries + prefix, tmp_n - prefix,
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
