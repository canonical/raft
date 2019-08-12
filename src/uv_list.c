#include <string.h>

#include "uv.h"

static const char *ignored[] = {".", "..", "metadata1", "metadata2", NULL};

/* Return true if the given filename should be ignored. */
static bool shouldIgnore(const char *filename)
{
    const char **cursor = ignored;
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

int uvList(struct uv *uv,
           struct uvSnapshotInfo *snapshots[],
           size_t *n_snapshots,
           struct uvSegmentInfo *segments[],
           size_t *n_segments)
{
    struct dirent **dirents;
    int n_dirents;
    int i;
    char errmsg[2048];
    int rv;

    rv = uvScanDir(uv->dir, &dirents, &n_dirents, errmsg);
    if (rv != 0) {
        uvErrorf(uv, "scan %s: %s", uv->dir, errmsg);
        return RAFT_IOERR;
    }

    *snapshots = NULL;
    *n_snapshots = 0;

    *segments = NULL;
    *n_segments = 0;

    for (i = 0; i < n_dirents; i++) {
        struct dirent *entry = dirents[i];
        const char *filename = entry->d_name;
        bool appended;

        /* If an error occurred while processing a preceeding entry or if we
         * know that this is not a segment filename, just free it and skip to
         * the next one. */
        if (rv != 0 || shouldIgnore(filename)) {
            if (rv == 0) {
                uvDebugf(uv, "ignore %s", filename);
            }
            goto next;
        }

        /* Append to the snapshot list if it's a snapshot metadata filename and
         * a valid associated snapshot file exists. */
        rv = uvSnapshotInfoAppendIfMatch(uv, filename, snapshots, n_snapshots,
                                         &appended);
        if (appended || rv != 0) {
            if (rv == 0) {
                uvDebugf(uv, "snapshot %s", filename);
            }
            goto next;
        }

        /* Append to the segment list if it's a segment filename */
        rv = uvSegmentInfoAppendIfMatch(entry->d_name, segments, n_segments,
                                        &appended);
        if (appended || rv != 0) {
            if (rv == 0) {
                uvDebugf(uv, "segment %s", filename);
            }
            goto next;
        }

	uvDebugf(uv, "ignore %s", filename);

    next:
        free(dirents[i]);
    }
    free(dirents);

    if (rv != 0 && *segments != NULL) {
        raft_free(*segments);
    }

    if (*snapshots != NULL) {
        uvSnapshotSort(*snapshots, *n_snapshots);
    }

    if (*segments != NULL) {
        uvSegmentSort(*segments, *n_segments);
    }

    return rv;
}
