#include <string.h>

#include "assert.h"
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
    struct uv_fs_s req;
    struct uv_dirent_s entry;
    int n;
    int i;
    int rv;

    n = uv_fs_scandir(NULL, &req, uv->dir, 0, NULL);
    if (n < 0) {
        uvErrorf(uv, "scan %s: %s", uv->dir, uv_strerror(n));
        return RAFT_IOERR;
    }

    *snapshots = NULL;
    *n_snapshots = 0;

    *segments = NULL;
    *n_segments = 0;

    rv = 0;

    for (i = 0; i < n; i++) {
        const char *filename;
        bool appended;

        rv = uv_fs_scandir_next(&req, &entry);
        assert(rv == 0); /* Can't fail in libuv */

        filename = entry.name;

        /* If an error occurred while processing a preceeding entry or if we
         * know that this is not a segment filename, just free it and skip to
         * the next one. */
        if (rv != 0 || shouldIgnore(filename)) {
            if (rv == 0) {
                uvDebugf(uv, "ignore %s", filename);
            }
            continue;
        }

        /* Append to the snapshot list if it's a snapshot metadata filename and
         * a valid associated snapshot file exists. */
        rv = uvSnapshotInfoAppendIfMatch(uv, filename, snapshots, n_snapshots,
                                         &appended);
        if (appended || rv != 0) {
            if (rv == 0) {
                uvDebugf(uv, "snapshot %s", filename);
            }
            continue;
        }

        /* Append to the segment list if it's a segment filename */
        rv = uvSegmentInfoAppendIfMatch(entry.name, segments, n_segments,
                                        &appended);
        if (appended || rv != 0) {
            if (rv == 0) {
                uvDebugf(uv, "segment %s", filename);
            }
            continue;
        }

        uvDebugf(uv, "ignore %s", filename);
    }

    rv = uv_fs_scandir_next(&req, &entry);
    assert(rv == UV_EOF);
    rv = 0;

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
