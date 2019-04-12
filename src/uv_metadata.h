/* Read and write Raft metadata state. */

#ifndef UV_METADATA_H_
#define UV_METADATA_H_

#include "../include/raft.h"

#include "os.h"

/* Information persisted in a single metadata file. */
struct uvMetadata
{
    unsigned long long version; /* Monotonically increasing version */
    raft_term term;             /* Current term */
    unsigned voted_for;         /* Server ID of last vote, or 0 */
};

/* Load Raft metadata from disk, choosing the most recent version (either the
 * metadata1 or metadata2 file). */
int uvMetadataLoad(struct raft_io *io,
                   const osDir dir,
                   struct uvMetadata *metadata);

/* Store the given metadata to disk, writing the appropriate metadata file
 * according to the metadata version (if the version is odd, write metadata1,
 * otherwise write metadata2). */
int uvMetadataStore(struct raft_io *io,
                    const osDir dir,
                    const struct uvMetadata *metadata);

#endif /* UV_METADATA_H_ */
