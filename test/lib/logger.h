#ifndef TEST_LOGGER_H_
#define TEST_LOGGER_H_

#include "../../include/raft.h"

#define FIXTURE_LOGGER struct raft_logger logger
#define SETUP_LOGGER                                 \
    {                                                \
        raft_stream_logger_init(&f->logger, stderr); \
        f->logger.level = RAFT_DEBUG;                \
    }

/* Add @N servers to the given @CONFIGURATION pointer. The will be @N_VOTING
 * voting servers. */
#define CONFIGURATION_ADD_N

#endif /* TEST_CONFIGURATION_H */
