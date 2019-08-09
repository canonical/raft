/**
 * Raft snapshot test helpers.
 */

#ifndef TEST_SNAPSHOT_H
#define TEST_SNAPSHOT_H

#include "../../include/raft.h"

#include "../../src/configuration.h"

/**
 * Allocate and create the given snapshot, using the given @LAST_INDEX,
 * @LAST_TERM, the given @CONF, and generating an FSM snapshot using @X and @Y.
 */
#define CREATE_SNAPSHOT(SNAPSHOT, LAST_INDEX, LAST_TERM, CONF, CONF_INDEX, X, \
                        Y)                                                    \
    SNAPSHOT = raft_malloc(sizeof *SNAPSHOT);                                 \
    munit_assert_ptr_not_null(SNAPSHOT);                                      \
    SNAPSHOT->index = LAST_INDEX;                                             \
    SNAPSHOT->term = LAST_TERM;                                               \
    SNAPSHOT->configuration = CONF;                                           \
    SNAPSHOT->configuration_index = CONF_INDEX;                               \
    test_fsm_encode_snapshot(X, Y, &SNAPSHOT->bufs, &SNAPSHOT->n_bufs)

#endif /* TEST_CONFIGURATION_H */
