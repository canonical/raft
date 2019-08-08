/* Raft servers configuration test helpers. */

#ifndef TEST_CONFIGURATION_H_
#define TEST_CONFIGURATION_H_

#include "../../src/configuration.h"

#define FIXTURE_CONFIGURATION struct raft_configuration configuration;
#define SETUP_CONFIGURATION raft_configuration_init(&f->configuration)
#define TEAR_DOWN_CONFIGURATION raft_configuration_close(&f->configuration)

/* Add @N servers to the given @CONFIGURATION pointer. The will be @N_VOTING
 * voting servers. */
#define CONFIGURATION_ADD_N

#endif /* TEST_CONFIGURATION_H_ */
