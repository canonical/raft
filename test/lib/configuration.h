/**
 * Raft servers configuration test helpers.
 */

#ifndef TEST_CONFIGURATION_H
#define TEST_CONFIGURATION_H

#include "../../src/configuration.h"

#define FIXTURE_CONFIGURATION struct raft_configuration configuration;
#define SETUP_CONFIGURATION raft_configuration_init(&f->configuration)
#define TEAR_DOWN_CONFIGURATION raft_configuration_close(&f->configuration)

#endif /* TEST_CONFIGURATION_H */
