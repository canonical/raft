/* Raft servers configuration test helpers. */

#ifndef TEST_CONFIGURATION_H_
#define TEST_CONFIGURATION_H_

#include "../../src/configuration.h"

#define FIXTURE_CONFIGURATION struct raft_configuration configuration;
#define SETUP_CONFIGURATION raft_configuration_init(&f->configuration)
#define TEAR_DOWN_CONFIGURATION raft_configuration_close(&f->configuration)

/* Initialize the given struct configuration pointer with @N servers. */
#define CONFIGURATION_CREATE(CONF, N)                               \
    {                                                               \
        unsigned i_;                                                \
        int rv__;                                                   \
        raft_configuration_init(CONF);                              \
        for (i_ = 0; i_ < N; i_++) {                                \
            unsigned id = i_ + 1;                                   \
            char address[16];                                       \
            sprintf(address, "%u", id);                             \
            rv__ = raft_configuration_add(CONF, id, address, true); \
            munit_assert_int(rv__, ==, 0);                          \
        }                                                           \
    }

#define CONFIGURATION_CLOSE(CONF) raft_configuration_close(CONF)

#endif /* TEST_CONFIGURATION_H_ */
