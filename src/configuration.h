/**
 * Hold information about all servers in a raft configuration.
 */

#ifndef RAFT_CONFIGURATION_H
#define RAFT_CONFIGURATION_H

#include "../include/raft.h"

/**
 * Get the server with the given ID, if any.
 */
const struct raft_server *raft_configuration__get(
    const struct raft_configuration *c,
    const unsigned id);

/**
 * Return the number of voting servers.
 */
size_t raft_configuration__n_voting(const struct raft_configuration *c);

/**
 * Return the index of the server with the given ID (relative to the c->servers
 * array). If there's no server with the given ID, return the number of servers.
 */
size_t raft_configuration__index(const struct raft_configuration *c,
                                 const unsigned id);

/**
 * Return the index of the voting server with the given ID (relative to the sub
 * array of c->servers that has only voting servers). If there's no server with
 * the given ID, or if it's not flagged as voting, return the number of servers.
 */
size_t raft_configuration__voting_index(const struct raft_configuration *c,
                                        const unsigned id);

/**
 * Add all servers in c1 to c2.
 */
int raft_configuration__copy(const struct raft_configuration *c1,
                             struct raft_configuration *c2);

#endif /* RAFT_CONFIGURATION_H */
