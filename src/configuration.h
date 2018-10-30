/**
 * Hold information about all servers in a raft configuration.
 */

#ifndef RAFT_CONFIGURATION_H
#define RAFT_CONFIGURATION_H

#include "../include/raft.h"

/**
 * Get the server with the given ID, if any.
 */
const struct raft_server *raft_configuration__get(struct raft_configuration *c,
                                                  const unsigned id);

/**
 * Return the number of voting servers.
 */
size_t raft_configuration__n_voting(struct raft_configuration *c);

/**
 * Return the index of the server with the given ID (relative to the c->servers
 * array). If there's no server with the given ID, return the number of servers.
 */
size_t raft_configuration__index(struct raft_configuration *c, const unsigned id);

/**
 * Return the index of the voting server with the given ID (relative to the sub
 * array of c->servers that has only voting servers). If there's no server with
 * the given ID, or if it's not flagged as voting, return the number of servers.
 */
size_t raft_configuration__voting_index(struct raft_configuration *c,
                                        const unsigned id);

#endif /* RAFT_CONFIGURATION_H */
