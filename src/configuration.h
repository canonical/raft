/**
 * Modify and inspect @raft_configuration objects.
 */

#ifndef CONFIGURATION_H_
#define CONFIGURATION_H_

#include "../include/raft.h"

/**
 * Return the number of voting servers.
 */
size_t configuration__n_voting(const struct raft_configuration *c);

/**
 * Return the index of the server with the given ID (relative to the c->servers
 * array). If there's no server with the given ID, return the number of servers.
 */
size_t configuration__index_of(const struct raft_configuration *c, unsigned id);

/**
 * Return the index of the voting server with the given ID (relative to the sub
 * array of c->servers that has only voting servers). If there's no server with
 * the given ID, or if it's not flagged as voting, return the number of servers.
 */
size_t configuration__index_of_voting(const struct raft_configuration *c,
                                      unsigned id);

/**
 * Get the server with the given ID, or #NULL if no matching server is found.
 */
const struct raft_server *configuration__get(const struct raft_configuration *c,
                                             unsigned id);

/**
 * Remove a server from a raft configuration. The given ID must match the one of
 * an existing server in the configuration.
 */
int configuration__remove(struct raft_configuration *c, const unsigned id);

/**
 * Add all servers in c1 to c2 (which must be empty).
 */
int configuration__copy(const struct raft_configuration *c1,
                        struct raft_configuration *c2);

/**
 * Number of bytes needed to encode the given configuration object.
 */
size_t configuration__encoded_size(const struct raft_configuration *c);

/**
 * Encode to the given pre-allocated buffer.
 */
void configuration__encode_to_buf(const struct raft_configuration *c,
                                  void *buf);

/**
 * Encode a raft configuration object.
 *
 * The memory of the returned buffer is allocated using raft_malloc(), and
 * client code is responsible for releasing it when no longer needed. The raft
 * library makes no use of that memory after this function returns.
 */
int configuration__encode(const struct raft_configuration *c,
                          struct raft_buffer *buf);

/**
 * Populate a configuration object by decoding the given serialized payload.
 */
int configuration__decode(const struct raft_buffer *buf,
                          struct raft_configuration *c);

#endif /* CONFIGURATION_H_ */
