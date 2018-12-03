#include <assert.h>
#include <string.h>

#include "binary.h"
#include "configuration.h"

void raft_configuration_init(struct raft_configuration *c)
{
    c->servers = NULL;
    c->n = 0;
}

void raft_configuration_close(struct raft_configuration *c)
{
    size_t i;

    assert(c != NULL);
    assert(c->n == 0 || c->servers != NULL);

    for (i = 0; i < c->n; i++) {
        raft_free(c->servers[i].address);
    }

    if (c->servers != NULL) {
        raft_free(c->servers);
    }
}

size_t raft_configuration__index(const struct raft_configuration *c,
                                 const unsigned id)
{
    size_t i;

    assert(c != NULL);

    for (i = 0; i < c->n; i++) {
        if (c->servers[i].id == id) {
            return i;
        }
    }

    return c->n;
}

size_t raft_configuration__voting_index(const struct raft_configuration *c,
                                        const unsigned id)
{
    size_t i;
    size_t j = 0;

    assert(c != NULL);

    for (i = 0; i < c->n; i++) {
        if (c->servers[i].id == id) {
            if (c->servers[i].voting) {
                return j;
            }
            return c->n;
        }
        if (c->servers[i].voting) {
            j++;
        }
    }

    return c->n;
}

const struct raft_server *raft_configuration__get(
    const struct raft_configuration *c,
    const unsigned id)
{
    size_t i;

    assert(c != NULL);
    assert(id > 0);

    /* Grab the index of the server with the given ID */
    i = raft_configuration__index(c, id);

    if (i == c->n) {
      /* No server with matching ID. */
      return NULL;
    }

    assert(i < c->n);

    return &c->servers[i];
}

size_t raft_configuration__n_voting(const struct raft_configuration *c)
{
    size_t i;
    size_t n = 0;

    assert(c != NULL);

    for (i = 0; i < c->n; i++) {
        if (c->servers[i].voting) {
            n++;
        }
    }

    return n;
}

int raft_configuration__copy(const struct raft_configuration *c1,
                             struct raft_configuration *c2)
{
    size_t i;
    int rv;

    assert(c1 != NULL);
    assert(c2 != NULL);

    for (i = 0; i < c1->n; i++) {
        struct raft_server *server = &c1->servers[i];
        rv = raft_configuration_add(c2, server->id, server->address,
                                    server->voting);
        if (rv != 0) {
            return rv;
        }
    }

    return 0;
}

int raft_configuration_add(struct raft_configuration *c,
                           const unsigned id,
                           const char *address,
                           const bool voting)
{
    struct raft_server *servers;
    struct raft_server *server;
    size_t i;

    assert(c != NULL);
    assert(id != 0);

    /* Check that neither the given id or address is already in use */
    for (i = 0; i < c->n; i++) {
        server = &c->servers[i];
        if (server->id == id) {
            return RAFT_ERR_DUP_SERVER_ID;
        }
        if (strcmp(server->address, address) == 0) {
            return RAFT_ERR_DUP_SERVER_ADDRESS;
        }
    }

    /* Grow the servers array */
    servers = raft_calloc(c->n + 1, sizeof *server);
    if (servers == NULL) {
        return RAFT_ERR_NOMEM;
    }
    memcpy(servers, c->servers, c->n * sizeof *server);

    /* Fill the newly allocated slot (the last one) with the given details. */
    server = &servers[c->n];

    server->id = id;

    server->address = raft_malloc(strlen(address) + 1);
    if (server->address == NULL) {
        raft_free(servers);
        return RAFT_ERR_NOMEM;
    }
    strcpy(server->address, address);

    server->voting = voting;

    if (c->servers != NULL) {
        raft_free(c->servers);
    }

    c->n++;
    c->servers = servers;

    return 0;
}

int raft_configuration_remove(struct raft_configuration *c, const unsigned id)
{
    size_t i;
    size_t j;
    struct raft_server *servers;

    assert(c != NULL);

    i = raft_configuration__index(c, id);
    if (i == c->n) {
        return RAFT_ERR_UNKNOWN_SERVER_ID;
    }

    assert(i < c->n);

    /* If this is the last server in the configuration, reset everything. */
    if (c->n - 1 == 0) {
        raft_free(c->servers[0].address);
        raft_free(c->servers);
        c->n = 0;
        c->servers = NULL;
        return 0;
    }

    /* Shrink the servers array. */
    servers = raft_calloc(c->n - 1, sizeof *servers);
    if (servers == NULL) {
        return RAFT_ERR_NOMEM;
    }

    /* Copy the first part of the servers array into a new array, excluding the
     * i'th server. */
    for (j = 0; j < i; j++) {
        servers[j] = c->servers[j];
    }

    /* Copy the second part of the servers array into a new array. */
    for (j = i + 1; j < c->n; j++) {
        servers[j - 1] = c->servers[j];
    }

    /* Release the address of the server that was deleted. */
    raft_free(c->servers[i].address);

    /* Release the old servers array */
    raft_free(c->servers);

    c->servers = servers;
    c->n--;

    return 0;
}
