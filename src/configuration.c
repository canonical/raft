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

    for (i = 0; i < c->n; i++) {
        raft_free(c->servers[i].address);
    }

    if (c->servers != NULL) {
        raft_free(c->servers);
    }
}

const struct raft_server *raft_configuration__get(
    const struct raft_configuration *c,
    const unsigned id)
{
    size_t i;

    assert(c != NULL);

    for (i = 0; i < c->n; i++) {
        const struct raft_server *server = &c->servers[i];
        if (server->id == id) {
            return server;
        }
    }

    return NULL;
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

    if (id == 0) {
        return RAFT_ERR_BAD_SERVER_ID;
    }

    if (address == NULL) {
        return RAFT_ERR_NO_SERVER_ADDRESS;
    }

    for (i = 0; i < c->n; i++) {
        server = &c->servers[i];
        if (server->id == id) {
            return RAFT_ERR_DUP_SERVER_ID;
        }
        if (strcmp(server->address, address) == 0) {
            return RAFT_ERR_DUP_SERVER_ADDRESS;
        }
    }

    servers = raft_calloc(c->n + 1, sizeof *server);
    if (servers == NULL) {
        return RAFT_ERR_NOMEM;
    }
    memcpy(servers, c->servers, c->n * sizeof *server);

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

    /* Copy the first part of the servers array into a new array, excluding the
     * i'th server. */
    servers = raft_calloc(c->n - 1, sizeof *servers);
    if (servers == NULL) {
        return RAFT_ERR_NOMEM;
    }
    memcpy(servers, c->servers, i * sizeof *servers);

    raft_free(c->servers[i].address);

    for (j = i + 1; j < c->n; j++) {
        servers[j - 1] = c->servers[j];
    }

    raft_free(c->servers);

    c->servers = servers;
    c->n--;

    return 0;
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
