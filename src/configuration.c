#include <assert.h>
#include <string.h>

#include "binary.h"
#include "configuration.h"

#define RAFT_CONFIGURATION__ENCODE_VERSION 1

/* We rely on the size of char to be 8 bits. */
#ifdef static_assert
static_assert(sizeof(char) == sizeof(uint8_t), "Size of 'char' is not 8 bits");
#endif

void raft_configuration_init(struct raft_configuration *c)
{
    c->servers = NULL;
    c->n = 0;
}

void raft_configuration_close(struct raft_configuration *c)
{
    assert(c != NULL);

    if (c->servers != NULL) {
        raft_free(c->servers);
    }
}

const struct raft_server *raft_configuration__get(struct raft_configuration *c,
                                                  const unsigned id)
{
    size_t i;

    assert(c != NULL);

    for (i = 0; i < c->n; i++) {
        struct raft_server *server = &c->servers[i];
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

    assert(c != NULL);

    if (id == 0) {
        return RAFT_ERR_BAD_SERVER_ID;
    }

    if (address == NULL) {
        return RAFT_ERR_NO_SERVER_ADDRESS;
    }

    if (raft_configuration__get(c, id) != NULL) {
        return RAFT_ERR_DUP_SERVER_ID;
    }

    servers = raft_realloc(c->servers, (c->n + 1) * sizeof *server);
    if (servers == NULL) {
        return RAFT_ERR_NOMEM;
    }

    server = &servers[c->n];

    server->id = id;
    server->address = address;
    server->voting = voting;

    c->n++;
    c->servers = servers;

    return 0;
}

size_t raft_configuration__n_voting(struct raft_configuration *c)
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

size_t raft_configuration__index(struct raft_configuration *c,
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

size_t raft_configuration__voting_index(struct raft_configuration *c,
                                        const unsigned id)
{
    size_t i;
    size_t index = 0;

    assert(c != NULL);

    for (i = 0; i < c->n; i++) {
        if (c->servers[i].id == id) {
            if (c->servers[i].voting) {
                return index;
            }
            return c->n;
        }
        if (c->servers[i].voting) {
            index++;
        }
    }
    return c->n;
}
