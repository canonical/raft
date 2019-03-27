#include "heartbeat.h"
#include "assert.h"
#include "logging.h"

/* Set to 1 to enable tracing. */
#if 1
#define tracef(MSG, ...) debugf(r->io, "heartbeat: " MSG, __VA_ARGS__)
#else
#define tracef(MSG, ...)
#endif

/* static void send_cb(struct raft_io_send *req, int status) */
/* { */
/*     (void)status; */
/*     raft_free(req); */
/* } */

/* static int send_heartbeat(struct raft *r, unsigned i) */
/* { */
/*     struct raft_server *server = &r->configuration.servers[i]; */
/*     struct raft_message message; */
/*     struct raft_append_entries *args = &message.append_entries; */
/*     struct raft_io_send *req; */
/*     int rc; */

/*     assert(r->state == RAFT_LEADER); */
/*     assert(server != NULL); */
/*     assert(server->id != r->id); */
/*     assert(server->id != 0); */
/*     assert(r->leader_state.progress != NULL); */

/*     args->term = r->current_term; */
/*     args->leader_id = r->id; */
/*     args->prev_log_index = 0; */
/*     args->prev_log_term = 0; */
/*     args->leader_commit = r->commit_index; */
/*     args->entries = NULL; */
/*     args->n_entries = 0; */

/*     message.type = RAFT_IO_APPEND_ENTRIES; */
/*     message.server_id = server->id; */
/*     message.server_address = server->address; */

/*     tracef("to server %d", server->id); */

/*     req = raft_malloc(sizeof *req); */
/*     if (req == NULL) { */
/*         rc = RAFT_ENOMEM; */
/*         goto err; */
/*     } */
/*     rc = r->io->send(r->io, req, &message, send_cb); */
/*     if (rc != 0) { */
/*         goto err_after_req_alloc; */
/*     } */

/*     return 0; */

/* err_after_req_alloc: */
/*     raft_free(req); */
/* err: */
/*     assert(rc != 0); */
/*     return rc; */
/* } */

/* void heartbeat__send(struct raft *r) */
/* { */
/*     unsigned i; */
/*     for (i = 0; i < r->configuration.n; i++) { */
/*         struct raft_server *server = &r->configuration.servers[i]; */
/*         int rc; */
/*         if (server->id == r->id) { */
/*             continue; */
/*         } */
/*         rc = send_heartbeat(r, i); */
/*         if (rc != 0 && rc != RAFT_ERR_IO_CONNECT) { */
/*             /\* This is not a critical failure, let's just log it. *\/ */
/*             warnf(r->io, "send heartbeat to server %ld: %s", server->id, */
/*                   raft_strerror(rc)); */
/*         } */
/*     } */
/* } */
