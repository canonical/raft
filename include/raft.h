#ifndef RAFT_H
#define RAFT_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#if RAFT_IO_STUB
#include "raft/io_stub.h"
#endif

#if RAFT_IO_UV
#include "raft/io_uv.h"
#endif

/**
 * Error codes.
 */
enum {
    RAFT_ERR_NOMEM = 1,
    RAFT_ERR_INTERNAL,
    RAFT_ERR_BAD_SERVER_ID,
    RAFT_ERR_UNKNOWN_SERVER_ID,
    RAFT_ERR_DUP_SERVER_ID,
    RAFT_ERR_DUP_SERVER_ADDRESS,
    RAFT_ERR_SERVER_ALREADY_VOTING,
    RAFT_ERR_EMPTY_CONFIGURATION,
    RAFT_ERR_CONFIGURATION_NOT_EMPTY,
    RAFT_ERR_MALFORMED,
    RAFT_ERR_NO_SPACE,
    RAFT_ERR_BUSY,
    RAFT_ERR_NOT_LEADER,
    RAFT_ERR_SHUTDOWN,
    RAFT_ERR_CONFIGURATION_BUSY,
    RAFT_ERR_IO,
    RAFT_ERR_IO_CORRUPT,
    RAFT_ERR_IO_ABORTED,
    RAFT_ERR_IO_NAMETOOLONG,
    RAFT_ERR_IO_MALFORMED,
    RAFT_ERR_IO_NOTEMPTY,
    RAFT_ERR_IO_TOOBIG,
    RAFT_ERR_IO_CONNECT
};

/**
 * Map error codes to error messages.
 */
#define RAFT_ERRNO_MAP(X)                                                \
    X(RAFT_ERR_NOMEM, "out of memory")                                   \
    X(RAFT_ERR_INTERNAL, "internal error")                               \
    X(RAFT_ERR_BAD_SERVER_ID, "server ID is not valid")                  \
    X(RAFT_ERR_UNKNOWN_SERVER_ID, "server ID is unknown")                \
    X(RAFT_ERR_DUP_SERVER_ID, "server ID already in use")                \
    X(RAFT_ERR_DUP_SERVER_ADDRESS, "server address already in use")      \
    X(RAFT_ERR_SERVER_ALREADY_VOTING, "server is already voting")        \
    X(RAFT_ERR_EMPTY_CONFIGURATION, "configuration has no servers")      \
    X(RAFT_ERR_CONFIGURATION_NOT_EMPTY, "configuration has servers")     \
    X(RAFT_ERR_MALFORMED, "encoded data is malformed")                   \
    X(RAFT_ERR_NO_SPACE, "no space left on device")                      \
    X(RAFT_ERR_BUSY, "an append entries request is already in progress") \
    X(RAFT_ERR_NOT_LEADER, "server is not the leader")                   \
    X(RAFT_ERR_CONFIGURATION_BUSY,                                       \
      "a configuration change is already in progress")                   \
    X(RAFT_ERR_IO, "I/O error")                                          \
    X(RAFT_ERR_IO_CORRUPT, "persisted data is corrupted")                \
    X(RAFT_ERR_IO_ABORTED, "backend was stopped or has errored")         \
    X(RAFT_ERR_IO_NAMETOOLONG, "data directory path is too long")        \
    X(RAFT_ERR_IO_MALFORMED, "encoded data is malformed")                \
    X(RAFT_ERR_IO_NOTEMPTY, "persisted log is not empty")                \
    X(RAFT_ERR_IO_TOOBIG, "data is too big")                             \
    X(RAFT_ERR_IO_CONNECT, "no connection to remote server available")

/**
 * Return the error message describing the given error code.
 */
const char *raft_strerror(int errnum);

/**
 * User-definable dynamic memory allocation functions.
 *
 * The @data field will be passed as first argument to all functions.
 */
struct raft_heap
{
    void *data; /* User data */
    void *(*malloc)(void *data, size_t size);
    void (*free)(void *data, void *ptr);
    void *(*calloc)(void *data, size_t nmemb, size_t size);
    void *(*realloc)(void *data, void *ptr, size_t size);
    void *(*aligned_alloc)(void *data, size_t alignment, size_t size);
};

void *raft_malloc(size_t size);
void raft_free(void *ptr);
void *raft_calloc(size_t nmemb, size_t size);
void *raft_realloc(void *ptr, size_t size);
void *raft_aligned_alloc(size_t alignment, size_t size);

/**
 * Use a custom dynamic memory allocator.
 */
void raft_heap_set(struct raft_heap *heap);

/**
 * Use the default dynamic memory allocator (from the stdlib). This clears any
 * custom allocator specified with @raft_heap_set.
 */
void raft_heap_set_default();

/**
 * Hold the value of a raft term. Guaranteed to be at least 64-bit long.
 */
typedef unsigned long long raft_term;

/**
 * Hold the value of a raft entry index. Guaranteed to be at least 64-bit long.
 */
typedef unsigned long long raft_index;

/**
 * Logging levels.
 */
enum { RAFT_DEBUG, RAFT_INFO, RAFT_WARN, RAFT_ERROR };

/**
 * Handle log messages at different levels.
 *
 * The @data field will be passed as first argument to the @emit function.
 */
struct raft_logger
{
    void *data;
    void (*emit)(void *data, int level, const char *fmt, va_list args);
};

/**
 * Emit a message with level #RAFT_DEBUG
 */
void raft_debugf(struct raft_logger *logger, const char *format, ...);

/**
 * Emit a message with level #RAFT_INFO
 */
void raft_infof(struct raft_logger *logger, const char *format, ...);

/**
 * Emit a message with level #RAFT_WARN
 */
void raft_warnf(struct raft_logger *logger, const char *format, ...);

/**
 * Emit a message with level #RAFT_ERROR
 */
void raft_errorf(struct raft_logger *logger, const char *format, ...);

/**
 * Default logger, emitting messages to stderr.
 */
extern struct raft_logger raft_default_logger;

/**
 * Optionally set the server ID that the default logger will include in emitted
 * messages. The default is to not include any server ID in emitted messages.
 */
void raft_default_logger_set_server_id(unsigned id);

/**
 * Emit only messages of this level or above. The default is #RAFT_WARN.
 */
void raft_default_logger_set_level(int level);

/**
 * A data buffer.
 */
struct raft_buffer
{
    void *base; /* Pointer to the buffer data */
    size_t len; /* Length of the buffer. */
};

/**
 * Hold information about a single server in the cluster configuration.
 */
struct raft_server
{
    unsigned id;   /* Server ID, must be greater than zero. */
    char *address; /* Server address. User defined. */
    bool voting;   /* Whether this is a voting server. */
};

/**
 * Hold information about all servers part of the cluster.
 */
struct raft_configuration
{
    struct raft_server *servers; /* Array of servers member of the cluster. */
    unsigned n;                  /* Number of servers in the array. */
};

void raft_configuration_init(struct raft_configuration *c);

void raft_configuration_close(struct raft_configuration *c);

/**
 * Add a server to a raft configuration.
 *
 * The @id must be greater than zero and @address point to a valid string.
 *
 * If @id or @address are already in use by another server in the configuration,
 * an error is returned.
 *
 * The @address string will be copied and can be released after this function
 * returns.
 */
int raft_configuration_add(struct raft_configuration *c,
                           const unsigned id,
                           const char *address,
                           const bool voting);

/**
 * Remove a server from a raft configuration. The given ID must match the one of
 * an existing server in the configuration.
 */
int raft_configuration_remove(struct raft_configuration *c, const unsigned id);

/**
 * Encode a raft configuration object.
 *
 * The memory of the returned buffer is allocated using raft_malloc(), and
 * client code is responsible for releasing it when no longer needed. The raft
 * library makes no use of that memory after this function returns.
 */
int raft_configuration_encode(const struct raft_configuration *c,
                              struct raft_buffer *buf);

/**
 * Populate a configuration object by decoding the given serialized payload.
 */
int raft_configuration_decode(const struct raft_buffer *buf,
                              struct raft_configuration *c);

/**
 * Log entry types.
 */
enum { RAFT_LOG_COMMAND = 1, RAFT_LOG_CONFIGURATION };

/**
 * A single entry in the raft log.
 *
 * From Figure 3.1:
 *
 *   Each contains [either a] command for the state machine [or a configuration
 *   change], and term when entry was received by the leader.
 *
 * An entry that originated from this raft instance while it was the leader
 * (typically via client calls to raft_propose()) should normaly have a @buf
 * attribute that points directly to the memory that was originally allocated by
 * the client itself to contain the entry data, and the @batch attribute is set
 * to #NULL.
 *
 * An entry that was received from the network upon an AppendEntries RPC or that
 * was loaded from disk at startup should normally have a @batch attribute that
 * points to a contiguous chunk of memory containing the data of the entry
 * itself plus possibly the data for other entries that were received or loaded
 * with it in the same request. In this case the @buf pointer will be equal to
 * the @batch pointer plus an offset, that locates the position of the entry's
 * data within the batch.
 *
 * When the @batch attribute is not #NULL the raft library will take care of
 * releasing that memory only once there are no more references to the
 * associated entries.
 *
 * This arrangement makes it possible to perform "zero copy" I/O in most cases.
 */
struct raft_entry
{
    raft_term term;         /* Term in which the entry was created */
    unsigned short type;    /* Entry type (FSM command or config change) */
    struct raft_buffer buf; /* Entry data */
    void *batch;            /* Batch that buf's memory points to, if any. */
};

/**
 * Counter for outstanding references to a log entry. When an entry is first
 * appended to the log, its refcount is set to one (the log itself is the only
 * one referencing the entry). Whenever an entry is included in an I/O request
 * (write entries to disk or send entries to other servers) its refcount is
 * increased by one. Whenever an entry gets deleted from the log its refcount is
 * decreased by one, likewise whenever an I/O request is completed the refcount
 * of the relevant entries is decreased by one. When the refcount drops to zero
 * the memory pointed to by its @buf attribute gets released, or if the @batch
 * attribute is non-NULL a check is made to see if there's any other entry of
 * the same batch with a non-zero refcount, and the memory pointed at by @batch
 * itself is released if there's no such other entry.
 */
struct raft_entry_ref
{
    raft_term term;              /* Term of the entry being ref-counted */
    raft_index index;            /* Index of the entry being ref-counted */
    unsigned short count;        /* Number of references */
    struct raft_entry_ref *next; /* Next item in the bucket (for collisions) */
};

/**
 * In-memory cache of the persistent raft log stored on disk.
 *
 * The raft log cache is implemented as a circular buffer of log entries, which
 * makes some common operations (e.g. deleting the first N entries when
 * snapshotting) very efficient.
 */
struct raft_log
{
    struct raft_entry *entries;  /* Buffer of log entries. */
    size_t size;                 /* Number of available slots in the buffer */
    size_t front, back;          /* Indexes of used slots [front, back). */
    raft_index offset;           /* Index offest of the first entry. */
    struct raft_entry_ref *refs; /* Log entries reference counts hash table */
    size_t refs_size;            /* Size of the reference counts hash table */
};

/**
 * Hold the arguments of a RequestVote RPC (figure 3.1).
 *
 * The RequestVote RPC is invoked by candidates to gather votes (figure 3.1).
 */
struct raft_request_vote
{
    raft_term term;            /* Candidate's term. */
    unsigned candidate_id;     /* ID of the server requesting the vote. */
    raft_index last_log_index; /* Index of candidate's last log entry. */
    raft_index last_log_term;  /* Term of log entry at last_log_index. */
};

/**
 * Hold the result of a RequestVote RPC (figure 3.1).
 */
struct raft_request_vote_result
{
    raft_term term;    /* Receiver's current_term (candidate updates itself). */
    bool vote_granted; /* True means candidate received vote. */
};

/**
 * Hold the arguments of an AppendEntries RPC.
 *
 * The AppendEntries RPC is invoked by the leader to replicate log entries. It's
 * also used as heartbeat (figure 3.1).
 */
struct raft_append_entries
{
    raft_term term;             /* Leader's term. */
    unsigned leader_id;         /* So follower can redirect clients. */
    raft_index prev_log_index;  /* Index of log entry preceeding new ones. */
    raft_term prev_log_term;    /* Term of entry at prev_log_index. */
    raft_index leader_commit;   /* Leader's commit_index. */
    struct raft_entry *entries; /* Log entries to append. */
    unsigned n_entries;         /* Size of the log entries array. */
};

/**
 * Hold the result of an AppendEntries RPC (figure 3.1).
 */
struct raft_append_entries_result
{
    raft_term term; /* Receiver's current_term, for leader to update itself. */
    bool success; /* True if follower had entry matching prev_log_index/term. */
    raft_index last_log_index; /* Receiver's last log entry index, as hint */
};

/**
 * Type codes for raft I/O requests.
 */
enum {
    RAFT_IO_APPEND_ENTRIES = 1,
    RAFT_IO_APPEND_ENTRIES_RESULT,
    RAFT_IO_REQUEST_VOTE,
    RAFT_IO_REQUEST_VOTE_RESULT
};

struct raft_message
{
    unsigned short type;
    unsigned server_id;
    const char *server_address;
    union {
        struct raft_request_vote request_vote;
        struct raft_request_vote_result request_vote_result;
        struct raft_append_entries append_entries;
        struct raft_append_entries_result append_entries_result;
    };
};

/**
 * I/O backend interface implementing periodic ticks, log store read/writes
 * and send/receive of network RPCs.
 */
struct raft_io
{
    /**
     * API version implemented by this instance. Currently 1.
     */
    int version;

    /**
     * Custom user data.
     */
    void *data;

    /**
     * Start the backend.
     *
     * From now on the implementation must start accepting RPC requests and must
     * invoke the @tick callback every @msecs milliseconds. The @recv callback
     * must be invoked when receiving a message.
     */
    int (*start)(const struct raft_io *io,
                 unsigned id,
                 const char *address,
                 unsigned msecs,
                 void *data,
                 void (*tick)(void *data, unsigned elapsed),
                 void (*recv)(void *data, struct raft_message *message));

    /**
     * Immediately cancel any in-progress I/O and stop invoking the tick
     * function.
     */
    int (*stop)(const struct raft_io *io, void *data, void (*cb)(void *data));

    /**
     * Read persisted state from storage.
     *
     * The implementation must synchronously read the current state from
     * disk.
     *
     * The entries array must be allocated with raft_malloc. Once the
     * request is completed ownership of such memory is transfered to the
     * raft instance.
     *
     * This request is guaranteed to be the very first request issued agaist the
     * backend. No further load request will be issued.
     */
    int (*load)(const struct raft_io *io,
                raft_term *term,
                unsigned *voted_for,
                raft_index *start_index,
                struct raft_entry **entries,
                size_t *n_entries);

    /**
     * Bootstrap a server belonging to a new cluster.
     *
     * The I/O implementation must synchronously persist the given configuration
     * as the first entry of the log. The current persisted term must be set to
     * 1 and the vote to nil.
     *
     * If an attempt is made to bootstrap a server that has already some sate,
     * then #RAFT_IO_CANTBOOTSTRAP must be returned.
     */
    int (*bootstrap)(const struct raft_io *io,
                     const struct raft_configuration *conf);

    /**
     * Synchronously persist current term (and nil vote). The implementation
     * MUST ensure that the change is durable before returning (e.g. using
     * fdatasync() or #O_DSYNC).
     */
    int (*set_term)(struct raft_io *io, const raft_term term);

    /**
     * Synchronously persist who we voted for. The implementation MUST ensure
     * that the change is durable before returning (e.g. using fdatasync() or
     * #O_DIRECT).
     */
    int (*set_vote)(struct raft_io *io, const unsigned server_id);

    /**
     * Asynchronously append the given entries to the log.
     *
     * The implementation is guaranteed that the memory holding the given
     * entries will not be released until the @cb callback is invoked.
     */
    int (*append)(const struct raft_io *io,
                  const struct raft_entry entries[],
                  unsigned n,
                  void *data,
                  void (*cb)(void *data, int status));

    /**
     * Synchronously delete all log entries from the given index onwards.
     */
    int (*truncate)(const struct raft_io *io, raft_index index);

    /**
     * Asynchronously send a message.
     */
    int (*send)(const struct raft_io *io,
                const struct raft_message *message,
                void *data,
                void (*cb)(void *data, int status));
};

/**
 * Interface for the user-implemented finate state machine replicated through
 * Raft.
 */
struct raft_fsm
{
    int version; /* API version implemented by this instance. Currently 1. */
    void *data;  /* Custom user data. */

    /**
     * Apply a committed RAFT_LOG_COMMAND entry to the state machine.
     */
    int (*apply)(struct raft_fsm *fsm, const struct raft_buffer *buf);
};

/**
 * State codes.
 */
enum {
    RAFT_STATE_UNAVAILABLE,
    RAFT_STATE_FOLLOWER,
    RAFT_STATE_CANDIDATE,
    RAFT_STATE_LEADER
};

/**
 * Server state names ('unavailable', 'follower', 'candidate', 'leader'),
 * indexed by state code.
 */
extern const char *raft_state_names[];

/**
 * Event types IDs.
 */
enum {
    /**
     * Fired when the server state changes.
     *
     * The event data is a pointer to an unsigned short integer holding the
     * value of the previous state.
     *
     * The initial state is always RAFT_STATE_FOLLOWER.
     */
    RAFT_EVENT_STATE_CHANGE = 0,

    /**
     * Fired when a log command was committed and applied.
     *
     * The event data is a pointer to a @raft_index holding the index of the log
     * entry that was applied.
     */
    RAFT_EVENT_COMMAND_APPLIED,

    /**
     * Fired when a new configuration was committed and applied.
     *
     * The event data is a pointer to the new @raft_configuration.
     */
    RAFT_EVENT_CONFIGURATION_APPLIED,

    /**
     * Fired after @raft_promote has been called, but the server to be promoted
     * hasn't caught up with logs within a reasonable amount of time or if this
     * server has lost leadership while waiting for the server to be promoted to
     * catch up.
     *
     * The event data is a pointer to an unsigned int holding the ID of the
     * server that was being promoted.
     */
    RAFT_EVENT_PROMOTION_ABORTED
};

/**
 * Number of available event types.
 */
#define RAFT_EVENT_N (RAFT_EVENT_PROMOTION_ABORTED + 1)

/**
 * Hold and drive the state of a single raft server in a cluster.
 */
struct raft
{
    /**
     * Logger to use to emit messages.
     */
    struct raft_logger *logger;

    /**
     * User-defined disk and network I/O interface implementation.
     */
    struct raft_io *io;

    /**
     * User-defined finite state machine to apply command to.
     */
    struct raft_fsm *fsm;

    /**
     * Server ID of this raft instance.
     */
    unsigned id;

    /**
     * Server address of this raft instance.
     */
    char *address;

    /**
     * Custom user data. It will be passed back to callbacks registered with
     * raft_watch().
     */
    void *data;

    /**
     * The fields below are a cache of the server's persistent state, updated on
     * stable storage before responding to RPCs (Figure 3.1).
     */
    raft_term current_term; /* Latest term server has seen. */
    unsigned voted_for;     /* Candidate that received vote in current term. */
    struct raft_log log;    /* Log entries. */

    /**
     * Current membership configuration (Chapter 4).
     *
     * At any given moment the current configuration can be committed or
     * uncommitted.
     *
     * If a server is voting, the log entry with index 1 must always contain the
     * first committed configuration.
     *
     * The possible scenarios are:
     *
     * 1. #configuration_index and #configuration_uncommited_index are both
     *    zero. This should only happen when a brand new server starts joining a
     *    cluster and is waiting to receive log entries from the current
     *    leader. In this case #configuration must be empty and have no servers.
     *
     * 2. #configuration_index is non-zero while #configuration_uncommited_index
     *    is zero. In this case the content of #configuration must match the one
     *    of the log entry at #configuration_index.
     *
     * 3. #configuration_index and #configuration_uncommited_index are both
     *    non-zero, with the latter being greater than the former. In this case
     *    the content of #configuration must match the one of the log entry at
     *    #configuration_uncommitted_index.
     */
    struct raft_configuration configuration;
    raft_index configuration_index;
    raft_index configuration_uncommitted_index;

    /**
     * Election timeout in milliseconds (default 1000).
     *
     * From 3.4:
     *
     *   Raft uses a heartbeat mechanism to trigger leader election. When
     *   servers start up, they begin as followers. A server remains in follower
     *   state as long as it receives valid RPCs from a leader or
     *   candidate. Leaders send periodic heartbeats (AppendEntries RPCs that
     *   carry no log entries) to all followers in order to maintain their
     *   authority. If a follower receives no communication over a period of
     *   time called the election timeout, then it assumes there is no viable
     *   leader and begins an election to choose a new leader.
     *
     * This is the baseline value and will be randomized between 1x and 2x.
     *
     * See raft_change_election_timeout() to customize the value of this
     * attribute.
     */
    unsigned election_timeout;

    /**
     * Heartbeat timeout in milliseconds (default 100). This is relevant only
     * for when the raft instance is in leader state: empty AppendEntries RPCs
     * will be sent if this amount of milliseconds elapses without any
     * user-triggered AppendEntries RCPs being sent.
     *
     * From Figure 3.1:
     *
     *   [Leaders] Send empty AppendEntries RPC during idle periods to prevent
     *   election timeouts.
     */
    unsigned heartbeat_timeout;

    /**
     * The fields below hold the part of the server's volatile state which
     * is always applicable regardless of the whether the server is
     * follower, candidate or leader (Figure 3.1). This state is rebuilt
     * automatically after a server restart.
     */
    raft_index commit_index; /* Highest log entry known to be committed */
    raft_index last_applied; /* Highest log entry applied to the FSM */

    /**
     * Current server state of this raft instance, along with a union defining
     * state-specific values.
     */
    unsigned short state;
    union {
        struct
        {
            /**
             * The fields below hold the part of the server's volatile state
             * which is specific to followers.
             */
            unsigned current_leader_id;
        } follower_state;

        struct
        {
            /**
             * The fields below hold the part of the server's volatile state
             * which is specific to candidates. This state is reinitialized
             * after the server starts a new election round.
             */
            bool *votes; /* For each server, whether vote was granted */
        } candidate_state;

        struct
        {
            /**
             * The fields below hold the part of the server's volatile state
             * which is specific to leaders (Figure 3.1). This state is
             * reinitialized after the server gets elected.
             */
            raft_index *next_index;  /* For each server, next entry to send */
            raft_index *match_index; /* For each server, highest applied idx */

            /**
             * Fields used to track the progress of pushing entries to the
             * server being promoted (4.2.1 Catching up new servers).
             */
            unsigned promotee_id;        /* ID of server being promoted, or 0 */
            unsigned short round_number; /* Number of the current sync round */
            raft_index round_index;      /* Target of the current round */
            unsigned round_duration;     /* Duration of the current round */
        } leader_state;
    };

    /**
     * Random generator. Defaults to stdlib rand().
     */
    int (*rand)();

    /**
     * Current election timeout. Randomized from election_timeout.
     *
     * From §9.3:
     *
     *   We recommend using a timeout range that is ten times the one-way
     *   network latency (even if the true network latency is five times greater
     *   than anticipated, most clusters would still be able to elect a leader
     *   in a timely manner).
     */
    unsigned election_timeout_rand;

    /**
     * For followers and candidates, time elapsed since the last election
     * started, in millisecond. For leaders time elapsed since the last
     * AppendEntries RPC, in milliseconds.
     */
    unsigned timer;

    /**
     * Registered watchers.
     */
    void (*watchers[RAFT_EVENT_N])(void *, int, void *);

    /**
     * Callback to invoke once a stop request has completed.
     */
    struct
    {
        void *data;
        void (*cb)(void *data);
    } stop;
};

/**
 * Initialize a raft server object.
 */
int raft_init(struct raft *r,
              struct raft_logger *logger,
              struct raft_io *io,
              struct raft_fsm *fsm,
              void *data,
              unsigned id,
              const char *address);

/**
 * Close a raft instance, deallocating all used resources.
 */
void raft_close(struct raft *r);

/**
 * Bootstrap this raft instance using the given configuration. The instance must
 * not have been started yet and must be completely pristine.
 */
int raft_bootstrap(struct raft *r, const struct raft_configuration *conf);

/**
 * Start this raft instance.
 */
int raft_start(struct raft *r);

/**
 * Stop this raft instance. This should be called only after a successful call
 * to @raft_start.
 */
int raft_stop(struct raft *r, void *data, void (*cb)(void *data));

/**
 * Set a custom rand() function.
 */
void raft_set_rand(struct raft *r, int (*rand)());

/**
 * Set the election timeout.
 *
 * Every raft instance is initialized with a default election timeout of 1000
 * milliseconds. If you wish to tweak it, call this function before starting
 * your event loop.
 *
 * From Chapter 9:
 *
 *   We recommend a range that is 10–20 times the one-way network latency, which
 *   keeps split votes rates under 40% in all cases for reasonably sized
 *   clusters, and typically results in much lower rates.
 */
void raft_set_election_timeout(struct raft *r, const unsigned election_timeout);

/**
 * Human readable version of the current state.
 */
const char *raft_state_name(struct raft *r);

/**
 * Propose to append new FSM commands to the log.
 *
 * If this server is the leader, it will create @n new log entries of type
 * #RAFT_LOG_COMMAND using the given buffers as their payloads, append them to
 * its own log and attempt to replicate them on other servers by sending
 * AppendEntries RPCs.
 *
 * The memory pointed at by the @base attribute of each #raft_buffer in the
 * given array must have been allocated with raft_malloc() or a compatible
 * allocator. If this function returns 0, the ownership of this memory is
 * implicitely transferred to the raft library, which will take care of
 * releasing it when appropriate. Any further client access to such memory leads
 * to undefined behavior.
 */
int raft_propose(struct raft *r,
                 const struct raft_buffer bufs[],
                 const unsigned n);

/**
 * Add a new non-voting server to the cluster configuration.
 */
int raft_add_server(struct raft *r, const unsigned id, const char *address);

/**
 * Promote the given new non-voting server to be a voting one.
 */
int raft_promote(struct raft *r, const unsigned id);

/**
 * Remove the given server from the cluster configuration.
 */
int raft_remove_server(struct raft *r, const unsigned id);

/**
 * Register a callback to be fired upon the given event.
 *
 * The @cb callback will be invoked the next time the event with the given ID
 * occurs and will be passed back the @data pointer set on @r, the event ID and
 * a pointer to event-specific information.
 *
 * At most one callback can be registered for each event. Passing a NULL
 * callback disable notifications for that event.
 */
void raft_watch(struct raft *r, int event, void (*cb)(void *, int, void *));

#endif /* RAFT_H */
