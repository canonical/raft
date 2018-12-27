#include "../lib/munit.h"

extern MunitSuite raft_checksum_suites[];
extern MunitSuite raft_client_suites[];
extern MunitSuite raft_configuration_suites[];
extern MunitSuite raft_context_suites[];
extern MunitSuite raft_election_suites[];
extern MunitSuite raft_encoding_suites[];
extern MunitSuite raft_error_suites[];
extern MunitSuite raft_io_suites[];
extern MunitSuite raft_io_queue_suites[];
#if RAFT_IO_STUB
extern MunitSuite raft_io_stub_suites[];
#endif
#if RAFT_IO_UV
extern MunitSuite raft_io_uv_suites[];
extern MunitSuite raft_io_uv_fs_suites[];
#endif
extern MunitSuite raft_log_suites[];
extern MunitSuite raft_logger_suites[];
extern MunitSuite raft_queue_suites[];
extern MunitSuite raft_replication_suites[];
extern MunitSuite raft_rpc_request_vote_suites[];
extern MunitSuite raft_rpc_append_entries_suites[];
extern MunitSuite raft_tick_suites[];
extern MunitSuite raft_suites[];

static MunitSuite suites[] = {
    {"checksum", NULL, raft_checksum_suites, 1, 0},
    {"client", NULL, raft_client_suites, 1, 0},
    {"configuration", NULL, raft_configuration_suites, 1, 0},
    {"context", NULL, raft_context_suites, 1, 0},
    {"election", NULL, raft_election_suites, 1, 0},
    {"encoding", NULL, raft_encoding_suites, 1, 0},
    {"error", NULL, raft_error_suites, 1, 0},
    {"io", NULL, raft_io_suites, 1, 0},
    {"io-queue", NULL, raft_io_queue_suites, 1, 0},
#if RAFT_IO_STUB
    {"io-stub", NULL, raft_io_stub_suites, 1, 0},
#endif
#if RAFT_IO_UV
    {"io-uv", NULL, raft_io_uv_suites, 1, 0},
    {"io-uv-fs", NULL, raft_io_uv_fs_suites, 1, 0},
#endif
    {"log", NULL, raft_log_suites, 1, 0},
    {"logger", NULL, raft_logger_suites, 1, 0},
    {"queue", NULL, raft_queue_suites, 1, 0},
    {"replication", NULL, raft_replication_suites, 1, 0},
    {"rpc-request-vote", NULL, raft_rpc_request_vote_suites, 1, 0},
    {"rpc-append_entries", NULL, raft_rpc_append_entries_suites, 1, 0},
    {"tick", NULL, raft_tick_suites, 1, 0},
    {"raft", NULL, raft_suites, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};

static MunitSuite suite = {(char *)"", NULL, suites, 1, 0};

/* Test runner executable */
int main(int argc, char *argv[MUNIT_ARRAY_PARAM(argc + 1)])
{
    return munit_suite_main(&suite, (void *)"unit", argc, argv);
}
