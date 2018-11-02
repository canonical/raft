#include "../lib/munit.h"

extern MunitSuite raft_election_suites[];
extern MunitSuite raft_liveness_suites[];
extern MunitSuite raft_membership_suites[];
extern MunitSuite raft_replication_suites[];

static MunitSuite suites[] = {
    {"election", NULL, raft_election_suites, 1, 0},
    {"liveness", NULL, raft_liveness_suites, 1, 0},
    {"membership", NULL, raft_membership_suites, 1, 0},
    {"replication", NULL, raft_replication_suites, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};

static MunitSuite suite = {(char *)"", NULL, suites, 1, 0};

/* Test runner executable */
int main(int argc, char *argv[MUNIT_ARRAY_PARAM(argc + 1)])
{
    return munit_suite_main(&suite, (void *)"unit", argc, argv);
}
