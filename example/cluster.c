#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define N_SERVERS 3 /* Number of servers in the example cluster */

static void __fork_server(const char *top_dir, int i, int *pid)
{
    *pid = fork();

    if (*pid == 0) {
        char *dir = malloc(strlen(top_dir) + strlen("/D") + 1);
        char *id = malloc(2);
        char *argv[] = {"./example-server", dir, id, NULL};
        char *envp[] = {NULL};

        sprintf(dir, "%s/%d", top_dir, i + 1);
        sprintf(id, "%d", i + 1);

        execve("./example-server", argv, envp);
    }
}

int main(int argc, char *argv[])
{
    const char *top_dir = "/tmp/raft";
    struct stat sb;
    int pids[N_SERVERS];
    int i;
    int rv;

    if (argc > 2) {
        printf("usage: example-cluster [<dir>]\n");
        return 1;
    }

    if (argc == 2) {
        top_dir = argv[1];
    }

    /* Make sure the top level directory exists. */
    rv = stat(top_dir, &sb);
    if (rv != 0) {
        if (errno == ENOENT) {
            rv = mkdir(top_dir, 0700);
            if (rv != 0) {
                printf("error: create top level directory '%s': %s", top_dir,
                       strerror(errno));
                return 1;
            }
        } else {
            printf("error: access top level directory '%s': %s", top_dir,
                   strerror(errno));
            return 1;
        }
    } else if ((sb.st_mode & S_IFMT) != S_IFDIR) {
        printf("error: path '%s' is not a directory", top_dir);
        return 1;
    }

    /* Spawn the cluster nodes */
    for (i = 0; i < N_SERVERS; i++) {
        __fork_server(top_dir, i, &pids[i]);
    }

    while (1) {
        struct timespec interval;
        int status;

        /* Sleep a little bit. */
        interval.tv_sec = 11 + random() % 5;
        interval.tv_nsec = 0;

        rv = nanosleep(&interval, NULL);
        if (rv != 0) {
            printf("error: sleep: %s", strerror(errno));
        }

        /* Kill a random server. */
        i = random() % N_SERVERS;

        rv = kill(pids[i], SIGINT);
        if (rv != 0) {
            printf("error: kill server %d: %s", i, strerror(errno));
        }

        waitpid(pids[i], &status, 0);

        __fork_server(top_dir, i, &pids[i]);
    }

    return 0;
}
