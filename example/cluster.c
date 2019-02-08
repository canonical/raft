#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define N_SERVERS 3 /* Number of servers in the example cluster */

int main(int argc, char *argv[])
{
    const char *top_dir = "/tmp/raft";
    struct stat sb;
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
        int pid = fork();

        if (pid == 0) {
            char *dir = malloc(strlen(top_dir) + strlen("/D") + 1);
            char *id = malloc(2);
            char *argv[] = {"./example-server", dir, id, NULL};
            char *envp[] = {NULL};

            sprintf(dir, "%s/%d", top_dir, i + 1);
            sprintf(id, "%d", i + 1);

            execve("./example-server", argv, envp);
        }
    }

    for (i = 0; i < N_SERVERS; i++) {
        int status;
        waitpid(-1, &status, 0);
    }

    return 0;
}
