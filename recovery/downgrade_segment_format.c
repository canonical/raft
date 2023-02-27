#include <malloc.h>

#include "raft.h"
#include "../src/uv.h"

int main(int argc, char *argv[])
{
    int rv;
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct uv *uv = malloc(sizeof(*uv));
    if (argc != 2) {
        fprintf(stderr, "expect a single 'dir' argument.\n");
        return 2;
    }
    if (uv == NULL) {
        fprintf(stderr, "MALLOC\n");
        return 1;
    }

    strncpy(uv->dir, argv[1], UV__DIR_LEN-1);
    uv->dir[UV__DIR_LEN-1] = '\0';

    rv = UvSegmentConvertDirToFormat(uv, 1, errmsg);
    if (rv != 0) {
        fprintf(stderr, "downgrading segments failed: %s\n", errmsg);
    } else {
        printf("downgrading segments success\n");
    }
    return rv;
}
