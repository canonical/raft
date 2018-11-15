#include <dirent.h>
#include <string.h>
#include <unistd.h>

#include "fs.h"

#define TEST_DIR_TEMPLATE "/tmp/raft-test-XXXXXX"

char *test_dir_setup(const MunitParameter params[])
{
    char *dir = munit_malloc(strlen(TEST_DIR_TEMPLATE) + 1);

    (void)params;

    strcpy(dir, TEST_DIR_TEMPLATE);
    munit_assert_ptr_not_null(mkdtemp(dir));

    return dir;
}

static int test_dir__remove_entry(const struct dirent *entry)
{
    int rv;

    if (strcmp(entry->d_name, ".") || strcmp(entry->d_name, "..")) {
        return 0;
    }

    rv = unlink(entry->d_name);
    munit_assert_int(rv, ==, 0);

    return 0;
}

void test_dir_tear_down(char *dir)
{
    struct dirent **namelist;
    int rv;

    rv = scandir(dir, &namelist, test_dir__remove_entry, alphasort);
    munit_assert_int(rv, ==, 0);

    rv = rmdir(dir);
    munit_assert_int(rv, ==, 0);

    free(dir);
}
