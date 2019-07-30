#include "../lib/runner.h"

#include "../../src/os.h"

TEST_MODULE(path);

/* Join a directory path and a filename into a full path. */
TEST_CASE(join, NULL)
{
    const osDir dir = "/foo";
    const osFilename filename = "bar";
    osPath path;
    (void)data;
    (void)params;
    osJoin(dir, filename, path);
    munit_assert_string_equal(path, "/foo/bar");
    return MUNIT_OK;
}

/* Extract the directory name from a full path. */
TEST_CASE(dirname, NULL) {
    const osPath path = "/foo/bar";
    osDir dir;
    (void)data;
    (void)params;
    osDirname(path, dir);
    munit_assert_string_equal(dir, "/foo");
    return MUNIT_OK;
}
