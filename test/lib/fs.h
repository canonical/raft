/**
 * File-system related test utilties.
 */

#ifndef TEST_FS_H
#define TEST_FS_H

#include "munit.h"

/**
 * Munit parameter defining the file system type the temporary directory should
 * be created in.
 *
 * The various file systems must have been previously setup with the fs.sh
 * script.
 */
#define TEST_DIR_FS_TYPE "dir-fs-type"

/**
 * Create a temporary test directory.
 */
char *test_dir_setup(const MunitParameter params[]);

/**
 * Recursively remove a temporary directory.
 */
void test_dir_tear_down(char *dir);

/**
 * Write the given @buf to the given @filename in the given @dir.
 */
void test_dir_write_file(char *dir, const char *filename, void *buf, size_t n);

/**
 * Read into @buf the content of the given @filename in the given @dir.
 */
void test_dir_read_file(char *dir, const char *filename, void *buf, size_t n);

#endif /* TEST_IO_H */
