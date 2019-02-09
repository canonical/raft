/**
 * File-system related test utilties.
 */

#ifndef TEST_FS_H
#define TEST_FS_H

#include <linux/aio_abi.h>

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
 * List of all supported file system types.
 */
extern char *test_dir_fs_type_supported[];

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
void test_dir_write_file(const char *dir,
                         const char *filename,
                         const void *buf,
                         const size_t n);

/**
 * Write the given @filename and fill it with zeros.
 */
void test_dir_write_file_with_zeros(const char *dir,
                                    const char *filename,
                                    const size_t n);

/**
 * Append the given @buf to the given @filename in the given @dir.
 */
void test_dir_append_file(const char *dir,
                          const char *filename,
                          const void *buf,
                          const size_t n);

/**
 * Overwrite @n bytes of the given file with the given @buf data.
 *
 * If @whence is zero, overwrite the first @n bytes of the file. If @whence is
 * positive overwrite the @n bytes starting at offset @whence. If @whence is
 * negative overwrite @n bytes starting at @whence bytes from the end of the
 * file.
 */
void test_dir_overwrite_file(const char *dir,
                             const char *filename,
                             const void *buf,
                             const size_t n,
                             const off_t whence);

/**
 * Overwrite the @n bytes of the given file with zeros.
 */
void test_dir_overwrite_file_with_zeros(const char *dir,
                                        const char *filename,
                                        const size_t n,
                                        const off_t whence);

/**
 * Truncate the given file, leaving only the first @n bytes.
 */
void test_dir_truncate_file(const char *dir,
                            const char *filename,
                            const size_t n);

/**
 * Read into @buf the content of the given @filename in the given @dir.
 */
void test_dir_read_file(const char *dir,
                        const char *filename,
                        void *buf,
                        const size_t n);

/**
 * Make the given directory not executable, so files can't be open.
 */
void test_dir_unexecutable(const char *dir);

/**
 * Make the given file not readable.
 */
void test_dir_unreadable_file(const char *dir, const char *filename);

/**
 * Check if the given directory has the given file.
 */
bool test_dir_has_file(const char *dir, const char *filename);

/**
 * Fill the underlying file system of the given dir, leaving only n bytes free.
 */
void test_dir_fill(const char *dir, const size_t n);

/**
 * Fill the AIO subsystem resources by allocating a lot of events to the given
 * context, and leaving only @n events available for subsequent calls to
 * @io_setup.
 */
void test_aio_fill(aio_context_t *ctx, unsigned n);

#endif /* TEST_IO_H */
