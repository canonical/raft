/* Test directory utilties.
 *
 * This module sports helpers to create temporary directories backed by various
 * file systems, read/write files in them, check for the presence of files
 * etc. */

#ifndef TEST_DIR_H_
#define TEST_DIR_H_

#include <linux/aio_abi.h>

#include "munit.h"

/* Munit parameter defining the file system type backing the temporary directory
 * created by test_dir_setup().
 *
 * The various file systems must have been previously setup with the fs.sh
 * script. */
#define DIR_FS_PARAM "dir-fs"

#define FIXTURE_DIR char *dir
#define SETUP_DIR f->dir = setUpDir(params, user_data)
#define SETUP_DIR_OR_SKIP                                            \
    SETUP_DIR;                                                       \
    if (f->dir == NULL) { /* Desired fs not available, skip test. */ \
        free(f);                                                     \
        return NULL;                                                 \
    }
#define TEAR_DOWN_DIR tearDownDir(f->dir)

/* List of all supported file system types. */
extern char *test_dir_all[];

/* List containing only the tmpfs type. */
extern char *test_dir_tmpfs[];

/* List containing only the btrfs fs type. */
extern char *test_dir_btrfs[];

/* List containing only the zfs fs type. */
extern char *test_dir_zfs[];

/* List containing all fs types that properly support AIO (i.e. truly async AIO
 * that never blocks). */
extern char *test_dir_aio[];

/* List containing all fs types that do not properly support AIO. */
extern char *test_dir_no_aio[];

/* Contain a single DIR_FS_PARAM parameter set to all supported file system
 * types. */
extern MunitParameterEnum dir_all_params[];

/* Contain a single DIR_FS_PARAM parameter set to tmpfs. */
extern MunitParameterEnum dir_tmpfs_params[];

/* Contain a single DIR_FS_PARAM parameter set to btrfs. */
extern MunitParameterEnum dir_btrfs_params[];

/* Contain a single DIR_FS_PARAM parameter set to zfs. */
extern MunitParameterEnum dir_zfs_params[];

/* Contain a single DIR_FS_PARAM parameter set to all file systems with
 * proper AIO support (i.e. NOWAIT works). */
extern MunitParameterEnum dir_aio_params[];

/* Contain a single DIR_FS_PARAM parameter set to all file systems without
 * proper AIO support (i.e. NOWAIT does not work). */
extern MunitParameterEnum dir_no_aio_params[];

/* Create a temporary test directory.
 *
 * Return a pointer the path of the created directory. */
void *setUpDir(const MunitParameter params[], void *user_data);

/* Create a temporary test directory backed by tmpfs.
 *
 * Return a pointer the path of the created directory, or NULL if no tmpfs file
 * system is available. */
void *setUpTmpfsDir(const MunitParameter params[], void *user_data);

/* Create a temporary test directory backed by ext4.
 *
 * Return a pointer the path of the created directory, or NULL if no ext4 file
 * system is available. */
void *setUpExt4Dir(const MunitParameter params[], void *user_data);

/* Create a temporary test directory backed by btrfs.
 *
 * Return a pointer the path of the created directory, or NULL if no btrfs file
 * system is available. */
void *setUpBtrfsDir(const MunitParameter params[], void *user_data);

/* Create a temporary test directory backed by zfs.
 *
 * Return a pointer the path of the created directory, or NULL if no zfs file
 * system is available. */
void *setUpZfsDir(const MunitParameter params[], void *user_data);

/* Create a temporary test directory backed by xfs.
 *
 * Return a pointer the path of the created directory, or NULL if no xfs file
 * system is available. */
void *setUpXfsDir(const MunitParameter params[], void *user_data);

/* Recursively remove a temporary directory. */
void tearDownDir(void *data);

/* Recursively remove a temporary directory. */
void test_dir_remove(char *dir);

/* Write the given @buf to the given @filename in the given @dir. */
void test_dir_write_file(const char *dir,
                         const char *filename,
                         const void *buf,
                         const size_t n);

/* Write the given @filename and fill it with zeros. */
void test_dir_write_file_with_zeros(const char *dir,
                                    const char *filename,
                                    const size_t n);

/* Append the given @buf to the given @filename in the given @dir. */
void test_dir_append_file(const char *dir,
                          const char *filename,
                          const void *buf,
                          const size_t n);

/* Overwrite @n bytes of the given file with the given @buf data.
 *
 * If @whence is zero, overwrite the first @n bytes of the file. If @whence is
 * positive overwrite the @n bytes starting at offset @whence. If @whence is
 * negative overwrite @n bytes starting at @whence bytes from the end of the
 * file. */
void test_dir_overwrite_file(const char *dir,
                             const char *filename,
                             const void *buf,
                             const size_t n,
                             const off_t whence);

/* Overwrite the @n bytes of the given file with zeros. */
void test_dir_overwrite_file_with_zeros(const char *dir,
                                        const char *filename,
                                        const size_t n,
                                        const off_t whence);

/* Truncate the given file, leaving only the first @n bytes. */
void test_dir_truncate_file(const char *dir,
                            const char *filename,
                            const size_t n);

/* Grow the given file to the given size, filling the new bytes with zeros. */
void test_dir_grow_file(const char *dir, const char *filename, const size_t n);

/* Rename a file in the given directory from filename1 to filename2. */
void test_dir_rename_file(const char *dir,
                          const char *filename1,
                          const char *filename2);

/* Remove a file. */
void test_dir_remove_file(const char *dir, const char *filename);

/* Read into @buf the content of the given @filename in the given @dir. */
void test_dir_read_file(const char *dir,
                        const char *filename,
                        void *buf,
                        const size_t n);

/* Return true if the given directory exists. */
bool test_dir_exists(const char *dir);

/* Make the given directory not executable, so files can't be open. */
void test_dir_unexecutable(const char *dir);

/* Make the given directory not writable. */
void test_dir_unwritable(const char *dir);

/* Make the given file not readable. */
void test_dir_unreadable_file(const char *dir, const char *filename);

/* Check if the given directory has the given file. */
bool test_dir_has_file(const char *dir, const char *filename);

/* Fill the underlying file system of the given dir, leaving only n bytes free.
 */
void test_dir_fill(const char *dir, const size_t n);

/* Fill the AIO subsystem resources by allocating a lot of events to the given
 * context, and leaving only @n events available for subsequent calls to
 * @io_setup.
 *
 * Return -1 if it looks like there is another process already using the AIO
 * subsytem, which would most probably make the calling test flaky because there
 * won't be exactly @n events available anymore. */
int test_aio_fill(aio_context_t *ctx, unsigned n);

/* Destroy the given AIO context. */
void test_aio_destroy(aio_context_t ctx);

#endif /* TEST_DIR_H_ */
