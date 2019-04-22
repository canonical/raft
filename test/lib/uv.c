#include <stdlib.h>

#include "../../src/byte.h"
#include "../../src/configuration.h"
#include "../../src/uv_encoding.h"

#include "fs.h"
#include "uv.h"

#define __WORD_SIZE sizeof(uint64_t)

size_t test_io_uv_write_snapshot_meta_file(const char *dir,
                                           raft_term term,
                                           raft_index index,
                                           unsigned long long timestamp,
                                           unsigned configuration_n,
                                           raft_index configuration_index)
{
    char filename[strlen("snapshot-N-N-N.meta") + 20 * 3 + 1];
    struct raft_configuration configuration;
    struct raft_buffer configuration_buf;
    void *buf;
    void *cursor;
    size_t size;
    unsigned crc;
    unsigned i;
    int rv;

    raft_configuration_init(&configuration);
    for (i = 0; i < configuration_n; i++) {
        unsigned id = i + 1;
        char address[16];
        sprintf(address, "%u", id);
        rv = raft_configuration_add(&configuration, id, address, true);
        munit_assert_int(rv, ==, 0);
    }
    rv = configurationEncode(&configuration, &configuration_buf);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);

    size = __WORD_SIZE + /* Format version */ __WORD_SIZE + /* CRC checksum */
           __WORD_SIZE + /* Configuration index  */
           __WORD_SIZE + /* Length of encoded configuration */
           configuration_buf.len /* Encoded configuration */;
    buf = munit_malloc(size);
    cursor = buf;

    bytePut64(&cursor, 1);                     /* Format version */
    bytePut64(&cursor, 0);                     /* CRC sums placeholder */
    bytePut64(&cursor, configuration_index);   /* Configuration index */
    bytePut64(&cursor, configuration_buf.len); /* Encoded configuration */
    memcpy(cursor, configuration_buf.base, configuration_buf.len);

    sprintf(filename, "snapshot-%llu-%llu-%llu.meta", term, index, timestamp);

    crc = byteCrc32(buf + (__WORD_SIZE * 2), size - (__WORD_SIZE * 2), 0);
    cursor = buf + __WORD_SIZE;
    bytePut64(&cursor, crc);

    test_dir_write_file(dir, filename, buf, size);

    raft_free(configuration_buf.base);
    free(buf);

    return size;
}

size_t test_io_uv_write_snapshot_data_file(const char *dir,
                                           raft_term term,
                                           raft_index index,
                                           unsigned long long timestamp,
                                           void *buf,
                                           size_t size)
{
    char filename[strlen("snapshot-N-N-N") + 20 * 3 + 1];

    sprintf(filename, "snapshot-%llu-%llu-%llu", term, index, timestamp);
    test_dir_write_file(dir, filename, buf, size);

    return size;
}
