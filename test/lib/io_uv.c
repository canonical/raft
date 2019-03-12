#include <stdlib.h>

#include "../../src/byte.h"
#include "../../src/configuration.h"
#include "../../src/io_uv_encoding.h"

#include "fs.h"
#include "io_uv.h"

#define __WORD_SIZE sizeof(uint64_t)

static size_t test__io_uv_create_segment(const char *dir,
                                         const char *filename,
                                         int n,
                                         int data);

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
    rv = configuration__encode(&configuration, &configuration_buf);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);

    size = __WORD_SIZE + /* Format version */ __WORD_SIZE + /* CRC checksum */
           __WORD_SIZE + /* Configuration index  */
           __WORD_SIZE + /* Length of encoded configuration */
           configuration_buf.len /* Encoded configuration */;
    buf = munit_malloc(size);
    cursor = buf;

    byte__put64(&cursor, 1);                     /* Format version */
    byte__put64(&cursor, 0);                     /* CRC sums placeholder */
    byte__put64(&cursor, configuration_index);   /* Configuration index */
    byte__put64(&cursor, configuration_buf.len); /* Encoded configuration */
    memcpy(cursor, configuration_buf.base, configuration_buf.len);

    sprintf(filename, "snapshot-%llu-%llu-%llu.meta", term, index, timestamp);

    crc = byte__crc32(buf + (__WORD_SIZE * 2), size - (__WORD_SIZE * 2), 0);
    cursor = buf + __WORD_SIZE;
    byte__put64(&cursor, crc);

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

size_t test_io_uv_write_open_segment_file(const char *dir,
                                          unsigned long long counter,
                                          int n,
                                          int data)
{
    char filename[strlen("open-N") + 1];
    sprintf(filename, "open-%llu", counter);
    return test__io_uv_create_segment(dir, filename, n, data);
}

size_t test_io_uv_write_closed_segment_file(const char *dir,
                                            raft_index first_index,
                                            int n,
                                            int data)
{
    char filename[strlen("N-N") + 20 * 2 + 1];
    sprintf(filename, "%llu-%llu", first_index,
            (raft_index)(first_index + n - 1));
    return test__io_uv_create_segment(dir, filename, n, data);
}

static size_t test__io_uv_create_segment(const char *dir,
                                         const char *filename,
                                         int n,
                                         int data)
{
    int i;
    uint8_t *buf;
    size_t size;
    void *cursor;
    unsigned crc1;
    unsigned crc2;
    uint8_t *batch; /* Start of the batch */
    size_t header_size = io_uv__sizeof_batch_header(1);
    size_t data_size = __WORD_SIZE;

    size = __WORD_SIZE /* Format version */;
    size += (__WORD_SIZE /* Checksums */ + header_size + data_size) * (size_t)n;
    buf = munit_malloc(size);
    cursor = buf;
    byte__put64(&cursor, 1); /* Format version */
    batch = cursor;

    for (i = 0; i < (int)n; i++) {
        byte__put64(&cursor, 0);               /* CRC sums placeholder */
        byte__put64(&cursor, 1);               /* Number of entries */
        byte__put64(&cursor, 1);               /* Entry term */
        byte__put8(&cursor, RAFT_COMMAND); /* Entry type */
        byte__put8(&cursor, 0);                /* Unused */
        byte__put8(&cursor, 0);                /* Unused */
        byte__put8(&cursor, 0);                /* Unused */
        byte__put32(&cursor, 8);               /* Size of entry data */
        byte__put64(&cursor, data);            /* Entry data */

        cursor = batch + __WORD_SIZE;
        crc1 = byte__crc32(cursor, header_size, 0);
        crc2 = byte__crc32(cursor + header_size, data_size, 0);
        cursor = batch;
        byte__put32(&cursor, crc1); /* Header checksum */
        byte__put32(&cursor, crc2); /* Data checksum */
        batch += __WORD_SIZE + header_size + data_size;
        cursor = batch;
        data++;
    }

    test_dir_write_file(dir, filename, buf, size);
    free(buf);

    return size;
}
