#include "../../src/binary.h"
#include "../../src/checksum.h"
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
                                           unsigned long long timestamp)
{
    char filename[strlen("snapshot-00000000000000000001-00000000000000000001-"
                         "00000000000000000001.meta") +
                  1];
    uint8_t buf[8];
    size_t size = 8;

    sprintf(filename, "snapshot-%020llu-%020llu-%020llu.meta", term, index,
            timestamp);
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
    char filename[strlen("00000000000000000001-00000000000000000001") + 1];
    sprintf(filename, "%020llu-%020llu", first_index,
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
    size_t header_size = raft_io_uv_sizeof__batch_header(1);
    size_t data_size = __WORD_SIZE;

    size = __WORD_SIZE /* Format version */;
    size += (__WORD_SIZE /* Checksums */ + header_size + data_size) * (size_t)n;
    buf = munit_malloc(size);
    cursor = buf;
    raft__put64(&cursor, 1); /* Format version */
    batch = cursor;

    for (i = 0; i < (int)n; i++) {
        raft__put64(&cursor, 0);               /* CRC sums placeholder */
        raft__put64(&cursor, 1);               /* Number of entries */
        raft__put64(&cursor, 1);               /* Entry term */
        raft__put8(&cursor, RAFT_LOG_COMMAND); /* Entry type */
        raft__put8(&cursor, 0);                /* Unused */
        raft__put8(&cursor, 0);                /* Unused */
        raft__put8(&cursor, 0);                /* Unused */
        raft__put32(&cursor, 8);               /* Size of entry data */
        raft__put64(&cursor, data);            /* Entry data */

        cursor = batch + __WORD_SIZE;
        crc1 = raft__crc32(cursor, header_size, 0);
        crc2 = raft__crc32(cursor + header_size, data_size, 0);
        cursor = batch;
        raft__put32(&cursor, crc1); /* Header checksum */
        raft__put32(&cursor, crc2); /* Data checksum */
        batch += __WORD_SIZE + header_size + data_size;
        cursor = batch;
        data++;
    }

    test_dir_write_file(dir, filename, buf, size);
    free(buf);

    return size;
}
