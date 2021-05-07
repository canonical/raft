#include "compress.h"

#ifdef LZ4_AVAILABLE
#include <lz4frame.h>
#endif
#include <string.h>

#include "assert.h"
#include "err.h"


int Compress(struct raft_buffer bufs[], unsigned n_bufs,
             struct raft_buffer *compressed, char *errmsg)
{
#ifndef LZ4_AVAILABLE
    (void) bufs;
    (void) n_bufs;
    (void) compressed;
    ErrMsgPrintf(errmsg, "LZ4 not available");
    return RAFT_INVALID;
#else
    assert(bufs != NULL);
    assert(n_bufs > 0);
    assert(compressed != NULL);

    int rv = RAFT_IOERR;
    size_t src_size = 0;
    size_t max_dst_size = 0;
    size_t ret = 0;
    size_t offset = 0;

    /* Set LZ4 preferences */
    LZ4F_preferences_t lz4_pref;
    memset(&lz4_pref, 0, sizeof(lz4_pref));
    lz4_pref.frameInfo.contentChecksumFlag = 1;

    /* Determine total uncompressed size and size of compressed buffer */
    for (unsigned i = 0; i < n_bufs; ++i) {
        src_size += bufs[i].len;
        max_dst_size += LZ4F_compressBound(bufs[i].len, &lz4_pref);
    }
    /* Allow room for the lz4 header */
    max_dst_size += LZ4F_HEADER_SIZE_MAX_RAFT;

    /* contentSize has no impact on LZ4F_compressBound and is needed to allocate a
     * correctly sized buffer when decompressing */
    lz4_pref.frameInfo.contentSize = src_size;

    LZ4F_compressionContext_t ctx;
    size_t const ctx_create = LZ4F_createCompressionContext(&ctx, LZ4F_VERSION);
    if (LZ4F_isError(ctx_create)) {
        ErrMsgPrintf(errmsg, "LZ4F_createDecompressionContext %s",
                     LZ4F_getErrorName(ctx_create));
        rv = RAFT_NOMEM;
        goto err;
    }

    compressed->base = raft_malloc(max_dst_size);
    if (compressed->base == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_ctx_alloc;
    }

    /* Returns the size of the lz4 header, data should be written after the
     * header */
    offset = LZ4F_compressBegin(ctx, compressed->base, max_dst_size, &lz4_pref);
    if (LZ4F_isError(offset)) {
        ErrMsgPrintf(errmsg, "LZ4F_compressBegin %s", LZ4F_getErrorName(offset));
        rv = RAFT_IOERR;
        goto err_after_buff_alloc;
    }

    /* Compress all buffers */
    for (unsigned i = 0; i < n_bufs; ++i) {
        ret = LZ4F_compressUpdate(ctx, (char*)compressed->base + offset,
                                  max_dst_size - offset, bufs[i].base,
                                  bufs[i].len, NULL);
        if (LZ4F_isError(ret)) {
            ErrMsgPrintf(errmsg, "LZ4F_compressUpdate %s",
                         LZ4F_getErrorName(ret));
            rv = RAFT_IOERR;
            goto err_after_buff_alloc;
        }
        offset += ret;
    }

    /* Finalize compression */
    ret = LZ4F_compressEnd(ctx, (char*)compressed->base + offset,
                           max_dst_size - offset, NULL);
    if (LZ4F_isError(ret)) {
        ErrMsgPrintf(errmsg, "LZ4F_compressEnd %s", LZ4F_getErrorName(ret));
        rv = RAFT_IOERR;
        goto err_after_buff_alloc;
    }

    offset += ret;
    compressed->len = offset;

    LZ4F_freeCompressionContext(ctx);
    return 0;

err_after_buff_alloc:
    raft_free(compressed->base);
    compressed->base = NULL;
err_after_ctx_alloc:
    LZ4F_freeCompressionContext(ctx);
err:
    return rv;
#endif /* LZ4_AVAILABLE */
}

int Decompress(struct raft_buffer buf, struct raft_buffer *decompressed,
               char *errmsg)
{
#ifndef LZ4_AVAILABLE
    (void) buf;
    (void) decompressed;
    ErrMsgPrintf(errmsg, "LZ4 not available");
    return RAFT_INVALID;
#else
    assert(decompressed != NULL);

    int rv = RAFT_IOERR;
    size_t src_offset = 0;
    size_t dst_offset = 0;
    size_t src_size = 0;
    size_t dst_size = 0;
    size_t ret = 0;

    LZ4F_decompressionContext_t ctx;
    if (LZ4F_isError(LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION))) {
        ErrMsgPrintf(errmsg, "LZ4F_createDecompressionContext");
        rv = RAFT_NOMEM;
        goto err;
    }

    src_size = buf.len;
    LZ4F_frameInfo_t frameInfo = {0};
    /* `src_size` will contain the size of the LZ4 Frame Header after the call,
     * decompression must resume at that offset. */
    ret = LZ4F_getFrameInfo(ctx, &frameInfo, buf.base, &src_size);
    if (LZ4F_isError(ret)) {
        ErrMsgPrintf(errmsg, "LZ4F_getFrameInfo %s", LZ4F_getErrorName(ret));
        rv = RAFT_IOERR;
        goto err_after_ctx_alloc;
    }
    src_offset = src_size;

    decompressed->base = raft_malloc(frameInfo.contentSize);
    decompressed->len = frameInfo.contentSize;
    if (decompressed->base == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_ctx_alloc;
    }

    ret = 1;
    while (ret != 0) {
        src_size = buf.len - src_offset;
        dst_size = decompressed->len - dst_offset;
        /* `dst_size` will contain the number of bytes written to decompressed->base,
         * while `src_size` will contain the number of bytes consumed from
         * buf.base */
        ret = LZ4F_decompress(ctx, (char*)decompressed->base + dst_offset, &dst_size,
                              (char*)buf.base + src_offset, &src_size, NULL);
        if (LZ4F_isError(ret)) {
            ErrMsgPrintf(errmsg, "LZ4F_decompress %s", LZ4F_getErrorName(ret));
            rv = RAFT_IOERR;
            goto err_after_buff_alloc;
        }
        src_offset += src_size;
        dst_offset += dst_size;
    }

    if (LZ4F_freeDecompressionContext(ctx) != 0) {
        rv = RAFT_IOERR;
        goto err_after_buff_alloc;
    }

    return 0;

err_after_buff_alloc:
    raft_free(decompressed->base);
    decompressed->base = NULL;
err_after_ctx_alloc:
    LZ4F_freeDecompressionContext(ctx);
err:
    return rv;
#endif /* LZ4_AVAILABLE */
}

