#include "stdlib.h"

#include "../include/raft.h"

static void *default_malloc(void *data, size_t size)
{
    (void)data;
    return malloc(size);
}

static void default_free(void *data, void *ptr)
{
    (void)data;
    free(ptr);
}

static void *default_calloc(void *data, size_t nmemb, size_t size)
{
    (void)data;
    return calloc(nmemb, size);
}

static void *default_realloc(void *data, void *ptr, size_t size)
{
    (void)data;
    return realloc(ptr, size);
}

static void *default_aligned_alloc(void *data, size_t alignment, size_t size)
{
    (void)data;
    return aligned_alloc(alignment, size);
}

struct raft_heap default_heap = {
    NULL,                 /* data */
    default_malloc,       /* malloc */
    default_free,         /* free */
    default_calloc,       /* calloc */
    default_realloc,      /* realloc */
    default_aligned_alloc /* aligned_alloc */
};

struct raft_heap *current_heap = &default_heap;

void *raft_malloc(size_t size)
{
    return current_heap->malloc(current_heap->data, size);
}

void raft_free(void *ptr)
{
    current_heap->free(current_heap->data, ptr);
}

void *raft_calloc(size_t nmemb, size_t size)
{
    return current_heap->calloc(current_heap->data, nmemb, size);
}

void *raft_realloc(void *ptr, size_t size)
{
    return current_heap->realloc(current_heap->data, ptr, size);
}

void raft_heap_set(struct raft_heap *heap)
{
    current_heap = heap;
}

void raft_heap_set_default()
{
    current_heap = &default_heap;
}
