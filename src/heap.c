#include "stdlib.h"

#include "../include/raft.h"

static void *raft__heap_malloc(void *data, size_t size)
{
    (void)data;
    return malloc(size);
}

static void raft__free(void *data, void *ptr)
{
    (void)data;
    free(ptr);
}

static void *raft__calloc(void *data, size_t nmemb, size_t size)
{
    (void)data;
    return calloc(nmemb, size);
}

static void *raft__realloc(void *data, void *ptr, size_t size)
{
    (void)data;
    return realloc(ptr, size);
}

struct raft_heap raft_heap__default = {
    NULL, raft__heap_malloc, raft__free, raft__calloc, raft__realloc,
};

struct raft_heap *raft_heap__current = &raft_heap__default;

void *raft_malloc(size_t size)
{
    return raft_heap__current->malloc(raft_heap__current->data, size);
}

void raft_free(void *ptr)
{
    raft_heap__current->free(raft_heap__current->data, ptr);
}

void *raft_calloc(size_t nmemb, size_t size)
{
    return raft_heap__current->calloc(raft_heap__current->data, nmemb, size);
}

void *raft_realloc(void *ptr, size_t size)
{
    return raft_heap__current->realloc(raft_heap__current->data, ptr, size);
}

void raft_heap_set(struct raft_heap *heap)
{
    raft_heap__current = heap;
}

void raft_heap_set_default()
{
    raft_heap__current = &raft_heap__default;
}
