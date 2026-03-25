#include <assert.h>
#include <string.h>
#include <stdlib.h>

static int g_malloc_calls = 0;
static int g_realloc_calls = 0;
static int g_fail_malloc_on = -1;
static int g_fail_realloc_on = -1;

static void reset_alloc_failures(void)
{
    g_malloc_calls = 0;
    g_realloc_calls = 0;
    g_fail_malloc_on = -1;
    g_fail_realloc_on = -1;
}

static void *test_malloc(size_t size)
{
    g_malloc_calls++;
    if (g_fail_malloc_on > 0 && g_malloc_calls == g_fail_malloc_on) {
        return NULL;
    }
    return ::malloc(size);
}

static void *test_realloc(void *ptr, size_t size)
{
    g_realloc_calls++;
    if (g_fail_realloc_on > 0 && g_realloc_calls == g_fail_realloc_on) {
        return NULL;
    }
    return ::realloc(ptr, size);
}

#define malloc test_malloc
#define realloc test_realloc
#include "../taos_sink.cpp"
#undef malloc
#undef realloc

static void reset_sink_state(void)
{
    memset(&g_taos, 0, sizeof(g_taos));
    taos_queue_init(&g_taos.queue);
    assert(nng_mtx_alloc(&g_taos.mtx) == 0);
    assert(nng_cv_alloc(&g_taos.cv, g_taos.mtx) == 0);
    g_taos.started = true;
    g_taos.running = true;
}

static void free_sink_state(void)
{
    taos_queue_free(&g_taos.queue);
    if (g_taos.cv != NULL) {
        nng_cv_free(g_taos.cv);
    }
    if (g_taos.mtx != NULL) {
        nng_mtx_free(g_taos.mtx);
    }
    memset(&g_taos, 0, sizeof(g_taos));
}

static void test_base64_single_byte_padding(void)
{
    reset_alloc_failures();
    char *encoded = base64_encode("a", 1);
    assert(encoded != NULL);
    assert(strcmp(encoded, "YQ==") == 0);
    free(encoded);
}

static void test_enqueue_fails_when_deep_copy_allocation_fails(void)
{
    reset_alloc_failures();
    reset_sink_state();

    taos_rule_result result = {};
    result.topic = "sensor/temp";
    result.client_id = "client-1";
    result.username = "tester";
    result.payload = "abc";
    result.payload_len = 3;
    result.timestamp_ms = 1;

    g_fail_malloc_on = 1;
    int rc = taos_sink_enqueue(&result);
    assert(rc != 0);
    assert(g_taos.queue.len == 0);

    free_sink_state();
}

static void test_enqueue_fails_when_queue_growth_fails(void)
{
    reset_alloc_failures();
    reset_sink_state();

    taos_rule_result result = {};
    result.topic = "sensor/temp";
    result.client_id = "client-1";
    result.username = "tester";
    result.payload = "abc";
    result.payload_len = 3;
    result.timestamp_ms = 1;

    g_fail_realloc_on = 1;
    int rc = taos_sink_enqueue(&result);
    assert(rc != 0);
    assert(g_taos.queue.len == 0);

    free_sink_state();
}

int main(void)
{
    test_base64_single_byte_padding();
    test_enqueue_fails_when_deep_copy_allocation_fails();
    test_enqueue_fails_when_queue_growth_fails();
    return 0;
}
