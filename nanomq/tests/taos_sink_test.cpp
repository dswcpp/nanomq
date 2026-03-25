#include <assert.h>
#include <string.h>
#include <stdlib.h>

static int g_malloc_calls = 0;
static int g_calloc_calls = 0;
static int g_realloc_calls = 0;
static int g_fail_malloc_on = -1;
static int g_fail_calloc_on = -1;
static int g_fail_realloc_on = -1;

static void reset_alloc_failures(void)
{
    g_malloc_calls = 0;
    g_calloc_calls = 0;
    g_realloc_calls = 0;
    g_fail_malloc_on = -1;
    g_fail_calloc_on = -1;
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

static void *test_calloc(size_t nmemb, size_t size)
{
    g_calloc_calls++;
    if (g_fail_calloc_on > 0 && g_calloc_calls == g_fail_calloc_on) {
        return NULL;
    }
    return ::calloc(nmemb, size);
}

#define malloc test_malloc
#define calloc test_calloc
#define realloc test_realloc
#include "../taos_sink.cpp"
#undef malloc
#undef calloc
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

static void test_build_batch_insert_sql_fails_when_escape_alloc_fails(void)
{
    reset_alloc_failures();

    taos_queued_item item = {};
    item.qos = 0;
    item.packet_id = 1;
    item.topic = strdup("sensor/temp");
    item.client_id = strdup("client-1");
    item.username = strdup("tester");
    item.payload = strdup("abc");
    item.payload_len = 3;
    item.timestamp_ms = 1;

    reset_alloc_failures();
    g_fail_calloc_on = 1;
    char *sql = build_batch_insert_sql(&item, 1, "mqtt_rule", "mqtt_data");
    assert(sql == NULL);

    taos_item_free(&item);
}

static void test_build_batch_insert_sql_fails_when_fragment_alloc_fails(void)
{
    reset_alloc_failures();

    taos_queued_item item = {};
    item.qos = 0;
    item.packet_id = 1;
    item.topic = strdup("sensor/temp");
    item.client_id = strdup("client-1");
    item.username = strdup("tester");
    item.payload = strdup("abc");
    item.payload_len = 3;
    item.timestamp_ms = 1;

    reset_alloc_failures();
    g_fail_malloc_on = 2;
    char *sql = build_batch_insert_sql(&item, 1, "mqtt_rule", "mqtt_data");
    assert(sql == NULL);

    taos_item_free(&item);
}

static void test_start_fails_when_db_copy_allocation_fails(void)
{
    reset_alloc_failures();

    taos_sink_config cfg = {};
    cfg.host = "127.0.0.1";
    cfg.port = 6041;
    cfg.username = "root";
    cfg.password = "taosdata";
    cfg.db = "mqtt_rule";
    cfg.stable = "mqtt_data";

    g_fail_malloc_on = 5;
    int rc = taos_sink_start(&cfg);
    assert(rc != 0);
    assert(taos_sink_is_started() == 0);
}

static void test_enqueue_fails_when_sink_state_is_incomplete(void)
{
    reset_alloc_failures();
    memset(&g_taos, 0, sizeof(g_taos));
    g_taos.started = true;

    taos_rule_result result = {};
    result.topic = "sensor/temp";
    result.client_id = "client-1";
    result.username = "tester";
    result.payload = "abc";
    result.payload_len = 3;
    result.timestamp_ms = 1;

    int rc = taos_sink_enqueue(&result);
    assert(rc != 0);

    memset(&g_taos, 0, sizeof(g_taos));
}

static void test_start_does_not_set_running_on_early_failure(void)
{
    reset_alloc_failures();
    memset(&g_taos, 0, sizeof(g_taos));

    taos_sink_config cfg = {};
    cfg.host = "127.0.0.1";
    cfg.port = 6041;
    cfg.username = "root";
    cfg.password = "taosdata";
    cfg.db = "mqtt_rule";
    cfg.stable = "mqtt_data";

    g_fail_malloc_on = 5;
    int rc = taos_sink_start(&cfg);
    assert(rc != 0);
    assert(g_taos.running == false);
    assert(taos_sink_is_started() == 0);
}

static void test_start_marks_running_only_after_init_db_succeeds(void)
{
    reset_alloc_failures();
    memset(&g_taos, 0, sizeof(g_taos));

    taos_sink_config cfg = {};
    cfg.host = "127.0.0.1";
    cfg.port = 6041;
    cfg.username = "root";
    cfg.password = "taosdata";
    cfg.db = "mqtt_rule";
    cfg.stable = "mqtt_data";

    g_fail_malloc_on = 6;
    int rc = taos_sink_start(&cfg);
    assert(rc != 0);
    assert(g_taos.running == false);
    assert(taos_sink_is_started() == 0);
}

static void test_duplicate_start_with_different_target_fails(void)
{
    reset_alloc_failures();
    memset(&g_taos, 0, sizeof(g_taos));

    g_taos.started = true;
    g_taos.url = strdup("http://127.0.0.1:6041/rest/sql");
    g_taos.auth_header = strdup("Basic xxx");
    g_taos.db = strdup("mqtt_rule");
    g_taos.stable = strdup("mqtt_data");

    taos_sink_config cfg = {};
    cfg.host = "127.0.0.1";
    cfg.port = 6041;
    cfg.username = "root";
    cfg.password = "taosdata";
    cfg.db = "another_db";
    cfg.stable = "other_table";

    int rc = taos_sink_start(&cfg);
    assert(rc != 0);

    taos_sink_reset_partial_state();
}

int main(void)
{
    test_base64_single_byte_padding();
    test_enqueue_fails_when_deep_copy_allocation_fails();
    test_enqueue_fails_when_queue_growth_fails();
    test_build_batch_insert_sql_fails_when_escape_alloc_fails();
    test_build_batch_insert_sql_fails_when_fragment_alloc_fails();
    test_start_fails_when_db_copy_allocation_fails();
    test_enqueue_fails_when_sink_state_is_incomplete();
    test_start_does_not_set_running_on_early_failure();
    test_start_marks_running_only_after_init_db_succeeds();
    test_duplicate_start_with_different_target_fails();
    return 0;
}
