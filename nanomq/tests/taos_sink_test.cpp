#include <assert.h>
#include <string.h>
#include <stdlib.h>

static int g_malloc_calls = 0;
static int g_calloc_calls = 0;
static int g_realloc_calls = 0;
static int g_fail_malloc_on = -1;
static int g_fail_calloc_on = -1;
static int g_fail_realloc_on = -1;
static int g_init_db_calls = 0;
static int g_init_db_fail_after = -1;

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

static int fake_taos_sink_init_db(
    const char *url, const char *auth_hdr, const char *db, const char *stable)
{
    (void) url;
    (void) auth_hdr;
    (void) db;
    (void) stable;

    g_init_db_calls++;
    if (g_init_db_fail_after > 0 && g_init_db_calls >= g_init_db_fail_after) {
        return -1;
    }
    return 0;
}

static void reset_init_db_stub(void)
{
    g_init_db_calls = 0;
    g_init_db_fail_after = -1;
    g_taos_init_db_fn = fake_taos_sink_init_db;
}

static void reset_sink_state(void)
{
    memset(&g_taos, 0, sizeof(g_taos));
    taos_queue_init(&g_taos.queue);
    assert(nng_mtx_alloc(&g_taos.mtx) == 0);
    assert(nng_cv_alloc(&g_taos.cv, g_taos.mtx) == 0);
    assert(taos_sink_add_stable("mqtt_data") == 0);
    g_taos.started = true;
    g_taos.running = true;
    reset_alloc_failures();
}

static void free_sink_state(void)
{
    taos_queue_free(&g_taos.queue);
    if (g_taos.stables != NULL) {
        for (size_t i = 0; i < g_taos.stable_count; i++) {
            free(g_taos.stables[i]);
        }
        free(g_taos.stables);
    }
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
    item.stable = strdup("mqtt_data");
    item.payload_len = 3;
    item.timestamp_ms = 1;

    reset_alloc_failures();
    g_fail_calloc_on = 1;
    char *sql = build_batch_insert_sql(&item, 1, "mqtt_rule");
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
    item.stable = strdup("mqtt_data");
    item.payload_len = 3;
    item.timestamp_ms = 1;

    reset_alloc_failures();
    g_fail_malloc_on = 2;
    char *sql = build_batch_insert_sql(&item, 1, "mqtt_rule");
    assert(sql == NULL);

    taos_item_free(&item);
}

static void test_start_fails_when_db_copy_allocation_fails(void)
{
    reset_alloc_failures();
    reset_init_db_stub();

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
    reset_init_db_stub();
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
    reset_init_db_stub();
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
    reset_init_db_stub();
    memset(&g_taos, 0, sizeof(g_taos));

    g_taos.started = true;
    g_taos.url = strdup("http://127.0.0.1:6041/rest/sql");
    g_taos.auth_header = strdup("Basic xxx");
    g_taos.db = strdup("mqtt_rule");
    assert(taos_sink_add_stable("mqtt_data") == 0);

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

static void test_duplicate_start_with_same_effective_target_succeeds(void)
{
    reset_alloc_failures();
    reset_init_db_stub();
    memset(&g_taos, 0, sizeof(g_taos));

    g_taos.started = true;
    g_taos.url = strdup("http://127.0.0.1:6041/rest/sql");
    g_taos.auth_header = build_auth_header("root", "taosdata");
    g_taos.db = strdup("mqtt_rule");
    assert(taos_sink_add_stable("mqtt_data") == 0);

    taos_sink_config cfg = {};
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.username = NULL;
    cfg.password = NULL;
    cfg.db = "mqtt_rule";
    cfg.stable = "mqtt_data";

    int rc = taos_sink_start(&cfg);
    assert(rc == 0);
    assert(g_init_db_calls == 0);

    taos_sink_reset_partial_state();
}

static void test_duplicate_start_with_different_table_same_db_succeeds(void)
{
    reset_alloc_failures();
    reset_init_db_stub();
    memset(&g_taos, 0, sizeof(g_taos));

    g_taos.started = true;
    g_taos.url = strdup("http://127.0.0.1:6041/rest/sql");
    g_taos.auth_header = build_auth_header("root", "taosdata");
    g_taos.db = strdup("mqtt_rule");
    assert(taos_sink_add_stable("mqtt_data") == 0);

    taos_sink_config cfg = {};
    cfg.host = "127.0.0.1";
    cfg.port = 6041;
    cfg.username = "root";
    cfg.password = "taosdata";
    cfg.db = "mqtt_rule";
    cfg.stable = "mqtt_status";

    int rc = taos_sink_start(&cfg);
    assert(rc == 0);
    assert(g_init_db_calls == 1);
    assert(g_taos.stable_count == 2);
    assert(taos_sink_has_stable("mqtt_status") == true);

    taos_sink_reset_partial_state();
}

static void test_start_with_four_tables_same_db_succeeds(void)
{
    reset_alloc_failures();
    reset_init_db_stub();
    memset(&g_taos, 0, sizeof(g_taos));

    taos_sink_config cfg1 = {};
    cfg1.host = "127.0.0.1";
    cfg1.port = 6041;
    cfg1.username = "root";
    cfg1.password = "taosdata";
    cfg1.db = "mqtt_rule";
    cfg1.stable = "mqtt_data_1";

    taos_sink_config cfg2 = cfg1;
    cfg2.stable = "mqtt_data_2";

    taos_sink_config cfg3 = cfg1;
    cfg3.stable = "mqtt_data_3";

    taos_sink_config cfg4 = cfg1;
    cfg4.stable = "mqtt_data_4";

    assert(taos_sink_start(&cfg1) == 0);
    assert(taos_sink_start(&cfg2) == 0);
    assert(taos_sink_start(&cfg3) == 0);
    assert(taos_sink_start(&cfg4) == 0);

    assert(g_init_db_calls == 4);
    assert(g_taos.stable_count == 4);
    assert(taos_sink_has_stable("mqtt_data_1") == true);
    assert(taos_sink_has_stable("mqtt_data_2") == true);
    assert(taos_sink_has_stable("mqtt_data_3") == true);
    assert(taos_sink_has_stable("mqtt_data_4") == true);

    taos_sink_stop_all();
}

static void test_duplicate_start_with_different_credentials_fails(void)
{
    reset_alloc_failures();
    reset_init_db_stub();
    memset(&g_taos, 0, sizeof(g_taos));

    g_taos.started = true;
    g_taos.url = strdup("http://127.0.0.1:6041/rest/sql");
    g_taos.auth_header = build_auth_header("root", "taosdata");
    g_taos.db = strdup("mqtt_rule");
    assert(taos_sink_add_stable("mqtt_data") == 0);

    taos_sink_config cfg = {};
    cfg.host = "127.0.0.1";
    cfg.port = 6041;
    cfg.username = "other";
    cfg.password = "secret";
    cfg.db = "mqtt_rule";
    cfg.stable = "mqtt_data";

    int rc = taos_sink_start(&cfg);
    assert(rc != 0);

    taos_sink_reset_partial_state();
}

static void test_duplicate_start_with_incomplete_config_fails(void)
{
    reset_alloc_failures();
    reset_init_db_stub();
    memset(&g_taos, 0, sizeof(g_taos));

    g_taos.started = true;
    g_taos.url = strdup("http://127.0.0.1:6041/rest/sql");
    g_taos.auth_header = build_auth_header("root", "taosdata");
    g_taos.db = strdup("mqtt_rule");
    assert(taos_sink_add_stable("mqtt_data") == 0);

    taos_sink_config cfg = {};
    cfg.host = "127.0.0.1";

    int rc = taos_sink_start(&cfg);
    assert(rc != 0);

    taos_sink_reset_partial_state();
}

static void test_build_batch_insert_sql_supports_multiple_stables(void)
{
    taos_queued_item items[2] = {};

    items[0].qos = 0;
    items[0].packet_id = 1;
    items[0].topic = strdup("sensor/temp");
    items[0].client_id = strdup("client-a");
    items[0].username = strdup("tester");
    items[0].payload = strdup("ab");
    items[0].stable = strdup("mqtt_data");
    items[0].payload_len = 2;
    items[0].timestamp_ms = 1;

    items[1].qos = 1;
    items[1].packet_id = 2;
    items[1].topic = strdup("sensor/status");
    items[1].client_id = strdup("client-b");
    items[1].username = strdup("tester");
    items[1].payload = strdup("cd");
    items[1].stable = strdup("mqtt_status");
    items[1].payload_len = 2;
    items[1].timestamp_ms = 2;

    char *sql = build_batch_insert_sql(items, 2, "mqtt_rule");
    assert(sql != NULL);
    assert(strstr(sql, "mqtt_rule.mqtt_data_client_a USING mqtt_rule.mqtt_data") != NULL);
    assert(strstr(sql, "mqtt_rule.mqtt_status_client_b USING mqtt_rule.mqtt_status") != NULL);

    free(sql);
    taos_item_free(&items[0]);
    taos_item_free(&items[1]);
}

static void test_build_batch_insert_sql_supports_four_stables(void)
{
    taos_queued_item items[4] = {};
    const char *topics[4] = {
        "sensor/1", "sensor/2", "sensor/3", "sensor/4"
    };
    const char *clients[4] = {
        "client-1", "client-2", "client-3", "client-4"
    };
    const char *stables[4] = {
        "mqtt_data_1", "mqtt_data_2", "mqtt_data_3", "mqtt_data_4"
    };

    for (int i = 0; i < 4; i++) {
        items[i].qos = i % 2;
        items[i].packet_id = i + 1;
        items[i].topic = strdup(topics[i]);
        items[i].client_id = strdup(clients[i]);
        items[i].username = strdup("tester");
        items[i].payload = strdup("ab");
        items[i].stable = strdup(stables[i]);
        items[i].payload_len = 2;
        items[i].timestamp_ms = i + 1;
    }

    char *sql = build_batch_insert_sql(items, 4, "mqtt_rule");
    assert(sql != NULL);
    assert(strstr(sql, "mqtt_rule.mqtt_data_1_client_1 USING mqtt_rule.mqtt_data_1") != NULL);
    assert(strstr(sql, "mqtt_rule.mqtt_data_2_client_2 USING mqtt_rule.mqtt_data_2") != NULL);
    assert(strstr(sql, "mqtt_rule.mqtt_data_3_client_3 USING mqtt_rule.mqtt_data_3") != NULL);
    assert(strstr(sql, "mqtt_rule.mqtt_data_4_client_4 USING mqtt_rule.mqtt_data_4") != NULL);

    free(sql);
    for (int i = 0; i < 4; i++) {
        taos_item_free(&items[i]);
    }
}

int main(void)
{
    reset_init_db_stub();
    test_base64_single_byte_padding();
    test_enqueue_fails_when_deep_copy_allocation_fails();
    test_enqueue_fails_when_queue_growth_fails();
    test_build_batch_insert_sql_fails_when_escape_alloc_fails();
    test_build_batch_insert_sql_fails_when_fragment_alloc_fails();
    test_build_batch_insert_sql_supports_multiple_stables();
    test_build_batch_insert_sql_supports_four_stables();
    test_start_fails_when_db_copy_allocation_fails();
    test_enqueue_fails_when_sink_state_is_incomplete();
    test_start_does_not_set_running_on_early_failure();
    test_start_marks_running_only_after_init_db_succeeds();
    test_duplicate_start_with_different_target_fails();
    test_duplicate_start_with_same_effective_target_succeeds();
    test_duplicate_start_with_different_table_same_db_succeeds();
    test_start_with_four_tables_same_db_succeeds();
    test_duplicate_start_with_different_credentials_fails();
    test_duplicate_start_with_incomplete_config_fails();
    return 0;
}
