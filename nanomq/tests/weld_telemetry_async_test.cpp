#include <assert.h>
#include <stdint.h>
#include <string.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

#include "../weld_telemetry_async.cpp"

static std::mutex              g_parse_mutex;
static std::condition_variable g_parse_cv;
static int                     g_parse_calls = 0;
static int                     g_parse_result = 0;
static bool                    g_block_parse = false;
static bool                    g_release_parse = false;
static rule_taos               g_last_rule;
static std::string             g_last_host;
static std::string             g_last_username;
static std::string             g_last_password;
static std::string             g_last_db;
static std::string             g_last_table;
static std::string             g_last_topic;
static std::string             g_last_payload;
static uint8_t                 g_last_qos = 0;
static uint16_t                g_last_packet_id = 0;

extern "C" int
weld_telemetry_handle_publish_raw(const rule_taos *taos_rule,
    const char *topic_name, uint8_t qos, uint16_t packet_id,
    const uint8_t *payload, uint32_t payload_len)
{
    std::unique_lock<std::mutex> lock(g_parse_mutex);
    g_parse_calls++;
    memset(&g_last_rule, 0, sizeof(g_last_rule));
    if (taos_rule != NULL) {
        g_last_rule = *taos_rule;
        g_last_host.assign(taos_rule->host != NULL ? taos_rule->host : "");
        g_last_username.assign(
            taos_rule->username != NULL ? taos_rule->username : "");
        g_last_password.assign(
            taos_rule->password != NULL ? taos_rule->password : "");
        g_last_db.assign(taos_rule->db != NULL ? taos_rule->db : "");
        g_last_table.assign(taos_rule->table != NULL ? taos_rule->table : "");
    }
    g_last_topic.assign(topic_name != NULL ? topic_name : "");
    g_last_payload.assign(
        payload != NULL ? reinterpret_cast<const char *>(payload) : "",
        payload != NULL ? payload_len : 0);
    g_last_qos = qos;
    g_last_packet_id = packet_id;
    g_parse_cv.notify_all();
    while (g_block_parse && !g_release_parse) {
        g_parse_cv.wait(lock);
    }
    return g_parse_result;
}

static void
reset_parse_stub(void)
{
    std::lock_guard<std::mutex> lock(g_parse_mutex);
    g_parse_calls = 0;
    g_parse_result = 0;
    g_block_parse = false;
    g_release_parse = false;
    memset(&g_last_rule, 0, sizeof(g_last_rule));
    g_last_host.clear();
    g_last_username.clear();
    g_last_password.clear();
    g_last_db.clear();
    g_last_table.clear();
    g_last_topic.clear();
    g_last_payload.clear();
    g_last_qos = 0;
    g_last_packet_id = 0;
}

static void
reset_async_state(void)
{
    weld_telemetry_async_stop_all();
    reset_parse_stub();
    std::lock_guard<std::mutex> lock(g_async_state.mutex);
    g_async_state.worker_max = WELD_PARSE_WORKER_MAX_DEFAULT;
    g_async_state.queue_max_msg = WELD_PARSE_QUEUE_MAX_MSG_DEFAULT;
    g_async_state.queue_max_bytes = WELD_PARSE_QUEUE_MAX_BYTES_DEFAULT;
}

static int
wait_for_counter(uint64_t *counter, uint64_t expected)
{
    for (int i = 0; i < 200; ++i) {
        {
            std::lock_guard<std::mutex> lock(g_async_state.mutex);
            if (*counter >= expected) {
                return 0;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return -1;
}

static rule_taos
make_rule(int port)
{
    rule_taos rule;
    memset(&rule, 0, sizeof(rule));
    rule.host = (char *) "127.0.0.1";
    rule.port = port;
    rule.username = (char *) "root";
    rule.password = (char *) "taosdata";
    rule.db = (char *) "mqtt_rule";
    rule.table = (char *) "weld_current_raw";
    rule.parser = RULE_TAOS_PARSER_WELD_TELEMETRY;
    return rule;
}

static void
test_invalid_args_are_rejected(void)
{
    reset_async_state();

    rule_taos rule = make_rule(6041);
    const uint8_t payload[] = "abc";

    assert(weld_telemetry_async_enqueue(
               NULL, "topic", 1, 1, payload, sizeof(payload) - 1) ==
        WELD_TELEMETRY_ASYNC_ERR_INVALID_ARG);
    rule.host = NULL;
    assert(weld_telemetry_async_enqueue(
               &rule, "topic", 1, 1, payload, sizeof(payload) - 1) ==
        WELD_TELEMETRY_ASYNC_ERR_INVALID_ARG);
}

static void
test_enqueue_copies_task_metadata_without_worker_consumption(void)
{
    reset_async_state();
    load_runtime_config_once();

    {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        g_async_state.started = true;
        g_async_state.running = true;
        g_async_state.queue_max_msg = 4;
        g_async_state.queue_max_bytes = 4096;
    }

    rule_taos rule = make_rule(6042);
    const char *topic = "weld/test/topic";
    const uint8_t payload[] = "{\"msg\":\"hello\"}";

    assert(weld_telemetry_async_enqueue(
               &rule, topic, 1, 9, payload, sizeof(payload) - 1) == 0);

    std::lock_guard<std::mutex> lock(g_async_state.mutex);
    assert(g_async_state.queue.size() == 1);
    assert(g_async_state.enqueue_msg_count == 1);
    const AsyncPublishTask &task = g_async_state.queue.front();
    assert(task.rule.host == "127.0.0.1");
    assert(task.rule.port == 6042);
    assert(task.rule.username == "root");
    assert(task.rule.password == "taosdata");
    assert(task.rule.db == "mqtt_rule");
    assert(task.rule.table == "weld_current_raw");
    assert(task.topic == topic);
    assert(task.payload == "{\"msg\":\"hello\"}");
    assert(task.qos == 1);
    assert(task.packet_id == 9);
    assert(g_async_state.queued_bytes == task_bytes(task));
}

static void
test_oversized_message_is_rejected(void)
{
    reset_async_state();
    load_runtime_config_once();

    {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        g_async_state.queue_max_bytes = 32;
    }

    rule_taos rule = make_rule(6041);
    const uint8_t payload[] = "{\"msg\":\"this payload is intentionally too large\"}";

    assert(weld_telemetry_async_enqueue(
               &rule, "weld/test/topic", 0, 1, payload, sizeof(payload) - 1) ==
        WELD_TELEMETRY_ASYNC_ERR_OVERSIZED);
}

static void
test_queue_full_is_rejected_when_manual_started_state_is_full(void)
{
    reset_async_state();
    load_runtime_config_once();

    {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        g_async_state.started = true;
        g_async_state.running = true;
        g_async_state.queue_max_msg = 1;
        g_async_state.queue_max_bytes = 4096;
    }

    rule_taos rule = make_rule(6041);
    const uint8_t payload[] = "{\"msg\":\"ok\"}";

    assert(weld_telemetry_async_enqueue(
               &rule, "weld/test/topic", 0, 1, payload, sizeof(payload) - 1) == 0);
    assert(weld_telemetry_async_enqueue(
               &rule, "weld/test/topic", 0, 2, payload, sizeof(payload) - 1) ==
        WELD_TELEMETRY_ASYNC_ERR_QUEUE_FULL);

    std::lock_guard<std::mutex> lock(g_async_state.mutex);
    assert(g_async_state.queue.size() == 1);
    assert(g_async_state.enqueue_msg_count == 1);
    assert(g_async_state.drop_msg_count == 1);
}

static void
test_worker_processes_successful_task(void)
{
    reset_async_state();
    load_runtime_config_once();

    {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        g_async_state.worker_max = 1;
    }

    rule_taos rule = make_rule(0);
    rule.username = NULL;
    rule.password = NULL;
    const uint8_t payload[] = "{\"msg\":\"ok\"}";

    assert(weld_telemetry_async_enqueue(
               &rule, "weld/test/success", 1, 11, payload, sizeof(payload) - 1) == 0);
    assert(wait_for_counter(&g_async_state.parse_ok_count, 1) == 0);

    std::lock_guard<std::mutex> lock(g_parse_mutex);
    assert(g_parse_calls == 1);
    assert(g_last_rule.port == 6041);
    assert(g_last_host == "127.0.0.1");
    assert(g_last_db == "mqtt_rule");
    assert(g_last_table == "weld_current_raw");
    assert(g_last_username == "");
    assert(g_last_password == "");
    assert(g_last_topic == "weld/test/success");
    assert(g_last_payload == "{\"msg\":\"ok\"}");
    assert(g_last_qos == 1);
    assert(g_last_packet_id == 11);
}

static void
test_worker_records_parse_failure(void)
{
    reset_async_state();
    load_runtime_config_once();

    {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        g_async_state.worker_max = 1;
    }
    {
        std::lock_guard<std::mutex> lock(g_parse_mutex);
        g_parse_result = -1;
    }

    rule_taos rule = make_rule(6041);
    const uint8_t payload[] = "{\"msg\":\"fail\"}";

    assert(weld_telemetry_async_enqueue(
               &rule, "weld/test/fail", 0, 12, payload, sizeof(payload) - 1) == 0);
    assert(wait_for_counter(&g_async_state.parse_fail_count, 1) == 0);

    std::lock_guard<std::mutex> lock(g_parse_mutex);
    assert(g_parse_calls == 1);
    assert(g_last_topic == "weld/test/fail");
}

static void
test_queue_full_due_to_bytes_is_rejected(void)
{
    reset_async_state();
    load_runtime_config_once();

    rule_taos rule = make_rule(6041);
    const uint8_t payload[] = "{\"msg\":\"payload-with-some-bytes\"}";

    {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        g_async_state.started = true;
        g_async_state.running = true;
        AsyncPublishTask probe;
        probe.rule.host = "127.0.0.1";
        probe.rule.port = 6041;
        probe.rule.username = "root";
        probe.rule.password = "taosdata";
        probe.rule.db = "mqtt_rule";
        probe.rule.table = "weld_current_raw";
        probe.topic = "weld/test/topic";
        probe.payload.assign(
            reinterpret_cast<const char *>(payload), sizeof(payload) - 1);
        probe.qos = 0;
        probe.packet_id = 1;
        const size_t bytes = task_bytes(probe);
        g_async_state.queue_max_msg = 4;
        g_async_state.queue_max_bytes = bytes + 4;
    }

    assert(weld_telemetry_async_enqueue(
               &rule, "weld/test/topic", 0, 1, payload, sizeof(payload) - 1) == 0);
    assert(weld_telemetry_async_enqueue(
               &rule, "weld/test/topic", 0, 2, payload, sizeof(payload) - 1) ==
        WELD_TELEMETRY_ASYNC_ERR_QUEUE_FULL);

    std::lock_guard<std::mutex> lock(g_async_state.mutex);
    assert(g_async_state.queue.size() == 1);
    assert(g_async_state.drop_msg_count == 1);
}

static void
test_stop_all_clears_queue_and_counters(void)
{
    reset_async_state();

    {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        g_async_state.started = true;
        g_async_state.running = true;
        g_async_state.enqueue_msg_count = 3;
        g_async_state.drop_msg_count = 1;
        g_async_state.parse_ok_count = 2;
        g_async_state.parse_fail_count = 1;
        g_async_state.queued_bytes = 99;
        g_async_state.queue_high_watermark_msg = 3;
        g_async_state.queue_high_watermark_bytes = 99;
        AsyncPublishTask task;
        task.rule.host = "127.0.0.1";
        task.rule.db = "mqtt_rule";
        task.rule.table = "weld_current_raw";
        task.topic = "weld/test/queued";
        task.payload = "{\"msg\":\"queued\"}";
        g_async_state.queue.push_back(task);
    }

    weld_telemetry_async_stop_all();

    std::lock_guard<std::mutex> lock(g_async_state.mutex);
    assert(!g_async_state.started);
    assert(!g_async_state.running);
    assert(g_async_state.queue.empty());
    assert(g_async_state.queued_bytes == 0);
    assert(g_async_state.enqueue_msg_count == 0);
    assert(g_async_state.drop_msg_count == 0);
    assert(g_async_state.parse_ok_count == 0);
    assert(g_async_state.parse_fail_count == 0);
    assert(g_async_state.queue_high_watermark_msg == 0);
    assert(g_async_state.queue_high_watermark_bytes == 0);
}

static void
test_stop_all_is_safe_when_not_started(void)
{
    reset_async_state();
    weld_telemetry_async_stop_all();

    std::lock_guard<std::mutex> lock(g_async_state.mutex);
    assert(!g_async_state.started);
    assert(!g_async_state.running);
    assert(g_async_state.queue.empty());
}

int
main()
{
    test_invalid_args_are_rejected();
    test_enqueue_copies_task_metadata_without_worker_consumption();
    test_oversized_message_is_rejected();
    test_queue_full_is_rejected_when_manual_started_state_is_full();
    test_worker_processes_successful_task();
    test_worker_records_parse_failure();
    test_queue_full_due_to_bytes_is_rejected();
    test_stop_all_clears_queue_and_counters();
    test_stop_all_is_safe_when_not_started();
    weld_telemetry_async_stop_all();
    return 0;
}
