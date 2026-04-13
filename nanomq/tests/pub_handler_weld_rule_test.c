#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "include/pub_handler.h"
#include "nng/protocol/mqtt/mqtt_parser.h"
#include "nng/supplemental/nanolib/cvector.h"
#include "../taos_sink.hpp"
#include "../weld_telemetry_async.h"

static int      g_async_calls      = 0;
static int      g_async_return_code = WELD_TELEMETRY_ASYNC_ENQUEUE_OK;
static char     g_async_topic[256];
static char     g_async_payload[1024];
static uint8_t  g_async_qos        = 0;
static uint16_t g_async_packet_id  = 0;
static rule_taos g_async_rule;

static int              g_taos_sink_calls = 0;
static taos_sink_config g_last_taos_cfg;
static taos_rule_result g_last_taos_result;
static char             g_taos_topic[256];
static char             g_taos_client_id[128];
static char             g_taos_username[128];
static char             g_taos_payload[1024];

int
weld_telemetry_async_enqueue(const rule_taos *taos_rule,
    const char *topic_name, uint8_t qos, uint16_t packet_id,
    const uint8_t *payload, uint32_t payload_len)
{
    assert(taos_rule != NULL);
    assert(topic_name != NULL);
    assert(payload != NULL);

    g_async_calls++;
    memset(&g_async_rule, 0, sizeof(g_async_rule));
    g_async_rule = *taos_rule;
    snprintf(g_async_topic, sizeof(g_async_topic), "%s", topic_name);
    memcpy(g_async_payload, payload, payload_len);
    g_async_payload[payload_len] = '\0';
    g_async_qos = qos;
    g_async_packet_id = packet_id;
    return g_async_return_code;
}

int
taos_sink_enqueue_with_config(
    const taos_sink_config *cfg, const taos_rule_result *result)
{
    assert(cfg != NULL);
    assert(result != NULL);

    g_taos_sink_calls++;
    g_last_taos_cfg = *cfg;
    g_last_taos_result = *result;

    snprintf(g_taos_topic, sizeof(g_taos_topic), "%s",
        result->topic != NULL ? result->topic : "");
    snprintf(g_taos_client_id, sizeof(g_taos_client_id), "%s",
        result->client_id != NULL ? result->client_id : "");
    snprintf(g_taos_username, sizeof(g_taos_username), "%s",
        result->username != NULL ? result->username : "");
    memcpy(g_taos_payload, result->payload, result->payload_len);
    g_taos_payload[result->payload_len] = '\0';

    g_last_taos_result.topic = g_taos_topic;
    g_last_taos_result.client_id = g_taos_client_id;
    g_last_taos_result.username = g_taos_username;
    g_last_taos_result.payload = g_taos_payload;
    return 0;
}

static void
reset_stubs(void)
{
    g_async_calls = 0;
    g_async_return_code = WELD_TELEMETRY_ASYNC_ENQUEUE_OK;
    memset(&g_async_rule, 0, sizeof(g_async_rule));
    memset(g_async_topic, 0, sizeof(g_async_topic));
    memset(g_async_payload, 0, sizeof(g_async_payload));
    g_async_qos = 0;
    g_async_packet_id = 0;

    g_taos_sink_calls = 0;
    memset(&g_last_taos_cfg, 0, sizeof(g_last_taos_cfg));
    memset(&g_last_taos_result, 0, sizeof(g_last_taos_result));
    memset(g_taos_topic, 0, sizeof(g_taos_topic));
    memset(g_taos_client_id, 0, sizeof(g_taos_client_id));
    memset(g_taos_username, 0, sizeof(g_taos_username));
    memset(g_taos_payload, 0, sizeof(g_taos_payload));
}

static void
init_work_with_rule(nano_work *work, conf *cfg, rule *rules,
    const rule_taos *taos_rule, rule_taos_parser_type parser,
    const char *topic, const char *payload)
{
    memset(work, 0, sizeof(*work));
    memset(cfg, 0, sizeof(*cfg));

    rule r;
    memset(&r, 0, sizeof(r));
    r.enabled = true;
    r.forword_type = RULE_FORWORD_TAOS;
    r.topic = (char *) topic;
    r.taos = (rule_taos *) taos_rule;
    r.taos->parser = parser;
    cvector_push_back(rules, r);

    cfg->rule_eng.option = RULE_ENG_TAOS;
    cfg->rule_eng.rules = rules;

    static pub_packet_struct packet;
    memset(&packet, 0, sizeof(packet));
    packet.fixed_header.qos = 1;
    packet.var_header.publish.packet_id = 77;
    packet.var_header.publish.topic_name.body = (char *) topic;
    packet.var_header.publish.topic_name.len = strlen(topic);
    packet.payload.data = (uint8_t *) payload;
    packet.payload.len = strlen(payload);

    work->config = cfg;
    work->pub_packet = &packet;
}

static void
init_work_runtime(nano_work *work, conf *cfg)
{
    memset(work, 0, sizeof(*work));
    memset(cfg, 0, sizeof(*cfg));
    work->proto = PROTO_MQTT_BROKER;
    work->proto_ver = MQTT_PROTOCOL_VERSION_v311;
    work->config = cfg;
    dbtree_create(&work->db);
}

static nng_msg *
make_publish_msg_v311_qos1(
    const char *topic, uint16_t packet_id, const char *payload)
{
    nng_msg *msg = NULL;
    size_t   topic_len = strlen(topic);
    size_t   payload_len = strlen(payload);
    uint32_t remaining_len = (uint32_t) (2 + topic_len + 2 + payload_len);
    uint8_t  fixed_header[5] = { 0x32, 0, 0, 0, 0 };
    size_t   fixed_header_len = 1;
    uint8_t  topic_header[2] = {
        (uint8_t) ((topic_len >> 8) & 0xff),
        (uint8_t) (topic_len & 0xff),
    };
    uint8_t packet_id_buf[2] = {
        (uint8_t) ((packet_id >> 8) & 0xff),
        (uint8_t) (packet_id & 0xff),
    };

    assert(remaining_len < 268435456U);
    do {
        uint8_t encoded = (uint8_t) (remaining_len % 128U);
        remaining_len /= 128U;
        if (remaining_len > 0U) {
            encoded |= 0x80U;
        }
        fixed_header[fixed_header_len++] = encoded;
    } while (remaining_len > 0U);
    assert(nng_msg_alloc(&msg, 0) == 0);
    assert(nng_msg_append(msg, topic_header, sizeof(topic_header)) == 0);
    assert(nng_msg_append(msg, topic, topic_len) == 0);
    assert(nng_msg_append(msg, packet_id_buf, sizeof(packet_id_buf)) == 0);
    assert(nng_msg_append(msg, payload, payload_len) == 0);
    assert(nng_msg_header_append(msg, fixed_header, fixed_header_len) == 0);
    return msg;
}

static void
free_runtime_work(nano_work *work)
{
    if (work->pub_packet != NULL) {
        free_pub_packet(work->pub_packet);
        work->pub_packet = NULL;
    }
    if (work->msg != NULL) {
        nng_msg_free(work->msg);
        work->msg = NULL;
    }
    if (work->cparam != NULL) {
        conn_param_free(work->cparam);
        work->cparam = NULL;
    }
    if (work->db != NULL) {
        dbtree_destory(work->db);
        work->db = NULL;
    }
}

static void
test_weld_rule_uses_async_enqueue(void)
{
    const char *topic = "weld/v1/unit/telemetry/current";
    const char *payload = "{\"msg_id\":\"001\",\"ts_us\":1710000000000000}";
    nano_work work;
    conf cfg;
    rule *rules = NULL;
    rule_taos taos_rule;

    memset(&taos_rule, 0, sizeof(taos_rule));
    taos_rule.host = "127.0.0.1";
    taos_rule.port = 6041;
    taos_rule.username = "root";
    taos_rule.password = "taosdata";
    taos_rule.db = "mqtt_rule";
    taos_rule.table = "weld_current_raw";

    reset_stubs();
    init_work_with_rule(
        &work, &cfg, rules, &taos_rule, RULE_TAOS_PARSER_WELD_TELEMETRY,
        topic, payload);
    rules = cfg.rule_eng.rules;

    assert(rule_engine_insert_sql(&work) == 0);
    assert(g_async_calls == 1);
    assert(g_taos_sink_calls == 0);
    assert(strcmp(g_async_topic, topic) == 0);
    assert(strcmp(g_async_payload, payload) == 0);
    assert(g_async_qos == 1);
    assert(g_async_packet_id == 77);
    assert(strcmp(g_async_rule.table, "weld_current_raw") == 0);
    assert(strcmp(g_async_rule.db, "mqtt_rule") == 0);

    cvector_free(rules);
}

static void
test_weld_power_topic_uses_async_enqueue_with_realistic_payload(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb03";
    const char *payload =
        "{"
        "\"spec_ver\":\"1.1.0\","
        "\"msg_id\":\"realistic-001\","
        "\"msg_class\":\"telemetry\","
        "\"ts_ms\":1775098453860,"
        "\"seq\":2652,"
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chb03\","
        "\"device_type\":\"current_transducer\","
        "\"signal_type\":\"current\","
        "\"device_model\":\"CHB-200-5V\","
        "\"channel_id\":\"ai2\","
        "\"window\":{\"start_us\":1775098453860667,\"duration_ms\":100,\"sample_rate_hz\":10,\"point_count\":1},"
        "\"data\":{\"layout\":\"uniform_series_binary\",\"point_count\":1,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"encoding\":\"base64\",\"value_type\":\"float32\","
        "\"byte_order\":\"little_endian\",\"payload\":\"ZkYfQw==\"}"
        "}";
    nano_work work;
    conf cfg;
    rule *rules = NULL;
    rule_taos taos_rule;

    memset(&taos_rule, 0, sizeof(taos_rule));
    taos_rule.host = "127.0.0.1";
    taos_rule.port = 6041;
    taos_rule.username = "root";
    taos_rule.password = "taosdata";
    taos_rule.db = "mqtt_rule";
    taos_rule.table = "weld_current_raw";

    reset_stubs();
    init_work_with_rule(
        &work, &cfg, rules, &taos_rule, RULE_TAOS_PARSER_WELD_TELEMETRY,
        topic, payload);
    rules = cfg.rule_eng.rules;

    assert(rule_engine_insert_sql(&work) == 0);
    assert(g_async_calls == 1);
    assert(g_taos_sink_calls == 0);
    assert(strcmp(g_async_topic, topic) == 0);
    assert(strcmp(g_async_payload, payload) == 0);
    assert(strcmp(g_async_rule.table, "weld_current_raw") == 0);

    cvector_free(rules);
}

static void
test_weld_rule_async_fail_does_not_fallback(void)
{
    const char *topic = "weld/v1/unit/telemetry/current";
    const char *payload = "{\"msg_id\":\"002\",\"ts_us\":1710000000000001}";
    nano_work work;
    conf cfg;
    rule *rules = NULL;
    rule_taos taos_rule;

    memset(&taos_rule, 0, sizeof(taos_rule));
    taos_rule.host = "127.0.0.1";
    taos_rule.port = 6041;
    taos_rule.username = "root";
    taos_rule.password = "taosdata";
    taos_rule.db = "mqtt_rule";
    taos_rule.table = "weld_current_raw";

    reset_stubs();
    g_async_return_code = WELD_TELEMETRY_ASYNC_ERR_QUEUE_FULL;
    init_work_with_rule(
        &work, &cfg, rules, &taos_rule, RULE_TAOS_PARSER_WELD_TELEMETRY,
        topic, payload);
    rules = cfg.rule_eng.rules;

    assert(rule_engine_insert_sql(&work) == 0);
    assert(g_async_calls == 1);
    assert(g_taos_sink_calls == 0);

    cvector_free(rules);
}

static void
test_topic_mismatch_skips_taos_paths(void)
{
    const char *payload = "{\"msg_id\":\"003\",\"ts_us\":1710000000000002}";
    nano_work work;
    conf cfg;
    rule *rules = NULL;
    rule_taos taos_rule;

    memset(&taos_rule, 0, sizeof(taos_rule));
    taos_rule.host = "127.0.0.1";
    taos_rule.port = 6041;
    taos_rule.username = "root";
    taos_rule.password = "taosdata";
    taos_rule.db = "mqtt_rule";
    taos_rule.table = "weld_current_raw";

    reset_stubs();
    init_work_with_rule(
        &work, &cfg, rules, &taos_rule, RULE_TAOS_PARSER_WELD_TELEMETRY,
        "weld/v1/unit/telemetry/current", payload);
    rules = cfg.rule_eng.rules;
    work.pub_packet->var_header.publish.topic_name.body =
        "weld/v1/unit/telemetry/voltage";
    work.pub_packet->var_header.publish.topic_name.len =
        strlen("weld/v1/unit/telemetry/voltage");

    assert(rule_engine_insert_sql(&work) == 0);
    assert(g_async_calls == 0);
    assert(g_taos_sink_calls == 0);

    cvector_free(rules);
}

static void
test_handle_pub_then_weld_rule_uses_async_enqueue(void)
{
    const char *topic = "weld/v1/unit/telemetry/current";
    const char *payload = "{\"msg_id\":\"raw-001\",\"ts_us\":1710000000000003}";
    nano_work work;
    conf cfg;
    rule *rules = NULL;
    rule_taos taos_rule;
    conn_param *cp = NULL;
    struct pipe_content pipe_ct;

    memset(&pipe_ct, 0, sizeof(pipe_ct));
    memset(&taos_rule, 0, sizeof(taos_rule));
    taos_rule.host = "127.0.0.1";
    taos_rule.port = 6041;
    taos_rule.username = "root";
    taos_rule.password = "taosdata";
    taos_rule.db = "mqtt_rule";
    taos_rule.table = "weld_current_raw";

    reset_stubs();
    init_work_runtime(&work, &cfg);

    {
        rule r;
        memset(&r, 0, sizeof(r));
        r.enabled = true;
        r.forword_type = RULE_FORWORD_TAOS;
        r.topic = (char *) topic;
        r.taos = &taos_rule;
        r.taos->parser = RULE_TAOS_PARSER_WELD_TELEMETRY;
        cvector_push_back(rules, r);
    }
    cfg.rule_eng.option = RULE_ENG_TAOS;
    cfg.rule_eng.rules = rules;

    assert(conn_param_alloc(&cp) == 0);
    conn_param_set_clientid(cp, "client-raw-001");
    work.cparam = cp;
    work.msg = make_publish_msg_v311_qos1(topic, 305, payload);

    dbhash_init_pipe_table();
    assert(handle_pub(&work, &pipe_ct, MQTT_PROTOCOL_VERSION_v311, false) ==
        SUCCESS);
    assert(work.pub_packet != NULL);
    assert(strcmp(work.pub_packet->var_header.publish.topic_name.body, topic) == 0);
    assert(work.pub_packet->var_header.publish.packet_id == 305);
    assert(work.pub_packet->fixed_header.qos == 1);
    assert(work.pub_packet->payload.len == strlen(payload));
    assert(memcmp(work.pub_packet->payload.data, payload, strlen(payload)) == 0);

    assert(rule_engine_insert_sql(&work) == 0);
    assert(g_async_calls == 1);
    assert(strcmp(g_async_topic, topic) == 0);
    assert(strcmp(g_async_payload, payload) == 0);
    assert(g_async_qos == 1);
    assert(g_async_packet_id == 305);
    assert(g_taos_sink_calls == 0);

    dbhash_destroy_pipe_table();
    cvector_free(rules);
    free_runtime_work(&work);
}

static void
test_handle_pub_then_realistic_weld_power_rule_uses_async_enqueue(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv04";
    const char *payload =
        "{"
        "\"spec_ver\":\"1.1.0\","
        "\"msg_id\":\"realistic-raw-001\","
        "\"msg_class\":\"telemetry\","
        "\"ts_ms\":1775098453960,"
        "\"seq\":2653,"
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chv04\","
        "\"device_type\":\"voltage_transducer\","
        "\"signal_type\":\"voltage\","
        "\"device_model\":\"CHV-100-100\","
        "\"channel_id\":\"ai3\","
        "\"task_id\":\"110120119\","
        "\"window\":{\"start_us\":1775098453960908,\"duration_ms\":100,\"sample_rate_hz\":10,\"point_count\":1},"
        "\"data\":{\"layout\":\"uniform_series_binary\",\"point_count\":1,"
        "\"fields\":[{\"name\":\"voltage\",\"unit\":\"V\"}],"
        "\"encoding\":\"base64\",\"value_type\":\"float32\","
        "\"byte_order\":\"little_endian\",\"payload\":\"KZxXQw==\"}"
        "}";
    nano_work work;
    conf cfg;
    rule *rules = NULL;
    rule_taos taos_rule;
    conn_param *cp = NULL;
    struct pipe_content pipe_ct;

    memset(&pipe_ct, 0, sizeof(pipe_ct));
    memset(&taos_rule, 0, sizeof(taos_rule));
    taos_rule.host = "127.0.0.1";
    taos_rule.port = 6041;
    taos_rule.username = "root";
    taos_rule.password = "taosdata";
    taos_rule.db = "mqtt_rule";
    taos_rule.table = "weld_voltage_raw";

    reset_stubs();
    init_work_runtime(&work, &cfg);

    {
        rule r;
        memset(&r, 0, sizeof(r));
        r.enabled = true;
        r.forword_type = RULE_FORWORD_TAOS;
        r.topic = (char *) topic;
        r.taos = &taos_rule;
        r.taos->parser = RULE_TAOS_PARSER_WELD_TELEMETRY;
        cvector_push_back(rules, r);
    }
    cfg.rule_eng.option = RULE_ENG_TAOS;
    cfg.rule_eng.rules = rules;

    assert(conn_param_alloc(&cp) == 0);
    conn_param_set_clientid(cp, "client-realistic-raw-001");
    work.cparam = cp;
    work.msg = make_publish_msg_v311_qos1(topic, 405, payload);

    dbhash_init_pipe_table();
    assert(handle_pub(&work, &pipe_ct, MQTT_PROTOCOL_VERSION_v311, false) ==
        SUCCESS);
    assert(rule_engine_insert_sql(&work) == 0);
    assert(g_async_calls == 1);
    assert(strcmp(g_async_topic, topic) == 0);
    assert(strcmp(g_async_payload, payload) == 0);
    assert(g_async_qos == 1);
    assert(g_async_packet_id == 405);
    assert(strcmp(g_async_rule.table, "weld_voltage_raw") == 0);
    assert(g_taos_sink_calls == 0);

    dbhash_destroy_pipe_table();
    cvector_free(rules);
    free_runtime_work(&work);
}

static void
test_handle_pub_then_non_weld_rule_uses_taos_sink_enqueue(void)
{
    const char *topic = "sensor/v1/general";
    const char *payload = "{\"value\":45.6}";
    nano_work work;
    conf cfg;
    rule *rules = NULL;
    rule_taos taos_rule;
    conn_param *cp = NULL;
    struct pipe_content pipe_ct;

    memset(&pipe_ct, 0, sizeof(pipe_ct));
    memset(&taos_rule, 0, sizeof(taos_rule));
    taos_rule.host = "127.0.0.1";
    taos_rule.port = 0;
    taos_rule.username = "root";
    taos_rule.password = "taosdata";
    taos_rule.db = "mqtt_rule";
    taos_rule.table = "mqtt_data";

    reset_stubs();
    init_work_runtime(&work, &cfg);

    {
        rule r;
        memset(&r, 0, sizeof(r));
        r.enabled = true;
        r.forword_type = RULE_FORWORD_TAOS;
        r.topic = (char *) topic;
        r.taos = &taos_rule;
        r.taos->parser = RULE_TAOS_PARSER_NONE;
        cvector_push_back(rules, r);
    }
    cfg.rule_eng.option = RULE_ENG_TAOS;
    cfg.rule_eng.rules = rules;

    assert(conn_param_alloc(&cp) == 0);
    conn_param_set_clientid(cp, "client-raw-002");
    work.cparam = cp;
    work.msg = make_publish_msg_v311_qos1(topic, 306, payload);

    dbhash_init_pipe_table();
    assert(handle_pub(&work, &pipe_ct, MQTT_PROTOCOL_VERSION_v311, false) ==
        SUCCESS);
    assert(rule_engine_insert_sql(&work) == 0);
    assert(g_async_calls == 0);
    assert(g_taos_sink_calls == 1);
    assert(strcmp(g_last_taos_cfg.host, "127.0.0.1") == 0);
    assert(g_last_taos_cfg.port == 6041);
    assert(strcmp(g_last_taos_cfg.db, "mqtt_rule") == 0);
    assert(strcmp(g_last_taos_cfg.stable, "mqtt_data") == 0);
    assert(strcmp(g_last_taos_result.topic, topic) == 0);
    assert(strcmp(g_last_taos_result.client_id, "client-raw-002") == 0);
    assert(strcmp(g_last_taos_result.payload, payload) == 0);
    assert(g_last_taos_result.qos == 1);
    assert(g_last_taos_result.packet_id == 306);
    assert(g_last_taos_result.timestamp_ms > 0);

    dbhash_destroy_pipe_table();
    cvector_free(rules);
    free_runtime_work(&work);
}

static void
test_non_weld_rule_uses_taos_sink_enqueue(void)
{
    const char *topic = "sensor/v1/general";
    const char *payload = "{\"value\":12.3}";
    nano_work work;
    conf cfg;
    rule *rules = NULL;
    rule_taos taos_rule;
    conn_param *cp = NULL;

    memset(&taos_rule, 0, sizeof(taos_rule));
    taos_rule.host = "127.0.0.1";
    taos_rule.port = 0;
    taos_rule.username = "root";
    taos_rule.password = "taosdata";
    taos_rule.db = "mqtt_rule";
    taos_rule.table = "mqtt_data";

    reset_stubs();
    init_work_with_rule(
        &work, &cfg, rules, &taos_rule, RULE_TAOS_PARSER_NONE,
        topic, payload);
    rules = cfg.rule_eng.rules;

    assert(conn_param_alloc(&cp) == 0);
    conn_param_set_clientid(cp, "client-001");
    work.cparam = cp;

    assert(rule_engine_insert_sql(&work) == 0);
    assert(g_async_calls == 0);
    assert(g_taos_sink_calls == 1);
    assert(strcmp(g_last_taos_cfg.host, "127.0.0.1") == 0);
    assert(g_last_taos_cfg.port == 6041);
    assert(strcmp(g_last_taos_cfg.db, "mqtt_rule") == 0);
    assert(strcmp(g_last_taos_cfg.stable, "mqtt_data") == 0);
    assert(strcmp(g_last_taos_result.topic, topic) == 0);
    assert(strcmp(g_last_taos_result.client_id, "client-001") == 0);
    assert(strcmp(g_last_taos_result.username, "") == 0);
    assert(strcmp(g_last_taos_result.payload, payload) == 0);
    assert(g_last_taos_result.qos == 1);
    assert(g_last_taos_result.packet_id == 77);
    assert(g_last_taos_result.timestamp_ms > 0);

    conn_param_free(cp);
    cvector_free(rules);
}

int
main(void)
{
    test_weld_rule_uses_async_enqueue();
    test_weld_power_topic_uses_async_enqueue_with_realistic_payload();
    test_weld_rule_async_fail_does_not_fallback();
    test_topic_mismatch_skips_taos_paths();
    test_handle_pub_then_weld_rule_uses_async_enqueue();
    test_handle_pub_then_realistic_weld_power_rule_uses_async_enqueue();
    test_handle_pub_then_non_weld_rule_uses_taos_sink_enqueue();
    test_non_weld_rule_uses_taos_sink_enqueue();
    return 0;
}
