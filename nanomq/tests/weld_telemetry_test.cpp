#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zstd.h>

extern "C" {
#include "../weld_telemetry.c"
}

static int          g_enqueue_count = 0;
static int          g_enqueue_fail = 0;
static int          g_can_accept = 1;
static weld_taos_row g_rows[8];
static char         g_stables[8][64];
static char         g_channel_ids[8][32];
static char         g_task_ids[8][160];
static char         g_raw_units[8][32];
static char         g_cal_versions[8][64];
static char         g_encodings[8][32];
static char         g_payloads[8][8192];
static char         g_quality_texts[8][64];
static char         g_source_buses[8][32];
static char         g_source_ports[8][64];
static char         g_source_protocols[8][32];

extern "C" int
weld_taos_sink_enqueue_rows_with_config(const taos_sink_config *cfg,
    const weld_taos_row *rows, size_t row_count)
{
    (void) cfg;
    assert(rows != NULL);
    if (g_enqueue_fail) {
        return -1;
    }
    for (size_t i = 0; i < row_count; ++i) {
        const weld_taos_row *row = &rows[i];
        assert(g_enqueue_count < (int) (sizeof(g_rows) / sizeof(g_rows[0])));
        g_rows[g_enqueue_count++] = *row;
        if (row->stable != NULL) {
            snprintf(g_stables[g_enqueue_count - 1],
                sizeof(g_stables[g_enqueue_count - 1]), "%s", row->stable);
            g_rows[g_enqueue_count - 1].stable = g_stables[g_enqueue_count - 1];
        }
        if (row->channel_id != NULL) {
            snprintf(g_channel_ids[g_enqueue_count - 1],
                sizeof(g_channel_ids[g_enqueue_count - 1]), "%s",
                row->channel_id);
            g_rows[g_enqueue_count - 1].channel_id =
                g_channel_ids[g_enqueue_count - 1];
        }
        if (row->task_id != NULL) {
            snprintf(g_task_ids[g_enqueue_count - 1],
                sizeof(g_task_ids[g_enqueue_count - 1]), "%s",
                row->task_id);
            g_rows[g_enqueue_count - 1].task_id =
                g_task_ids[g_enqueue_count - 1];
        }
        if (row->raw_adc_unit != NULL) {
            snprintf(g_raw_units[g_enqueue_count - 1],
                sizeof(g_raw_units[g_enqueue_count - 1]), "%s",
                row->raw_adc_unit);
            g_rows[g_enqueue_count - 1].raw_adc_unit =
                g_raw_units[g_enqueue_count - 1];
        }
        if (row->cal_version != NULL) {
            snprintf(g_cal_versions[g_enqueue_count - 1],
                sizeof(g_cal_versions[g_enqueue_count - 1]), "%s",
                row->cal_version);
            g_rows[g_enqueue_count - 1].cal_version =
                g_cal_versions[g_enqueue_count - 1];
        }
        if (row->encoding != NULL) {
            snprintf(g_encodings[g_enqueue_count - 1],
                sizeof(g_encodings[g_enqueue_count - 1]), "%s",
                row->encoding);
            g_rows[g_enqueue_count - 1].encoding =
                g_encodings[g_enqueue_count - 1];
        }
        if (row->payload != NULL) {
            snprintf(g_payloads[g_enqueue_count - 1],
                sizeof(g_payloads[g_enqueue_count - 1]), "%s",
                row->payload);
            g_rows[g_enqueue_count - 1].payload =
                g_payloads[g_enqueue_count - 1];
        }
        if (row->quality_text != NULL) {
            snprintf(g_quality_texts[g_enqueue_count - 1],
                sizeof(g_quality_texts[g_enqueue_count - 1]), "%s",
                row->quality_text);
            g_rows[g_enqueue_count - 1].quality_text =
                g_quality_texts[g_enqueue_count - 1];
        }
        if (row->source_bus != NULL) {
            snprintf(g_source_buses[g_enqueue_count - 1],
                sizeof(g_source_buses[g_enqueue_count - 1]), "%s",
                row->source_bus);
            g_rows[g_enqueue_count - 1].source_bus =
                g_source_buses[g_enqueue_count - 1];
        }
        if (row->source_port != NULL) {
            snprintf(g_source_ports[g_enqueue_count - 1],
                sizeof(g_source_ports[g_enqueue_count - 1]), "%s",
                row->source_port);
            g_rows[g_enqueue_count - 1].source_port =
                g_source_ports[g_enqueue_count - 1];
        }
        if (row->source_protocol != NULL) {
            snprintf(g_source_protocols[g_enqueue_count - 1],
                sizeof(g_source_protocols[g_enqueue_count - 1]), "%s",
                row->source_protocol);
            g_rows[g_enqueue_count - 1].source_protocol =
                g_source_protocols[g_enqueue_count - 1];
        }
    }
    return 0;
}

extern "C" int
weld_taos_sink_can_accept_rows_with_config(
    const taos_sink_config *cfg, size_t row_count)
{
    (void) cfg;
    (void) row_count;
    return g_can_accept;
}

extern "C" int
weld_taos_sink_enqueue_row_with_config(
    const taos_sink_config *cfg, const weld_taos_row *row)
{
    return weld_taos_sink_enqueue_rows_with_config(cfg, row, 1);
}

extern "C" int
weld_taos_sink_is_started(void)
{
    return 1;
}

extern "C" void
weld_taos_sink_stop_all(void)
{
}

static void
reset_rows(void)
{
    g_enqueue_count = 0;
    g_enqueue_fail = 0;
    g_can_accept = 1;
    memset(g_rows, 0, sizeof(g_rows));
    memset(g_stables, 0, sizeof(g_stables));
    memset(g_channel_ids, 0, sizeof(g_channel_ids));
    memset(g_task_ids, 0, sizeof(g_task_ids));
    memset(g_raw_units, 0, sizeof(g_raw_units));
    memset(g_cal_versions, 0, sizeof(g_cal_versions));
    memset(g_encodings, 0, sizeof(g_encodings));
    memset(g_payloads, 0, sizeof(g_payloads));
    memset(g_quality_texts, 0, sizeof(g_quality_texts));
    memset(g_source_buses, 0, sizeof(g_source_buses));
    memset(g_source_ports, 0, sizeof(g_source_ports));
    memset(g_source_protocols, 0, sizeof(g_source_protocols));
}

static rule_taos
make_rule(const char *table)
{
    rule_taos rule;
    memset(&rule, 0, sizeof(rule));
    rule.host = (char *) "127.0.0.1";
    rule.port = 6041;
    rule.username = (char *) "root";
    rule.password = (char *) "taosdata";
    rule.db = (char *) "mqtt_rule";
    rule.table = (char *) table;
    rule.parser = RULE_TAOS_PARSER_WELD_TELEMETRY;
    return rule;
}

static pub_packet_struct
make_packet(const char *topic, const char *payload)
{
    pub_packet_struct packet;
    memset(&packet, 0, sizeof(packet));
    packet.fixed_header.qos = 1;
    packet.var_header.publish.packet_id = 7;
    packet.var_header.publish.topic_name.body = (char *) topic;
    packet.var_header.publish.topic_name.len = strlen(topic);
    packet.payload.data = (uint8_t *) payload;
    packet.payload.len = strlen(payload);
    return packet;
}

static void
encode_float32_le_value(float value, unsigned char *out)
{
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    unsigned char tmp[sizeof(float)];
    memcpy(tmp, &value, sizeof(value));
    out[0] = tmp[3];
    out[1] = tmp[2];
    out[2] = tmp[1];
    out[3] = tmp[0];
#else
    memcpy(out, &value, sizeof(value));
#endif
}

static char *
encode_uniform_payload_base64(const float *values, size_t point_count)
{
    size_t raw_len = point_count * sizeof(float);
    unsigned char *raw = (unsigned char *) malloc(raw_len);
    char *encoded = NULL;

    assert(raw != NULL);
    for (size_t i = 0; i < point_count; ++i) {
        encode_float32_le_value(values[i], raw + (i * sizeof(float)));
    }

    encoded = (char *) malloc(BASE64_ENCODE_OUT_SIZE(raw_len));
    assert(encoded != NULL);
    assert(base64_encode(raw, (unsigned int) raw_len, encoded) > 0);
    free(raw);
    return encoded;
}

static char *
encode_uniform_payload_zstd_base64(const float *values, size_t point_count)
{
    size_t raw_len = point_count * sizeof(float);
    size_t compressed_cap = ZSTD_compressBound(raw_len);
    unsigned char *raw = (unsigned char *) malloc(raw_len);
    unsigned char *compressed =
        (unsigned char *) malloc(compressed_cap);
    size_t compressed_len = 0;
    char *encoded = NULL;

    assert(raw != NULL);
    assert(compressed != NULL);
    for (size_t i = 0; i < point_count; ++i) {
        encode_float32_le_value(values[i], raw + (i * sizeof(float)));
    }

    compressed_len =
        ZSTD_compress(compressed, compressed_cap, raw, raw_len, 1);
    assert(!ZSTD_isError(compressed_len));

    encoded =
        (char *) malloc(BASE64_ENCODE_OUT_SIZE(compressed_len));
    assert(encoded != NULL);
    assert(base64_encode(
               compressed, (unsigned int) compressed_len, encoded) > 0);

    free(compressed);
    free(raw);
    return encoded;
}

static char *
make_telemetry_payload_with_data_block(const char *device_id,
    const char *msg_id, const char *spec_ver, const char *device_type,
    const char *device_model, const char *signal_type, const char *channel_id,
    const char *raw_unit, const char *data_block, const char *task_id = NULL)
{
    char task_id_field[192];
    const char *template_str =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"%s\","
        "\"msg_id\":\"%s\","
        "\"spec_ver\":\"%s\","
        "%s"
        "\"ts_ms\":1710000000000,"
        "\"seq\":111,"
        "\"device_type\":\"%s\","
        "\"device_model\":\"%s\","
        "\"signal_type\":\"%s\","
        "\"channel_id\":\"%s\","
        "\"quality\":{\"code\":0,\"text\":\"ok\"},"
        "\"raw\":{\"adc_unit\":\"%s\"},"
        "\"calibration\":{\"version\":\"cal-bin\",\"k\":1.1,\"b\":0.3},"
        "\"data\":%s"
        "}";
    if (task_id != NULL && task_id[0] != '\0') {
        snprintf(task_id_field, sizeof(task_id_field),
            "\"task_id\":\"%s\",", task_id);
    } else {
        task_id_field[0] = '\0';
    }
    int len = snprintf(NULL, 0, template_str, device_id, msg_id, spec_ver,
        task_id_field, device_type, device_model, signal_type, channel_id,
        raw_unit, data_block);
    char *json = (char *) malloc((size_t) len + 1);

    assert(json != NULL);
    snprintf(json, (size_t) len + 1, template_str, device_id, msg_id, spec_ver,
        task_id_field, device_type, device_model, signal_type, channel_id,
        raw_unit, data_block);
    return json;
}

static char *
make_uniform_payload_json(const char *device_id, const char *msg_id,
    const char *signal_type, const char *channel_id, const char *encoding,
    const char *encoded_payload, int data_point_count, int window_point_count,
    long long start_us, long long sample_rate_hz, const char *task_id = NULL)
{
    const char *device_type = strcmp(signal_type, "current") == 0 ?
        "current_transducer" :
        "voltage_transducer";
    const char *device_model = strcmp(signal_type, "current") == 0 ?
        "CHB-2KE" :
        "CHV-2KE";
    const char *field_name = strcmp(signal_type, "current") == 0 ?
        "current" :
        "voltage";
    const char *field_unit = strcmp(signal_type, "current") == 0 ?
        "A" :
        "V";
    const char *raw_unit = strcmp(signal_type, "current") == 0 ?
        "mV" :
        "V";
    const char *data_template =
        "{"
        "\"layout\":\"uniform_series_binary\","
        "\"point_count\":%d,"
        "\"fields\":[{\"name\":\"%s\",\"unit\":\"%s\"}],"
        "\"value_type\":\"float32\","
        "\"byte_order\":\"little_endian\","
        "\"encoding\":\"%s\","
        "\"payload\":\"%s\","
        "\"window\":{\"start_us\":%lld,\"sample_rate_hz\":%lld,\"point_count\":%d}"
        "}"
        ;
    int data_len = snprintf(NULL, 0, data_template, data_point_count,
        field_name, field_unit, encoding, encoded_payload, start_us,
        sample_rate_hz, window_point_count);
    char *data_block = (char *) malloc((size_t) data_len + 1);
    char *json = NULL;

    assert(data_block != NULL);
    snprintf(data_block, (size_t) data_len + 1, data_template,
        data_point_count, field_name, field_unit, encoding, encoded_payload,
        start_us, sample_rate_hz, window_point_count);
    json = make_telemetry_payload_with_data_block(device_id, msg_id, "1.1.0",
        device_type, device_model, signal_type, channel_id, raw_unit,
        data_block, task_id);
    free(data_block);
    return json;
}

static char *
make_uniform_payload_json_root_window(const char *device_id, const char *msg_id,
    const char *signal_type, const char *channel_id, const char *encoding,
    const char *encoded_payload, int data_point_count, int window_point_count,
    long long start_us, long long sample_rate_hz, const char *task_id = NULL)
{
    const char *device_type = strcmp(signal_type, "current") == 0 ?
        "current_transducer" :
        "voltage_transducer";
    const char *device_model = strcmp(signal_type, "current") == 0 ?
        "CHB-2KE" :
        "CHV-2KE";
    const char *field_name = strcmp(signal_type, "current") == 0 ?
        "current" :
        "voltage";
    const char *field_unit = strcmp(signal_type, "current") == 0 ?
        "A" :
        "V";
    const char *raw_unit = strcmp(signal_type, "current") == 0 ?
        "mV" :
        "V";
    char task_id_field[192];
    const char *template_str =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"%s\","
        "\"msg_id\":\"%s\","
        "\"spec_ver\":\"1.1.0\","
        "%s"
        "\"ts_ms\":1710000000000,"
        "\"seq\":111,"
        "\"device_type\":\"%s\","
        "\"device_model\":\"%s\","
        "\"signal_type\":\"%s\","
        "\"channel_id\":\"%s\","
        "\"window\":{\"start_us\":%lld,\"sample_rate_hz\":%lld,\"point_count\":%d},"
        "\"quality\":{\"code\":0,\"text\":\"ok\"},"
        "\"raw\":{\"adc_unit\":\"%s\"},"
        "\"calibration\":{\"version\":\"cal-bin\",\"k\":1.1,\"b\":0.3},"
        "\"data\":{"
        "\"layout\":\"uniform_series_binary\","
        "\"point_count\":%d,"
        "\"fields\":[{\"name\":\"%s\",\"unit\":\"%s\"}],"
        "\"value_type\":\"float32\","
        "\"byte_order\":\"little_endian\","
        "\"encoding\":\"%s\","
        "\"payload\":\"%s\""
        "}"
        "}";
    if (task_id != NULL && task_id[0] != '\0') {
        snprintf(task_id_field, sizeof(task_id_field),
            "\"task_id\":\"%s\",", task_id);
    } else {
        task_id_field[0] = '\0';
    }

    int len = snprintf(NULL, 0, template_str, device_id, msg_id, task_id_field,
        device_type, device_model, signal_type, channel_id, start_us,
        sample_rate_hz, window_point_count, raw_unit, data_point_count,
        field_name, field_unit, encoding, encoded_payload);
    char *json = (char *) malloc((size_t) len + 1);

    assert(json != NULL);
    snprintf(json, (size_t) len + 1, template_str, device_id, msg_id,
        task_id_field, device_type, device_model, signal_type, channel_id,
        start_us, sample_rate_hz, window_point_count, raw_unit,
        data_point_count, field_name, field_unit, encoding, encoded_payload);
    return json;
}

static void
test_current_message_expands_points(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chb01\","
        "\"msg_id\":\"msg-001\","
        "\"spec_ver\":\"1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":12,"
        "\"device_type\":\"current_transducer\","
        "\"device_model\":\"CHB-2KE\","
        "\"signal_type\":\"current\","
        "\"channel_id\":\"ai1\","
        "\"quality\":{\"code\":0,\"text\":\"ok\"},"
        "\"raw\":{\"adc_unit\":\"mV\"},"
        "\"calibration\":{\"version\":\"cal-1\",\"k\":1.5,\"b\":0.2},"
        "\"data\":{"
        "\"point_count\":2,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"points\":["
        "{\"ts_ms\":1000,\"values\":[12.5]},"
        "{\"ts_ms\":1001,\"values\":[12.7]}"
        "]"
        "}"
        "}";

    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 2);
    assert(strcmp(g_rows[0].stable, "weld_current_point") == 0);
    assert(g_rows[0].ts_us == 1000 * 1000LL);
    assert(g_rows[1].ts_us == 1001 * 1000LL);
    assert(g_rows[0].recv_ts_us > 0);
    assert(g_rows[0].recv_ts_us == g_rows[1].recv_ts_us);
    assert(g_rows[0].has_current == 1);
    assert(g_rows[1].has_current == 1);
    assert(g_rows[0].current == 12.5);
    assert(g_rows[1].current == 12.7);
    assert(strcmp(g_rows[0].raw_adc_unit, "mV") == 0);
    assert(strcmp(g_rows[0].cal_version, "cal-1") == 0);
    assert(g_rows[0].has_cal_k == 1);
    assert(g_rows[0].has_cal_b == 1);
    assert(g_rows[0].cal_k == 1.5);
    assert(g_rows[0].cal_b == 0.2);
    assert(g_rows[0].has_quality_code == 1);
    assert(g_rows[0].quality_code == 0);
}

static void
test_uniform_current_message_base64_expands_points(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const float values[] = { 12.5f, 12.75f, 13.0f };
    char *encoded_payload = encode_uniform_payload_base64(values, 3);
    char *payload = make_uniform_payload_json("chb01", "msg-uniform-current",
        "current", "ai5", "base64", encoded_payload, 3, 3, 4000000, 1000);
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 3);
    assert(strcmp(g_rows[0].stable, "weld_current_point") == 0);
    assert(g_rows[0].ts_us == 4000000);
    assert(g_rows[1].ts_us == 4001000);
    assert(g_rows[2].ts_us == 4002000);
    assert(g_rows[0].has_current == 1);
    assert(g_rows[1].has_current == 1);
    assert(g_rows[2].has_current == 1);
    assert(fabs(g_rows[0].current - 12.5) < 1e-6);
    assert(fabs(g_rows[1].current - 12.75) < 1e-6);
    assert(fabs(g_rows[2].current - 13.0) < 1e-6);
    assert(strcmp(g_rows[0].raw_adc_unit, "mV") == 0);
    assert(strcmp(g_rows[0].cal_version, "cal-bin") == 0);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_voltage_message_zstd_expands_points(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv01";
    const float values[] = { 24.1f, 24.2f, 24.3f };
    char *encoded_payload = encode_uniform_payload_zstd_base64(values, 3);
    char *payload = make_uniform_payload_json("chv01", "msg-uniform-voltage",
        "voltage", "ai6", "zstd_base64_f32_le", encoded_payload, 3, 3,
        8000000, 2000);
    rule_taos rule = make_rule("weld_voltage_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 3);
    assert(strcmp(g_rows[0].stable, "weld_voltage_point") == 0);
    assert(g_rows[0].ts_us == 8000000);
    assert(g_rows[1].ts_us == 8000500);
    assert(g_rows[2].ts_us == 8001000);
    assert(g_rows[0].has_voltage == 1);
    assert(g_rows[1].has_voltage == 1);
    assert(g_rows[2].has_voltage == 1);
    assert(fabs(g_rows[0].voltage - 24.1) < 1e-5);
    assert(fabs(g_rows[1].voltage - 24.2) < 1e-5);
    assert(fabs(g_rows[2].voltage - 24.3) < 1e-5);
    assert(strcmp(g_rows[0].cal_version, "cal-bin") == 0);

    free(payload);
    free(encoded_payload);
}

static void
test_current_message_maps_task_id_when_present(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const char *data_block =
        "{"
        "\"point_count\":1,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"points\":["
        "{\"ts_ms\":1100,\"values\":[12.9]}"
        "]"
        "}";
    char *payload = make_telemetry_payload_with_data_block("chb01",
        "msg-task-id-current", "1.0", "current_transducer", "CHB-2KE",
        "current", "ai15", "mV", data_block, "110120119");
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 1);
    assert(g_rows[0].task_id != NULL);
    assert(strcmp(g_rows[0].task_id, "110120119") == 0);

    free(payload);
}

static void
test_current_message_keeps_task_id_null_when_missing(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const char *data_block =
        "{"
        "\"point_count\":1,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"points\":["
        "{\"ts_ms\":1200,\"values\":[13.1]}"
        "]"
        "}";
    char *payload = make_telemetry_payload_with_data_block("chb01",
        "msg-task-id-missing", "1.0", "current_transducer", "CHB-2KE",
        "current", "ai16", "mV", data_block);
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 1);
    assert(g_rows[0].task_id == NULL);

    free(payload);
}

static void
test_uniform_current_message_maps_task_id_when_present(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const float values[] = { 14.5f, 14.75f };
    char *encoded_payload = encode_uniform_payload_base64(values, 2);
    char *payload = make_uniform_payload_json("chb01",
        "msg-uniform-task-id", "current", "ai17", "base64",
        encoded_payload, 2, 2, 17000000, 1000, "110120119");
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 2);
    assert(g_rows[0].task_id != NULL);
    assert(strcmp(g_rows[0].task_id, "110120119") == 0);
    assert(g_rows[1].task_id != NULL);
    assert(strcmp(g_rows[1].task_id, "110120119") == 0);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_current_message_accepts_root_level_window(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb03";
    const float values[] = { 15.5f, 15.75f };
    char *encoded_payload = encode_uniform_payload_base64(values, 2);
    char *payload = make_uniform_payload_json_root_window("chb03",
        "msg-uniform-root-window", "current", "ai2", "base64",
        encoded_payload, 2, 2, 27000000, 10);
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 2);
    assert(strcmp(g_rows[0].stable, "weld_current_point") == 0);
    assert(g_rows[0].ts_us == 27000000);
    assert(g_rows[1].ts_us == 27100000);
    assert(fabs(g_rows[0].current - 15.5) < 1e-6);
    assert(fabs(g_rows[1].current - 15.75) < 1e-6);
    assert(g_rows[0].task_id == NULL);
    assert(g_rows[1].task_id == NULL);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_current_root_window_maps_task_id_when_present(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb03";
    const float values[] = { 18.25f };
    char *encoded_payload = encode_uniform_payload_base64(values, 1);
    char *payload = make_uniform_payload_json_root_window("chb03",
        "msg-uniform-root-window-task-id", "current", "ai2", "base64",
        encoded_payload, 1, 1, 29000000, 10, "110120119");
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 1);
    assert(g_rows[0].task_id != NULL);
    assert(strcmp(g_rows[0].task_id, "110120119") == 0);
    assert(g_rows[0].ts_us == 29000000);
    assert(fabs(g_rows[0].current - 18.25) < 1e-6);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_voltage_message_accepts_root_level_window(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv04";
    const float values[] = { 205.5f, 205.75f };
    char *encoded_payload = encode_uniform_payload_base64(values, 2);
    char *payload = make_uniform_payload_json_root_window("chv04",
        "msg-uniform-root-window-voltage", "voltage", "ai3", "base64",
        encoded_payload, 2, 2, 31000000, 10);
    rule_taos rule = make_rule("weld_voltage_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 2);
    assert(strcmp(g_rows[0].stable, "weld_voltage_point") == 0);
    assert(g_rows[0].ts_us == 31000000);
    assert(g_rows[1].ts_us == 31100000);
    assert(fabs(g_rows[0].voltage - 205.5) < 1e-6);
    assert(fabs(g_rows[1].voltage - 205.75) < 1e-6);
    assert(g_rows[0].task_id == NULL);
    assert(g_rows[1].task_id == NULL);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_current_root_window_preserves_metadata_fields(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb03";
    const float values[] = { 166.25f };
    char *encoded_payload = encode_uniform_payload_base64(values, 1);
    const char *template_str =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chb03\","
        "\"msg_id\":\"msg-uniform-root-window-meta\","
        "\"spec_ver\":\"1.1.0\","
        "\"task_id\":\"110120119\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":111,"
        "\"device_type\":\"current_transducer\","
        "\"device_model\":\"CHB-200-5V\","
        "\"signal_type\":\"current\","
        "\"channel_id\":\"ai2\","
        "\"source\":{\"bus\":\"usb_daq\",\"port\":\"daq0\",\"protocol\":\"analog_input\"},"
        "\"collect\":{\"period_ms\":100,\"timeout_ms\":50,\"retries\":0},"
        "\"window\":{\"start_us\":32000000,\"sample_rate_hz\":10,\"point_count\":1},"
        "\"quality\":{\"code\":0,\"text\":\"ok\"},"
        "\"raw\":{\"adc_unit\":\"V\"},"
        "\"calibration\":{\"version\":\"cal-20260326-01\",\"k\":5000.0,\"b\":0.0},"
        "\"data\":{"
        "\"layout\":\"uniform_series_binary\","
        "\"point_count\":1,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"value_type\":\"float32\","
        "\"byte_order\":\"little_endian\","
        "\"encoding\":\"base64\","
        "\"payload\":\"%s\""
        "}"
        "}";
    int payload_len = snprintf(NULL, 0, template_str, encoded_payload);
    char *payload = (char *) malloc((size_t) payload_len + 1);
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet;

    assert(payload != NULL);
    snprintf(payload, (size_t) payload_len + 1, template_str, encoded_payload);
    packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 1);
    assert(strcmp(g_rows[0].raw_adc_unit, "V") == 0);
    assert(strcmp(g_rows[0].cal_version, "cal-20260326-01") == 0);
    assert(g_rows[0].has_cal_k == 1);
    assert(g_rows[0].cal_k == 5000.0);
    assert(g_rows[0].has_cal_b == 1);
    assert(g_rows[0].cal_b == 0.0);
    assert(strcmp(g_rows[0].source_bus, "usb_daq") == 0);
    assert(strcmp(g_rows[0].source_port, "daq0") == 0);
    assert(strcmp(g_rows[0].source_protocol, "analog_input") == 0);
    assert(g_rows[0].has_collect_period_ms == 1);
    assert(g_rows[0].collect_period_ms == 100);
    assert(g_rows[0].has_collect_timeout_ms == 1);
    assert(g_rows[0].collect_timeout_ms == 50);
    assert(g_rows[0].has_collect_retries == 1);
    assert(g_rows[0].collect_retries == 0);
    assert(g_rows[0].task_id != NULL);
    assert(strcmp(g_rows[0].task_id, "110120119") == 0);
    assert(fabs(g_rows[0].current - 166.25) < 1e-6);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_voltage_root_window_zstd_maps_task_id_when_present(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv04";
    const float values[] = { 208.5f, 208.75f };
    char *encoded_payload = encode_uniform_payload_zstd_base64(values, 2);
    char *payload = make_uniform_payload_json_root_window("chv04",
        "msg-uniform-root-window-voltage-task-id", "voltage", "ai3",
        "zstd_base64_f32_le", encoded_payload, 2, 2, 32500000, 20,
        "110120119");
    rule_taos rule = make_rule("weld_voltage_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 2);
    assert(g_rows[0].task_id != NULL);
    assert(strcmp(g_rows[0].task_id, "110120119") == 0);
    assert(g_rows[1].task_id != NULL);
    assert(strcmp(g_rows[1].task_id, "110120119") == 0);
    assert(g_rows[0].ts_us == 32500000);
    assert(g_rows[1].ts_us == 32550000);
    assert(fabs(g_rows[0].voltage - 208.5) < 1e-5);
    assert(fabs(g_rows[1].voltage - 208.75) < 1e-5);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_root_window_point_count_mismatch_is_rejected(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb03";
    const float values[] = { 16.1f, 16.2f };
    char *encoded_payload = encode_uniform_payload_base64(values, 2);
    const char *template_str =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chb03\","
        "\"msg_id\":\"msg-uniform-root-window-count-mismatch\","
        "\"spec_ver\":\"1.1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":111,"
        "\"device_type\":\"current_transducer\","
        "\"device_model\":\"CHB-2KE\","
        "\"signal_type\":\"current\","
        "\"channel_id\":\"ai2\","
        "\"window\":{\"start_us\":33000000,\"sample_rate_hz\":10,\"point_count\":1},"
        "\"quality\":{\"code\":0,\"text\":\"ok\"},"
        "\"raw\":{\"adc_unit\":\"mV\"},"
        "\"calibration\":{\"version\":\"cal-bin\",\"k\":1.1,\"b\":0.3},"
        "\"data\":{"
        "\"layout\":\"uniform_series_binary\","
        "\"point_count\":2,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"value_type\":\"float32\","
        "\"byte_order\":\"little_endian\","
        "\"encoding\":\"base64\","
        "\"payload\":\"%s\""
        "}"
        "}";
    int payload_len = snprintf(NULL, 0, template_str, encoded_payload);
    char *payload = (char *) malloc((size_t) payload_len + 1);
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet;

    assert(payload != NULL);
    snprintf(payload, (size_t) payload_len + 1, template_str, encoded_payload);
    packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_root_window_missing_sample_rate_is_rejected(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv04";
    const float values[] = { 206.1f };
    char *encoded_payload = encode_uniform_payload_base64(values, 1);
    const char *template_str =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chv04\","
        "\"msg_id\":\"msg-uniform-root-window-missing-rate\","
        "\"spec_ver\":\"1.1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":111,"
        "\"device_type\":\"voltage_transducer\","
        "\"device_model\":\"CHV-2KE\","
        "\"signal_type\":\"voltage\","
        "\"channel_id\":\"ai3\","
        "\"window\":{\"start_us\":34000000,\"point_count\":1},"
        "\"quality\":{\"code\":0,\"text\":\"ok\"},"
        "\"raw\":{\"adc_unit\":\"V\"},"
        "\"calibration\":{\"version\":\"cal-bin\",\"k\":1.1,\"b\":0.3},"
        "\"data\":{"
        "\"layout\":\"uniform_series_binary\","
        "\"point_count\":1,"
        "\"fields\":[{\"name\":\"voltage\",\"unit\":\"V\"}],"
        "\"value_type\":\"float32\","
        "\"byte_order\":\"little_endian\","
        "\"encoding\":\"base64\","
        "\"payload\":\"%s\""
        "}"
        "}";
    int payload_len = snprintf(NULL, 0, template_str, encoded_payload);
    char *payload = (char *) malloc((size_t) payload_len + 1);
    rule_taos rule = make_rule("weld_voltage_point");
    pub_packet_struct packet;

    assert(payload != NULL);
    snprintf(payload, (size_t) payload_len + 1, template_str, encoded_payload);
    packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_data_window_takes_precedence_over_root_window(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv04";
    const float values[] = { 207.1f, 207.2f };
    char *encoded_payload = encode_uniform_payload_base64(values, 2);
    const char *template_str =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chv04\","
        "\"msg_id\":\"msg-uniform-window-priority\","
        "\"spec_ver\":\"1.1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":111,"
        "\"device_type\":\"voltage_transducer\","
        "\"device_model\":\"CHV-2KE\","
        "\"signal_type\":\"voltage\","
        "\"channel_id\":\"ai3\","
        "\"window\":{\"start_us\":35000000,\"sample_rate_hz\":10,\"point_count\":2},"
        "\"quality\":{\"code\":0,\"text\":\"ok\"},"
        "\"raw\":{\"adc_unit\":\"V\"},"
        "\"calibration\":{\"version\":\"cal-bin\",\"k\":1.1,\"b\":0.3},"
        "\"data\":{"
        "\"layout\":\"uniform_series_binary\","
        "\"point_count\":2,"
        "\"fields\":[{\"name\":\"voltage\",\"unit\":\"V\"}],"
        "\"value_type\":\"float32\","
        "\"byte_order\":\"little_endian\","
        "\"encoding\":\"base64\","
        "\"payload\":\"%s\","
        "\"window\":{\"start_us\":36000000,\"sample_rate_hz\":20,\"point_count\":2}"
        "}"
        "}";
    int payload_len = snprintf(NULL, 0, template_str, encoded_payload);
    char *payload = (char *) malloc((size_t) payload_len + 1);
    rule_taos rule = make_rule("weld_voltage_point");
    pub_packet_struct packet;

    assert(payload != NULL);
    snprintf(payload, (size_t) payload_len + 1, template_str, encoded_payload);
    packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 2);
    assert(g_rows[0].ts_us == 36000000);
    assert(g_rows[1].ts_us == 36050000);
    assert(fabs(g_rows[0].voltage - 207.1) < 1e-5);
    assert(fabs(g_rows[1].voltage - 207.2) < 1e-5);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_root_window_reconstructs_fractional_intervals(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb03";
    const float values[] = { 101.0f, 102.0f, 103.0f };
    char *encoded_payload = encode_uniform_payload_base64(values, 3);
    char *payload = make_uniform_payload_json_root_window("chb03",
        "msg-uniform-root-window-fractional", "current", "ai2", "base64",
        encoded_payload, 3, 3, 37000000, 3);
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 3);
    assert(g_rows[0].ts_us == 37000000);
    assert(g_rows[1].ts_us == 37333333);
    assert(g_rows[2].ts_us == 37666666);
    assert(fabs(g_rows[0].current - 101.0) < 1e-6);
    assert(fabs(g_rows[1].current - 102.0) < 1e-6);
    assert(fabs(g_rows[2].current - 103.0) < 1e-6);

    free(payload);
    free(encoded_payload);
}

static void
test_duplicate_ts_ms_rejects_whole_message(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/env/th01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"th01\","
        "\"msg_id\":\"msg-dup\","
        "\"spec_ver\":\"1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":13,"
        "\"device_type\":\"temp_humidity_transmitter\","
        "\"device_model\":\"TH-485\","
        "\"signal_type\":\"environment\","
        "\"quality\":{\"code\":0},"
        "\"data\":{"
        "\"point_count\":2,"
        "\"fields\":["
        "{\"name\":\"temperature\",\"unit\":\"C\"},"
        "{\"name\":\"humidity\",\"unit\":\"pct_rh\"}"
        "],"
        "\"points\":["
        "{\"ts_ms\":2000,\"values\":[25.1,60.2]},"
        "{\"ts_ms\":2000,\"values\":[25.2,60.3]}"
        "]"
        "}"
        "}";

    rule_taos rule = make_rule("weld_env_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);
}

static void
test_duplicate_ts_us_rejects_whole_message(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chv01\","
        "\"msg_id\":\"msg-dup-us\","
        "\"spec_ver\":\"1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":14,"
        "\"device_type\":\"voltage_transducer\","
        "\"device_model\":\"CHV-2KE\","
        "\"signal_type\":\"voltage\","
        "\"quality\":{\"code\":0},"
        "\"data\":{"
        "\"point_count\":2,"
        "\"fields\":[{\"name\":\"voltage\",\"unit\":\"V\"}],"
        "\"points\":["
        "{\"ts_us\":3000001,\"values\":[24.1]},"
        "{\"ts_us\":3000001,\"values\":[24.2]}"
        "]"
        "}"
        "}";

    rule_taos rule = make_rule("weld_voltage_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);
}

static void
test_enqueue_failure_rejects_whole_message(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chb01\","
        "\"msg_id\":\"msg-queue-full\","
        "\"spec_ver\":\"1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":21,"
        "\"device_type\":\"current_transducer\","
        "\"device_model\":\"CHB-2KE\","
        "\"signal_type\":\"current\","
        "\"quality\":{\"code\":0},"
        "\"data\":{"
        "\"point_count\":2,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"points\":["
        "{\"ts_ms\":3000,\"values\":[10.1]},"
        "{\"ts_ms\":3001,\"values\":[10.2]}"
        "]"
        "}"
        "}";

    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    g_enqueue_fail = 1;
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);
}

static void
test_capacity_precheck_rejects_before_enqueue(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chb01\","
        "\"msg_id\":\"msg-overload\","
        "\"spec_ver\":\"1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":31,"
        "\"device_type\":\"current_transducer\","
        "\"device_model\":\"CHB-2KE\","
        "\"signal_type\":\"current\","
        "\"quality\":{\"code\":0},"
        "\"data\":{"
        "\"point_count\":2,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"points\":["
        "{\"ts_ms\":4000,\"values\":[11.1]},"
        "{\"ts_ms\":4001,\"values\":[11.2]}"
        "]"
        "}"
        "}";

    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    g_can_accept = 0;
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);
}

static void
test_handle_publish_raw_accepts_ts_us_points(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chb01\","
        "\"msg_id\":\"msg-raw-ts-us\","
        "\"spec_ver\":\"1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":41,"
        "\"device_type\":\"current_transducer\","
        "\"device_model\":\"CHB-2KE\","
        "\"signal_type\":\"current\","
        "\"channel_id\":\"ai2\","
        "\"quality\":{\"code\":0,\"text\":\"ok\"},"
        "\"data\":{"
        "\"point_count\":1,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"points\":["
        "{\"ts_us\":4000001,\"values\":[13.5]}"
        "]"
        "}"
        "}";

    rule_taos rule = make_rule("weld_current_point");

    reset_rows();
    assert(weld_telemetry_handle_publish_raw(&rule, topic, 1, 9,
               (const uint8_t *) payload, (uint32_t) strlen(payload)) == 0);
    assert(g_enqueue_count == 1);
    assert(strcmp(g_rows[0].stable, "weld_current_point") == 0);
    assert(g_rows[0].ts_us == 4000001);
    assert(g_rows[0].recv_ts_us > 0);
    assert(g_rows[0].packet_id == 9);
    assert(g_rows[0].qos == 1);
    assert(strcmp(g_rows[0].channel_id, "ai2") == 0);
    assert(g_rows[0].current == 13.5);
}

static void
test_environment_message_maps_temperature_and_humidity(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/env/th01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"th01\","
        "\"msg_id\":\"msg-env-001\","
        "\"spec_ver\":\"1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":51,"
        "\"device_type\":\"temp_humidity_transmitter\","
        "\"device_model\":\"TH-485\","
        "\"signal_type\":\"environment\","
        "\"quality\":{\"code\":0,\"text\":\"ok\"},"
        "\"source\":{\"bus\":\"rs485\",\"port\":\"ttyS1\",\"protocol\":\"modbus\"},"
        "\"collect\":{\"period_ms\":1000,\"timeout_ms\":200,\"retries\":2},"
        "\"data\":{"
        "\"point_count\":1,"
        "\"fields\":["
        "{\"name\":\"temperature\",\"unit\":\"C\"},"
        "{\"name\":\"humidity\",\"unit\":\"pct_rh\"}"
        "],"
        "\"points\":["
        "{\"ts_ms\":5000,\"values\":[25.1,60.2]}"
        "]"
        "}"
        "}";

    rule_taos rule = make_rule("weld_env_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 1);
    assert(strcmp(g_rows[0].stable, "weld_env_point") == 0);
    assert(g_rows[0].ts_us == 5000 * 1000LL);
    assert(g_rows[0].has_temperature == 1);
    assert(g_rows[0].temperature == 25.1);
    assert(g_rows[0].has_humidity == 1);
    assert(g_rows[0].humidity == 60.2);
    assert(strcmp(g_rows[0].source_bus, "rs485") == 0);
    assert(strcmp(g_rows[0].source_port, "ttyS1") == 0);
    assert(strcmp(g_rows[0].source_protocol, "modbus") == 0);
    assert(g_rows[0].has_collect_period_ms == 1);
    assert(g_rows[0].collect_period_ms == 1000);
    assert(g_rows[0].has_collect_timeout_ms == 1);
    assert(g_rows[0].collect_timeout_ms == 200);
    assert(g_rows[0].has_collect_retries == 1);
    assert(g_rows[0].collect_retries == 2);
}

static void
test_voltage_message_maps_voltage_and_calibration(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chv01\","
        "\"msg_id\":\"msg-voltage-001\","
        "\"spec_ver\":\"1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":61,"
        "\"device_type\":\"voltage_transducer\","
        "\"device_model\":\"CHV-2KE\","
        "\"signal_type\":\"voltage\","
        "\"channel_id\":\"ai3\","
        "\"quality\":{\"code\":0,\"text\":\"ok\"},"
        "\"raw\":{\"adc_unit\":\"mV\"},"
        "\"calibration\":{\"version\":\"cal-v\",\"k\":2.5,\"b\":0.4},"
        "\"data\":{"
        "\"point_count\":1,"
        "\"fields\":[{\"name\":\"voltage\",\"unit\":\"V\"}],"
        "\"points\":["
        "{\"ts_us\":6000001,\"values\":[24.1]}"
        "]"
        "}"
        "}";

    rule_taos rule = make_rule("weld_voltage_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 1);
    assert(strcmp(g_rows[0].stable, "weld_voltage_point") == 0);
    assert(g_rows[0].ts_us == 6000001);
    assert(g_rows[0].has_voltage == 1);
    assert(g_rows[0].voltage == 24.1);
    assert(strcmp(g_rows[0].raw_adc_unit, "mV") == 0);
    assert(strcmp(g_rows[0].cal_version, "cal-v") == 0);
    assert(g_rows[0].has_cal_k == 1);
    assert(g_rows[0].cal_k == 2.5);
    assert(g_rows[0].has_cal_b == 1);
    assert(g_rows[0].cal_b == 0.4);
}

static void
test_invalid_point_timestamp_shape_rejects_message(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chb01\","
        "\"msg_id\":\"msg-bad-ts-shape\","
        "\"spec_ver\":\"1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":71,"
        "\"device_type\":\"current_transducer\","
        "\"device_model\":\"CHB-2KE\","
        "\"signal_type\":\"current\","
        "\"quality\":{\"code\":0},"
        "\"data\":{"
        "\"point_count\":1,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"points\":["
        "{\"ts_ms\":7000,\"ts_us\":7000001,\"values\":[11.1]}"
        "]"
        "}"
        "}";

    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);
}

static void
test_gas_flow_message_maps_instant_and_total_flow(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/flow/fm01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"fm01\","
        "\"msg_id\":\"msg-flow-001\","
        "\"spec_ver\":\"1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":81,"
        "\"device_type\":\"gas_flow_meter\","
        "\"device_model\":\"FM-100\","
        "\"signal_type\":\"gas_flow\","
        "\"quality\":{\"code\":0,\"text\":\"ok\"},"
        "\"data\":{"
        "\"point_count\":1,"
        "\"fields\":["
        "{\"name\":\"instant_flow\",\"unit\":\"L/min\"},"
        "{\"name\":\"total_flow\",\"unit\":\"L\"}"
        "],"
        "\"points\":["
        "{\"ts_ms\":8000,\"values\":[15.5,101.2]}"
        "]"
        "}"
        "}";

    rule_taos rule = make_rule("weld_flow_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 1);
    assert(strcmp(g_rows[0].stable, "weld_flow_point") == 0);
    assert(g_rows[0].has_instant_flow == 1);
    assert(g_rows[0].instant_flow == 15.5);
    assert(g_rows[0].has_total_flow == 1);
    assert(g_rows[0].total_flow == 101.2);
}

static void
test_signal_type_and_target_table_mismatch_is_rejected(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chb01\","
        "\"msg_id\":\"msg-table-mismatch\","
        "\"spec_ver\":\"1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":91,"
        "\"device_type\":\"current_transducer\","
        "\"device_model\":\"CHB-2KE\","
        "\"signal_type\":\"current\","
        "\"quality\":{\"code\":0},"
        "\"data\":{"
        "\"point_count\":1,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"points\":["
        "{\"ts_ms\":9000,\"values\":[10.1]}"
        "]"
        "}"
        "}";

    rule_taos rule = make_rule("weld_voltage_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);
}

static void
test_point_count_mismatch_is_rejected(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chb01\","
        "\"msg_id\":\"msg-point-count-mismatch\","
        "\"spec_ver\":\"1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":101,"
        "\"device_type\":\"current_transducer\","
        "\"device_model\":\"CHB-2KE\","
        "\"signal_type\":\"current\","
        "\"quality\":{\"code\":0},"
        "\"data\":{"
        "\"point_count\":2,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"points\":["
        "{\"ts_ms\":10000,\"values\":[10.1]}"
        "]"
        "}"
        "}";

    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);
}

static void
test_uniform_point_count_mismatch_is_rejected(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const float values[] = { 10.1f, 10.2f, 10.3f };
    char *encoded_payload = encode_uniform_payload_base64(values, 3);
    char *payload = make_uniform_payload_json("chb01",
        "msg-uniform-point-count-mismatch", "current", "ai7", "base64",
        encoded_payload, 3, 2, 9000000, 1000);
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_unsupported_encoding_is_rejected(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv01";
    const float values[] = { 22.1f, 22.2f };
    char *encoded_payload = encode_uniform_payload_base64(values, 2);
    char *payload = make_uniform_payload_json("chv01",
        "msg-uniform-unsupported-encoding", "voltage", "ai8",
        "snappy_base64_f32_le", encoded_payload, 2, 2, 9100000, 1000);
    rule_taos rule = make_rule("weld_voltage_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_missing_window_is_rejected(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const float values[] = { 19.1f };
    char *encoded_payload = encode_uniform_payload_base64(values, 1);
    const char *data_template =
        "{"
        "\"layout\":\"uniform_series_binary\","
        "\"point_count\":1,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"value_type\":\"float32\","
        "\"byte_order\":\"little_endian\","
        "\"encoding\":\"base64\","
        "\"payload\":\"%s\""
        "}";
    int data_len = snprintf(NULL, 0, data_template, encoded_payload);
    char *data_block = (char *) malloc((size_t) data_len + 1);
    char *payload = NULL;
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet;

    assert(data_block != NULL);
    snprintf(data_block, (size_t) data_len + 1, data_template, encoded_payload);
    payload = make_telemetry_payload_with_data_block("chb01",
        "msg-uniform-missing-window", "1.1.0", "current_transducer",
        "CHB-2KE", "current", "ai9", "mV", data_block);
    packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);

    free(payload);
    free(data_block);
    free(encoded_payload);
}

static void
test_uniform_field_count_greater_than_one_is_rejected(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const float values[] = { 18.1f, 18.2f };
    char *encoded_payload = encode_uniform_payload_base64(values, 2);
    const char *data_template =
        "{"
        "\"layout\":\"uniform_series_binary\","
        "\"point_count\":2,"
        "\"fields\":["
        "{\"name\":\"current\",\"unit\":\"A\"},"
        "{\"name\":\"voltage\",\"unit\":\"V\"}"
        "],"
        "\"value_type\":\"float32\","
        "\"byte_order\":\"little_endian\","
        "\"encoding\":\"base64\","
        "\"payload\":\"%s\","
        "\"window\":{\"start_us\":12000000,\"sample_rate_hz\":1000,\"point_count\":2}"
        "}";
    int data_len = snprintf(NULL, 0, data_template, encoded_payload);
    char *data_block = (char *) malloc((size_t) data_len + 1);
    char *payload = NULL;
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet;

    assert(data_block != NULL);
    snprintf(data_block, (size_t) data_len + 1, data_template, encoded_payload);
    payload = make_telemetry_payload_with_data_block("chb01",
        "msg-uniform-too-many-fields", "1.1.0", "current_transducer",
        "CHB-2KE", "current", "ai10", "mV", data_block);
    packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);

    free(payload);
    free(data_block);
    free(encoded_payload);
}

static void
test_uniform_field_name_mismatch_is_rejected(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const float values[] = { 17.1f, 17.2f };
    char *encoded_payload = encode_uniform_payload_base64(values, 2);
    const char *data_template =
        "{"
        "\"layout\":\"uniform_series_binary\","
        "\"point_count\":2,"
        "\"fields\":[{\"name\":\"voltage\",\"unit\":\"V\"}],"
        "\"value_type\":\"float32\","
        "\"byte_order\":\"little_endian\","
        "\"encoding\":\"base64\","
        "\"payload\":\"%s\","
        "\"window\":{\"start_us\":13000000,\"sample_rate_hz\":1000,\"point_count\":2}"
        "}";
    int data_len = snprintf(NULL, 0, data_template, encoded_payload);
    char *data_block = (char *) malloc((size_t) data_len + 1);
    char *payload = NULL;
    rule_taos rule = make_rule("weld_current_point");
    pub_packet_struct packet;

    assert(data_block != NULL);
    snprintf(data_block, (size_t) data_len + 1, data_template, encoded_payload);
    payload = make_telemetry_payload_with_data_block("chb01",
        "msg-uniform-field-mismatch", "1.1.0", "current_transducer",
        "CHB-2KE", "current", "ai11", "mV", data_block);
    packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);

    free(payload);
    free(data_block);
    free(encoded_payload);
}

static void
test_uniform_base64_payload_length_mismatch_is_rejected(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv01";
    const float values[] = { 21.1f, 21.2f };
    char *encoded_payload = encode_uniform_payload_base64(values, 2);
    char *payload = make_uniform_payload_json("chv01",
        "msg-uniform-length-mismatch", "voltage", "ai12", "base64",
        encoded_payload, 3, 3, 14000000, 1000);
    rule_taos rule = make_rule("weld_voltage_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_zstd_decode_failure_is_rejected(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv01";
    const float values[] = { 23.1f, 23.2f };
    char *encoded_payload = encode_uniform_payload_base64(values, 2);
    char *payload = make_uniform_payload_json("chv01",
        "msg-uniform-zstd-bad-payload", "voltage", "ai13",
        "zstd_base64_f32_le", encoded_payload, 2, 2, 15000000, 1000);
    rule_taos rule = make_rule("weld_voltage_point");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);

    free(payload);
    free(encoded_payload);
}

static void
test_uniform_environment_signal_type_is_rejected(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/env/th01";
    const float values[] = { 26.1f };
    char *encoded_payload = encode_uniform_payload_base64(values, 1);
    const char *data_template =
        "{"
        "\"layout\":\"uniform_series_binary\","
        "\"point_count\":1,"
        "\"fields\":[{\"name\":\"temperature\",\"unit\":\"C\"}],"
        "\"value_type\":\"float32\","
        "\"byte_order\":\"little_endian\","
        "\"encoding\":\"base64\","
        "\"payload\":\"%s\","
        "\"window\":{\"start_us\":16000000,\"sample_rate_hz\":1000,\"point_count\":1}"
        "}";
    int data_len = snprintf(NULL, 0, data_template, encoded_payload);
    char *data_block = (char *) malloc((size_t) data_len + 1);
    char *payload = NULL;
    rule_taos rule = make_rule("weld_env_point");
    pub_packet_struct packet;

    assert(data_block != NULL);
    snprintf(data_block, (size_t) data_len + 1, data_template, encoded_payload);
    payload = make_telemetry_payload_with_data_block("th01",
        "msg-uniform-env", "1.1.0", "temp_humidity_transmitter", "TH-485",
        "environment", "ai14", "mV", data_block);
    packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);

    free(payload);
    free(data_block);
    free(encoded_payload);
}

static void
test_handle_publish_raw_rejects_invalid_json(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const uint8_t payload[] = "{invalid-json";
    rule_taos rule = make_rule("weld_current_point");

    reset_rows();
    assert(weld_telemetry_handle_publish_raw(
               &rule, topic, 1, 19, payload, sizeof(payload) - 1) != 0);
    assert(g_enqueue_count == 0);
}

static void
test_raw_current_uniform_message_is_stored_as_single_row(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const float values[] = { 1.25f, 2.5f, 3.75f };
    char *encoded_payload = encode_uniform_payload_base64(values, 3);
    char *payload = make_uniform_payload_json("chb01", "msg-raw-current",
        "current", "ai21", "base64", encoded_payload, 3, 3, 21000000LL,
        5000LL, "110120119");
    rule_taos rule = make_rule("weld_current_raw");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 1);
    assert(strcmp(g_rows[0].stable, "weld_current_raw") == 0);
    assert(g_rows[0].has_current == 0);
    assert(g_rows[0].has_voltage == 0);
    assert(g_rows[0].ts_us == 21000000LL);
    assert(g_rows[0].has_window_start_us == 1);
    assert(g_rows[0].window_start_us == 21000000LL);
    assert(g_rows[0].has_sample_rate_hz == 1);
    assert(g_rows[0].sample_rate_hz == 5000);
    assert(g_rows[0].has_point_count == 1);
    assert(g_rows[0].point_count == 3);
    assert(strcmp(g_rows[0].encoding, "base64") == 0);
    assert(strcmp(g_rows[0].payload, encoded_payload) == 0);
    assert(strcmp(g_rows[0].task_id, "110120119") == 0);
    assert(strcmp(g_rows[0].raw_adc_unit, "mV") == 0);

    free(payload);
    free(encoded_payload);
}

static void
test_raw_voltage_uniform_message_accepts_root_window(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv01";
    const float values[] = { 10.0f, 20.0f };
    char *encoded_payload = encode_uniform_payload_zstd_base64(values, 2);
    char *payload = make_uniform_payload_json_root_window("chv01",
        "msg-raw-voltage", "voltage", "ai22", "zstd_base64_f32_le",
        encoded_payload, 2, 2, 22000000LL, 10000LL, "110120120");
    rule_taos rule = make_rule("weld_voltage_raw");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) == 0);
    assert(g_enqueue_count == 1);
    assert(strcmp(g_rows[0].stable, "weld_voltage_raw") == 0);
    assert(g_rows[0].ts_us == 22000000LL);
    assert(g_rows[0].window_start_us == 22000000LL);
    assert(g_rows[0].sample_rate_hz == 10000);
    assert(g_rows[0].point_count == 2);
    assert(strcmp(g_rows[0].encoding, "zstd_base64_f32_le") == 0);
    assert(strcmp(g_rows[0].payload, encoded_payload) == 0);
    assert(strcmp(g_rows[0].task_id, "110120120") == 0);

    free(payload);
    free(encoded_payload);
}

static void
test_raw_current_rejects_points_layout(void)
{
    const char *topic =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    const char *payload =
        "{"
        "\"msg_class\":\"telemetry\","
        "\"gateway_id\":\"gw3568_01\","
        "\"device_id\":\"chb01\","
        "\"msg_id\":\"msg-raw-points\","
        "\"spec_ver\":\"1.1.0\","
        "\"ts_ms\":1710000000000,"
        "\"seq\":111,"
        "\"device_type\":\"current_transducer\","
        "\"device_model\":\"CHB-2KE\","
        "\"signal_type\":\"current\","
        "\"channel_id\":\"ai1\","
        "\"quality\":{\"code\":0},"
        "\"data\":{\"point_count\":1,"
        "\"fields\":[{\"name\":\"current\",\"unit\":\"A\"}],"
        "\"points\":[{\"ts_us\":21000000,\"values\":[12.5]}]}"
        "}";
    rule_taos rule = make_rule("weld_current_raw");
    pub_packet_struct packet = make_packet(topic, payload);

    reset_rows();
    assert(weld_telemetry_handle_publish(&rule, &packet) != 0);
    assert(g_enqueue_count == 0);
}

int
main()
{
    test_current_message_expands_points();
    test_uniform_current_message_base64_expands_points();
    test_uniform_voltage_message_zstd_expands_points();
    test_current_message_maps_task_id_when_present();
    test_current_message_keeps_task_id_null_when_missing();
    test_uniform_current_message_maps_task_id_when_present();
    test_uniform_current_message_accepts_root_level_window();
    test_uniform_current_root_window_maps_task_id_when_present();
    test_uniform_voltage_message_accepts_root_level_window();
    test_uniform_current_root_window_preserves_metadata_fields();
    test_uniform_voltage_root_window_zstd_maps_task_id_when_present();
    test_uniform_root_window_point_count_mismatch_is_rejected();
    test_uniform_root_window_missing_sample_rate_is_rejected();
    test_uniform_data_window_takes_precedence_over_root_window();
    test_uniform_root_window_reconstructs_fractional_intervals();
    test_duplicate_ts_ms_rejects_whole_message();
    test_duplicate_ts_us_rejects_whole_message();
    test_enqueue_failure_rejects_whole_message();
    test_capacity_precheck_rejects_before_enqueue();
    test_handle_publish_raw_accepts_ts_us_points();
    test_environment_message_maps_temperature_and_humidity();
    test_voltage_message_maps_voltage_and_calibration();
    test_invalid_point_timestamp_shape_rejects_message();
    test_gas_flow_message_maps_instant_and_total_flow();
    test_signal_type_and_target_table_mismatch_is_rejected();
    test_point_count_mismatch_is_rejected();
    test_uniform_point_count_mismatch_is_rejected();
    test_uniform_unsupported_encoding_is_rejected();
    test_uniform_missing_window_is_rejected();
    test_uniform_field_count_greater_than_one_is_rejected();
    test_uniform_field_name_mismatch_is_rejected();
    test_uniform_base64_payload_length_mismatch_is_rejected();
    test_uniform_zstd_decode_failure_is_rejected();
    test_uniform_environment_signal_type_is_rejected();
    test_handle_publish_raw_rejects_invalid_json();
    test_raw_current_uniform_message_is_stored_as_single_row();
    test_raw_voltage_uniform_message_accepts_root_window();
    test_raw_current_rejects_points_layout();
    return 0;
}
