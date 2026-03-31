#include <assert.h>
#include <stdio.h>
#include <string.h>

extern "C" {
#include "../weld_telemetry.c"
}

static int          g_enqueue_count = 0;
static int          g_enqueue_fail = 0;
static int          g_can_accept = 1;
static weld_taos_row g_rows[8];
static char         g_stables[8][64];
static char         g_channel_ids[8][32];
static char         g_raw_units[8][32];
static char         g_cal_versions[8][64];

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
    memset(g_raw_units, 0, sizeof(g_raw_units));
    memset(g_cal_versions, 0, sizeof(g_cal_versions));
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

int
main()
{
    test_current_message_expands_points();
    test_duplicate_ts_ms_rejects_whole_message();
    test_enqueue_failure_rejects_whole_message();
    test_capacity_precheck_rejects_before_enqueue();
    return 0;
}
