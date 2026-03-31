#include <assert.h>
#include <string.h>

#include <string>

#include "../weld_taos_sink.cpp"

static int g_exec_calls = 0;

extern "C" int
taos_sink_valid_identifier(const char *name)
{
    return name != NULL && name[0] != '\0' ? 1 : 0;
}

static TdExecStatus
fake_tdengine_exec(const std::string &url, const std::string &auth,
    const std::string &sql, std::string *response_out)
{
    (void) url;
    (void) auth;
    if (response_out != NULL) {
        *response_out = "{\"code\":0}";
    }
    g_exec_calls++;
    if (sql.find("bad-row") != std::string::npos) {
        return TdExecStatus::NonRetryableError;
    }
    return TdExecStatus::Ok;
}

static WeldQueuedRow
make_row(const char *msg_id, int64_t ts_us)
{
    WeldQueuedRow row;
    row.stable = "weld_current_point";
    row.msg_id = msg_id;
    row.ts_us = ts_us;
    row.recv_ts_us = ts_us + 10;
    row.seq = 1;
    row.topic_name =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    row.spec_ver = "1.0";
    row.has_current = true;
    row.current = 10.5;
    row.qos = 1;
    row.packet_id = 7;
    row.version = "v1";
    row.site_id = "sz01";
    row.line_id = "line_01";
    row.station_id = "station_03";
    row.gateway_id = "gw3568_01";
    row.device_id = "chb01";
    row.device_type = "current_transducer";
    row.device_model = "CHB-2KE";
    row.metric_group = "power";
    row.signal_type = "current";
    row.channel_id = "ai1";
    return row;
}

static void
reset_state(void)
{
    g_exec_calls = 0;
    g_state.url = "http://127.0.0.1:6041/rest/sql";
    g_state.auth_header = "Basic test";
    g_state.db = "mqtt_rule";
    g_state.stables.clear();
    g_state.queue.clear();
    g_state.enqueue_msg_count.store(0);
    g_state.enqueue_row_count.store(0);
    g_state.drop_msg_count.store(0);
    g_state.drop_row_count.store(0);
    g_state.batch_ok_count.store(0);
    g_state.batch_fail_count.store(0);
    g_state.row_ok_count.store(0);
    g_state.row_fail_count.store(0);
    g_tdengine_exec_fn = fake_tdengine_exec;
}

static void
test_non_retryable_batch_failure_splits_and_keeps_good_rows(void)
{
    reset_state();

    std::vector<WeldQueuedRow> rows;
    rows.push_back(make_row("good-row", 1001));
    rows.push_back(make_row("bad-row", 1002));

    assert(flush_rows_with_retry(rows, 0, true) != 0);
    assert(g_exec_calls == 3);
    assert(g_state.batch_ok_count.load() == 1);
    assert(g_state.batch_fail_count.load() == 1);
    assert(g_state.row_ok_count.load() == 1);
    assert(g_state.row_fail_count.load() == 1);
}

int
main()
{
    test_non_retryable_batch_failure_splits_and_keeps_good_rows();
    return 0;
}
