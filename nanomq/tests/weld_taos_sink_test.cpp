#include <assert.h>
#include <string.h>

#include <string>
#include <vector>

#include "../weld_taos_sink.cpp"

static int g_exec_calls = 0;
static int g_create_db_failures_remaining = 0;
static int g_create_stable_failures_remaining = 0;
static TdExecStatus g_show_create_status = TdExecStatus::Ok;
static std::string g_show_create_response =
    "{\"code\":0,\"data\":[[\"CREATE DATABASE mqtt_rule PRECISION 'us'\"]]}";
static std::vector<std::string> g_exec_sqls;

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
    g_exec_calls++;
    g_exec_sqls.push_back(sql);

    if (sql.find("CREATE DATABASE IF NOT EXISTS") == 0 &&
        g_create_db_failures_remaining > 0) {
        g_create_db_failures_remaining--;
        return TdExecStatus::RetryableError;
    }
    if (sql.find("SHOW CREATE DATABASE") == 0) {
        if (response_out != NULL) {
            *response_out = g_show_create_response;
        }
        return g_show_create_status;
    }
    if (sql.find("CREATE STABLE IF NOT EXISTS") == 0 &&
        g_create_stable_failures_remaining > 0) {
        g_create_stable_failures_remaining--;
        return TdExecStatus::RetryableError;
    }
    if (response_out != NULL) {
        *response_out = "{\"code\":0}";
    }
    if (sql.find("bad-row") != std::string::npos) {
        return TdExecStatus::NonRetryableError;
    }
    return TdExecStatus::Ok;
}

static WeldQueuedRow
make_row(const char *msg_id, int64_t ts_us)
{
    WeldQueuedRow row;
    row.stable = "weld_current_raw";
    row.msg_id = msg_id;
    row.ts_us = ts_us;
    row.recv_ts_us = ts_us + 10;
    row.seq = 1;
    row.topic_name =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    row.spec_ver = "1.0";
    row.has_task_id = true;
    row.task_id = "110120119";
    row.has_window_start_us = true;
    row.window_start_us = ts_us;
    row.has_sample_rate_hz = true;
    row.sample_rate_hz = 20000;
    row.has_point_count = true;
    row.point_count = 2000;
    row.has_encoding = true;
    row.encoding = "zstd_base64_f32_le";
    row.has_payload = true;
    row.payload = "QUJDREVGRw==";
    row.has_raw_adc_unit = true;
    row.raw_adc_unit = "adc";
    row.has_cal_version = true;
    row.cal_version = "cal-v1";
    row.has_cal_k = true;
    row.cal_k = 1.2;
    row.has_cal_b = true;
    row.cal_b = -0.1;
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

static taos_sink_config
make_cfg(const char *stable)
{
    taos_sink_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.host = "127.0.0.1";
    cfg.port = 6041;
    cfg.username = "root";
    cfg.password = "taosdata";
    cfg.db = "mqtt_rule";
    cfg.stable = stable;
    return cfg;
}

static weld_taos_row
make_public_row(const char *msg_id, int64_t ts_us)
{
    weld_taos_row row;
    memset(&row, 0, sizeof(row));
    row.stable = "weld_current_raw";
    row.ts_us = ts_us;
    row.recv_ts_us = ts_us + 10;
    row.msg_id = msg_id;
    row.seq = 1;
    row.topic_name =
        "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01";
    row.spec_ver = "1.0";
    row.task_id = "110120119";
    row.has_window_start_us = 1;
    row.window_start_us = ts_us;
    row.has_sample_rate_hz = 1;
    row.sample_rate_hz = 20000;
    row.has_point_count = 1;
    row.point_count = 2000;
    row.encoding = "zstd_base64_f32_le";
    row.payload = "QUJDREVGRw==";
    row.raw_adc_unit = "adc";
    row.cal_version = "cal-v1";
    row.has_cal_k = 1;
    row.cal_k = 1.2;
    row.has_cal_b = 1;
    row.cal_b = -0.1;
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
    g_tdengine_exec_fn = fake_tdengine_exec;
    weld_taos_sink_stop_all();
    g_exec_calls = 0;
    g_exec_sqls.clear();
    g_create_db_failures_remaining = 0;
    g_create_stable_failures_remaining = 0;
    g_show_create_status = TdExecStatus::Ok;
    g_show_create_response =
        "{\"code\":0,\"data\":[[\"CREATE DATABASE mqtt_rule PRECISION 'us'\"]]}";
    g_state.url = "http://127.0.0.1:6041/rest/sql";
    g_state.auth_header = "Basic test";
    g_state.db = "mqtt_rule";
    g_state.running = false;
    g_state.started = false;
    g_state.batch_size = 8;
    g_state.flush_ms = 1;
    g_state.queue_max = 8;
    g_state.worker_max = 1;
    g_state.stables.clear();
    g_state.queue.clear();
    g_state.db_precision_verified = false;
    g_state.enqueue_msg_count.store(0);
    g_state.enqueue_row_count.store(0);
    g_state.drop_msg_count.store(0);
    g_state.drop_row_count.store(0);
    g_state.batch_ok_count.store(0);
    g_state.batch_fail_count.store(0);
    g_state.batch_split_count.store(0);
    g_state.non_retryable_fail_count.store(0);
    g_state.row_ok_count.store(0);
    g_state.row_fail_count.store(0);
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

static void
test_ensure_database_ready_initializes_db_precision_and_stable(void)
{
    reset_state();

    taos_sink_config cfg = make_cfg("weld_current_raw");
    assert(ensure_database_ready(&cfg) == 0);
    assert(g_state.db_precision_verified);
    assert(g_state.stables.count("weld_current_raw") == 1);
    assert(g_exec_sqls.size() == 3);
    assert(g_exec_sqls[0] ==
        "CREATE DATABASE IF NOT EXISTS mqtt_rule PRECISION 'us'");
    assert(g_exec_sqls[1] == "SHOW CREATE DATABASE mqtt_rule");
    assert(g_exec_sqls[2].find(
               "CREATE STABLE IF NOT EXISTS mqtt_rule.weld_current_raw") == 0);
    assert(g_exec_sqls[2].find("payload VARCHAR(49152)") != std::string::npos);
    assert(g_exec_sqls[2].find("signal_type VARCHAR(32)") != std::string::npos);
}

static void
test_ensure_database_ready_retries_stable_creation_and_uses_cache(void)
{
    reset_state();

    taos_sink_config cfg = make_cfg("weld_current_raw");
    g_create_stable_failures_remaining = 2;
    assert(ensure_database_ready(&cfg) == 0);
    assert(g_state.stables.count("weld_current_raw") == 1);
    assert(g_exec_calls == 5);

    g_exec_sqls.clear();
    g_exec_calls = 0;
    assert(ensure_database_ready(&cfg) == 0);
    assert(g_exec_calls == 1);
    assert(g_exec_sqls.size() == 1);
    assert(g_exec_sqls[0] ==
        "CREATE DATABASE IF NOT EXISTS mqtt_rule PRECISION 'us'");
}

static void
test_ensure_database_ready_fails_when_precision_is_not_us(void)
{
    reset_state();

    taos_sink_config cfg = make_cfg("weld_current_raw");
    g_show_create_response =
        "{\"code\":0,\"data\":[[\"CREATE DATABASE mqtt_rule PRECISION 'ms'\"]]}";
    assert(ensure_database_ready(&cfg) != 0);
    assert(!g_state.db_precision_verified);
    assert(g_state.stables.empty());
    assert(g_exec_sqls.size() == 2);
}

static void
test_enqueue_rows_with_config_rejects_when_queue_full(void)
{
    reset_state();

    taos_sink_config cfg = make_cfg("weld_current_raw");
    g_state.auth_header = build_auth_header(&cfg);
    g_state.started = true;
    g_state.running = true;
    g_state.queue_max = 1;
    g_state.db_precision_verified = true;
    g_state.stables.insert("weld_current_raw");

    weld_taos_row row1 = make_public_row("queue-1", 1001);
    weld_taos_row row2 = make_public_row("queue-2", 1002);

    assert(weld_taos_sink_enqueue_rows_with_config(&cfg, &row1, 1) == 0);
    assert(weld_taos_sink_enqueue_rows_with_config(&cfg, &row2, 1) != 0);
    assert(g_state.queue.size() == 1);
    assert(g_state.enqueue_msg_count.load() == 1);
    assert(g_state.drop_msg_count.load() == 1);
    assert(g_state.drop_row_count.load() == 1);
}

static void
test_ensure_database_ready_fails_when_show_create_query_fails(void)
{
    reset_state();

    taos_sink_config cfg = make_cfg("weld_current_raw");
    g_show_create_status = TdExecStatus::RetryableError;
    assert(ensure_database_ready(&cfg) != 0);
    assert(g_exec_sqls.size() == 2);
    assert(!g_state.db_precision_verified);
}

static void
test_enqueue_rows_with_config_rejects_stable_mismatch(void)
{
    reset_state();

    taos_sink_config cfg = make_cfg("weld_voltage_raw");
    weld_taos_row row = make_public_row("mismatch-1", 1001);
    assert(weld_taos_sink_enqueue_rows_with_config(&cfg, &row, 1) != 0);
    assert(g_state.queue.empty());
}

static void
test_build_current_raw_insert_writes_payload_and_task_id(void)
{
    WeldQueuedRow with_task = make_row("task-id-row", 3001);
    std::string with_task_sql = build_current_raw_insert("mqtt_rule", with_task);
    WeldQueuedRow without_task = make_row("no-task-row", 3002);
    std::string without_task_sql;

    without_task.has_task_id = false;
    without_task.task_id.clear();
    without_task_sql = build_current_raw_insert("mqtt_rule", without_task);

    assert(with_task_sql.find("'110120119'") != std::string::npos);
    assert(with_task_sql.find("'zstd_base64_f32_le'") != std::string::npos);
    assert(with_task_sql.find("'QUJDREVGRw=='") != std::string::npos);
    assert(without_task_sql.find("'1.0',NULL,'current',1,7") != std::string::npos);
}

static void
test_stop_all_flushes_pending_rows(void)
{
    reset_state();

    g_state.started = true;
    g_state.running = false;
    g_state.url = "http://127.0.0.1:6041/rest/sql";
    g_state.auth_header = "Basic test";
    g_state.db = "mqtt_rule";
    g_state.queue.push_back(make_row("flush-on-stop", 2001));

    weld_taos_sink_stop_all();

    assert(g_exec_calls == 1);
    assert(g_exec_sqls.size() == 1);
    assert(g_exec_sqls[0].find("flush-on-stop") != std::string::npos);
    assert(g_state.started == false);
    assert(g_state.queue.empty());
}

static void
test_can_accept_rows_rejects_invalid_args(void)
{
    reset_state();

    taos_sink_config cfg = make_cfg("weld_current_raw");
    assert(weld_taos_sink_can_accept_rows_with_config(NULL, 1) == -1);
    assert(weld_taos_sink_can_accept_rows_with_config(&cfg, 0) == -1);
    cfg.stable = "invalid-stable-name!";
    assert(weld_taos_sink_can_accept_rows_with_config(&cfg, 1) == -1);
}

static void
test_build_host_header_matches_nng_default_behavior(void)
{
    nng_url *default_port_url = NULL;
    nng_url *custom_port_url  = NULL;

    assert(nng_url_parse(&default_port_url, "http://127.0.0.1:80/rest/sql") ==
        0);
    assert(build_host_header(default_port_url) == "127.0.0.1");

    assert(nng_url_parse(&custom_port_url, "http://127.0.0.1:6041/rest/sql") ==
        0);
    assert(build_host_header(custom_port_url) == "127.0.0.1:6041");

    nng_http_req *req = NULL;
    assert(nng_http_req_alloc(&req, custom_port_url) == 0);
    assert(strcmp(nng_http_req_get_header(req, "Host"), "127.0.0.1:6041") == 0);

    nng_http_req_reset(req);
    assert(nng_http_req_get_header(req, "Host") == NULL);
    assert(nng_http_req_set_uri(req, custom_port_url->u_requri) == 0);
    assert(nng_http_req_set_header(
               req, "Host", build_host_header(custom_port_url).c_str()) == 0);
    assert(strcmp(nng_http_req_get_header(req, "Host"), "127.0.0.1:6041") == 0);

    nng_http_req_free(req);
    nng_url_free(custom_port_url);
    nng_url_free(default_port_url);
}

static void
test_flush_rows_rejects_payloads_beyond_varchar_limit(void)
{
    reset_state();

    std::vector<WeldQueuedRow> rows;
    WeldQueuedRow row = make_row("too-large-payload", 4001);

    row.payload.assign(WELD_RAW_PAYLOAD_VARCHAR_MAX + 1, 'A');
    rows.push_back(row);

    assert(flush_rows_with_retry(rows, 0, true) != 0);
    assert(g_exec_calls == 0);
    assert(g_state.batch_fail_count.load() == 1);
}

static void
test_flush_rows_splits_large_raw_batch_before_exec(void)
{
    reset_state();

    std::vector<WeldQueuedRow> rows;
    for (int i = 0; i < 24; ++i) {
        WeldQueuedRow row = make_row(("large-" + std::to_string(i)).c_str(), 5001 + i);
        row.payload.assign(45000, (char) ('A' + (i % 26)));
        rows.push_back(row);
    }

    assert(flush_rows_with_retry(rows, 0, true) == 0);
    assert(g_exec_calls == 2);
    assert(g_state.batch_split_count.load() == 1);
    assert(g_state.batch_ok_count.load() == 2);
}

int
main()
{
    test_non_retryable_batch_failure_splits_and_keeps_good_rows();
    test_ensure_database_ready_initializes_db_precision_and_stable();
    test_ensure_database_ready_retries_stable_creation_and_uses_cache();
    test_ensure_database_ready_fails_when_precision_is_not_us();
    test_ensure_database_ready_fails_when_show_create_query_fails();
    test_enqueue_rows_with_config_rejects_when_queue_full();
    test_enqueue_rows_with_config_rejects_stable_mismatch();
    test_build_current_raw_insert_writes_payload_and_task_id();
    test_stop_all_flushes_pending_rows();
    test_can_accept_rows_rejects_invalid_args();
    test_flush_rows_rejects_payloads_beyond_varchar_limit();
    test_flush_rows_splits_large_raw_batch_before_exec();
    test_build_host_header_matches_nng_default_behavior();
    weld_taos_sink_stop_all();
    return 0;
}
