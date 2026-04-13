#include "weld_taos_sink.hpp"

#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <atomic>
#include <condition_variable>
#include <chrono>
#include <deque>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "nng/nng.h"
#include "nng/supplemental/http/http.h"
#include "nng/supplemental/nanolib/log.h"
#include "nng/supplemental/util/platform.h"

namespace {

constexpr size_t WELD_BATCH_SIZE_DEFAULT = 1000;
constexpr int    WELD_FLUSH_MS_DEFAULT   = 100;
constexpr size_t WELD_QUEUE_MAX_DEFAULT  = 40000;
constexpr size_t WELD_WORKER_MAX_DEFAULT = 8;
constexpr int    WELD_RETRY_LIMIT = 2;
constexpr int    WELD_STOP_FLUSH_RETRY_LIMIT = 1;
constexpr int    WELD_RETRY_BASE_MS = 50;
constexpr int    WELD_RETRY_JITTER_MS = 20;
constexpr int    WELD_STATS_LOG_MS = 30000;
constexpr size_t TDENGINE_NAME_LIMIT = 192;
constexpr size_t TDENGINE_HASH_LEN   = 16;
constexpr size_t TDENGINE_PREFIX_LEN =
    TDENGINE_NAME_LIMIT - 1 - TDENGINE_HASH_LEN;
constexpr size_t TDENGINE_SQL_MAX_BYTES = 1024 * 1024;
constexpr size_t WELD_RAW_PAYLOAD_VARCHAR_MAX = 49152;
constexpr size_t TDENGINE_ROW_MAX_BYTES = 64 * 1024;

enum class StableKind {
    Env,
    Flow,
    Current,
    Voltage,
    CurrentRaw,
    VoltageRaw,
    Unknown,
};

enum class TdExecStatus {
    Ok,
    RetryableError,
    NonRetryableError,
};

using TdExecFn = TdExecStatus (*)(
    const std::string &, const std::string &, const std::string &, std::string *);

struct WeldQueuedRow {
    std::string stable;

    int64_t     ts_us;
    int64_t     recv_ts_us;
    std::string msg_id;
    int64_t     seq;
    std::string topic_name;
    std::string spec_ver;
    bool        has_task_id;
    std::string task_id;

    bool        has_temperature;
    double      temperature;
    bool        has_humidity;
    double      humidity;

    bool        has_instant_flow;
    double      instant_flow;
    bool        has_total_flow;
    double      total_flow;

    bool        has_current;
    double      current;
    bool        has_voltage;
    double      voltage;
    bool        has_window_start_us;
    int64_t     window_start_us;
    bool        has_sample_rate_hz;
    int         sample_rate_hz;
    bool        has_point_count;
    int         point_count;
    bool        has_encoding;
    std::string encoding;
    bool        has_payload;
    std::string payload;

    bool        has_raw_adc_unit;
    std::string raw_adc_unit;
    bool        has_cal_version;
    std::string cal_version;
    bool        has_cal_k;
    double      cal_k;
    bool        has_cal_b;
    double      cal_b;

    bool        has_quality_code;
    int         quality_code;
    bool        has_quality_text;
    std::string quality_text;

    bool        has_source_bus;
    std::string source_bus;
    bool        has_source_port;
    std::string source_port;
    bool        has_source_protocol;
    std::string source_protocol;

    bool        has_collect_period_ms;
    int         collect_period_ms;
    bool        has_collect_timeout_ms;
    int         collect_timeout_ms;
    bool        has_collect_retries;
    int         collect_retries;

    int         qos;
    int         packet_id;

    std::string version;
    std::string site_id;
    std::string line_id;
    std::string station_id;
    std::string gateway_id;
    std::string device_id;
    std::string device_type;
    std::string device_model;
    std::string metric_group;
    std::string signal_type;
    std::string channel_id;
};

struct WeldStatsSnapshot {
    size_t   queue_depth = 0;
    uint64_t enqueue_msg_count = 0;
    uint64_t enqueue_row_count = 0;
    uint64_t drop_msg_count = 0;
    uint64_t drop_row_count = 0;
    uint64_t batch_ok_count = 0;
    uint64_t batch_fail_count = 0;
    uint64_t batch_split_count = 0;
    uint64_t non_retryable_fail_count = 0;
    uint64_t row_ok_count = 0;
    uint64_t row_fail_count = 0;
};

struct WeldState {
    std::string              url;
    std::string              auth_header;
    std::string              db;
    std::set<std::string>    stables;
    bool                     db_precision_verified = false;
    size_t                   batch_size = WELD_BATCH_SIZE_DEFAULT;
    int                      flush_ms = WELD_FLUSH_MS_DEFAULT;
    size_t                   queue_max = WELD_QUEUE_MAX_DEFAULT;
    size_t                   worker_max = WELD_WORKER_MAX_DEFAULT;
    std::deque<WeldQueuedRow> queue;
    bool                     running = false;
    bool                     started = false;
    std::vector<std::thread> workers;
    std::mutex               mutex;
    std::condition_variable  cv;
    std::atomic<uint64_t>    enqueue_msg_count { 0 };
    std::atomic<uint64_t>    enqueue_row_count { 0 };
    std::atomic<uint64_t>    drop_msg_count { 0 };
    std::atomic<uint64_t>    drop_row_count { 0 };
    std::atomic<uint64_t>    batch_ok_count { 0 };
    std::atomic<uint64_t>    batch_fail_count { 0 };
    std::atomic<uint64_t>    batch_split_count { 0 };
    std::atomic<uint64_t>    non_retryable_fail_count { 0 };
    std::atomic<uint64_t>    row_ok_count { 0 };
    std::atomic<uint64_t>    row_fail_count { 0 };
    WeldStatsSnapshot        stats_last_snapshot;
    std::chrono::steady_clock::time_point stats_last_log_tp;
    bool                     stats_window_ready = false;
};

WeldState g_state;
std::mutex g_start_mutex;

static size_t
read_env_size_with_min(
    const char *name, size_t default_value, size_t min_value)
{
    const char *raw = getenv(name);
    if (raw == NULL || raw[0] == '\0') {
        return default_value;
    }

    char *end = NULL;
    unsigned long long value = strtoull(raw, &end, 10);
    if (end == raw || (end != NULL && *end != '\0')) {
        log_warn("weld_taos_sink: ignore invalid env %s=%s", name, raw);
        return default_value;
    }

    if (value < min_value) {
        log_warn("weld_taos_sink: env %s=%s below minimum %zu, clamp to %zu",
            name, raw, min_value, min_value);
        return min_value;
    }

    return (size_t) value;
}

static int
read_env_int_with_min(const char *name, int default_value, int min_value)
{
    const char *raw = getenv(name);
    if (raw == NULL || raw[0] == '\0') {
        return default_value;
    }

    char *end = NULL;
    long value = strtol(raw, &end, 10);
    if (end == raw || (end != NULL && *end != '\0')) {
        log_warn("weld_taos_sink: ignore invalid env %s=%s", name, raw);
        return default_value;
    }

    if (value < min_value) {
        log_warn("weld_taos_sink: env %s=%s below minimum %d, clamp to %d",
            name, raw, min_value, min_value);
        return min_value;
    }

    return (int) value;
}

static void
load_runtime_config_once(void)
{
    static std::once_flag loaded_once;
    std::call_once(loaded_once, [] {
        g_state.batch_size = read_env_size_with_min(
            "NANOMQ_WELD_BATCH_SIZE", WELD_BATCH_SIZE_DEFAULT, 1);
        g_state.flush_ms = read_env_int_with_min(
            "NANOMQ_WELD_FLUSH_MS", WELD_FLUSH_MS_DEFAULT, 1);
        g_state.queue_max = read_env_size_with_min(
            "NANOMQ_WELD_QUEUE_MAX", WELD_QUEUE_MAX_DEFAULT, 1);
        g_state.worker_max = read_env_size_with_min(
            "NANOMQ_WELD_WORKER_MAX", WELD_WORKER_MAX_DEFAULT, 1);
    });
}

static StableKind
get_stable_kind(const std::string &stable)
{
    if (stable == "weld_env_point") {
        return StableKind::Env;
    }
    if (stable == "weld_flow_point") {
        return StableKind::Flow;
    }
    if (stable == "weld_current_point") {
        return StableKind::Current;
    }
    if (stable == "weld_voltage_point") {
        return StableKind::Voltage;
    }
    if (stable == "weld_current_raw") {
        return StableKind::CurrentRaw;
    }
    if (stable == "weld_voltage_raw") {
        return StableKind::VoltageRaw;
    }
    return StableKind::Unknown;
}

static const char b64_table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static std::string
base64_encode(const std::string &src)
{
    std::string out;
    out.reserve(4 * ((src.size() + 2) / 3));

    size_t i = 0;
    while (i + 2 < src.size()) {
        unsigned char a = static_cast<unsigned char>(src[i++]);
        unsigned char b = static_cast<unsigned char>(src[i++]);
        unsigned char c = static_cast<unsigned char>(src[i++]);
        out.push_back(b64_table[a >> 2]);
        out.push_back(b64_table[((a & 0x03) << 4) | (b >> 4)]);
        out.push_back(b64_table[((b & 0x0F) << 2) | (c >> 6)]);
        out.push_back(b64_table[c & 0x3F]);
    }

    if (i < src.size()) {
        unsigned char a = static_cast<unsigned char>(src[i++]);
        bool has_second = i < src.size();
        unsigned char b = has_second ? static_cast<unsigned char>(src[i++]) : 0;
        out.push_back(b64_table[a >> 2]);
        out.push_back(b64_table[((a & 0x03) << 4) | (b >> 4)]);
        out.push_back(has_second ? b64_table[(b & 0x0F) << 2] : '=');
        out.push_back('=');
    }

    return out;
}

static std::string
build_auth_header(const taos_sink_config *cfg)
{
    const char *user = cfg->username != NULL ? cfg->username : "root";
    const char *pass = cfg->password != NULL ? cfg->password : "taosdata";
    std::string creds = std::string(user) + ":" + pass;
    return "Basic " + base64_encode(creds);
}

static const char *
default_port_for_scheme(const char *scheme)
{
    if (scheme == NULL) {
        return "";
    }
    if (strcmp(scheme, "http") == 0) {
        return "80";
    }
    if (strcmp(scheme, "https") == 0) {
        return "443";
    }
    return "";
}

static std::string
build_host_header(const nng_url *url)
{
    if (url == NULL) {
        return "";
    }

    const char *default_port = default_port_for_scheme(url->u_scheme);
    if (url->u_port != NULL && default_port[0] != '\0' &&
        strcmp(url->u_port, default_port) == 0) {
        return url->u_hostname != NULL ? url->u_hostname : "";
    }
    return url->u_host != NULL ? url->u_host : "";
}

static bool
response_contains_precision_us(const std::string &response)
{
    bool has_precision = false;
    bool has_us = false;
    for (size_t i = 0; i < response.size(); ++i) {
        if (i + 9 <= response.size()) {
            std::string token = response.substr(i, 9);
            for (char &ch : token) {
                ch = (char) tolower((unsigned char) ch);
            }
            if (token == "precision") {
                has_precision = true;
            }
        }
        if (i + 4 <= response.size()) {
            std::string token = response.substr(i, 4);
            for (char &ch : token) {
                ch = (char) tolower((unsigned char) ch);
            }
            if (token == "'us'") {
                has_us = true;
            }
        }
    }
    return has_precision && has_us;
}

static int
extract_tdengine_code(const std::string &response)
{
    const std::string marker = "\"code\":";
    const size_t      pos    = response.find(marker);
    if (pos == std::string::npos) {
        return -1;
    }

    size_t begin = pos + marker.size();
    while (begin < response.size() &&
           isspace((unsigned char) response[begin])) {
        ++begin;
    }

    bool negative = false;
    if (begin < response.size() && response[begin] == '-') {
        negative = true;
        ++begin;
    }

    int value = 0;
    bool found_digit = false;
    while (begin < response.size() &&
           isdigit((unsigned char) response[begin])) {
        found_digit = true;
        value = value * 10 + (response[begin] - '0');
        ++begin;
    }
    if (!found_digit) {
        return -1;
    }
    return negative ? -value : value;
}

static bool
is_non_retryable_tdengine_error(const std::string &response)
{
    const int code = extract_tdengine_code(response);
    if (code == 1547) {
        return true;
    }

    std::string lowered = response;
    for (char &ch : lowered) {
        ch = (char) tolower((unsigned char) ch);
    }

    return lowered.find("timestamp data out of range") != std::string::npos ||
           lowered.find("syntax error") != std::string::npos ||
           lowered.find("invalid table name") != std::string::npos ||
           lowered.find("table does not exist") != std::string::npos ||
           lowered.find("database not exist") != std::string::npos ||
           lowered.find("authentication") != std::string::npos ||
           lowered.find("invalid password") != std::string::npos;
}

struct TdHttpReuseContext {
    std::string      url_str;
    nng_url         *url = NULL;
    nng_http_client *client = NULL;
    nng_http_req    *req = NULL;
    nng_http_res    *res = NULL;
    nng_aio         *aio = NULL;

    ~TdHttpReuseContext()
    {
        reset();
    }

    void
    reset()
    {
        if (aio != NULL) {
            nng_aio_free(aio);
            aio = NULL;
        }
        if (res != NULL) {
            nng_http_res_free(res);
            res = NULL;
        }
        if (req != NULL) {
            nng_http_req_free(req);
            req = NULL;
        }
        if (client != NULL) {
            nng_http_client_free(client);
            client = NULL;
        }
        if (url != NULL) {
            nng_url_free(url);
            url = NULL;
        }
        url_str.clear();
    }

    TdExecStatus
    ensure_transport(const std::string &target_url)
    {
        if (url_str == target_url && url != NULL && client != NULL &&
            req != NULL && res != NULL && aio != NULL) {
            return TdExecStatus::Ok;
        }

        reset();

        int rv;
        if ((rv = nng_url_parse(&url, target_url.c_str())) != 0) {
            log_error("weld_taos_sink: url parse failed: %s", nng_strerror(rv));
            return TdExecStatus::NonRetryableError;
        }
        if ((rv = nng_http_client_alloc(&client, url)) != 0) {
            log_error("weld_taos_sink: client alloc failed: %s",
                nng_strerror(rv));
            reset();
            return TdExecStatus::RetryableError;
        }
        if ((rv = nng_http_req_alloc(&req, url)) != 0) {
            log_error("weld_taos_sink: req alloc failed: %s", nng_strerror(rv));
            reset();
            return TdExecStatus::RetryableError;
        }
        if ((rv = nng_http_res_alloc(&res)) != 0) {
            log_error("weld_taos_sink: res alloc failed: %s", nng_strerror(rv));
            reset();
            return TdExecStatus::RetryableError;
        }
        if ((rv = nng_aio_alloc(&aio, NULL, NULL)) != 0) {
            log_error("weld_taos_sink: aio alloc failed: %s", nng_strerror(rv));
            reset();
            return TdExecStatus::RetryableError;
        }

        url_str = target_url;
        return TdExecStatus::Ok;
    }
};

static TdExecStatus
tdengine_exec_http(const std::string &url_str, const std::string &auth_hdr,
    const std::string &sql, std::string *response_out)
{
    thread_local TdHttpReuseContext context;
    TdExecStatus                    rc = context.ensure_transport(url_str);

    if (rc != TdExecStatus::Ok) {
        return rc;
    }

    nng_http_req_reset(context.req);
    nng_http_res_reset(context.res);
    if (nng_http_req_set_uri(
            context.req,
            (context.url->u_requri != NULL && context.url->u_requri[0] != '\0')
                ? context.url->u_requri
                : "/") != 0) {
        log_error("weld_taos_sink: req set uri failed");
        return TdExecStatus::RetryableError;
    }
    const std::string host_header = build_host_header(context.url);
    if (host_header.empty() ||
        nng_http_req_set_header(context.req, "Host", host_header.c_str()) !=
            0) {
        log_error("weld_taos_sink: req set host failed");
        return TdExecStatus::RetryableError;
    }
    nng_http_req_set_method(context.req, "POST");
    nng_http_req_set_header(context.req, "Authorization", auth_hdr.c_str());
    nng_http_req_set_header(context.req, "Content-Type", "text/plain");
    nng_http_req_copy_data(context.req, sql.c_str(), sql.size());

    nng_aio_set_timeout(context.aio, 5000);
    // Reusing a persistent nng_http_conn here has produced intermittent
    // TDengine HTTP 400 responses during bootstrap SQL. Reuse the allocated
    // client/request objects, but let each transact manage its own connection.
    nng_http_client_transact(
        context.client, context.req, context.res, context.aio);
    nng_aio_wait(context.aio);

    const int rv = nng_aio_result(context.aio);
    if (rv != 0) {
        log_error(
            "weld_taos_sink: http transact failed: %s", nng_strerror(rv));
        return TdExecStatus::RetryableError;
    }

    {
        uint16_t status = nng_http_res_get_status(context.res);
        void    *body   = NULL;
        size_t   body_len = 0;
        nng_http_res_get_data(context.res, &body, &body_len);
        std::string response(
            body != NULL ? (char *) body : "", body != NULL ? body_len : 0);

        if (response_out != NULL) {
            *response_out = response;
        }

        if (status != NNG_HTTP_STATUS_OK) {
            log_error("weld_taos_sink: HTTP %d body: %.*s", status,
                (int) body_len, body != NULL ? (char *) body : "(null)");
            rc = status >= 500 ? TdExecStatus::RetryableError :
                                 TdExecStatus::NonRetryableError;
            return rc;
        }

        if (response.find("\"code\":0") != std::string::npos) {
            rc = TdExecStatus::Ok;
        } else {
            log_error("weld_taos_sink: TDengine error: %s", response.c_str());
            rc = is_non_retryable_tdengine_error(response) ?
                     TdExecStatus::NonRetryableError :
                     TdExecStatus::RetryableError;
        }
    }
    return rc;
}

static TdExecFn g_tdengine_exec_fn = tdengine_exec_http;

static TdExecStatus
tdengine_exec_detailed(const std::string &url_str, const std::string &auth_hdr,
    const std::string &sql, std::string *response_out)
{
    return g_tdengine_exec_fn(url_str, auth_hdr, sql, response_out);
}

static int
tdengine_exec(const std::string &url_str, const std::string &auth_hdr,
    const std::string &sql)
{
    return tdengine_exec_detailed(url_str, auth_hdr, sql, NULL) ==
                   TdExecStatus::Ok ?
               0 :
               -1;
}

static std::string
escape_sql_string(const std::string &value)
{
    std::string out;
    out.reserve(value.size() * 2 + 2);
    for (char ch : value) {
        if (ch == '\'' || ch == '\\') {
            out.push_back('\\');
        }
        out.push_back(ch);
    }
    return out;
}

static std::string
sql_quoted(const std::string &value)
{
    return "'" + escape_sql_string(value) + "'";
}

static std::string
sql_optional_string(bool has_value, const std::string &value)
{
    if (!has_value) {
        return "NULL";
    }
    return sql_quoted(value);
}

static std::string
sql_optional_double(bool has_value, double value)
{
    if (!has_value) {
        return "NULL";
    }
    char buf[64];
    snprintf(buf, sizeof(buf), "%.17g", value);
    return std::string(buf);
}

static std::string
sql_optional_int(bool has_value, int64_t value)
{
    if (!has_value) {
        return "NULL";
    }
    char buf[64];
    snprintf(buf, sizeof(buf), "%lld", (long long) value);
    return std::string(buf);
}

static size_t
estimate_raw_row_bytes(const WeldQueuedRow &row)
{
    size_t bytes = sizeof(row);

    bytes += row.msg_id.size();
    bytes += row.topic_name.size();
    bytes += row.spec_ver.size();
    bytes += row.task_id.size();
    bytes += row.signal_type.size();
    bytes += row.encoding.size();
    bytes += row.payload.size();
    bytes += row.raw_adc_unit.size();
    bytes += row.cal_version.size();
    bytes += row.version.size();
    bytes += row.site_id.size();
    bytes += row.line_id.size();
    bytes += row.station_id.size();
    bytes += row.gateway_id.size();
    bytes += row.device_id.size();
    bytes += row.device_type.size();
    bytes += row.device_model.size();
    bytes += row.metric_group.size();
    bytes += row.channel_id.size();
    return bytes;
}

static int
validate_raw_row_size(const WeldQueuedRow &row)
{
    if ((get_stable_kind(row.stable) != StableKind::CurrentRaw) &&
        (get_stable_kind(row.stable) != StableKind::VoltageRaw)) {
        return 0;
    }

    if (!row.has_payload || row.payload.empty()) {
        log_error("weld_taos_sink: raw payload missing (msg_id=%s stable=%s)",
            row.msg_id.c_str(), row.stable.c_str());
        return -1;
    }
    if (row.payload.size() > WELD_RAW_PAYLOAD_VARCHAR_MAX) {
        log_error("weld_taos_sink: raw payload too large (%zu > %zu) "
                  "(msg_id=%s stable=%s)",
            row.payload.size(), WELD_RAW_PAYLOAD_VARCHAR_MAX,
            row.msg_id.c_str(), row.stable.c_str());
        return -1;
    }
    if (estimate_raw_row_bytes(row) > TDENGINE_ROW_MAX_BYTES) {
        log_error("weld_taos_sink: raw row too large (%zu > %zu) "
                  "(msg_id=%s stable=%s)",
            estimate_raw_row_bytes(row), TDENGINE_ROW_MAX_BYTES,
            row.msg_id.c_str(), row.stable.c_str());
        return -1;
    }
    return 0;
}

static uint64_t
fnv1a_64(const std::string &value)
{
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char ch : value) {
        hash ^= ch;
        hash *= 1099511628211ULL;
    }
    return hash;
}

static std::string
hash16(const std::string &value)
{
    char buf[17];
    snprintf(buf, sizeof(buf), "%016llx", (unsigned long long) fnv1a_64(value));
    return std::string(buf);
}

static std::string
sanitize_identifier(const std::string &value)
{
    std::string out;
    out.reserve(value.size());
    for (char ch : value) {
        out.push_back(isalnum((unsigned char) ch) ? ch : '_');
    }
    return out;
}

static std::string
make_sub_table_name(const WeldQueuedRow &row)
{
    std::string sanitized = sanitize_identifier(
        row.stable + "_" + row.gateway_id + "_" + row.device_id);
    if (sanitized.size() <= TDENGINE_NAME_LIMIT) {
        return sanitized;
    }
    return sanitized.substr(0, TDENGINE_PREFIX_LEN) + "_" + hash16(sanitized);
}

static std::string
build_env_insert(const std::string &db, const WeldQueuedRow &row)
{
    const std::string sub_table = make_sub_table_name(row);
    return db + "." + sub_table + " USING " + db + "." + row.stable +
           " TAGS(" + sql_quoted(row.version) + "," + sql_quoted(row.site_id) +
           "," + sql_quoted(row.line_id) + "," + sql_quoted(row.station_id) +
           "," + sql_quoted(row.gateway_id) + "," + sql_quoted(row.device_id) +
           "," + sql_quoted(row.device_type) + "," +
           sql_quoted(row.device_model) + "," + sql_quoted(row.metric_group) +
           "," + sql_quoted(row.signal_type) + "," +
           sql_quoted(row.channel_id) + ") VALUES(" +
           std::to_string((long long) row.ts_us) + "," +
           std::to_string((long long) row.recv_ts_us) + "," +
           sql_quoted(row.msg_id) + "," + std::to_string((long long) row.seq) +
           "," + sql_quoted(row.topic_name) + "," + sql_quoted(row.spec_ver) +
           "," + sql_optional_string(row.has_task_id, row.task_id) + "," +
           sql_optional_double(row.has_temperature, row.temperature) +
           "," + sql_optional_double(row.has_humidity, row.humidity) + "," +
           sql_optional_int(row.has_quality_code, row.quality_code) + "," +
           sql_optional_string(row.has_quality_text, row.quality_text) + "," +
           sql_optional_string(row.has_source_bus, row.source_bus) + "," +
           sql_optional_string(row.has_source_port, row.source_port) + "," +
           sql_optional_string(row.has_source_protocol, row.source_protocol) +
           "," + sql_optional_int(
                     row.has_collect_period_ms, row.collect_period_ms) +
           "," + sql_optional_int(
                     row.has_collect_timeout_ms, row.collect_timeout_ms) +
           "," + sql_optional_int(row.has_collect_retries, row.collect_retries) +
           "," + std::to_string(row.qos) + "," +
           std::to_string(row.packet_id) + ")";
}

static std::string
build_flow_insert(const std::string &db, const WeldQueuedRow &row)
{
    const std::string sub_table = make_sub_table_name(row);
    return db + "." + sub_table + " USING " + db + "." + row.stable +
           " TAGS(" + sql_quoted(row.version) + "," + sql_quoted(row.site_id) +
           "," + sql_quoted(row.line_id) + "," + sql_quoted(row.station_id) +
           "," + sql_quoted(row.gateway_id) + "," + sql_quoted(row.device_id) +
           "," + sql_quoted(row.device_type) + "," +
           sql_quoted(row.device_model) + "," + sql_quoted(row.metric_group) +
           "," + sql_quoted(row.signal_type) + "," +
           sql_quoted(row.channel_id) + ") VALUES(" +
           std::to_string((long long) row.ts_us) + "," +
           std::to_string((long long) row.recv_ts_us) + "," +
           sql_quoted(row.msg_id) + "," + std::to_string((long long) row.seq) +
           "," + sql_quoted(row.topic_name) + "," + sql_quoted(row.spec_ver) +
           "," + sql_optional_string(row.has_task_id, row.task_id) + "," +
           sql_optional_double(row.has_instant_flow, row.instant_flow) +
           "," + sql_optional_double(row.has_total_flow, row.total_flow) + "," +
           sql_optional_int(row.has_quality_code, row.quality_code) + "," +
           sql_optional_string(row.has_quality_text, row.quality_text) + "," +
           sql_optional_string(row.has_source_bus, row.source_bus) + "," +
           sql_optional_string(row.has_source_port, row.source_port) + "," +
           sql_optional_string(row.has_source_protocol, row.source_protocol) +
           "," + sql_optional_int(
                     row.has_collect_period_ms, row.collect_period_ms) +
           "," + sql_optional_int(
                     row.has_collect_timeout_ms, row.collect_timeout_ms) +
           "," + sql_optional_int(row.has_collect_retries, row.collect_retries) +
           "," + std::to_string(row.qos) + "," +
           std::to_string(row.packet_id) + ")";
}

static std::string
build_current_insert(const std::string &db, const WeldQueuedRow &row)
{
    const std::string sub_table = make_sub_table_name(row);
    return db + "." + sub_table + " USING " + db + "." + row.stable +
           " TAGS(" + sql_quoted(row.version) + "," + sql_quoted(row.site_id) +
           "," + sql_quoted(row.line_id) + "," + sql_quoted(row.station_id) +
           "," + sql_quoted(row.gateway_id) + "," + sql_quoted(row.device_id) +
           "," + sql_quoted(row.device_type) + "," +
           sql_quoted(row.device_model) + "," + sql_quoted(row.metric_group) +
           "," + sql_quoted(row.signal_type) + "," +
           sql_quoted(row.channel_id) + ") VALUES(" +
           std::to_string((long long) row.ts_us) + "," +
           std::to_string((long long) row.recv_ts_us) + "," +
           sql_quoted(row.msg_id) + "," + std::to_string((long long) row.seq) +
           "," + sql_quoted(row.topic_name) + "," + sql_quoted(row.spec_ver) +
           "," + sql_optional_string(row.has_task_id, row.task_id) + "," +
           sql_optional_double(row.has_current, row.current) + "," +
           sql_optional_string(row.has_raw_adc_unit, row.raw_adc_unit) + "," +
           sql_optional_string(row.has_cal_version, row.cal_version) + "," +
           sql_optional_double(row.has_cal_k, row.cal_k) + "," +
           sql_optional_double(row.has_cal_b, row.cal_b) + "," +
           sql_optional_int(row.has_quality_code, row.quality_code) + "," +
           sql_optional_string(row.has_quality_text, row.quality_text) + "," +
           sql_optional_string(row.has_source_bus, row.source_bus) + "," +
           sql_optional_string(row.has_source_port, row.source_port) + "," +
           sql_optional_string(row.has_source_protocol, row.source_protocol) +
           "," + sql_optional_int(
                     row.has_collect_period_ms, row.collect_period_ms) +
           "," + sql_optional_int(
                     row.has_collect_timeout_ms, row.collect_timeout_ms) +
           "," + sql_optional_int(row.has_collect_retries, row.collect_retries) +
           "," + std::to_string(row.qos) + "," +
           std::to_string(row.packet_id) + ")";
}

static std::string
build_voltage_insert(const std::string &db, const WeldQueuedRow &row)
{
    const std::string sub_table = make_sub_table_name(row);
    return db + "." + sub_table + " USING " + db + "." + row.stable +
           " TAGS(" + sql_quoted(row.version) + "," + sql_quoted(row.site_id) +
           "," + sql_quoted(row.line_id) + "," + sql_quoted(row.station_id) +
           "," + sql_quoted(row.gateway_id) + "," + sql_quoted(row.device_id) +
           "," + sql_quoted(row.device_type) + "," +
           sql_quoted(row.device_model) + "," + sql_quoted(row.metric_group) +
           "," + sql_quoted(row.signal_type) + "," +
           sql_quoted(row.channel_id) + ") VALUES(" +
           std::to_string((long long) row.ts_us) + "," +
           std::to_string((long long) row.recv_ts_us) + "," +
           sql_quoted(row.msg_id) + "," + std::to_string((long long) row.seq) +
           "," + sql_quoted(row.topic_name) + "," + sql_quoted(row.spec_ver) +
           "," + sql_optional_string(row.has_task_id, row.task_id) + "," +
           sql_optional_double(row.has_voltage, row.voltage) + "," +
           sql_optional_string(row.has_raw_adc_unit, row.raw_adc_unit) + "," +
           sql_optional_string(row.has_cal_version, row.cal_version) + "," +
           sql_optional_double(row.has_cal_k, row.cal_k) + "," +
           sql_optional_double(row.has_cal_b, row.cal_b) + "," +
           sql_optional_int(row.has_quality_code, row.quality_code) + "," +
           sql_optional_string(row.has_quality_text, row.quality_text) + "," +
           sql_optional_string(row.has_source_bus, row.source_bus) + "," +
           sql_optional_string(row.has_source_port, row.source_port) + "," +
           sql_optional_string(row.has_source_protocol, row.source_protocol) +
           "," + sql_optional_int(
                     row.has_collect_period_ms, row.collect_period_ms) +
           "," + sql_optional_int(
                     row.has_collect_timeout_ms, row.collect_timeout_ms) +
           "," + sql_optional_int(row.has_collect_retries, row.collect_retries) +
           "," + std::to_string(row.qos) + "," +
           std::to_string(row.packet_id) + ")";
}

static std::string
build_current_raw_insert(const std::string &db, const WeldQueuedRow &row)
{
    const std::string sub_table = make_sub_table_name(row);
    return db + "." + sub_table + " USING " + db + "." + row.stable +
           " TAGS(" + sql_quoted(row.version) + "," + sql_quoted(row.site_id) +
           "," + sql_quoted(row.line_id) + "," + sql_quoted(row.station_id) +
           "," + sql_quoted(row.gateway_id) + "," + sql_quoted(row.device_id) +
           "," + sql_quoted(row.device_type) + "," +
           sql_quoted(row.device_model) + "," + sql_quoted(row.metric_group) +
           "," + sql_quoted(row.channel_id) + ") VALUES(" +
           std::to_string((long long) row.ts_us) + "," +
           std::to_string((long long) row.recv_ts_us) + "," +
           sql_quoted(row.msg_id) + "," + std::to_string((long long) row.seq) +
           "," + sql_quoted(row.topic_name) + "," + sql_quoted(row.spec_ver) +
           "," + sql_optional_string(row.has_task_id, row.task_id) + "," +
           sql_quoted(row.signal_type) + "," + std::to_string(row.qos) + "," +
           std::to_string(row.packet_id) + "," +
           sql_optional_int(row.has_window_start_us, row.window_start_us) + "," +
           sql_optional_int(row.has_sample_rate_hz, row.sample_rate_hz) + "," +
           sql_optional_int(row.has_point_count, row.point_count) + "," +
           sql_optional_string(row.has_encoding, row.encoding) + "," +
           sql_optional_string(row.has_payload, row.payload) + "," +
           sql_optional_string(row.has_raw_adc_unit, row.raw_adc_unit) + "," +
           sql_optional_string(row.has_cal_version, row.cal_version) + "," +
           sql_optional_double(row.has_cal_k, row.cal_k) + "," +
           sql_optional_double(row.has_cal_b, row.cal_b) + ")";
}

static std::string
build_voltage_raw_insert(const std::string &db, const WeldQueuedRow &row)
{
    const std::string sub_table = make_sub_table_name(row);
    return db + "." + sub_table + " USING " + db + "." + row.stable +
           " TAGS(" + sql_quoted(row.version) + "," + sql_quoted(row.site_id) +
           "," + sql_quoted(row.line_id) + "," + sql_quoted(row.station_id) +
           "," + sql_quoted(row.gateway_id) + "," + sql_quoted(row.device_id) +
           "," + sql_quoted(row.device_type) + "," +
           sql_quoted(row.device_model) + "," + sql_quoted(row.metric_group) +
           "," + sql_quoted(row.channel_id) + ") VALUES(" +
           std::to_string((long long) row.ts_us) + "," +
           std::to_string((long long) row.recv_ts_us) + "," +
           sql_quoted(row.msg_id) + "," + std::to_string((long long) row.seq) +
           "," + sql_quoted(row.topic_name) + "," + sql_quoted(row.spec_ver) +
           "," + sql_optional_string(row.has_task_id, row.task_id) + "," +
           sql_quoted(row.signal_type) + "," + std::to_string(row.qos) + "," +
           std::to_string(row.packet_id) + "," +
           sql_optional_int(row.has_window_start_us, row.window_start_us) + "," +
           sql_optional_int(row.has_sample_rate_hz, row.sample_rate_hz) + "," +
           sql_optional_int(row.has_point_count, row.point_count) + "," +
           sql_optional_string(row.has_encoding, row.encoding) + "," +
           sql_optional_string(row.has_payload, row.payload) + "," +
           sql_optional_string(row.has_raw_adc_unit, row.raw_adc_unit) + "," +
           sql_optional_string(row.has_cal_version, row.cal_version) + "," +
           sql_optional_double(row.has_cal_k, row.cal_k) + "," +
           sql_optional_double(row.has_cal_b, row.cal_b) + ")";
}

static std::string
build_batch_insert_sql(const std::string &db,
    const std::vector<WeldQueuedRow> &rows)
{
    std::string sql = "INSERT INTO ";
    bool first = true;
    for (const WeldQueuedRow &row : rows) {
        std::string fragment;
        switch (get_stable_kind(row.stable)) {
        case StableKind::Env:
            fragment = build_env_insert(db, row);
            break;
        case StableKind::Flow:
            fragment = build_flow_insert(db, row);
            break;
        case StableKind::Current:
            fragment = build_current_insert(db, row);
            break;
        case StableKind::Voltage:
            fragment = build_voltage_insert(db, row);
            break;
        case StableKind::CurrentRaw:
            fragment = build_current_raw_insert(db, row);
            break;
        case StableKind::VoltageRaw:
            fragment = build_voltage_raw_insert(db, row);
            break;
        case StableKind::Unknown:
        default:
            return std::string();
        }

        if (!first) {
            sql.push_back(' ');
        }
        sql += fragment;
        first = false;
    }
    return sql;
}

static WeldStatsSnapshot
snapshot_stats(void)
{
    WeldStatsSnapshot snapshot;
    {
        std::lock_guard<std::mutex> lock(g_state.mutex);
        snapshot.queue_depth = g_state.queue.size();
    }
    snapshot.enqueue_msg_count = g_state.enqueue_msg_count.load();
    snapshot.enqueue_row_count = g_state.enqueue_row_count.load();
    snapshot.drop_msg_count = g_state.drop_msg_count.load();
    snapshot.drop_row_count = g_state.drop_row_count.load();
    snapshot.batch_ok_count = g_state.batch_ok_count.load();
    snapshot.batch_fail_count = g_state.batch_fail_count.load();
    snapshot.batch_split_count = g_state.batch_split_count.load();
    snapshot.non_retryable_fail_count = g_state.non_retryable_fail_count.load();
    snapshot.row_ok_count = g_state.row_ok_count.load();
    snapshot.row_fail_count = g_state.row_fail_count.load();
    return snapshot;
}

static void
log_stats_snapshot(void)
{
    const WeldStatsSnapshot snapshot = snapshot_stats();
    double                  elapsed_sec = 0.0;
    WeldStatsSnapshot       delta;
    const auto              now = std::chrono::steady_clock::now();

    if (g_state.stats_window_ready) {
        elapsed_sec = std::chrono::duration_cast<std::chrono::milliseconds>(
                          now - g_state.stats_last_log_tp)
                          .count() /
                      1000.0;
        delta.enqueue_msg_count =
            snapshot.enqueue_msg_count -
            g_state.stats_last_snapshot.enqueue_msg_count;
        delta.enqueue_row_count =
            snapshot.enqueue_row_count -
            g_state.stats_last_snapshot.enqueue_row_count;
        delta.drop_msg_count =
            snapshot.drop_msg_count - g_state.stats_last_snapshot.drop_msg_count;
        delta.drop_row_count =
            snapshot.drop_row_count - g_state.stats_last_snapshot.drop_row_count;
        delta.batch_ok_count =
            snapshot.batch_ok_count - g_state.stats_last_snapshot.batch_ok_count;
        delta.batch_fail_count =
            snapshot.batch_fail_count -
            g_state.stats_last_snapshot.batch_fail_count;
        delta.batch_split_count =
            snapshot.batch_split_count -
            g_state.stats_last_snapshot.batch_split_count;
        delta.non_retryable_fail_count =
            snapshot.non_retryable_fail_count -
            g_state.stats_last_snapshot.non_retryable_fail_count;
        delta.row_ok_count =
            snapshot.row_ok_count - g_state.stats_last_snapshot.row_ok_count;
        delta.row_fail_count =
            snapshot.row_fail_count - g_state.stats_last_snapshot.row_fail_count;
    }

    g_state.stats_last_snapshot = snapshot;
    g_state.stats_last_log_tp = now;
    g_state.stats_window_ready = true;

    log_info("weld_taos_sink: queue=%zu enqueue_msg=%llu enqueue_row=%llu "
             "drop_msg=%llu drop_row=%llu batch_ok=%llu batch_fail=%llu "
             "batch_split=%llu non_retryable_fail=%llu row_ok=%llu row_fail=%llu "
             "window_s=%.2f enqueue_msg_rate=%.2f enqueue_row_rate=%.2f "
             "drop_msg_rate=%.2f row_ok_rate=%.2f row_fail_rate=%.2f",
        snapshot.queue_depth,
        (unsigned long long) snapshot.enqueue_msg_count,
        (unsigned long long) snapshot.enqueue_row_count,
        (unsigned long long) snapshot.drop_msg_count,
        (unsigned long long) snapshot.drop_row_count,
        (unsigned long long) snapshot.batch_ok_count,
        (unsigned long long) snapshot.batch_fail_count,
        (unsigned long long) snapshot.batch_split_count,
        (unsigned long long) snapshot.non_retryable_fail_count,
        (unsigned long long) snapshot.row_ok_count,
        (unsigned long long) snapshot.row_fail_count,
        elapsed_sec,
        elapsed_sec > 0.0 ? delta.enqueue_msg_count / elapsed_sec : 0.0,
        elapsed_sec > 0.0 ? delta.enqueue_row_count / elapsed_sec : 0.0,
        elapsed_sec > 0.0 ? delta.drop_msg_count / elapsed_sec : 0.0,
        elapsed_sec > 0.0 ? delta.row_ok_count / elapsed_sec : 0.0,
        elapsed_sec > 0.0 ? delta.row_fail_count / elapsed_sec : 0.0);
}

static void
record_enqueue_success(size_t row_count)
{
    g_state.enqueue_msg_count.fetch_add(1);
    g_state.enqueue_row_count.fetch_add((uint64_t) row_count);
}

static void
record_enqueue_drop(size_t row_count)
{
    g_state.drop_msg_count.fetch_add(1);
    g_state.drop_row_count.fetch_add((uint64_t) row_count);
}

static void
record_batch_result(size_t row_count, bool success)
{
    if (success) {
        g_state.batch_ok_count.fetch_add(1);
        g_state.row_ok_count.fetch_add((uint64_t) row_count);
    } else {
        g_state.batch_fail_count.fetch_add(1);
        g_state.row_fail_count.fetch_add((uint64_t) row_count);
    }
}

static void
sleep_before_retry(std::mt19937 &rng)
{
    std::uniform_int_distribution<int> jitter(0, WELD_RETRY_JITTER_MS);
    nng_msleep((unsigned) (WELD_RETRY_BASE_MS + jitter(rng)));
}

static int
flush_rows_with_retry(
    const std::vector<WeldQueuedRow> &rows, int retry_limit, bool count_stats)
{
    if (rows.empty()) {
        return 0;
    }

    for (const WeldQueuedRow &row : rows) {
        if (validate_raw_row_size(row) != 0) {
            if (count_stats) {
                record_batch_result(rows.size(), false);
            }
            return -1;
        }
    }

    const std::string sql = build_batch_insert_sql(g_state.db, rows);
    if (sql.empty()) {
        log_error("weld_taos_sink: build batch sql failed (%zu rows)",
            rows.size());
        if (count_stats) {
            record_batch_result(rows.size(), false);
        }
        return -1;
    }
    if (sql.size() > TDENGINE_SQL_MAX_BYTES) {
        if (rows.size() > 1) {
            const size_t mid = rows.size() / 2;
            std::vector<WeldQueuedRow> left(rows.begin(), rows.begin() + mid);
            std::vector<WeldQueuedRow> right(rows.begin() + mid, rows.end());
            g_state.batch_split_count.fetch_add(1);
            log_warn("weld_taos_sink: batch sql too large, split batch "
                     "(rows=%zu, sql_bytes=%zu, left=%zu, right=%zu)",
                rows.size(), sql.size(), left.size(), right.size());
            const int left_rc =
                flush_rows_with_retry(left, retry_limit, count_stats);
            const int right_rc =
                flush_rows_with_retry(right, retry_limit, count_stats);
            return (left_rc == 0 && right_rc == 0) ? 0 : -1;
        }
        log_error("weld_taos_sink: single-row sql too large (%zu > %zu) "
                  "(msg_id=%s stable=%s)",
            sql.size(), TDENGINE_SQL_MAX_BYTES, rows[0].msg_id.c_str(),
            rows[0].stable.c_str());
        if (count_stats) {
            record_batch_result(rows.size(), false);
        }
        return -1;
    }

    std::mt19937 rng((unsigned) std::chrono::steady_clock::now()
                         .time_since_epoch()
                         .count());
    for (int attempt = 0; attempt <= retry_limit; ++attempt) {
        const TdExecStatus status =
            tdengine_exec_detailed(g_state.url, g_state.auth_header, sql, NULL);
        if (status == TdExecStatus::Ok) {
            if (count_stats) {
                record_batch_result(rows.size(), true);
            }
            return 0;
        }
        if (status == TdExecStatus::NonRetryableError) {
            if (rows.size() > 1) {
                const size_t mid = rows.size() / 2;
                std::vector<WeldQueuedRow> left(rows.begin(), rows.begin() + mid);
                std::vector<WeldQueuedRow> right(rows.begin() + mid, rows.end());
                g_state.batch_split_count.fetch_add(1);
                log_warn("weld_taos_sink: non-retryable batch failure, split batch "
                         "(rows=%zu, left=%zu, right=%zu, attempt=%d)",
                    rows.size(), left.size(), right.size(), attempt + 1);
                const int left_rc =
                    flush_rows_with_retry(left, retry_limit, count_stats);
                const int right_rc =
                    flush_rows_with_retry(right, retry_limit, count_stats);
                return (left_rc == 0 && right_rc == 0) ? 0 : -1;
            }
            g_state.non_retryable_fail_count.fetch_add(1);
            log_error("weld_taos_sink: non-retryable single-row insert failure "
                      "(msg_id=%s, stable=%s, ts_us=%lld)",
                rows[0].msg_id.c_str(), rows[0].stable.c_str(),
                (long long) rows[0].ts_us);
            break;
        }
        if (attempt < retry_limit) {
            sleep_before_retry(rng);
        }
    }

    log_error("weld_taos_sink: batch insert failed after %d attempt(s) (%zu rows)",
        retry_limit + 1, rows.size());
    if (count_stats) {
        record_batch_result(rows.size(), false);
    }
    return -1;
}

static std::string
build_create_stable_sql(const std::string &db, const std::string &stable)
{
    switch (get_stable_kind(stable)) {
    case StableKind::Env:
        return "CREATE STABLE IF NOT EXISTS " + db + "." + stable +
               " (ts TIMESTAMP, recv_ts TIMESTAMP, msg_id NCHAR(128), "
               "seq BIGINT, topic_name NCHAR(256), spec_ver NCHAR(32), "
               "task_id NCHAR(128), "
               "temperature DOUBLE, humidity DOUBLE, quality_code INT, "
               "quality_text NCHAR(64), source_bus NCHAR(32), "
               "source_port NCHAR(64), source_protocol NCHAR(32), "
               "collect_period_ms INT, collect_timeout_ms INT, "
               "collect_retries INT, qos INT, packet_id INT) "
               "TAGS (version NCHAR(16), site_id NCHAR(64), "
               "line_id NCHAR(64), station_id NCHAR(64), "
               "gateway_id NCHAR(64), device_id NCHAR(64), "
               "device_type NCHAR(64), device_model NCHAR(64), "
               "metric_group NCHAR(32), signal_type NCHAR(32), "
               "channel_id NCHAR(32))";
    case StableKind::Flow:
        return "CREATE STABLE IF NOT EXISTS " + db + "." + stable +
               " (ts TIMESTAMP, recv_ts TIMESTAMP, msg_id NCHAR(128), "
               "seq BIGINT, topic_name NCHAR(256), spec_ver NCHAR(32), "
               "task_id NCHAR(128), "
               "instant_flow DOUBLE, total_flow DOUBLE, quality_code INT, "
               "quality_text NCHAR(64), source_bus NCHAR(32), "
               "source_port NCHAR(64), source_protocol NCHAR(32), "
               "collect_period_ms INT, collect_timeout_ms INT, "
               "collect_retries INT, qos INT, packet_id INT) "
               "TAGS (version NCHAR(16), site_id NCHAR(64), "
               "line_id NCHAR(64), station_id NCHAR(64), "
               "gateway_id NCHAR(64), device_id NCHAR(64), "
               "device_type NCHAR(64), device_model NCHAR(64), "
               "metric_group NCHAR(32), signal_type NCHAR(32), "
               "channel_id NCHAR(32))";
    case StableKind::Current:
        return "CREATE STABLE IF NOT EXISTS " + db + "." + stable +
               " (ts TIMESTAMP, recv_ts TIMESTAMP, msg_id NCHAR(128), "
               "seq BIGINT, topic_name NCHAR(256), spec_ver NCHAR(32), "
               "task_id NCHAR(128), "
               "current DOUBLE, raw_adc_unit NCHAR(16), cal_version NCHAR(64), "
               "cal_k DOUBLE, cal_b DOUBLE, quality_code INT, "
               "quality_text NCHAR(64), source_bus NCHAR(32), "
               "source_port NCHAR(64), source_protocol NCHAR(32), "
               "collect_period_ms INT, collect_timeout_ms INT, "
               "collect_retries INT, qos INT, packet_id INT) "
               "TAGS (version NCHAR(16), site_id NCHAR(64), "
               "line_id NCHAR(64), station_id NCHAR(64), "
               "gateway_id NCHAR(64), device_id NCHAR(64), "
               "device_type NCHAR(64), device_model NCHAR(64), "
               "metric_group NCHAR(32), signal_type NCHAR(32), "
               "channel_id NCHAR(32))";
    case StableKind::Voltage:
        return "CREATE STABLE IF NOT EXISTS " + db + "." + stable +
               " (ts TIMESTAMP, recv_ts TIMESTAMP, msg_id NCHAR(128), "
               "seq BIGINT, topic_name NCHAR(256), spec_ver NCHAR(32), "
               "task_id NCHAR(128), "
               "voltage DOUBLE, raw_adc_unit NCHAR(16), cal_version NCHAR(64), "
               "cal_k DOUBLE, cal_b DOUBLE, quality_code INT, "
               "quality_text NCHAR(64), source_bus NCHAR(32), "
               "source_port NCHAR(64), source_protocol NCHAR(32), "
               "collect_period_ms INT, collect_timeout_ms INT, "
               "collect_retries INT, qos INT, packet_id INT) "
               "TAGS (version NCHAR(16), site_id NCHAR(64), "
               "line_id NCHAR(64), station_id NCHAR(64), "
               "gateway_id NCHAR(64), device_id NCHAR(64), "
               "device_type NCHAR(64), device_model NCHAR(64), "
               "metric_group NCHAR(32), signal_type NCHAR(32), "
               "channel_id NCHAR(32))";
    case StableKind::CurrentRaw:
        return "CREATE STABLE IF NOT EXISTS " + db + "." + stable +
               " (ts TIMESTAMP, recv_ts TIMESTAMP, msg_id VARCHAR(128), "
               "seq BIGINT, topic_name VARCHAR(256), spec_ver VARCHAR(32), "
               "task_id VARCHAR(128), signal_type VARCHAR(32), qos INT, "
               "packet_id INT, window_start_us BIGINT, sample_rate_hz INT, "
               "point_count INT, encoding VARCHAR(32), payload VARCHAR(49152), "
               "raw_adc_unit VARCHAR(16), cal_version VARCHAR(64), "
               "cal_k DOUBLE, cal_b DOUBLE) "
               "TAGS (version VARCHAR(16), site_id VARCHAR(64), "
               "line_id VARCHAR(64), station_id VARCHAR(64), "
               "gateway_id VARCHAR(64), device_id VARCHAR(64), "
               "device_type VARCHAR(64), device_model VARCHAR(64), "
               "metric_group VARCHAR(32), channel_id VARCHAR(32))";
    case StableKind::VoltageRaw:
        return "CREATE STABLE IF NOT EXISTS " + db + "." + stable +
               " (ts TIMESTAMP, recv_ts TIMESTAMP, msg_id VARCHAR(128), "
               "seq BIGINT, topic_name VARCHAR(256), spec_ver VARCHAR(32), "
               "task_id VARCHAR(128), signal_type VARCHAR(32), qos INT, "
               "packet_id INT, window_start_us BIGINT, sample_rate_hz INT, "
               "point_count INT, encoding VARCHAR(32), payload VARCHAR(49152), "
               "raw_adc_unit VARCHAR(16), cal_version VARCHAR(64), "
               "cal_k DOUBLE, cal_b DOUBLE) "
               "TAGS (version VARCHAR(16), site_id VARCHAR(64), "
               "line_id VARCHAR(64), station_id VARCHAR(64), "
               "gateway_id VARCHAR(64), device_id VARCHAR(64), "
               "device_type VARCHAR(64), device_model VARCHAR(64), "
               "metric_group VARCHAR(32), channel_id VARCHAR(32))";
    case StableKind::Unknown:
    default:
        return std::string();
    }
}

static int
ensure_database_precision_us(void)
{
    std::string response;

    if (tdengine_exec_detailed(g_state.url, g_state.auth_header,
            "SHOW CREATE DATABASE " + g_state.db, &response) !=
        TdExecStatus::Ok) {
        log_error("weld_taos_sink: query database precision failed: %s",
            g_state.db.c_str());
        return -1;
    }

    if (!response_contains_precision_us(response)) {
        log_error("weld_taos_sink: database %s precision is not us; "
                  "recreate the database with PRECISION 'us' or switch "
                  "to a new database name", g_state.db.c_str());
        log_error("weld_taos_sink: SHOW CREATE DATABASE response: %s",
            response.c_str());
        return -1;
    }

    g_state.db_precision_verified = true;
    return 0;
}

static int
ensure_database_ready(const taos_sink_config *cfg)
{
    if (tdengine_exec(g_state.url, g_state.auth_header,
            "CREATE DATABASE IF NOT EXISTS " + g_state.db +
                " PRECISION 'us'") != 0) {
        log_error("weld_taos_sink: create database failed");
        return -1;
    }

    if (!g_state.db_precision_verified &&
        ensure_database_precision_us() != 0) {
        return -1;
    }

    const std::string stable = cfg->stable != NULL ? cfg->stable : "";
    if (g_state.stables.find(stable) != g_state.stables.end()) {
        return 0;
    }

    const std::string ddl = build_create_stable_sql(g_state.db, stable);
    if (ddl.empty()) {
        log_error("weld_taos_sink: unsupported stable: %s", stable.c_str());
        return -1;
    }

    for (int i = 0; i < 20; ++i) {
        if (tdengine_exec(g_state.url, g_state.auth_header, ddl) == 0) {
            g_state.stables.insert(stable);
            return 0;
        }
        nng_msleep(100);
    }

    log_error("weld_taos_sink: create stable failed: %s", stable.c_str());
    return -1;
}

static int
validate_config_and_row(const taos_sink_config *cfg, const weld_taos_row *row)
{
    if (cfg == NULL || row == NULL || cfg->host == NULL || cfg->db == NULL ||
        row->stable == NULL) {
        return -1;
    }
    if (get_stable_kind(row->stable) == StableKind::Unknown) {
        log_error("weld_taos_sink: unsupported stable: %s", row->stable);
        return -1;
    }
    if (cfg->stable != NULL && cfg->stable[0] != '\0' &&
        strcmp(cfg->stable, row->stable) != 0) {
        log_error("weld_taos_sink: config stable mismatch: cfg=%s row=%s",
            cfg->stable, row->stable);
        return -1;
    }
    if (!taos_sink_valid_identifier(cfg->db) ||
        !taos_sink_valid_identifier(row->stable)) {
        log_error("weld_taos_sink: invalid db or stable identifier");
        return -1;
    }
    if ((get_stable_kind(row->stable) == StableKind::CurrentRaw ||
            get_stable_kind(row->stable) == StableKind::VoltageRaw) &&
        (row->has_window_start_us == 0 || row->has_sample_rate_hz == 0 ||
            row->has_point_count == 0 || row->encoding == NULL ||
            row->payload == NULL)) {
        log_error("weld_taos_sink: raw row missing required fields");
        return -1;
    }
    return 0;
}

static int
same_connection_target(const taos_sink_config *cfg)
{
    const int port = cfg->port > 0 ? cfg->port : 6041;
    std::string url = "http://" + std::string(cfg->host) + ":" +
                      std::to_string(port) + "/rest/sql";
    return g_state.url == url && g_state.auth_header == build_auth_header(cfg) &&
           g_state.db == cfg->db;
}

static size_t
choose_worker_count(void)
{
    load_runtime_config_once();
    const unsigned int detected = std::thread::hardware_concurrency();
    if (detected == 0) {
        return 2;
    }
    if ((size_t) detected < g_state.worker_max) {
        return (size_t) detected;
    }
    return g_state.worker_max;
}

static int
can_accept_rows_locked(size_t row_count)
{
    std::lock_guard<std::mutex> lock(g_state.mutex);
    if (row_count > g_state.queue_max) {
        return 0;
    }
    return g_state.queue.size() + row_count <= g_state.queue_max ? 1 : 0;
}

static int
start_locked(const taos_sink_config *cfg)
{
    if (cfg == NULL || cfg->host == NULL || cfg->db == NULL ||
        cfg->stable == NULL) {
        log_error("weld_taos_sink: invalid config");
        return -1;
    }

    if (g_state.started) {
        if (!same_connection_target(cfg)) {
            log_error(
                "weld_taos_sink: already started with different connection target");
            return -1;
        }
        return ensure_database_ready(cfg);
    }

    const int port = cfg->port > 0 ? cfg->port : 6041;
    g_state.url = "http://" + std::string(cfg->host) + ":" +
                  std::to_string(port) + "/rest/sql";
    g_state.auth_header = build_auth_header(cfg);
    g_state.db = cfg->db;

    if (ensure_database_ready(cfg) != 0) {
        g_state.url.clear();
        g_state.auth_header.clear();
        g_state.db.clear();
        g_state.stables.clear();
        g_state.db_precision_verified = false;
        return -1;
    }

    g_state.running = true;
    g_state.started = true;
    g_state.stats_last_snapshot = WeldStatsSnapshot {};
    g_state.stats_last_log_tp = std::chrono::steady_clock::now();
    g_state.stats_window_ready = true;
    const size_t worker_count = choose_worker_count();
    g_state.workers.clear();
    g_state.workers.reserve(worker_count);
    for (size_t worker_index = 0; worker_index < worker_count; ++worker_index) {
        g_state.workers.emplace_back([worker_index] {
            std::vector<WeldQueuedRow> batch;
            batch.reserve(g_state.batch_size);
            auto next_stats_log = std::chrono::steady_clock::now() +
                                  std::chrono::milliseconds(WELD_STATS_LOG_MS);

            for (;;) {
                {
                    std::unique_lock<std::mutex> lock(g_state.mutex);
                    g_state.cv.wait_for(lock,
                        std::chrono::milliseconds(g_state.flush_ms), [] {
                            return !g_state.queue.empty() || !g_state.running;
                        });

                    if (g_state.queue.empty() && !g_state.running) {
                        break;
                    }

                    while (!g_state.queue.empty() &&
                           batch.size() < g_state.batch_size) {
                        batch.push_back(std::move(g_state.queue.front()));
                        g_state.queue.pop_front();
                    }
                }

                if (!batch.empty()) {
                    (void) flush_rows_with_retry(
                        batch, WELD_RETRY_LIMIT, true);
                    batch.clear();
                }

                if (worker_index == 0) {
                    const auto now = std::chrono::steady_clock::now();
                    if (now >= next_stats_log) {
                        log_stats_snapshot();
                        next_stats_log =
                            now + std::chrono::milliseconds(WELD_STATS_LOG_MS);
                    }
                }
            }
        });
    }
    log_info("weld_taos_sink: started workers=%zu batch_size=%zu flush_ms=%d "
             "queue_max=%zu",
        worker_count, g_state.batch_size, g_state.flush_ms, g_state.queue_max);

    return 0;
}

static WeldQueuedRow
copy_row(const weld_taos_row *row)
{
    WeldQueuedRow out;
    out.stable           = row->stable != NULL ? row->stable : "";
    out.ts_us            = row->ts_us;
    out.recv_ts_us       = row->recv_ts_us;
    out.msg_id           = row->msg_id != NULL ? row->msg_id : "";
    out.seq              = row->seq;
    out.topic_name       = row->topic_name != NULL ? row->topic_name : "";
    out.spec_ver         = row->spec_ver != NULL ? row->spec_ver : "";
    out.has_task_id      = row->task_id != NULL && row->task_id[0] != '\0';
    out.task_id          = out.has_task_id ? row->task_id : "";
    out.has_temperature  = row->has_temperature != 0;
    out.temperature      = row->temperature;
    out.has_humidity     = row->has_humidity != 0;
    out.humidity         = row->humidity;
    out.has_instant_flow = row->has_instant_flow != 0;
    out.instant_flow     = row->instant_flow;
    out.has_total_flow   = row->has_total_flow != 0;
    out.total_flow       = row->total_flow;
    out.has_current      = row->has_current != 0;
    out.current          = row->current;
    out.has_voltage      = row->has_voltage != 0;
    out.voltage          = row->voltage;
    out.has_window_start_us = row->has_window_start_us != 0;
    out.window_start_us = row->window_start_us;
    out.has_sample_rate_hz = row->has_sample_rate_hz != 0;
    out.sample_rate_hz = row->sample_rate_hz;
    out.has_point_count = row->has_point_count != 0;
    out.point_count = row->point_count;
    out.has_encoding = row->encoding != NULL;
    out.encoding = row->encoding != NULL ? row->encoding : "";
    out.has_payload = row->payload != NULL;
    out.payload = row->payload != NULL ? row->payload : "";
    out.has_raw_adc_unit = row->raw_adc_unit != NULL;
    out.raw_adc_unit =
        row->raw_adc_unit != NULL ? row->raw_adc_unit : "";
    out.has_cal_version = row->cal_version != NULL;
    out.cal_version = row->cal_version != NULL ? row->cal_version : "";
    out.has_cal_k = row->has_cal_k != 0;
    out.cal_k = row->cal_k;
    out.has_cal_b = row->has_cal_b != 0;
    out.cal_b = row->cal_b;
    out.has_quality_code = row->has_quality_code != 0;
    out.quality_code = row->quality_code;
    out.has_quality_text = row->quality_text != NULL;
    out.quality_text =
        row->quality_text != NULL ? row->quality_text : "";
    out.has_source_bus = row->source_bus != NULL;
    out.source_bus = row->source_bus != NULL ? row->source_bus : "";
    out.has_source_port = row->source_port != NULL;
    out.source_port = row->source_port != NULL ? row->source_port : "";
    out.has_source_protocol = row->source_protocol != NULL;
    out.source_protocol =
        row->source_protocol != NULL ? row->source_protocol : "";
    out.has_collect_period_ms = row->has_collect_period_ms != 0;
    out.collect_period_ms = row->collect_period_ms;
    out.has_collect_timeout_ms = row->has_collect_timeout_ms != 0;
    out.collect_timeout_ms = row->collect_timeout_ms;
    out.has_collect_retries = row->has_collect_retries != 0;
    out.collect_retries = row->collect_retries;
    out.qos = row->qos;
    out.packet_id = row->packet_id;
    out.version = row->version != NULL ? row->version : "";
    out.site_id = row->site_id != NULL ? row->site_id : "";
    out.line_id = row->line_id != NULL ? row->line_id : "";
    out.station_id = row->station_id != NULL ? row->station_id : "";
    out.gateway_id = row->gateway_id != NULL ? row->gateway_id : "";
    out.device_id = row->device_id != NULL ? row->device_id : "";
    out.device_type = row->device_type != NULL ? row->device_type : "";
    out.device_model = row->device_model != NULL ? row->device_model : "";
    out.metric_group = row->metric_group != NULL ? row->metric_group : "";
    out.signal_type = row->signal_type != NULL ? row->signal_type : "";
    out.channel_id = row->channel_id != NULL ? row->channel_id : "";
    return out;
}

} // namespace

extern "C" int
weld_taos_sink_can_accept_rows_with_config(
    const taos_sink_config *cfg, size_t row_count)
{
    if (cfg == NULL || cfg->host == NULL || cfg->db == NULL ||
        cfg->stable == NULL || row_count == 0) {
        return -1;
    }
    if (get_stable_kind(cfg->stable) == StableKind::Unknown) {
        log_error("weld_taos_sink: unsupported stable: %s", cfg->stable);
        return -1;
    }
    if (!taos_sink_valid_identifier(cfg->db) ||
        !taos_sink_valid_identifier(cfg->stable)) {
        log_error("weld_taos_sink: invalid db or stable identifier");
        return -1;
    }

    std::lock_guard<std::mutex> guard(g_start_mutex);
    if (start_locked(cfg) != 0) {
        return -1;
    }
    return can_accept_rows_locked(row_count);
}

extern "C" int
weld_taos_sink_enqueue_rows_with_config(const taos_sink_config *cfg,
    const weld_taos_row *rows, size_t row_count)
{
    if (cfg == NULL || rows == NULL || row_count == 0) {
        return -1;
    }

    for (size_t i = 0; i < row_count; ++i) {
        if (validate_config_and_row(cfg, &rows[i]) != 0) {
            return -1;
        }
    }

    std::vector<WeldQueuedRow> copied;
    copied.reserve(row_count);
    for (size_t i = 0; i < row_count; ++i) {
        copied.push_back(copy_row(&rows[i]));
    }

    std::lock_guard<std::mutex> guard(g_start_mutex);
    taos_sink_config effective_cfg = *cfg;
    effective_cfg.stable = (cfg->stable != NULL && cfg->stable[0] != '\0')
                               ? cfg->stable
                               : rows[0].stable;
    if (start_locked(&effective_cfg) != 0) {
        return -1;
    }

    {
        std::lock_guard<std::mutex> lock(g_state.mutex);
        if (g_state.queue.size() + copied.size() > g_state.queue_max) {
            record_enqueue_drop(row_count);
            log_warn("weld_taos_sink: queue full, reject message (%zu rows, "
                     "queue=%zu, max=%zu)",
                row_count, g_state.queue.size(), g_state.queue_max);
            return -1;
        }
        for (WeldQueuedRow &row : copied) {
            g_state.queue.push_back(std::move(row));
        }
    }
    record_enqueue_success(row_count);
    g_state.cv.notify_all();
    return 0;
}

extern "C" int
weld_taos_sink_enqueue_row_with_config(
    const taos_sink_config *cfg, const weld_taos_row *row)
{
    if (row == NULL) {
        return -1;
    }
    return weld_taos_sink_enqueue_rows_with_config(cfg, row, 1);
}

extern "C" int
weld_taos_sink_is_started(void)
{
    std::lock_guard<std::mutex> guard(g_start_mutex);
    return g_state.started ? 1 : 0;
}

extern "C" void
weld_taos_sink_stop_all(void)
{
    std::lock_guard<std::mutex> guard(g_start_mutex);
    if (!g_state.started) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(g_state.mutex);
        g_state.running = false;
    }
    g_state.cv.notify_all();
    for (std::thread &worker : g_state.workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    g_state.workers.clear();

    if (!g_state.queue.empty()) {
        std::vector<WeldQueuedRow> rest;
        rest.reserve(g_state.queue.size());
        while (!g_state.queue.empty()) {
            rest.push_back(g_state.queue.front());
            g_state.queue.pop_front();
        }
        (void) flush_rows_with_retry(
            rest, WELD_STOP_FLUSH_RETRY_LIMIT, true);
    }

    log_stats_snapshot();
    g_state.url.clear();
    g_state.auth_header.clear();
    g_state.db.clear();
    g_state.stables.clear();
    g_state.db_precision_verified = false;
    g_state.queue.clear();
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
    g_state.stats_last_snapshot = WeldStatsSnapshot {};
    g_state.stats_last_log_tp = std::chrono::steady_clock::time_point {};
    g_state.stats_window_ready = false;
    g_state.started = false;
}
