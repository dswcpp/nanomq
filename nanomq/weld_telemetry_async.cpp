#include "weld_telemetry_async.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "nng/supplemental/nanolib/log.h"
#include "weld_telemetry.h"

namespace {

constexpr size_t WELD_PARSE_WORKER_MAX_DEFAULT = 8;
constexpr size_t WELD_PARSE_QUEUE_MAX_MSG_DEFAULT = 2048;
constexpr size_t WELD_PARSE_QUEUE_MAX_BYTES_DEFAULT = 256 * 1024 * 1024;
constexpr double WELD_PARSE_QUEUE_PRESSURE_WARN_RATIO = 0.80;
constexpr auto WELD_PARSE_QUEUE_PRESSURE_WARN_INTERVAL =
    std::chrono::seconds(5);

struct AsyncRuleConfig {
    std::string host;
    int         port = 6041;
    std::string username;
    std::string password;
    std::string db;
    std::string table;
};

struct AsyncPublishTask {
    AsyncRuleConfig rule;
    std::string     topic;
    std::string     payload;
    uint8_t         qos = 0;
    uint16_t        packet_id = 0;
};

struct AsyncStatsSnapshot {
    size_t   queue_depth = 0;
    size_t   queue_bytes = 0;
    uint64_t enqueue_msg_count = 0;
    uint64_t drop_msg_count = 0;
    uint64_t parse_ok_count = 0;
    uint64_t parse_fail_count = 0;
};

struct AsyncState {
    std::deque<AsyncPublishTask> queue;
    std::vector<std::thread>     workers;
    std::mutex                   mutex;
    std::condition_variable      cv;
    size_t                       worker_max = WELD_PARSE_WORKER_MAX_DEFAULT;
    size_t                       queue_max_msg = WELD_PARSE_QUEUE_MAX_MSG_DEFAULT;
    size_t                       queue_max_bytes = WELD_PARSE_QUEUE_MAX_BYTES_DEFAULT;
    size_t                       queued_bytes = 0;
    uint64_t                     enqueue_msg_count = 0;
    uint64_t                     drop_msg_count = 0;
    uint64_t                     parse_ok_count = 0;
    uint64_t                     parse_fail_count = 0;
    AsyncStatsSnapshot           stats_last_snapshot;
    std::chrono::steady_clock::time_point stats_last_log_tp;
    std::chrono::steady_clock::time_point pressure_last_warn_tp;
    size_t                       queue_high_watermark_msg = 0;
    size_t                       queue_high_watermark_bytes = 0;
    bool                         stats_window_ready = false;
    bool                         running = false;
    bool                         started = false;
};

AsyncState g_async_state;
std::mutex g_async_start_mutex;

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
        log_warn("weld_telemetry_async: ignore invalid env %s=%s", name, raw);
        return default_value;
    }

    if (value < min_value) {
        log_warn(
            "weld_telemetry_async: env %s=%s below minimum %zu, clamp to %zu",
            name, raw, min_value, min_value);
        return min_value;
    }

    return (size_t) value;
}

static void
load_runtime_config_once(void)
{
    static std::once_flag loaded_once;
    std::call_once(loaded_once, [] {
        g_async_state.worker_max = read_env_size_with_min(
            "NANOMQ_WELD_PARSE_WORKER_MAX", WELD_PARSE_WORKER_MAX_DEFAULT, 1);
        g_async_state.queue_max_msg = read_env_size_with_min(
            "NANOMQ_WELD_PARSE_QUEUE_MAX_MSG",
            WELD_PARSE_QUEUE_MAX_MSG_DEFAULT, 1);
        g_async_state.queue_max_bytes = read_env_size_with_min(
            "NANOMQ_WELD_PARSE_QUEUE_MAX_BYTES",
            WELD_PARSE_QUEUE_MAX_BYTES_DEFAULT, 1024);
    });
}

static size_t
choose_worker_count(void)
{
    load_runtime_config_once();
    const unsigned int detected = std::thread::hardware_concurrency();
    if (detected == 0) {
        return 2;
    }
    return detected < g_async_state.worker_max ? (size_t) detected :
                                                 g_async_state.worker_max;
}

static size_t
task_bytes(const AsyncPublishTask &task)
{
    return task.topic.size() + task.payload.size() + task.rule.host.size() +
           task.rule.username.size() + task.rule.password.size() +
           task.rule.db.size() + task.rule.table.size() + 256;
}

static void
log_stats_snapshot(void)
{
    AsyncStatsSnapshot snapshot;
    AsyncStatsSnapshot delta;
    double             elapsed_sec = 0.0;
    const auto         now = std::chrono::steady_clock::now();

    {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        snapshot.queue_depth = g_async_state.queue.size();
        snapshot.queue_bytes = g_async_state.queued_bytes;
        snapshot.enqueue_msg_count = g_async_state.enqueue_msg_count;
        snapshot.drop_msg_count = g_async_state.drop_msg_count;
        snapshot.parse_ok_count = g_async_state.parse_ok_count;
        snapshot.parse_fail_count = g_async_state.parse_fail_count;

        if (g_async_state.stats_window_ready) {
            elapsed_sec = std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - g_async_state.stats_last_log_tp)
                              .count() /
                          1000.0;
            delta.enqueue_msg_count =
                snapshot.enqueue_msg_count -
                g_async_state.stats_last_snapshot.enqueue_msg_count;
            delta.drop_msg_count =
                snapshot.drop_msg_count -
                g_async_state.stats_last_snapshot.drop_msg_count;
            delta.parse_ok_count =
                snapshot.parse_ok_count -
                g_async_state.stats_last_snapshot.parse_ok_count;
            delta.parse_fail_count =
                snapshot.parse_fail_count -
                g_async_state.stats_last_snapshot.parse_fail_count;
        }

        g_async_state.stats_last_snapshot = snapshot;
        g_async_state.stats_last_log_tp = now;
        g_async_state.stats_window_ready = true;
    }

    size_t high_watermark_msg = 0;
    size_t high_watermark_bytes = 0;
    {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        high_watermark_msg = g_async_state.queue_high_watermark_msg;
        high_watermark_bytes = g_async_state.queue_high_watermark_bytes;
    }

    log_info("weld_telemetry_async: queue_msg=%zu queue_bytes=%zu "
             "enqueue_msg=%llu drop_msg=%llu parse_ok=%llu parse_fail=%llu "
             "high_watermark_msg=%zu high_watermark_bytes=%zu "
             "window_s=%.2f enqueue_rate=%.2f drop_rate=%.2f "
             "parse_ok_rate=%.2f parse_fail_rate=%.2f parse_total_rate=%.2f",
        snapshot.queue_depth, snapshot.queue_bytes,
        (unsigned long long) snapshot.enqueue_msg_count,
        (unsigned long long) snapshot.drop_msg_count,
        (unsigned long long) snapshot.parse_ok_count,
        (unsigned long long) snapshot.parse_fail_count,
        high_watermark_msg, high_watermark_bytes,
        elapsed_sec,
        elapsed_sec > 0.0 ? delta.enqueue_msg_count / elapsed_sec : 0.0,
        elapsed_sec > 0.0 ? delta.drop_msg_count / elapsed_sec : 0.0,
        elapsed_sec > 0.0 ? delta.parse_ok_count / elapsed_sec : 0.0,
        elapsed_sec > 0.0 ? delta.parse_fail_count / elapsed_sec : 0.0,
        elapsed_sec > 0.0 ?
            (delta.parse_ok_count + delta.parse_fail_count) / elapsed_sec :
            0.0);
}

static void
process_task(const AsyncPublishTask &task)
{
    rule_taos rule;

    memset(&rule, 0, sizeof(rule));

    rule.host = const_cast<char *>(task.rule.host.c_str());
    rule.port = task.rule.port;
    rule.username = const_cast<char *>(task.rule.username.c_str());
    rule.password = const_cast<char *>(task.rule.password.c_str());
    rule.db = const_cast<char *>(task.rule.db.c_str());
    rule.table = const_cast<char *>(task.rule.table.c_str());
    rule.parser = RULE_TAOS_PARSER_WELD_TELEMETRY;

    if (weld_telemetry_handle_publish_raw(&rule, task.topic.c_str(), task.qos,
            task.packet_id,
            reinterpret_cast<const uint8_t *>(task.payload.data()),
            (uint32_t) task.payload.size()) != 0) {
        {
            std::lock_guard<std::mutex> lock(g_async_state.mutex);
            g_async_state.parse_fail_count++;
        }
        log_error("weld_telemetry_async: async parse failed for topic=%s",
            task.topic.c_str());
    } else {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        g_async_state.parse_ok_count++;
    }
}

static int
start_locked(void)
{
    if (g_async_state.started) {
        return 0;
    }

    g_async_state.running = true;
    g_async_state.started = true;
    g_async_state.stats_last_snapshot = AsyncStatsSnapshot {};
    g_async_state.stats_last_log_tp = std::chrono::steady_clock::now();
    g_async_state.stats_window_ready = true;

    const size_t worker_count = choose_worker_count();
    g_async_state.workers.reserve(worker_count);
    for (size_t i = 0; i < worker_count; ++i) {
        g_async_state.workers.emplace_back([i] {
            auto next_stats_log = std::chrono::steady_clock::now() +
                                  std::chrono::seconds(30);
            for (;;) {
                AsyncPublishTask task;
                size_t           bytes = 0;
                {
                    std::unique_lock<std::mutex> lock(g_async_state.mutex);
                    g_async_state.cv.wait(lock, [] {
                        return !g_async_state.queue.empty() ||
                               !g_async_state.running;
                    });

                    if (g_async_state.queue.empty() && !g_async_state.running) {
                        break;
                    }

                    task = std::move(g_async_state.queue.front());
                    g_async_state.queue.pop_front();
                    bytes = task_bytes(task);
                    if (g_async_state.queued_bytes >= bytes) {
                        g_async_state.queued_bytes -= bytes;
                    } else {
                        g_async_state.queued_bytes = 0;
                    }
                }

                process_task(task);

                if (i == 0) {
                    const auto now = std::chrono::steady_clock::now();
                    if (now >= next_stats_log) {
                        log_stats_snapshot();
                        next_stats_log = now + std::chrono::seconds(30);
                    }
                }
            }
        });
    }

    log_info("weld_telemetry_async: started workers=%zu queue_max_msg=%zu "
             "queue_max_bytes=%zu env_worker_max=%s env_queue_max_msg=%s "
             "env_queue_max_bytes=%s",
        worker_count, g_async_state.queue_max_msg, g_async_state.queue_max_bytes,
        "NANOMQ_WELD_PARSE_WORKER_MAX",
        "NANOMQ_WELD_PARSE_QUEUE_MAX_MSG",
        "NANOMQ_WELD_PARSE_QUEUE_MAX_BYTES");
    return 0;
}

} // namespace

extern "C" int
weld_telemetry_async_enqueue(const rule_taos *taos_rule,
    const char *topic_name, uint8_t qos, uint16_t packet_id,
    const uint8_t *payload, uint32_t payload_len)
{
    if (taos_rule == NULL || taos_rule->host == NULL || taos_rule->db == NULL ||
        taos_rule->table == NULL || topic_name == NULL || payload == NULL ||
        payload_len == 0) {
        return WELD_TELEMETRY_ASYNC_ERR_INVALID_ARG;
    }

    AsyncPublishTask task;
    task.rule.host = taos_rule->host;
    task.rule.port = taos_rule->port > 0 ? taos_rule->port : 6041;
    task.rule.username = taos_rule->username != NULL ? taos_rule->username : "";
    task.rule.password = taos_rule->password != NULL ? taos_rule->password : "";
    task.rule.db = taos_rule->db;
    task.rule.table = taos_rule->table;
    task.topic = topic_name;
    task.payload.assign(
        reinterpret_cast<const char *>(payload), payload_len);
	task.qos = qos;
	task.packet_id = packet_id;

	load_runtime_config_once();
	const size_t bytes = task_bytes(task);
	if (bytes > g_async_state.queue_max_bytes) {
        log_warn("weld_telemetry_async: drop oversized message bytes=%zu topic=%s",
            bytes, task.topic.c_str());
        return WELD_TELEMETRY_ASYNC_ERR_OVERSIZED;
    }

    std::lock_guard<std::mutex> guard(g_async_start_mutex);
    if (start_locked() != 0) {
        return WELD_TELEMETRY_ASYNC_ERR_START_FAILED;
    }

    {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        if (g_async_state.queue.size() >= g_async_state.queue_max_msg ||
            g_async_state.queued_bytes + bytes > g_async_state.queue_max_bytes) {
            g_async_state.drop_msg_count++;
            log_warn("weld_telemetry_async: queue full, drop message "
                     "(topic=%s queue_msg=%zu queue_bytes=%zu queue_max_msg=%zu "
                     "queue_max_bytes=%zu)",
                task.topic.c_str(), g_async_state.queue.size(),
                g_async_state.queued_bytes, g_async_state.queue_max_msg,
                g_async_state.queue_max_bytes);
            return WELD_TELEMETRY_ASYNC_ERR_QUEUE_FULL;
        }
        g_async_state.enqueue_msg_count++;
        g_async_state.queued_bytes += bytes;
        g_async_state.queue.push_back(std::move(task));
        if (g_async_state.queue.size() > g_async_state.queue_high_watermark_msg) {
            g_async_state.queue_high_watermark_msg = g_async_state.queue.size();
        }
        if (g_async_state.queued_bytes > g_async_state.queue_high_watermark_bytes) {
            g_async_state.queue_high_watermark_bytes = g_async_state.queued_bytes;
        }
        const bool msgPressure =
            g_async_state.queue_max_msg > 0 &&
            g_async_state.queue.size() >= static_cast<size_t>(
                g_async_state.queue_max_msg * WELD_PARSE_QUEUE_PRESSURE_WARN_RATIO);
        const bool bytePressure =
            g_async_state.queue_max_bytes > 0 &&
            g_async_state.queued_bytes >= static_cast<size_t>(
                g_async_state.queue_max_bytes * WELD_PARSE_QUEUE_PRESSURE_WARN_RATIO);
        const auto now = std::chrono::steady_clock::now();
        if ((msgPressure || bytePressure) &&
            (g_async_state.pressure_last_warn_tp == std::chrono::steady_clock::time_point{} ||
             now - g_async_state.pressure_last_warn_tp >=
                 WELD_PARSE_QUEUE_PRESSURE_WARN_INTERVAL)) {
            g_async_state.pressure_last_warn_tp = now;
            log_warn("weld_telemetry_async: queue pressure topic=%s queue_msg=%zu/%zu "
                     "queue_bytes=%zu/%zu high_watermark_msg=%zu "
                     "high_watermark_bytes=%zu",
                g_async_state.queue.back().topic.c_str(),
                g_async_state.queue.size(), g_async_state.queue_max_msg,
                g_async_state.queued_bytes, g_async_state.queue_max_bytes,
                g_async_state.queue_high_watermark_msg,
                g_async_state.queue_high_watermark_bytes);
        }
    }

    g_async_state.cv.notify_one();
    return 0;
}

extern "C" void
weld_telemetry_async_stop_all(void)
{
    std::lock_guard<std::mutex> guard(g_async_start_mutex);
    if (!g_async_state.started) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        g_async_state.running = false;
    }
    g_async_state.cv.notify_all();

    for (std::thread &worker : g_async_state.workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    g_async_state.workers.clear();
    g_async_state.queue.clear();
    g_async_state.queued_bytes = 0;
    g_async_state.enqueue_msg_count = 0;
    g_async_state.drop_msg_count = 0;
    g_async_state.parse_ok_count = 0;
    g_async_state.parse_fail_count = 0;
    g_async_state.stats_last_snapshot = AsyncStatsSnapshot {};
    g_async_state.stats_last_log_tp = std::chrono::steady_clock::time_point {};
    g_async_state.pressure_last_warn_tp = std::chrono::steady_clock::time_point {};
    g_async_state.queue_high_watermark_msg = 0;
    g_async_state.queue_high_watermark_bytes = 0;
    g_async_state.stats_window_ready = false;
    g_async_state.running = false;
    g_async_state.started = false;
}
