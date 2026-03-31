#include "weld_telemetry_async.h"

#include <stdint.h>
#include <string.h>

#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "nng/supplemental/nanolib/log.h"
#include "weld_telemetry.h"

namespace {

constexpr size_t WELD_PARSE_WORKER_MAX = 4;
constexpr size_t WELD_PARSE_QUEUE_MAX_MSG = 256;
constexpr size_t WELD_PARSE_QUEUE_MAX_BYTES = 64 * 1024 * 1024;

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

struct AsyncState {
    std::deque<AsyncPublishTask> queue;
    std::vector<std::thread>     workers;
    std::mutex                   mutex;
    std::condition_variable      cv;
    size_t                       queued_bytes = 0;
    bool                         running = false;
    bool                         started = false;
};

AsyncState g_async_state;
std::mutex g_async_start_mutex;

static size_t
choose_worker_count(void)
{
    const unsigned int detected = std::thread::hardware_concurrency();
    if (detected == 0) {
        return 2;
    }
    return detected < WELD_PARSE_WORKER_MAX ? (size_t) detected :
                                              WELD_PARSE_WORKER_MAX;
}

static size_t
task_bytes(const AsyncPublishTask &task)
{
    return task.topic.size() + task.payload.size() + task.rule.host.size() +
           task.rule.username.size() + task.rule.password.size() +
           task.rule.db.size() + task.rule.table.size() + 256;
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
        log_error("weld_telemetry_async: async parse failed for topic=%s",
            task.topic.c_str());
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

    const size_t worker_count = choose_worker_count();
    g_async_state.workers.reserve(worker_count);
    for (size_t i = 0; i < worker_count; ++i) {
        g_async_state.workers.emplace_back([] {
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
            }
        });
    }

    log_info("weld_telemetry_async: started workers=%zu queue_max_msg=%zu "
             "queue_max_bytes=%zu",
        worker_count, WELD_PARSE_QUEUE_MAX_MSG, WELD_PARSE_QUEUE_MAX_BYTES);
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
        return -1;
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

    const size_t bytes = task_bytes(task);
    if (bytes > WELD_PARSE_QUEUE_MAX_BYTES) {
        log_warn("weld_telemetry_async: drop oversized message bytes=%zu topic=%s",
            bytes, task.topic.c_str());
        return -1;
    }

    std::lock_guard<std::mutex> guard(g_async_start_mutex);
    if (start_locked() != 0) {
        return -1;
    }

    {
        std::lock_guard<std::mutex> lock(g_async_state.mutex);
        if (g_async_state.queue.size() >= WELD_PARSE_QUEUE_MAX_MSG ||
            g_async_state.queued_bytes + bytes > WELD_PARSE_QUEUE_MAX_BYTES) {
            log_warn("weld_telemetry_async: queue full, drop message "
                     "(topic=%s queue_msg=%zu queue_bytes=%zu)",
                task.topic.c_str(), g_async_state.queue.size(),
                g_async_state.queued_bytes);
            return -1;
        }
        g_async_state.queued_bytes += bytes;
        g_async_state.queue.push_back(std::move(task));
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
    g_async_state.running = false;
    g_async_state.started = false;
}
