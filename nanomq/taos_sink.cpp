#include "taos_sink.hpp"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <ctype.h>
#include <mutex>

#include "nng/nng.h"
#include "nng/supplemental/http/http.h"
#include "nng/supplemental/util/platform.h"
#include "nng/supplemental/nanolib/log.h"

#define TAOS_BATCH_SIZE    100
#define TAOS_FLUSH_MS      100

// ---------- 队列元素（深拷贝） ----------

typedef struct {
    int      qos;
    int      packet_id;
    char    *topic;
    char    *client_id;
    char    *username;
    char    *payload;
    size_t   payload_len;
    int64_t  timestamp_ms;
} taos_queued_item;

// ---------- 简易动态数组 ----------

typedef struct {
    taos_queued_item *data;
    size_t            len;
    size_t            cap;
} taos_queue_t;

static void taos_queue_init(taos_queue_t *q)
{
    q->data = NULL;
    q->len  = 0;
    q->cap  = 0;
}

static int taos_queue_push(taos_queue_t *q, const taos_queued_item *item)
{
    if (q->len >= q->cap) {
        size_t new_cap = q->cap ? q->cap * 2 : 16;
        taos_queued_item *p = (taos_queued_item *) realloc(
            q->data, new_cap * sizeof(taos_queued_item));
        if (!p) return -1;
        q->data = p;
        q->cap  = new_cap;
    }
    q->data[q->len++] = *item;
    return 0;
}

static void taos_queue_remove_front(taos_queue_t *q, size_t count)
{
    if (count >= q->len) {
        q->len = 0;
        return;
    }
    size_t remain = q->len - count;
    memmove(q->data, q->data + count, remain * sizeof(taos_queued_item));
    q->len = remain;
}

static void taos_queue_free(taos_queue_t *q)
{
    free(q->data);
    q->data = NULL;
    q->len  = 0;
    q->cap  = 0;
}

// ---------- Base64 编码（用于 HTTP Basic Auth） ----------

static const char b64_table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static char *base64_encode(const char *src, size_t src_len)
{
    size_t out_len = 4 * ((src_len + 2) / 3) + 1;
    char *out = (char *) malloc(out_len);
    if (!out) return NULL;

    size_t i = 0, j = 0;
    while (i + 2 < src_len) {
        unsigned char a = (unsigned char) src[i++];
        unsigned char b = (unsigned char) src[i++];
        unsigned char c = (unsigned char) src[i++];
        out[j++] = b64_table[a >> 2];
        out[j++] = b64_table[((a & 0x03) << 4) | (b >> 4)];
        out[j++] = b64_table[((b & 0x0F) << 2) | (c >> 6)];
        out[j++] = b64_table[c & 0x3F];
    }
    if (i < src_len) {
        unsigned char a = (unsigned char) src[i++];
        bool has_second_byte = i < src_len;
        unsigned char b = has_second_byte ? (unsigned char) src[i++] : 0;
        out[j++] = b64_table[a >> 2];
        out[j++] = b64_table[((a & 0x03) << 4) | (b >> 4)];
        out[j++] = has_second_byte ? b64_table[(b & 0x0F) << 2] : '=';
        out[j++] = '=';
    }
    out[j] = '\0';
    return out;
}

// 构造 "Basic <base64(user:pass)>"
static char *build_auth_header(const char *user, const char *pass)
{
    size_t ulen = strlen(user);
    size_t plen = strlen(pass);
    size_t cred_len = ulen + 1 + plen;
    char *cred = (char *) malloc(cred_len + 1);
    if (!cred) return NULL;
    snprintf(cred, cred_len + 1, "%s:%s", user, pass);

    char *b64 = base64_encode(cred, cred_len);
    free(cred);
    if (!b64) return NULL;

    size_t hdr_len = 6 + strlen(b64) + 1; // "Basic " + b64 + \0
    char *hdr = (char *) malloc(hdr_len);
    if (!hdr) { free(b64); return NULL; }
    snprintf(hdr, hdr_len, "Basic %s", b64);
    free(b64);
    return hdr;
}

// ---------- 向 TDengine REST 接口发送一条 SQL（nng HTTP client） ----------

static int tdengine_exec(const char *url_str, const char *auth_hdr,
                         const char *sql)
{
    nng_url         *url    = NULL;
    nng_http_client *client = NULL;
    nng_http_req    *req    = NULL;
    nng_http_res    *res    = NULL;
    nng_aio         *aio    = NULL;
    int              rc     = -1;

    int rv;
    if ((rv = nng_url_parse(&url, url_str)) != 0) {
        log_error("taos_sink: url parse failed: %s", nng_strerror(rv));
        return -1;
    }
    if ((rv = nng_http_client_alloc(&client, url)) != 0) {
        log_error("taos_sink: client alloc failed: %s", nng_strerror(rv));
        goto out;
    }
    if ((rv = nng_http_req_alloc(&req, url)) != 0) {
        log_error("taos_sink: req alloc failed: %s", nng_strerror(rv));
        goto out;
    }
    if ((rv = nng_http_res_alloc(&res)) != 0) {
        log_error("taos_sink: res alloc failed: %s", nng_strerror(rv));
        goto out;
    }
    if ((rv = nng_aio_alloc(&aio, NULL, NULL)) != 0) {
        log_error("taos_sink: aio alloc failed: %s", nng_strerror(rv));
        goto out;
    }

    nng_http_req_set_method(req, "POST");
    nng_http_req_set_header(req, "Authorization", auth_hdr);
    nng_http_req_set_header(req, "Content-Type", "text/plain");
    nng_http_req_copy_data(req, sql, strlen(sql));

    nng_aio_set_timeout(aio, 5000);
    nng_http_client_transact(client, req, res, aio);
    nng_aio_wait(aio);

    if ((rv = nng_aio_result(aio)) != 0) {
        log_error("taos_sink: http transact failed: %s", nng_strerror(rv));
        goto out;
    }

    {
        uint16_t status = nng_http_res_get_status(res);
        void    *body   = NULL;
        size_t   body_len = 0;
        nng_http_res_get_data(res, &body, &body_len);

        if (status != NNG_HTTP_STATUS_OK) {
            log_error("taos_sink: HTTP %d body: %.*s",
                      status, (int) body_len, body ? (char *) body : "(null)");
            goto out;
        }

        if (!body || body_len == 0) {
            log_error("taos_sink: empty response");
            goto out;
        }

        char *body_str = (char *) malloc(body_len + 1);
        if (body_str) {
            memcpy(body_str, body, body_len);
            body_str[body_len] = '\0';
            if (strstr(body_str, "\"code\":0") != NULL) {
                rc = 0;
            } else {
                log_error("taos_sink: TDengine error: %s", body_str);
            }
            free(body_str);
        }
    }

out:
    if (aio)    nng_aio_free(aio);
    if (res)    nng_http_res_free(res);
    if (req)    nng_http_req_free(req);
    if (client) nng_http_client_free(client);
    if (url)    nng_url_free(url);
    return rc;
}

// ---------- 标识符校验 ----------

int taos_sink_valid_identifier(const char *name)
{
    if (!name || !name[0]) return 0;
    for (const char *p = name; *p; p++) {
        if (!((*p >= 'a' && *p <= 'z') ||
              (*p >= 'A' && *p <= 'Z') ||
              (*p >= '0' && *p <= '9') ||
              *p == '_')) {
            return 0;
        }
    }
    return 1;
}

// ---------- 字符串转义（单引号 + 反斜杠） ----------

static void sql_escape_bytes(const char *src, size_t src_len,
                             char *dst, size_t dst_size)
{
    if (!src || src_len == 0 || dst_size == 0) {
        if (dst_size > 0) dst[0] = '\0';
        return;
    }
    size_t j = 0;
    for (size_t i = 0; i < src_len && j + 2 < dst_size; i++) {
        if (src[i] == '\'' || src[i] == '\\') {
            dst[j++] = '\\';
        }
        dst[j++] = src[i];
    }
    dst[j] = '\0';
}

static void sql_escape_str(const char *src, char *dst, size_t dst_size)
{
    if (!src) {
        if (dst_size > 0) dst[0] = '\0';
        return;
    }
    sql_escape_bytes(src, strlen(src), dst, dst_size);
}

// ---------- payload Hex 编码 ----------

static const char hex_chars[] = "0123456789ABCDEF";

static void payload_to_hex(const char *src, size_t src_len,
                           char *dst, size_t dst_size)
{
    if (!src || src_len == 0 || dst_size < 3) {
        if (dst_size > 0) dst[0] = '\0';
        return;
    }
    size_t j = 0;
    size_t max_bytes = (dst_size - 1) / 2;
    size_t n = src_len < max_bytes ? src_len : max_bytes;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char) src[i];
        dst[j++] = hex_chars[c >> 4];
        dst[j++] = hex_chars[c & 0x0F];
    }
    dst[j] = '\0';
}

// ---------- 深拷贝辅助 ----------

static char *safe_strdup(const char *s)
{
    if (!s) return NULL;
    size_t len = strlen(s);
    char *d = (char *) malloc(len + 1);
    if (d) memcpy(d, s, len + 1);
    return d;
}

static void taos_item_free(taos_queued_item *item)
{
    free(item->topic);
    free(item->client_id);
    free(item->username);
    free(item->payload);
}

static int taos_item_deep_copy(taos_queued_item *dst, const taos_rule_result *src)
{
    memset(dst, 0, sizeof(*dst));

    dst->qos          = src->qos;
    dst->packet_id    = src->packet_id;
    dst->timestamp_ms = src->timestamp_ms > 0
                            ? src->timestamp_ms
                            : (int64_t) time(NULL) * 1000LL;

    dst->topic = safe_strdup(src->topic);
    if (src->topic && !dst->topic) {
        taos_item_free(dst);
        return -1;
    }

    dst->client_id = safe_strdup(src->client_id);
    if (src->client_id && !dst->client_id) {
        taos_item_free(dst);
        return -1;
    }

    dst->username = safe_strdup(src->username);
    if (src->username && !dst->username) {
        taos_item_free(dst);
        return -1;
    }

    if (src->payload && src->payload_len > 0) {
        size_t copy_len = src->payload_len < 1024 ? src->payload_len : 1024;
        if (src->payload_len > 1024) {
            log_warn("taos_sink: payload truncated from %zu to 1024 bytes",
                     src->payload_len);
        }
        dst->payload = (char *) malloc(copy_len);
        if (!dst->payload) {
            taos_item_free(dst);
            return -1;
        }
        memcpy(dst->payload, src->payload, copy_len);
        dst->payload_len = copy_len;
    }

    return 0;
}

// ---------- 全局 sink 状态 ----------

static struct {
    char       *url;         // "http://host:port/rest/sql"
    char       *auth_header; // "Basic <base64>"
    char       *db;
    char       *stable;

    nng_mtx    *mtx;
    nng_cv     *cv;
    nng_thread *thread;

    taos_queue_t queue;
    bool         running;
    bool         started;    // 防止 double-start/stop
} g_taos;

static std::mutex g_taos_start_mtx;

static void taos_sink_reset_partial_state(void)
{
    taos_queue_free(&g_taos.queue);
    free(g_taos.url);
    free(g_taos.auth_header);
    free(g_taos.db);
    free(g_taos.stable);
    g_taos.url         = NULL;
    g_taos.auth_header = NULL;
    g_taos.db          = NULL;
    g_taos.stable      = NULL;
    g_taos.mtx         = NULL;
    g_taos.cv          = NULL;
    g_taos.thread      = NULL;
    g_taos.running     = false;
    g_taos.started     = false;
}

// ---------- 子表名生成 ----------

static void make_sub_table(char *buf, size_t buf_size,
                           const char *stable, const char *client_id)
{
    if (client_id && client_id[0]) {
        int n = snprintf(buf, buf_size, "%s_", stable);
        for (const char *p = client_id;
             *p && n < (int)buf_size - 1; p++) {
            buf[n++] = (isalnum((unsigned char)*p)) ? *p : '_';
        }
        buf[n] = '\0';
    } else {
        snprintf(buf, buf_size, "%s_default", stable);
    }
}

// ---------- 动态 buffer ----------

typedef struct {
    char  *data;
    size_t len;
    size_t cap;
} dyn_buf;

static void dyn_buf_init(dyn_buf *b)
{
    b->data = NULL;
    b->len  = 0;
    b->cap  = 0;
}

// ---------- 批量 INSERT SQL 构造 ----------

static char *build_batch_insert_sql(taos_queued_item *items, size_t count,
                                    const char *db, const char *stable)
{
    size_t est = 256 + count * 2048;
    dyn_buf sql;
    dyn_buf_init(&sql);

    sql.data = (char *) malloc(est);
    if (!sql.data) return NULL;
    sql.cap = est;
    sql.len = 0;

    int n = snprintf(sql.data, sql.cap, "INSERT INTO ");
    sql.len = (size_t) n;

    for (size_t i = 0; i < count; i++) {
        taos_queued_item *item = &items[i];

        char sub_table[192];
        make_sub_table(sub_table, sizeof(sub_table), stable, item->client_id);

        size_t cid_len   = item->client_id ? strlen(item->client_id) : 0;
        size_t topic_len = item->topic     ? strlen(item->topic) : 0;
        size_t uname_len = item->username  ? strlen(item->username) : 0;
        size_t hex_size  = item->payload_len * 2 + 1;

        size_t esc_cid_sz   = cid_len * 2 + 1;
        size_t esc_topic_sz = topic_len * 2 + 1;
        size_t esc_uname_sz = uname_len * 2 + 1;

        size_t total_esc = esc_cid_sz + esc_topic_sz + esc_uname_sz + hex_size;
        char *esc_buf = (char *) calloc(1, total_esc);
        if (!esc_buf) {
            free(sql.data);
            return NULL;
        }

        char *esc_cid   = esc_buf;
        char *esc_topic = esc_cid + esc_cid_sz;
        char *esc_uname = esc_topic + esc_topic_sz;
        char *hex_pay   = esc_uname + esc_uname_sz;

        sql_escape_str(item->client_id, esc_cid,   esc_cid_sz);
        sql_escape_str(item->topic,     esc_topic,  esc_topic_sz);
        sql_escape_str(item->username,  esc_uname,  esc_uname_sz);

        if (item->payload_len > 0) {
            payload_to_hex(item->payload, item->payload_len, hex_pay, hex_size);
        }

        // 动态构造单条记录片段（避免栈缓冲区截断）
        size_t frag_est = strlen(db) * 2 + sizeof(sub_table)
                        + strlen(stable) + esc_cid_sz + esc_topic_sz
                        + esc_uname_sz + hex_size + 256;
        char *frag = (char *) malloc(frag_est);
        if (!frag) {
            free(esc_buf);
            free(sql.data);
            return NULL;
        }

        int frag_len = snprintf(frag, frag_est,
            "%s.%s USING %s.%s TAGS('%s') "
            "VALUES(%lld, %d, %d, '%s', '%s', '%s') ",
            db, sub_table,
            db, stable, esc_cid,
            (long long) item->timestamp_ms,
            item->qos, item->packet_id,
            esc_topic, esc_uname, hex_pay);

        free(esc_buf);

        size_t need = sql.len + (size_t) frag_len + 1;
        if (need > sql.cap) {
            size_t new_cap = sql.cap * 2;
            while (new_cap < need) new_cap *= 2;
            char *p = (char *) realloc(sql.data, new_cap);
            if (!p) {
                free(sql.data);
                free(frag);
                return NULL;
            }
            sql.data = p;
            sql.cap  = new_cap;
        }
        memcpy(sql.data + sql.len, frag, (size_t) frag_len);
        sql.len += (size_t) frag_len;
        sql.data[sql.len] = '\0';
        free(frag);
    }

    return sql.data;
}

// ---------- 批量 flush ----------

static void taos_sink_flush(taos_queued_item *items, size_t count)
{
    if (count == 0) return;

    char *sql = build_batch_insert_sql(items, count,
                                       g_taos.db, g_taos.stable);
    if (!sql) {
        log_error("taos_sink: build batch sql failed");
        return;
    }

    if (tdengine_exec(g_taos.url, g_taos.auth_header, sql) != 0) {
        log_error("taos_sink: batch insert failed (%zu items)", count);
    }

    free(sql);
}

// ---------- 消费者线程主循环 ----------

static void taos_sink_flush_thread(void *arg)
{
    (void) arg;

    while (true) {
        nng_mtx_lock(g_taos.mtx);

        while (g_taos.queue.len == 0 && g_taos.running) {
            nng_time deadline = nng_clock() + TAOS_FLUSH_MS;
            int rv = nng_cv_until(g_taos.cv, deadline);
            if (rv == NNG_ETIMEDOUT) break;
        }

        size_t qlen = g_taos.queue.len;
        bool   stop = !g_taos.running;

        if (qlen == 0 && stop) {
            nng_mtx_unlock(g_taos.mtx);
            break;
        }

        if (qlen == 0) {
            nng_mtx_unlock(g_taos.mtx);
            continue;
        }

        size_t take = qlen < TAOS_BATCH_SIZE ? qlen : TAOS_BATCH_SIZE;
        taos_queued_item *batch =
            (taos_queued_item *) malloc(take * sizeof(taos_queued_item));
        if (!batch) {
            nng_mtx_unlock(g_taos.mtx);
            log_error("taos_sink: malloc batch failed");
            nng_msleep(10);
            continue;
        }

        memcpy(batch, g_taos.queue.data, take * sizeof(taos_queued_item));
        taos_queue_remove_front(&g_taos.queue, take);

        nng_mtx_unlock(g_taos.mtx);

        taos_sink_flush(batch, take);

        for (size_t i = 0; i < take; i++) {
            taos_item_free(&batch[i]);
        }
        free(batch);
    }
}

// ---------- 初始化：建库建表 ----------

static int taos_sink_init_db(const char *url, const char *auth_hdr,
                             const char *db, const char *stable)
{
    size_t sql_len = strlen(db) + 64;
    char *sql = (char *) malloc(sql_len);
    if (!sql) return -1;

    snprintf(sql, sql_len, "CREATE DATABASE IF NOT EXISTS %s", db);
    if (tdengine_exec(url, auth_hdr, sql) != 0) {
        log_error("taos_sink: create database failed");
        free(sql);
        return -1;
    }
    free(sql);

    sql_len = strlen(db) + strlen(stable) + 256;
    sql = (char *) malloc(sql_len);
    if (!sql) return -1;

    snprintf(sql, sql_len,
        "CREATE STABLE IF NOT EXISTS %s.%s ("
        "ts TIMESTAMP,"
        "qos INT,"
        "packet_id INT,"
        "topic_name NCHAR(256),"
        "user_name NCHAR(256),"
        "payload_text NCHAR(2048)"
        ") TAGS (client_id NCHAR(128))",
        db, stable);

    for (int i = 0; i < 20; i++) {
        if (tdengine_exec(url, auth_hdr, sql) == 0) {
            free(sql);
            log_info("taos_sink: init ok: %s.%s", db, stable);
            return 0;
        }
        nng_msleep(100);
    }
    free(sql);
    log_error("taos_sink: create stable failed");
    return -1;
}

// ---------- 公共接口 ----------

static int taos_sink_same_target(const taos_sink_config *cfg)
{
    if (!cfg || !g_taos.db || !g_taos.stable) {
        return 0;
    }

    return strcmp(g_taos.db, cfg->db) == 0 &&
           strcmp(g_taos.stable, cfg->stable) == 0;
}

int taos_sink_start(const taos_sink_config *cfg)
{
    std::lock_guard<std::mutex> guard(g_taos_start_mtx);

    if (g_taos.started) {
        if (taos_sink_same_target(cfg)) {
            log_warn("taos_sink: already started, ignoring duplicate start");
            return 0;
        }
        log_error("taos_sink: already started with different target");
        return -1;
    }

    if (!cfg || !cfg->host || !cfg->db || !cfg->stable) {
        log_error("taos_sink: invalid config");
        return -1;
    }

    if (!taos_sink_valid_identifier(cfg->db)) {
        log_error("taos_sink: invalid db name: %s", cfg->db);
        return -1;
    }
    if (!taos_sink_valid_identifier(cfg->stable)) {
        log_error("taos_sink: invalid stable name: %s", cfg->stable);
        return -1;
    }

    int port = cfg->port > 0 ? cfg->port : 6041;
    const char *user = cfg->username ? cfg->username : "root";
    const char *pass = cfg->password ? cfg->password : "taosdata";

    // 构造 URL
    size_t url_len = strlen(cfg->host) + 32;
    g_taos.url = (char *) malloc(url_len);
    if (!g_taos.url) return -1;
    snprintf(g_taos.url, url_len, "http://%s:%d/rest/sql", cfg->host, port);

    // 构造 Authorization header
    g_taos.auth_header = build_auth_header(user, pass);
    if (!g_taos.auth_header) {
        free(g_taos.url);
        g_taos.url = NULL;
        return -1;
    }

    g_taos.db      = safe_strdup(cfg->db);
    g_taos.stable  = safe_strdup(cfg->stable);
    if (!g_taos.db || !g_taos.stable) {
        taos_sink_reset_partial_state();
        return -1;
    }
    taos_queue_init(&g_taos.queue);

    // 建库建表
    if (taos_sink_init_db(g_taos.url, g_taos.auth_header,
                          g_taos.db, g_taos.stable) != 0) {
        taos_sink_reset_partial_state();
        return -1;
    }

    g_taos.running = true;

    // 创建同步原语
    int rv;
    if ((rv = nng_mtx_alloc(&g_taos.mtx)) != 0) {
        log_error("taos_sink: nng_mtx_alloc: %s", nng_strerror(rv));
        taos_sink_reset_partial_state();
        return -1;
    }
    if ((rv = nng_cv_alloc(&g_taos.cv, g_taos.mtx)) != 0) {
        log_error("taos_sink: nng_cv_alloc: %s", nng_strerror(rv));
        nng_mtx_free(g_taos.mtx);
        g_taos.mtx = NULL;
        taos_sink_reset_partial_state();
        return -1;
    }

    // 启动消费者线程
    if ((rv = nng_thread_create(&g_taos.thread, taos_sink_flush_thread, NULL)) != 0) {
        log_error("taos_sink: nng_thread_create: %s", nng_strerror(rv));
        nng_cv_free(g_taos.cv);
        nng_mtx_free(g_taos.mtx);
        g_taos.cv = NULL;
        g_taos.mtx = NULL;
        taos_sink_reset_partial_state();
        return -1;
    }

    log_info("taos_sink: background thread started");
    g_taos.started = true;
    return 0;
}

int taos_sink_enqueue(const taos_rule_result *result)
{
    if (!result) return -1;

    nng_mtx *mtx = NULL;
    nng_cv  *cv  = NULL;
    bool     started = false;

    {
        std::lock_guard<std::mutex> guard(g_taos_start_mtx);
        started = g_taos.started;
        mtx     = g_taos.mtx;
        cv      = g_taos.cv;
    }

    if (!started || !mtx || !cv) return -1;

    taos_queued_item item;
    if (taos_item_deep_copy(&item, result) != 0) {
        log_error("taos_sink: deep copy failed");
        return -1;
    }

    int rv = 0;
    nng_mtx_lock(mtx);
    if (taos_queue_push(&g_taos.queue, &item) != 0) {
        rv = -1;
    } else if (g_taos.queue.len >= TAOS_BATCH_SIZE) {
        nng_cv_wake(cv);
    }
    nng_mtx_unlock(mtx);

    if (rv != 0) {
        taos_item_free(&item);
        log_error("taos_sink: queue push failed");
        return -1;
    }

    return 0;
}

int taos_sink_is_started(void)
{
    std::lock_guard<std::mutex> guard(g_taos_start_mtx);
    return g_taos.started ? 1 : 0;
}

void taos_sink_stop(void)
{
    std::lock_guard<std::mutex> guard(g_taos_start_mtx);

    if (!g_taos.started) {
        return;
    }
    g_taos.started = false;

    nng_mtx_lock(g_taos.mtx);
    g_taos.running = false;
    nng_cv_wake(g_taos.cv);
    nng_mtx_unlock(g_taos.mtx);

    nng_thread_destroy(g_taos.thread);

    // 线程已退出，此时不会有并发访问队列
    if (g_taos.queue.len > 0) {
        taos_sink_flush(g_taos.queue.data, g_taos.queue.len);
        for (size_t i = 0; i < g_taos.queue.len; i++) {
            taos_item_free(&g_taos.queue.data[i]);
        }
    }
    taos_queue_free(&g_taos.queue);

    nng_cv_free(g_taos.cv);
    nng_mtx_free(g_taos.mtx);

    free(g_taos.url);
    free(g_taos.auth_header);
    free(g_taos.db);
    free(g_taos.stable);

    g_taos.url         = NULL;
    g_taos.auth_header = NULL;
    g_taos.db          = NULL;
    g_taos.stable      = NULL;
    g_taos.mtx         = NULL;
    g_taos.cv          = NULL;
    g_taos.thread      = NULL;
    g_taos.running     = false;

    log_info("taos_sink: stopped");
}
