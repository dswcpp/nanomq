#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "../conf_api.c"

int
log_init(conf_log *log)
{
    (void) log;
    return 0;
}

int
log_fini(conf_log *log)
{
    (void) log;
    return 0;
}

static char *
dup_string(const char *s)
{
    return s == NULL ? NULL : nng_strdup(s);
}

static conf_http_header *
alloc_header_slot(void)
{
    conf_http_header *header = nng_zalloc(sizeof(*header));
    assert(header != NULL);
    return header;
}

static void
free_tls_members(conf_tls *tls)
{
    if (tls == NULL) {
        return;
    }
    nng_strfree(tls->url);
    nng_strfree(tls->ca);
    nng_strfree(tls->cert);
    nng_strfree(tls->key);
    nng_strfree(tls->key_password);
    memset(tls, 0, sizeof(*tls));
}

static void
free_auth_members(conf_auth *auth)
{
    if (auth == NULL) {
        return;
    }
    for (size_t i = 0; i < auth->count; ++i) {
        nng_strfree(auth->usernames[i]);
        nng_strfree(auth->passwords[i]);
    }
    cvector_free(auth->usernames);
    cvector_free(auth->passwords);
    auth->usernames = NULL;
    auth->passwords = NULL;
    auth->count = 0;
    if (auth->mtx != NULL) {
        nng_mtx_free(auth->mtx);
        auth->mtx = NULL;
    }
}

static void
free_auth_req_members(conf_auth_http_req *req)
{
    if (req == NULL) {
        return;
    }
    nng_strfree(req->url);
    nng_strfree(req->method);
    for (size_t i = 0; i < req->header_count; ++i) {
        if (req->headers[i] != NULL) {
            nng_strfree(req->headers[i]->key);
            nng_strfree(req->headers[i]->value);
            nng_free(req->headers[i], sizeof(conf_http_header));
        }
    }
    free(req->headers);
    for (size_t i = 0; i < req->param_count; ++i) {
        if (req->params[i] != NULL) {
            nng_strfree(req->params[i]->name);
            nng_free(req->params[i], sizeof(conf_http_param));
        }
    }
    free(req->params);
    free_tls_members(&req->tls);
    memset(req, 0, sizeof(*req));
}

static void
free_auth_http_members(conf_auth_http *auth)
{
    if (auth == NULL) {
        return;
    }
    free_auth_req_members(&auth->auth_req);
    free_auth_req_members(&auth->acl_req);
    free_auth_req_members(&auth->super_req);
}

static void
free_http_members(conf_http_server *http)
{
    if (http == NULL) {
        return;
    }
    nng_strfree(http->ip_addr);
    nng_strfree(http->username);
    nng_strfree(http->password);
    memset(http, 0, sizeof(*http));
}

static void
free_websocket_members(conf_websocket *ws)
{
    if (ws == NULL) {
        return;
    }
    nng_strfree(ws->url);
    nng_strfree(ws->tls_url);
    memset(ws, 0, sizeof(*ws));
}

static void
free_sqlite_members(conf_sqlite *sqlite)
{
    if (sqlite == NULL) {
        return;
    }
    nng_strfree(sqlite->mounted_file_path);
    memset(sqlite, 0, sizeof(*sqlite));
}

static topics *
make_topic(const char *remote, const char *local, const char *prefix,
    const char *suffix, uint8_t qos)
{
    topics *topic = nng_zalloc(sizeof(*topic));
    assert(topic != NULL);
    topic->remote_topic = dup_string(remote);
    topic->local_topic = dup_string(local);
    topic->prefix = dup_string(prefix);
    topic->suffix = dup_string(suffix);
    topic->qos = qos;
    return topic;
}

static void
free_topic(topics *topic)
{
    if (topic == NULL) {
        return;
    }
    nng_strfree(topic->remote_topic);
    nng_strfree(topic->local_topic);
    nng_strfree(topic->prefix);
    nng_strfree(topic->suffix);
    nng_free(topic, sizeof(*topic));
}

static void
free_user_properties(conf_user_property **props, size_t count)
{
    if (props == NULL) {
        return;
    }
    for (size_t i = 0; i < count; ++i) {
        if (props[i] != NULL) {
            nng_strfree(props[i]->key);
            nng_strfree(props[i]->value);
            nng_free(props[i], sizeof(conf_user_property));
        }
    }
    free(props);
}

static void
free_bridge_node_members(conf_bridge_node *node)
{
    if (node == NULL) {
        return;
    }
    nng_strfree(node->name);
    nng_strfree(node->address);
    nng_strfree(node->clientid);
    nng_strfree(node->username);
    nng_strfree(node->password);
    for (size_t i = 0; i < node->forwards_count; ++i) {
        free_topic(node->forwards_list[i]);
    }
    free(node->forwards_list);
    for (size_t i = 0; i < node->sub_count; ++i) {
        free_topic(node->sub_list[i]);
    }
    free(node->sub_list);
    free_tls_members(&node->tls);
    if (node->conn_properties != NULL) {
        free_user_properties(node->conn_properties->user_property,
            node->conn_properties->user_property_size);
        nng_free(node->conn_properties, sizeof(*node->conn_properties));
    }
    if (node->will_properties != NULL) {
        nng_strfree(node->will_properties->content_type);
        nng_strfree(node->will_properties->response_topic);
        nng_strfree(node->will_properties->correlation_data);
        free_user_properties(node->will_properties->user_property,
            node->will_properties->user_property_size);
        nng_free(node->will_properties, sizeof(*node->will_properties));
    }
    if (node->sub_properties != NULL) {
        free_user_properties(node->sub_properties->user_property,
            node->sub_properties->user_property_size);
        nng_free(node->sub_properties, sizeof(*node->sub_properties));
    }
}

static void
test_str_append_builds_concatenated_string(void)
{
    char *dest = NULL;

    assert(str_append(&dest, "alpha") == strlen("alpha"));
    assert(strcmp(dest, "alpha") == 0);
    assert(str_append(&dest, "-beta") == strlen("alpha-beta"));
    assert(strcmp(dest, "alpha-beta") == 0);

    free(dest);
}

static void
test_reload_config_getter_and_setter_convert_packet_sizes(void)
{
    conf  cfg = { 0 };
    cJSON *json = NULL;
    cJSON *patch = cJSON_CreateObject();

    cfg.property_size = 32;
    cfg.max_packet_size = 256 * 1024;
    cfg.client_max_packet_size = 128 * 1024;
    cfg.msq_len = 111;
    cfg.qos_duration = 42;
    cfg.backoff = 1.5f;
    cfg.allow_anonymous = true;
    cfg.parquet.enable = true;
    cfg.bridge_mode = true;

    json = get_reload_config(&cfg);
    assert(cJSON_GetObjectItem(json, "property_size")->valueint == 32);
    assert(cJSON_GetObjectItem(json, "max_packet_size")->valueint == 256);
    assert(cJSON_GetObjectItem(json, "client_max_packet_size")->valueint ==
        128);
    assert(cJSON_IsTrue(cJSON_GetObjectItem(json, "enable_mqtt_stream")));
    assert(cJSON_IsTrue(cJSON_GetObjectItem(json, "bridge_mode")));
    cJSON_Delete(json);

    cJSON_AddNumberToObject(patch, "property_size", 64);
    cJSON_AddNumberToObject(patch, "max_packet_size", 512);
    cJSON_AddNumberToObject(patch, "client_max_packet_size", 320);
    cJSON_AddNumberToObject(patch, "msq_len", 2048);
    cJSON_AddNumberToObject(patch, "qos_duration", 77);
    cJSON_AddNumberToObject(patch, "keepalive_backoff", 2.25);
    cJSON_AddBoolToObject(patch, "allow_anonymous", false);

    set_reload_config(patch, &cfg);
    assert(cfg.property_size == 64);
    assert(cfg.max_packet_size == 512 * 1024);
    assert(cfg.client_max_packet_size == 320 * 1024);
    assert(cfg.msq_len == 2048);
    assert(cfg.qos_duration == 77);
    assert(cfg.backoff > 2.24f && cfg.backoff < 2.26f);
    assert(cfg.allow_anonymous == false);
    cJSON_Delete(patch);
}

static void
test_basic_config_getter_and_setter_update_core_fields(void)
{
    conf  cfg = { 0 };
    cJSON *json = NULL;
    cJSON *patch = cJSON_CreateObject();

    cfg.url = dup_string("nmq-tcp://0.0.0.0:1883");
    cfg.num_taskq_thread = 4;
    cfg.max_taskq_thread = 8;
    cfg.parallel = 16;
    cfg.property_size = 32;
    cfg.daemon = true;
    cfg.max_packet_size = 1024 * 1024;
    cfg.client_max_packet_size = 512 * 1024;
    cfg.msq_len = 1024;
    cfg.qos_duration = 6;
    cfg.backoff = 1.75f;
    cfg.allow_anonymous = true;
    cfg.ipc_internal = false;

    json = get_basic_config(&cfg);
    assert(strcmp(cJSON_GetObjectItem(json, "url")->valuestring,
        "nmq-tcp://0.0.0.0:1883") == 0);
    assert(cJSON_GetObjectItem(json, "max_packet_size")->valueint == 1024);
    assert(cJSON_GetObjectItem(json, "client_max_packet_size")->valueint ==
        512);
    assert(cJSON_IsTrue(cJSON_GetObjectItem(json, "daemon")));
    cJSON_Delete(json);

    cJSON_AddStringToObject(patch, "url", "nmq-tcp://127.0.0.1:1884");
    cJSON_AddBoolToObject(patch, "enable", false);
    cJSON_AddBoolToObject(patch, "daemon", false);
    cJSON_AddNumberToObject(patch, "num_taskq_thread", 10);
    cJSON_AddNumberToObject(patch, "max_taskq_thread", 20);
    cJSON_AddNumberToObject(patch, "parallel", 40);
    cJSON_AddNumberToObject(patch, "property_size", 48);
    cJSON_AddNumberToObject(patch, "msq_len", 4096);
    cJSON_AddNumberToObject(patch, "qos_duration", 12);
    cJSON_AddBoolToObject(patch, "allow_anonymous", false);
    cJSON_AddBoolToObject(patch, "ipc_internal", true);
    cJSON_AddNumberToObject(patch, "max_packet_size", 2048);
    cJSON_AddNumberToObject(patch, "client_max_packet_size", 1536);
    cJSON_AddNumberToObject(patch, "keepalive_backoff", 2.5);

    set_basic_config(patch, &cfg);
    assert(strcmp(cfg.url, "nmq-tcp://127.0.0.1:1884") == 0);
    assert(cfg.enable == false);
    assert(cfg.daemon == false);
    assert(cfg.num_taskq_thread == 10);
    assert(cfg.max_taskq_thread == 20);
    assert(cfg.parallel == 40);
    assert(cfg.property_size == 48);
    assert(cfg.msq_len == 4096);
    assert(cfg.qos_duration == 12);
    assert(cfg.allow_anonymous == false);
    assert(cfg.ipc_internal == true);
    assert(cfg.max_packet_size == 2048 * 1024);
    assert(cfg.client_max_packet_size == 1536 * 1024);
    assert(cfg.backoff > 2.49f && cfg.backoff < 2.51f);

    cJSON_Delete(patch);
    nng_strfree(cfg.url);
}

static void
test_tls_config_getter_and_setter_update_inline_materials(void)
{
    conf_tls tls = { 0 };
    cJSON   *json = NULL;
    cJSON   *patch = cJSON_CreateObject();

    tls.enable = true;
    tls.url = dup_string("tls+nmq-tcp://0.0.0.0:8883");
    tls.key_password = dup_string("secret");
    tls.key = dup_string("key-data");
    tls.cert = dup_string("cert-data");
    tls.ca = dup_string("ca-data");
    tls.verify_peer = true;
    tls.set_fail = false;

    json = get_tls_config(&tls, false);
    assert(cJSON_IsTrue(cJSON_GetObjectItem(json, "enable")));
    assert(strcmp(cJSON_GetObjectItem(json, "key_password")->valuestring,
        "secret") == 0);
    assert(strcmp(cJSON_GetObjectItem(json, "cacert")->valuestring,
        "ca-data") == 0);
    cJSON_Delete(json);

    cJSON_AddBoolToObject(patch, "enable", false);
    cJSON_AddStringToObject(patch, "url", "tls+nmq-tcp://127.0.0.1:8883");
    cJSON_AddStringToObject(patch, "keypass", "next-secret");
    cJSON_AddStringToObject(patch, "key", "next-key");
    cJSON_AddStringToObject(patch, "cert", "next-cert");
    cJSON_AddStringToObject(patch, "cacert", "next-ca");
    cJSON_AddBoolToObject(patch, "verify_peer", false);
    cJSON_AddBoolToObject(patch, "fail_if_no_peer_cert", true);

    set_tls_config(patch, "/tmp/nanomq.conf", &tls, "tls.");
    assert(tls.enable == false);
    assert(strcmp(tls.url, "tls+nmq-tcp://127.0.0.1:8883") == 0);
    assert(strcmp(tls.key_password, "next-secret") == 0);
    assert(strcmp(tls.key, "next-key") == 0);
    assert(strcmp(tls.cert, "next-cert") == 0);
    assert(strcmp(tls.ca, "next-ca") == 0);
    assert(tls.verify_peer == false);
    assert(tls.set_fail == true);

    cJSON_Delete(patch);
    free_tls_members(&tls);
}

static void
test_http_websocket_and_sqlite_configs_round_trip(void)
{
    conf_http_server http = { 0 };
    conf_websocket   ws = { 0 };
    conf_sqlite      sqlite = { 0 };
    cJSON           *json = NULL;
    cJSON           *patch = NULL;

    http.enable = true;
    http.port = 8083;
    http.ip_addr = dup_string("0.0.0.0");
    http.username = dup_string("admin");
    http.password = dup_string("public");
    http.auth_type = JWT;

    json = get_http_config(&http);
    assert(cJSON_IsTrue(cJSON_GetObjectItem(json, "enable")));
    assert(cJSON_GetObjectItem(json, "port")->valueint == 8083);
    assert(strcmp(cJSON_GetObjectItem(json, "addr")->valuestring,
        "0.0.0.0") == 0);
    assert(strcmp(cJSON_GetObjectItem(json, "auth_type")->valuestring, "jwt") ==
        0);
    cJSON_Delete(json);

    patch = cJSON_CreateObject();
    cJSON_AddBoolToObject(patch, "enable", false);
    cJSON_AddNumberToObject(patch, "port", 8084);
    cJSON_AddStringToObject(patch, "username", "operator");
    cJSON_AddStringToObject(patch, "password", "token");
    cJSON_AddStringToObject(patch, "auth_type", "basic");
    set_http_config(patch, "/tmp/nanomq.conf", &http);
    assert(http.enable == false);
    assert(http.port == 8084);
    assert(strcmp(http.username, "operator") == 0);
    assert(strcmp(http.password, "token") == 0);
    assert(http.auth_type == BASIC);
    cJSON_Delete(patch);

    ws.enable = true;
    ws.tls_enable = false;
    ws.url = dup_string("nmq-ws://0.0.0.0:8083/mqtt");
    ws.tls_url = dup_string("nmq-wss://0.0.0.0:8086/mqtt");

    json = get_websocket_config(&ws);
    assert(strcmp(cJSON_GetObjectItem(json, "url")->valuestring,
        "nmq-ws://0.0.0.0:8083/mqtt") == 0);
    assert(cJSON_IsFalse(cJSON_GetObjectItem(json, "tls_enable")));
    cJSON_Delete(json);

    patch = cJSON_CreateObject();
    cJSON_AddBoolToObject(patch, "enable", false);
    cJSON_AddBoolToObject(patch, "tls_enable", true);
    cJSON_AddStringToObject(patch, "url", "nmq-ws://127.0.0.1:8084/mqtt");
    cJSON_AddStringToObject(patch, "tls_url", "nmq-wss://127.0.0.1:8087/mqtt");
    set_websocket_config(patch, "/tmp/nanomq.conf", &ws);
    assert(ws.enable == false);
    assert(ws.tls_enable == true);
    assert(strcmp(ws.url, "nmq-ws://127.0.0.1:8084/mqtt") == 0);
    assert(strcmp(ws.tls_url, "nmq-wss://127.0.0.1:8087/mqtt") == 0);
    cJSON_Delete(patch);

    sqlite.enable = true;
    sqlite.disk_cache_size = 1000;
    sqlite.flush_mem_threshold = 88;
    sqlite.resend_interval = 3600;
    sqlite.mounted_file_path = dup_string("/tmp/nanomq.db");

    json = get_sqlite_config(&sqlite);
    assert(cJSON_IsTrue(cJSON_GetObjectItem(json, "enable")));
    assert(cJSON_GetObjectItem(json, "disk_cache_size")->valueint == 1000);
    assert(strcmp(cJSON_GetObjectItem(json, "mounted_file_path")->valuestring,
        "/tmp/nanomq.db") == 0);
    cJSON_Delete(json);

    patch = cJSON_CreateObject();
    cJSON_AddBoolToObject(patch, "enable", false);
    cJSON_AddNumberToObject(patch, "disk_cache_size", 2048);
    cJSON_AddNumberToObject(patch, "flush_mem_threshold", 256);
    cJSON_AddNumberToObject(patch, "resend_interval", 7200);
    cJSON_AddStringToObject(patch, "mounted_file_path", "/tmp/cache.db");
    set_sqlite_config(patch, "/tmp/nanomq.conf", &sqlite, "sqlite.");
    assert(sqlite.enable == false);
    assert(sqlite.disk_cache_size == 2048);
    assert(sqlite.flush_mem_threshold == 256);
    assert(sqlite.resend_interval == 7200);
    assert(strcmp(sqlite.mounted_file_path, "/tmp/cache.db") == 0);
    cJSON_Delete(patch);

    free_http_members(&http);
    free_websocket_members(&ws);
    free_sqlite_members(&sqlite);
}

static void
test_auth_config_setter_and_reload_replace_credentials(void)
{
    conf_auth current = { 0 };
    conf_auth updated = { 0 };
    cJSON    *json = cJSON_CreateArray();
    cJSON    *entry = NULL;

    assert(nng_mtx_alloc(&current.mtx) == 0);
    cvector_push_back(current.usernames, dup_string("old-user"));
    cvector_push_back(current.passwords, dup_string("old-pass"));
    current.count = 1;

    entry = cJSON_CreateObject();
    cJSON_AddStringToObject(entry, "username", "alice");
    cJSON_AddStringToObject(entry, "password", "pw1");
    cJSON_AddItemToArray(json, entry);
    entry = cJSON_CreateObject();
    cJSON_AddStringToObject(entry, "username", "bob");
    cJSON_AddStringToObject(entry, "password", "pw2");
    cJSON_AddItemToArray(json, entry);

    set_auth_config(json, "/tmp/nanomq.conf", &current);
    assert(current.count == 2);
    assert(strcmp(current.usernames[0], "alice") == 0);
    assert(strcmp(current.passwords[1], "pw2") == 0);
    cJSON_Delete(json);

    json = get_auth_config(&current);
    assert(cJSON_GetArraySize(json) == 2);
    assert(strcmp(cJSON_GetObjectItem(cJSON_GetArrayItem(json, 0), "login")
                      ->valuestring,
        "alice") == 0);
    cJSON_Delete(json);

    assert(nng_mtx_alloc(&updated.mtx) == 0);
    cvector_push_back(updated.usernames, dup_string("solo"));
    cvector_push_back(updated.passwords, dup_string("only"));
    updated.count = 1;
    reload_auth_config(&current, &updated);
    assert(current.count == 1);
    assert(strcmp(current.usernames[0], "solo") == 0);
    assert(strcmp(current.passwords[0], "only") == 0);

    free_auth_members(&updated);
    free_auth_members(&current);
}

static void
test_auth_http_helpers_serialize_headers_params_and_tls(void)
{
    conf_auth_http auth = { 0 };
    cJSON         *headers = NULL;
    cJSON         *params = NULL;
    cJSON         *json = cJSON_CreateObject();
    cJSON         *req_json = NULL;
    cJSON         *dump = NULL;

    auth.enable = true;
    auth.timeout = 30;
    auth.connect_timeout = 5;
    auth.pool_size = 4;
    auth.cache_ttl = 60;

    auth.auth_req.header_count = 2;
    auth.auth_req.headers = calloc(2, sizeof(conf_http_header *));
    assert(auth.auth_req.headers != NULL);
    auth.auth_req.headers[0] = alloc_header_slot();
    auth.auth_req.headers[1] = alloc_header_slot();

    auth.acl_req.header_count = 1;
    auth.acl_req.headers = calloc(1, sizeof(conf_http_header *));
    assert(auth.acl_req.headers != NULL);
    auth.acl_req.headers[0] = alloc_header_slot();

    auth.super_req.header_count = 1;
    auth.super_req.headers = calloc(1, sizeof(conf_http_header *));
    assert(auth.super_req.headers != NULL);
    auth.super_req.headers[0] = alloc_header_slot();

    req_json = cJSON_CreateObject();
    cJSON_AddStringToObject(req_json, "url", "http://127.0.0.1/auth");
    cJSON_AddStringToObject(req_json, "method", "post");
    headers = cJSON_CreateObject();
    cJSON_AddStringToObject(headers, "X-Token", "abc");
    cJSON_AddStringToObject(headers, "X-Env", "prod");
    cJSON_AddItemToObject(req_json, "headers", headers);
    params = cJSON_CreateArray();
    cJSON_AddItemToArray(params, cJSON_CreateString("username"));
    cJSON_AddItemToArray(params, cJSON_CreateString("password"));
    cJSON_AddItemToArray(params, cJSON_CreateString("clientid"));
    cJSON_AddItemToArray(params, cJSON_CreateString("topic"));
    cJSON_AddItemToArray(params, cJSON_CreateString("mountpoint"));
    cJSON_AddItemToObject(req_json, "params", params);
    cJSON_AddItemToObject(json, "auth_req", req_json);

    req_json = cJSON_CreateObject();
    cJSON_AddStringToObject(req_json, "url", "http://127.0.0.1/acl");
    cJSON_AddStringToObject(req_json, "method", "get");
    headers = cJSON_CreateObject();
    cJSON_AddStringToObject(headers, "X-Acl", "yes");
    cJSON_AddItemToObject(req_json, "headers", headers);
    params = cJSON_CreateArray();
    cJSON_AddItemToArray(params, cJSON_CreateString("ipaddress"));
    cJSON_AddItemToObject(req_json, "params", params);
    cJSON_AddItemToObject(json, "acl_req", req_json);

    req_json = cJSON_CreateObject();
    cJSON_AddStringToObject(req_json, "url", "http://127.0.0.1/super");
    cJSON_AddStringToObject(req_json, "method", "get");
    headers = cJSON_CreateObject();
    cJSON_AddStringToObject(headers, "X-Super", "1");
    cJSON_AddItemToObject(req_json, "headers", headers);
    params = cJSON_CreateArray();
    cJSON_AddItemToArray(params, cJSON_CreateString("protocol"));
    cJSON_AddItemToObject(req_json, "params", params);
    cJSON_AddItemToObject(json, "super_req", req_json);

    cJSON_AddBoolToObject(json, "enable", true);
    cJSON_AddNumberToObject(json, "timeout", 30);
    cJSON_AddNumberToObject(json, "connect_timeout", 5);
    cJSON_AddNumberToObject(json, "pool_size", 4);
    cJSON_AddNumberToObject(json, "cache_ttl", 60);

    set_auth_http_config(json, "/tmp/nanomq.conf", &auth);
    assert(auth.enable == true);
    assert(strcmp(auth.auth_req.url, "http://127.0.0.1/auth") == 0);
    assert(strcmp(auth.auth_req.method, "post") == 0);
    assert(strcmp(auth.auth_req.headers[0]->key, "X-Token") == 0);
    assert(strcmp(auth.auth_req.headers[0]->value, "abc") == 0);
    assert(strcmp(auth.auth_req.headers[1]->key, "X-Env") == 0);
    assert(auth.auth_req.param_count == 5);
    assert(strcmp(auth.auth_req.params[0]->name, "username") == 0);
    assert(auth.auth_req.params[0]->type == USERNAME);
    assert(auth.auth_req.params[1]->type == PASSWORD);
    assert(auth.auth_req.params[2]->type == CLIENTID);
    assert(auth.auth_req.params[3]->type == TOPIC);
    assert(auth.auth_req.params[4]->type == MOUNTPOINT);
    assert(auth.acl_req.param_count == 1);
    assert(auth.acl_req.params[0]->type == IPADDRESS);
    assert(auth.super_req.param_count == 1);
    assert(auth.super_req.params[0]->type == PROTOCOL);
    cJSON_Delete(json);

    dump = get_auth_http_req_config(&auth.auth_req);
    assert(strcmp(cJSON_GetObjectItem(dump, "url")->valuestring,
        "http://127.0.0.1/auth") == 0);
    assert(strcmp(cJSON_GetObjectItem(cJSON_GetObjectItem(dump, "headers"),
                      "X-Token")
                      ->valuestring,
        "abc") == 0);
    assert(cJSON_GetArraySize(cJSON_GetObjectItem(dump, "params")) == 5);
    cJSON_Delete(dump);

    dump = get_auth_http_config(&auth);
    assert(cJSON_IsTrue(cJSON_GetObjectItem(dump, "enable")));
    assert(cJSON_GetObjectItem(dump, "timeout")->valueint == 30);
    assert(strcmp(cJSON_GetObjectItem(
                      cJSON_GetObjectItem(dump, "super_req"), "method")
                      ->valuestring,
        "get") == 0);
    cJSON_Delete(dump);

    free_auth_http_members(&auth);
}

static void
test_bridge_helpers_emit_v5_properties_and_node_filter(void)
{
    conf_bridge            bridge = { 0 };
    conf_bridge_node      *node = nng_zalloc(sizeof(*node));
    conf_user_property    *user_prop = nng_zalloc(sizeof(*user_prop));
    conf_user_property    *will_user_prop = nng_zalloc(sizeof(*will_user_prop));
    conf_user_property    *sub_user_prop = nng_zalloc(sizeof(*sub_user_prop));
    cJSON                 *json = NULL;
    cJSON                 *nodes = NULL;
    cJSON                 *node_json = NULL;
    cJSON                 *connector = NULL;

    assert(node != NULL);
    assert(user_prop != NULL);
    assert(will_user_prop != NULL);
    assert(sub_user_prop != NULL);

    node->name = dup_string("bridge-a");
    node->enable = true;
    node->parallel = 2;
    node->address = dup_string("mqtt.example.com:1883");
    node->clientid = dup_string("bridge-client");
    node->username = dup_string("bridge-user");
    node->password = dup_string("bridge-pass");
    node->clean_start = true;
    node->keepalive = 60;
    node->proto_ver = MQTT_PROTOCOL_VERSION_v5;
    node->tls.enable = true;
    node->tls.url = dup_string("tls+nmq-tcp://mqtt.example.com:8883");

    node->forwards_count = 1;
    node->forwards_list = calloc(1, sizeof(topics *));
    assert(node->forwards_list != NULL);
    node->forwards_list[0] =
        make_topic("remote/telemetry", "local/telemetry", "pre/", "/suf", 1);

    node->sub_count = 1;
    node->sub_list = calloc(1, sizeof(topics *));
    assert(node->sub_list != NULL);
    node->sub_list[0] = make_topic("remote/cmd", "local/cmd", NULL, NULL, 2);

    node->conn_properties = nng_zalloc(sizeof(*node->conn_properties));
    node->will_properties = nng_zalloc(sizeof(*node->will_properties));
    node->sub_properties = nng_zalloc(sizeof(*node->sub_properties));
    assert(node->conn_properties != NULL);
    assert(node->will_properties != NULL);
    assert(node->sub_properties != NULL);

    user_prop->key = dup_string("trace-id");
    user_prop->value = dup_string("abc123");
    node->conn_properties->session_expiry_interval = 10;
    node->conn_properties->receive_maximum = 20;
    node->conn_properties->maximum_packet_size = 1024;
    node->conn_properties->topic_alias_maximum = 3;
    node->conn_properties->request_response_info = 1;
    node->conn_properties->request_problem_info = 1;
    node->conn_properties->user_property_size = 1;
    node->conn_properties->user_property = calloc(1, sizeof(conf_user_property *));
    assert(node->conn_properties->user_property != NULL);
    node->conn_properties->user_property[0] = user_prop;

    will_user_prop->key = dup_string("source");
    will_user_prop->value = dup_string("sensor");
    node->will_properties->payload_format_indicator = 1;
    node->will_properties->message_expiry_interval = 66;
    node->will_properties->content_type = dup_string("application/json");
    node->will_properties->will_delay_interval = 7;
    node->will_properties->response_topic = dup_string("reply/topic");
    node->will_properties->correlation_data = dup_string("corr-1");
    node->will_properties->user_property_size = 1;
    node->will_properties->user_property =
        calloc(1, sizeof(conf_user_property *));
    assert(node->will_properties->user_property != NULL);
    node->will_properties->user_property[0] = will_user_prop;

    sub_user_prop->key = dup_string("scope");
    sub_user_prop->value = dup_string("all");
    node->sub_properties->identifier = 9;
    node->sub_properties->user_property_size = 1;
    node->sub_properties->user_property =
        calloc(1, sizeof(conf_user_property *));
    assert(node->sub_properties->user_property != NULL);
    node->sub_properties->user_property[0] = sub_user_prop;

    bridge.count = 1;
    bridge.nodes = calloc(1, sizeof(conf_bridge_node *));
    assert(bridge.nodes != NULL);
    bridge.nodes[0] = node;

    json = get_bridge_config(&bridge, "bridge-a");
    nodes = cJSON_GetObjectItem(json, "nodes");
    assert(cJSON_GetArraySize(nodes) == 1);
    node_json = cJSON_GetArrayItem(nodes, 0);
    assert(strcmp(cJSON_GetObjectItem(node_json, "name")->valuestring,
        "bridge-a") == 0);
    connector = cJSON_GetObjectItem(node_json, "connector");
    assert(strcmp(cJSON_GetObjectItem(connector, "server")->valuestring,
        "mqtt.example.com:1883") == 0);
    assert(strcmp(cJSON_GetObjectItem(cJSON_GetObjectItem(connector,
                          "conn_properties"),
                      "user_properties")
                      ->child->child->valuestring,
        "trace-id") == 0);
    assert(cJSON_GetObjectItem(cJSON_GetObjectItem(node_json, "sub_properties"),
               "identifier")
               ->valueint == 9);
    assert(strcmp(cJSON_GetObjectItem(cJSON_GetArrayItem(
                      cJSON_GetObjectItem(node_json, "forwards"), 0),
                      "prefix")
                      ->valuestring,
        "pre/") == 0);
    assert(strcmp(cJSON_GetObjectItem(cJSON_GetObjectItem(node_json, "tls"),
                      "url")
                      ->valuestring,
        "tls+nmq-tcp://mqtt.example.com:8883") == 0);
    cJSON_Delete(json);

    json = get_bridge_config(&bridge, "missing");
    assert(cJSON_GetArraySize(cJSON_GetObjectItem(json, "nodes")) == 0);
    cJSON_Delete(json);

    free(bridge.nodes);
    free_bridge_node_members(node);
    nng_free(node, sizeof(*node));
}

static void
test_reload_helpers_copy_new_values(void)
{
    conf        current = { 0 };
    conf        updated = { 0 };
    conf_sqlite sqlite_cur = { 0 };
    conf_sqlite sqlite_new = { 0 };

    current.property_size = 1;
    updated.property_size = 99;
    updated.max_packet_size = 2048;
    updated.client_max_packet_size = 4096;
    updated.msq_len = 123;
    updated.qos_duration = 456;
    updated.backoff = 3.5f;
    updated.allow_anonymous = false;

    reload_basic_config(&current, &updated);
    assert(current.property_size == 99);
    assert(current.max_packet_size == 2048);
    assert(current.client_max_packet_size == 4096);
    assert(current.msq_len == 123);
    assert(current.qos_duration == 456);
    assert(current.backoff > 3.49f && current.backoff < 3.51f);
    assert(current.allow_anonymous == false);

    sqlite_cur.flush_mem_threshold = 10;
    sqlite_new.flush_mem_threshold = 500;
    reload_sqlite_config(&sqlite_cur, &sqlite_new);
    assert(sqlite_cur.flush_mem_threshold == 500);
}

int
main(void)
{
    test_str_append_builds_concatenated_string();
    test_reload_config_getter_and_setter_convert_packet_sizes();
    test_basic_config_getter_and_setter_update_core_fields();
    test_tls_config_getter_and_setter_update_inline_materials();
    test_http_websocket_and_sqlite_configs_round_trip();
    test_auth_config_setter_and_reload_replace_credentials();
    test_auth_http_helpers_serialize_headers_params_and_tls();
    test_bridge_helpers_emit_v5_properties_and_node_filter();
    test_reload_helpers_copy_new_values();
    return 0;
}
