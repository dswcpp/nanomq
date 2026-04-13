#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define nng_send fake_nng_send
#define nng_timestamp fake_nng_timestamp
#include "../webhook_post.c"
#undef nng_send
#undef nng_timestamp

static char g_last_sent[8192];
static int  g_last_flags = -1;
static int  g_send_calls = 0;
static uint64_t g_fake_ts = 123456789ULL;

int
fake_nng_send(nng_socket sock, void *data, size_t size, int flags)
{
    (void) sock;
    assert(size < sizeof(g_last_sent));
    memcpy(g_last_sent, data, size);
    g_last_sent[size] = '\0';
    g_last_flags = flags;
    g_send_calls++;
    return 0;
}

uint64_t
fake_nng_timestamp(void)
{
    return g_fake_ts;
}

static void
reset_send_state(void)
{
    memset(g_last_sent, 0, sizeof(g_last_sent));
    g_last_flags = -1;
    g_send_calls = 0;
}

static conf_web_hook_rule *
make_rule(webhook_event event, const char *topic)
{
    conf_web_hook_rule *rule = calloc(1, sizeof(*rule));
    assert(rule != NULL);
    rule->event = event;
    rule->topic = (char *) topic;
    return rule;
}

static void
init_hook_conf(conf_web_hook *hook, webhook_event event, const char *topic,
    hook_payload_type payload_type)
{
    memset(hook, 0, sizeof(*hook));
    hook->enable = true;
    hook->encode_payload = payload_type;
    hook->rule_count = 1;
    hook->rules = calloc(1, sizeof(conf_web_hook_rule *));
    assert(hook->rules != NULL);
    hook->rules[0] = make_rule(event, topic);
}

static void
free_hook_conf(conf_web_hook *hook)
{
    if (hook->rules != NULL) {
        for (size_t i = 0; i < hook->rule_count; ++i) {
            free(hook->rules[i]);
        }
        free(hook->rules);
    }
    hook->rules = NULL;
    hook->rule_count = 0;
}

static pub_packet_struct
make_packet(const char *topic, const uint8_t *payload, uint32_t payload_len)
{
    pub_packet_struct packet;
    memset(&packet, 0, sizeof(packet));
    packet.fixed_header.qos = 1;
    packet.fixed_header.retain = 1;
    packet.var_header.publish.topic_name.body = (char *) topic;
    packet.var_header.publish.topic_name.len = strlen(topic);
    packet.payload.data = (uint8_t *) payload;
    packet.payload.len = payload_len;
    return packet;
}

static void
test_event_filter_helpers(void)
{
    conf_web_hook hook;

    init_hook_conf(&hook, MESSAGE_PUBLISH, "factory/+/evt", plain);
    assert(event_filter(&hook, MESSAGE_PUBLISH) == true);
    assert(event_filter(&hook, CLIENT_CONNACK) == false);
    assert(event_filter_with_topic(&hook, MESSAGE_PUBLISH, "factory/a/evt") ==
        true);
    assert(event_filter_with_topic(&hook, MESSAGE_PUBLISH, "factory/a/other") ==
        false);
    free_hook_conf(&hook);
}

static void
test_base64_no_padding_encoder_rewrites_special_chars(void)
{
    char out[64] = { 0 };
    unsigned int len =
        base64_no_padding_encode((const unsigned char *) "messagei+/", 10, out);
    char mapped[8] = { 0 };
    unsigned int idx = 0;

    assert(len > 0);
    assert(strcmp(out, "bWVzc2FnZWkrLw") == 0);

    set_char(mapped, &idx, 'i');
    set_char(mapped, &idx, '+');
    set_char(mapped, &idx, '/');
    mapped[idx] = '\0';
    assert(strcmp(mapped, "iAB") == 0);
}

static void
test_webhook_msg_publish_plain_payload(void)
{
    conf_web_hook    hook;
    nng_socket       sock = { 0 };
    const uint8_t    payload[] = "raw-payload";
    pub_packet_struct packet =
        make_packet("factory/line1/evt", payload, sizeof(payload) - 1);
    cJSON *json = NULL;

    reset_send_state();
    init_hook_conf(&hook, MESSAGE_PUBLISH, "factory/+/evt", plain);
    assert(webhook_msg_publish(&sock, &hook, &packet, NULL, "cid-1") == 0);
    assert(g_send_calls == 1);
    assert(g_last_flags == NNG_FLAG_NONBLOCK);

    json = cJSON_Parse(g_last_sent);
    assert(json != NULL);
    assert(strcmp(cJSON_GetObjectItem(json, "action")->valuestring,
        "message_publish") == 0);
    assert(strcmp(cJSON_GetObjectItem(json, "topic")->valuestring,
        "factory/line1/evt") == 0);
    assert(strcmp(cJSON_GetObjectItem(json, "payload")->valuestring,
        "raw-payload") == 0);
    assert(strcmp(cJSON_GetObjectItem(json, "from_username")->valuestring,
        "undefined") == 0);
    assert(strcmp(cJSON_GetObjectItem(json, "from_client_id")->valuestring,
        "cid-1") == 0);
    assert(cJSON_GetObjectItem(json, "ts")->valuedouble == (double) g_fake_ts);
    cJSON_Delete(json);
    free_hook_conf(&hook);
}

static void
test_webhook_msg_publish_base62_empty_payload(void)
{
    conf_web_hook    hook;
    nng_socket       sock = { 0 };
    const uint8_t    payload[] = "";
    pub_packet_struct packet =
        make_packet("factory/line1/evt", payload, 0);
    cJSON *json = NULL;

    reset_send_state();
    init_hook_conf(&hook, MESSAGE_PUBLISH, "factory/+/evt", base62);
    assert(webhook_msg_publish(&sock, &hook, &packet, "user-a", NULL) == 0);
    json = cJSON_Parse(g_last_sent);
    assert(json != NULL);
    assert(strcmp(cJSON_GetObjectItem(json, "payload")->valuestring, "") == 0);
    assert(cJSON_IsNull(cJSON_GetObjectItem(json, "from_client_id")));
    cJSON_Delete(json);
    free_hook_conf(&hook);
}

static void
test_webhook_msg_publish_filters_disabled_or_topic_mismatch(void)
{
    conf_web_hook    hook;
    nng_socket       sock = { 0 };
    const uint8_t    payload[] = "abc";
    pub_packet_struct packet =
        make_packet("factory/line1/evt", payload, sizeof(payload) - 1);

    reset_send_state();
    init_hook_conf(&hook, MESSAGE_PUBLISH, "other/+/evt", plain);
    assert(webhook_msg_publish(&sock, &hook, &packet, "u", "c") == -1);
    assert(g_send_calls == 0);
    free_hook_conf(&hook);

    reset_send_state();
    init_hook_conf(&hook, MESSAGE_PUBLISH, "factory/+/evt", plain);
    hook.enable = false;
    assert(webhook_msg_publish(&sock, &hook, &packet, "u", "c") == -1);
    assert(g_send_calls == 0);
    free_hook_conf(&hook);
}

static void
test_client_events_and_hook_entry(void)
{
    conf_web_hook hook;
    nng_socket    sock = { 0 };
    conn_param   *cp = NULL;
    nano_work     work;
    conf          cfg;
    pub_packet_struct packet =
        make_packet("factory/line1/evt", (const uint8_t *) "a", 1);
    cJSON *json = NULL;

    reset_send_state();
    init_hook_conf(&hook, CLIENT_CONNACK, NULL, plain);
    assert(webhook_client_connack(&sock, &hook, 4, 60, SUCCESS, NULL, "cid-x") ==
        0);
    json = cJSON_Parse(g_last_sent);
    assert(json != NULL);
    assert(strcmp(cJSON_GetObjectItem(json, "conn_ack")->valuestring,
        "success") == 0);
    cJSON_Delete(json);
    free_hook_conf(&hook);

    reset_send_state();
    init_hook_conf(&hook, CLIENT_DISCONNECTED, NULL, plain);
    assert(webhook_client_disconnect(&sock, &hook, 4, 60, 1, "user-x", "cid-x") ==
        0);
    json = cJSON_Parse(g_last_sent);
    assert(json != NULL);
    assert(strcmp(cJSON_GetObjectItem(json, "reason")->valuestring,
        "abnormal") == 0);
    cJSON_Delete(json);
    free_hook_conf(&hook);

    reset_send_state();
    memset(&work, 0, sizeof(work));
    memset(&cfg, 0, sizeof(cfg));
    init_hook_conf(&cfg.web_hook, MESSAGE_PUBLISH, "factory/+/evt", plain);
    work.config = &cfg;
    work.hook_sock = sock;
    work.flag = CMD_PUBLISH;
    work.pub_packet = &packet;
    assert(conn_param_alloc(&cp) == 0);
    conn_param_set_clientid(cp, "cid-hook");
    work.cparam = cp;

    assert(hook_entry(&work, 0) == 0);
    assert(g_send_calls == 1);
    assert(work.flag == 0);

    conn_param_free(cp);
    free_hook_conf(&cfg.web_hook);
}

int
main(void)
{
    test_event_filter_helpers();
    test_base64_no_padding_encoder_rewrites_special_chars();
    test_webhook_msg_publish_plain_payload();
    test_webhook_msg_publish_base62_empty_payload();
    test_webhook_msg_publish_filters_disabled_or_topic_mismatch();
    test_client_events_and_hook_entry();
    return 0;
}
