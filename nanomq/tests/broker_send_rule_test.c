#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "include/broker.h"
#include "include/pub_handler.h"
#include "nng/protocol/mqtt/mqtt_parser.h"
#include "nng/supplemental/nanolib/cvector.h"
#include "../weld_telemetry_async.h"

static int       g_async_calls = 0;
static char      g_last_topic[256];
static char      g_last_payload[1024];
static uint8_t   g_last_qos = 0;
static uint16_t  g_last_packet_id = 0;
static rule_taos g_last_rule;

int
weld_telemetry_async_enqueue(const rule_taos *taos_rule,
    const char *topic_name, uint8_t qos, uint16_t packet_id,
    const uint8_t *payload, uint32_t payload_len)
{
    assert(taos_rule != NULL);
    assert(topic_name != NULL);
    assert(payload != NULL);

    g_async_calls++;
    g_last_rule = *taos_rule;
    snprintf(g_last_topic, sizeof(g_last_topic), "%s", topic_name);
    memcpy(g_last_payload, payload, payload_len);
    g_last_payload[payload_len] = '\0';
    g_last_qos = qos;
    g_last_packet_id = packet_id;
    return WELD_TELEMETRY_ASYNC_ENQUEUE_OK;
}

static void
reset_stubs(void)
{
    g_async_calls = 0;
    memset(g_last_topic, 0, sizeof(g_last_topic));
    memset(g_last_payload, 0, sizeof(g_last_payload));
    g_last_qos = 0;
    g_last_packet_id = 0;
    memset(&g_last_rule, 0, sizeof(g_last_rule));
}

static struct pub_packet_struct *
make_pub_packet(const char *topic, const char *payload, uint16_t packet_id)
{
    struct pub_packet_struct *packet = nng_zalloc(sizeof(*packet));
    assert(packet != NULL);
    packet->fixed_header.packet_type = PUBLISH;
    packet->fixed_header.qos = 1;
    packet->var_header.publish.packet_id = packet_id;
    packet->var_header.publish.topic_name.body = nng_strdup(topic);
    packet->var_header.publish.topic_name.len = strlen(topic);
    packet->payload.data = (uint8_t *) nng_strdup(payload);
    packet->payload.len = strlen(payload);
    return packet;
}

static void
init_work(nano_work *work, conf *cfg, struct pipe_content *pipe_ct,
    rule *rules, const rule_taos *taos_rule, rule_taos_parser_type parser,
    const char *topic, const char *payload, uint16_t packet_id)
{
    mqtt_msg_info msg_info = { .pipe = 7 };

    memset(work, 0, sizeof(*work));
    memset(cfg, 0, sizeof(*cfg));
    memset(pipe_ct, 0, sizeof(*pipe_ct));

    rule r;
    memset(&r, 0, sizeof(r));
    r.enabled = true;
    r.forword_type = RULE_FORWORD_TAOS;
    r.topic = (char *) topic;
    r.taos = (rule_taos *) taos_rule;
    r.taos->parser = parser;
    cvector_push_back(rules, r);
    cfg->rule_eng.option = RULE_ENG_TAOS;
    cfg->rule_eng.rules = rules;

    cvector_push_back(pipe_ct->msg_infos, msg_info);

    work->state = SEND;
    work->proto = PROTO_MQTT_BROKER;
    work->flag = CMD_PUBLISH;
    work->config = cfg;
    work->pipe_ct = pipe_ct;
    work->pub_packet = make_pub_packet(topic, payload, packet_id);
    assert(nng_msg_alloc(&work->msg, 0) == 0);
    assert(conn_param_alloc(&work->cparam) == 0);
    conn_param_set_clientid(work->cparam, "broker-send-client");
}

static void
free_after_test(rule *rules, nano_work *work)
{
    if (rules != NULL) {
        cvector_free(rules);
    }
    if (work->msg != NULL) {
        nng_msg_free(work->msg);
        work->msg = NULL;
    }
    if (work->pub_packet != NULL) {
        free_pub_packet(work->pub_packet);
        work->pub_packet = NULL;
    }
}

static void
test_broker_send_state_runs_weld_async_rule(void)
{
    const char *topic = "weld/v1/unit/telemetry/current";
    const char *payload = "{\"msg_id\":\"send-001\",\"ts_us\":1710000000000004}";
    nano_work work;
    conf cfg;
    struct pipe_content pipe_ct;
    rule *rules = NULL;
    rule_taos taos_rule;

    memset(&taos_rule, 0, sizeof(taos_rule));
    taos_rule.host = "127.0.0.1";
    taos_rule.port = 6041;
    taos_rule.username = "root";
    taos_rule.password = "taosdata";
    taos_rule.db = "mqtt_rule";
    taos_rule.table = "weld_current_raw";

    reset_stubs();
    init_work(&work, &cfg, &pipe_ct, rules, &taos_rule,
        RULE_TAOS_PARSER_WELD_TELEMETRY, topic, payload, 401);
    rules = cfg.rule_eng.rules;

    broker_test_run_send_state(&work, 0);

    assert(g_async_calls == 1);
    assert(strcmp(g_last_topic, topic) == 0);
    assert(strcmp(g_last_payload, payload) == 0);
    assert(g_last_qos == 1);
    assert(g_last_packet_id == 401);
    assert(strcmp(g_last_rule.table, "weld_current_raw") == 0);
    assert(work.state == RECV);
    assert(work.flag == 0);
    assert(work.msg == NULL);
    assert(work.pub_packet == NULL);
    assert(work.pipe_ct->msg_infos == NULL);

    free_after_test(rules, &work);
}

int
main(void)
{
    test_broker_send_state_runs_weld_async_rule();
    return 0;
}
