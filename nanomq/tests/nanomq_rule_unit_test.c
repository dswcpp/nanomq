#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#if defined(SUPP_RULE_ENGINE)

#define nng_mqtt_msg_alloc fake_nng_mqtt_msg_alloc
#define nng_mqtt_msg_set_packet_type fake_nng_mqtt_msg_set_packet_type
#define nng_mqtt_msg_set_publish_dup fake_nng_mqtt_msg_set_publish_dup
#define nng_mqtt_msg_set_publish_qos fake_nng_mqtt_msg_set_publish_qos
#define nng_mqtt_msg_set_publish_payload fake_nng_mqtt_msg_set_publish_payload
#define nng_mqtt_msg_set_publish_topic fake_nng_mqtt_msg_set_publish_topic
#define nng_mqtt_msg_set_publish_property fake_nng_mqtt_msg_set_publish_property
#define nng_sendmsg fake_nng_sendmsg
#define nng_mqttv5_client_open fake_nng_mqttv5_client_open
#define nng_mqtt_client_open fake_nng_mqtt_client_open
#define nng_dialer_create fake_nng_dialer_create
#define nng_mqtt_msg_set_connect_keep_alive fake_nng_mqtt_msg_set_connect_keep_alive
#define nng_mqtt_msg_set_connect_proto_version fake_nng_mqtt_msg_set_connect_proto_version
#define nng_mqtt_msg_set_connect_clean_session fake_nng_mqtt_msg_set_connect_clean_session
#define nng_mqtt_msg_set_connect_client_id fake_nng_mqtt_msg_set_connect_client_id
#define nng_mqtt_msg_set_connect_user_name fake_nng_mqtt_msg_set_connect_user_name
#define nng_mqtt_msg_set_connect_password fake_nng_mqtt_msg_set_connect_password
#define nng_dialer_set_ptr fake_nng_dialer_set_ptr
#define nng_mqtt_set_connect_cb fake_nng_mqtt_set_connect_cb
#define nng_mqtt_set_disconnect_cb fake_nng_mqtt_set_disconnect_cb
#define nng_dialer_start fake_nng_dialer_start
#include "../nanomq_rule.c"
#undef nng_mqtt_msg_alloc
#undef nng_mqtt_msg_set_packet_type
#undef nng_mqtt_msg_set_publish_dup
#undef nng_mqtt_msg_set_publish_qos
#undef nng_mqtt_msg_set_publish_payload
#undef nng_mqtt_msg_set_publish_topic
#undef nng_mqtt_msg_set_publish_property
#undef nng_sendmsg
#undef nng_mqttv5_client_open
#undef nng_mqtt_client_open
#undef nng_dialer_create
#undef nng_mqtt_msg_set_connect_keep_alive
#undef nng_mqtt_msg_set_connect_proto_version
#undef nng_mqtt_msg_set_connect_clean_session
#undef nng_mqtt_msg_set_connect_client_id
#undef nng_mqtt_msg_set_connect_user_name
#undef nng_mqtt_msg_set_connect_password
#undef nng_dialer_set_ptr
#undef nng_mqtt_set_connect_cb
#undef nng_mqtt_set_disconnect_cb
#undef nng_dialer_start

static int        g_packet_type = 0;
static bool       g_publish_dup = false;
static uint8_t    g_publish_qos = 0;
static uint32_t   g_payload_len = 0;
static char       g_topic[128];
static uint8_t    g_payload[128];
static property  *g_publish_props = NULL;
static int        g_sendmsg_calls = 0;
static int        g_last_sendmsg_flags = 0;
static int        g_client_open_v4_calls = 0;
static int        g_client_open_v5_calls = 0;
static int        g_dialer_create_calls = 0;
static int        g_dialer_start_calls = 0;
static int        g_dialer_set_ptr_calls = 0;
static int        g_connect_cb_calls = 0;
static int        g_disconnect_cb_calls = 0;
static uint16_t   g_connect_keepalive = 0;
static uint8_t    g_connect_proto_ver = 0;
static bool       g_connect_clean_session = false;
static char       g_connect_clientid[64];
static char       g_connect_username[64];
static char       g_connect_password[64];
static char       g_dialer_address[128];
static char       g_dialer_opt[64];
static void      *g_dialer_ptr = NULL;
static int        g_fake_dialer_start_rc = 0;
static int        g_fake_dialer_create_rc = 0;

static void
reset_rule_stubs(void)
{
    g_packet_type = 0;
    g_publish_dup = false;
    g_publish_qos = 0;
    g_payload_len = 0;
    memset(g_topic, 0, sizeof(g_topic));
    memset(g_payload, 0, sizeof(g_payload));
    g_publish_props = NULL;
    g_sendmsg_calls = 0;
    g_last_sendmsg_flags = 0;
    g_client_open_v4_calls = 0;
    g_client_open_v5_calls = 0;
    g_dialer_create_calls = 0;
    g_dialer_start_calls = 0;
    g_dialer_set_ptr_calls = 0;
    g_connect_cb_calls = 0;
    g_disconnect_cb_calls = 0;
    g_connect_keepalive = 0;
    g_connect_proto_ver = 0;
    g_connect_clean_session = false;
    memset(g_connect_clientid, 0, sizeof(g_connect_clientid));
    memset(g_connect_username, 0, sizeof(g_connect_username));
    memset(g_connect_password, 0, sizeof(g_connect_password));
    memset(g_dialer_address, 0, sizeof(g_dialer_address));
    memset(g_dialer_opt, 0, sizeof(g_dialer_opt));
    g_dialer_ptr = NULL;
    g_fake_dialer_start_rc = 0;
    g_fake_dialer_create_rc = 0;
}

int
fake_nng_mqtt_msg_alloc(nng_msg **msg, size_t size)
{
    (void) size;
    *msg = malloc(1);
    assert(*msg != NULL);
    return 0;
}

void
fake_nng_mqtt_msg_set_packet_type(
    nng_msg *msg, nng_mqtt_packet_type packet_type)
{
    (void) msg;
    g_packet_type = packet_type;
}

void
fake_nng_mqtt_msg_set_publish_dup(nng_msg *msg, bool dup)
{
    (void) msg;
    g_publish_dup = dup;
}

void
fake_nng_mqtt_msg_set_publish_qos(nng_msg *msg, uint8_t qos)
{
    (void) msg;
    g_publish_qos = qos;
}

int
fake_nng_mqtt_msg_set_publish_payload(nng_msg *msg, uint8_t *payload, uint32_t len)
{
    (void) msg;
    assert(len < sizeof(g_payload));
    memcpy(g_payload, payload, len);
    g_payload_len = len;
    return 0;
}

int
fake_nng_mqtt_msg_set_publish_topic(nng_msg *msg, const char *topic)
{
    (void) msg;
    snprintf(g_topic, sizeof(g_topic), "%s", topic);
    return 0;
}

void
fake_nng_mqtt_msg_set_publish_property(nng_msg *msg, property *props)
{
    (void) msg;
    g_publish_props = props;
}

int
fake_nng_sendmsg(nng_socket sock, nng_msg *msg, int flags)
{
    (void) sock;
    g_sendmsg_calls++;
    g_last_sendmsg_flags = flags;
    free(msg);
    return 0;
}

int
fake_nng_mqttv5_client_open(nng_socket *sock)
{
    g_client_open_v5_calls++;
    sock->id = 51;
    return 0;
}

int
fake_nng_mqtt_client_open(nng_socket *sock)
{
    g_client_open_v4_calls++;
    sock->id = 41;
    return 0;
}

int
fake_nng_dialer_create(nng_dialer *dialer, nng_socket sock, const char *addr)
{
    (void) sock;
    g_dialer_create_calls++;
    snprintf(g_dialer_address, sizeof(g_dialer_address), "%s", addr);
    dialer->id = 77;
    return g_fake_dialer_create_rc;
}

void
fake_nng_mqtt_msg_set_connect_keep_alive(nng_msg *msg, uint16_t keepalive)
{
    (void) msg;
    g_connect_keepalive = keepalive;
}

void
fake_nng_mqtt_msg_set_connect_proto_version(nng_msg *msg, uint8_t proto_ver)
{
    (void) msg;
    g_connect_proto_ver = proto_ver;
}

void
fake_nng_mqtt_msg_set_connect_clean_session(nng_msg *msg, bool clean_session)
{
    (void) msg;
    g_connect_clean_session = clean_session;
}

void
fake_nng_mqtt_msg_set_connect_client_id(nng_msg *msg, const char *clientid)
{
    (void) msg;
    snprintf(g_connect_clientid, sizeof(g_connect_clientid), "%s", clientid);
}

void
fake_nng_mqtt_msg_set_connect_user_name(nng_msg *msg, const char *username)
{
    (void) msg;
    snprintf(g_connect_username, sizeof(g_connect_username), "%s", username);
}

void
fake_nng_mqtt_msg_set_connect_password(nng_msg *msg, const char *password)
{
    (void) msg;
    snprintf(g_connect_password, sizeof(g_connect_password), "%s", password);
}

int
fake_nng_dialer_set_ptr(nng_dialer dialer, const char *opt, void *ptr)
{
    (void) dialer;
    g_dialer_set_ptr_calls++;
    snprintf(g_dialer_opt, sizeof(g_dialer_opt), "%s", opt);
    g_dialer_ptr = ptr;
    return 0;
}

int
fake_nng_mqtt_set_connect_cb(nng_socket sock, nng_pipe_cb cb, void *arg)
{
    (void) sock;
    (void) cb;
    (void) arg;
    g_connect_cb_calls++;
    return 0;
}

int
fake_nng_mqtt_set_disconnect_cb(nng_socket sock, nng_pipe_cb cb, void *arg)
{
    (void) sock;
    (void) cb;
    (void) arg;
    g_disconnect_cb_calls++;
    return 0;
}

int
fake_nng_dialer_start(nng_dialer dialer, int flags)
{
    (void) dialer;
    assert(flags == NNG_FLAG_NONBLOCK);
    g_dialer_start_calls++;
    return g_fake_dialer_start_rc;
}

static void
test_nano_client_publish_builds_publish_message_and_sends_nonblocking(void)
{
    nng_socket sock = { 0 };
    uint8_t    payload[] = "telemetry";
    property   props = { 0 };

    reset_rule_stubs();
    assert(nano_client_publish(&sock, "weld/1/up", payload,
        sizeof(payload) - 1, 1, &props) == 0);
    assert(g_packet_type == NNG_MQTT_PUBLISH);
    assert(g_publish_dup == true);
    assert(g_publish_qos == 1);
    assert(g_payload_len == sizeof(payload) - 1);
    assert(memcmp(g_payload, payload, sizeof(payload) - 1) == 0);
    assert(strcmp(g_topic, "weld/1/up") == 0);
    assert(g_publish_props == &props);
    assert(g_sendmsg_calls == 1);
    assert(g_last_sendmsg_flags == NNG_FLAG_ALLOC);
}

static void
test_nano_client_sets_up_v4_connection_and_callbacks(void)
{
    nng_socket sock = { 0 };
    repub_t    repub = { 0 };

    reset_rule_stubs();
    repub.address = "mqtt-tcp://127.0.0.1:1883";
    repub.proto_ver = MQTT_PROTOCOL_VERSION_v311;
    repub.clientid = "cid-v4";
    repub.clean_start = true;
    repub.username = "user-v4";
    repub.password = "pass-v4";
    repub.keepalive = 45;

    assert(nano_client(&sock, &repub) == 0);
    assert(g_client_open_v4_calls == 1);
    assert(g_client_open_v5_calls == 0);
    assert(g_dialer_create_calls == 1);
    assert(strcmp(g_dialer_address, "mqtt-tcp://127.0.0.1:1883") == 0);
    assert(g_connect_keepalive == 45);
    assert(g_connect_proto_ver == MQTT_PROTOCOL_VERSION_v311);
    assert(g_connect_clean_session == true);
    assert(strcmp(g_connect_clientid, "cid-v4") == 0);
    assert(strcmp(g_connect_username, "user-v4") == 0);
    assert(strcmp(g_connect_password, "pass-v4") == 0);
    assert(g_dialer_set_ptr_calls == 1);
    assert(strcmp(g_dialer_opt, NNG_OPT_MQTT_CONNMSG) == 0);
    assert(g_dialer_ptr != NULL);
    assert(g_connect_cb_calls == 1);
    assert(g_disconnect_cb_calls == 1);
    assert(g_dialer_start_calls == 1);
    assert(repub.sock == &sock);
}

static void
test_nano_client_uses_v5_open_and_returns_dialer_error(void)
{
    nng_socket sock = { 0 };
    repub_t    repub = { 0 };

    reset_rule_stubs();
    repub.address = "mqtt-tcp://broker:1883";
    repub.proto_ver = MQTT_PROTOCOL_VERSION_v5;
    g_fake_dialer_create_rc = NNG_EADDRINVAL;

    assert(nano_client(&sock, &repub) == NNG_EADDRINVAL);
    assert(g_client_open_v5_calls == 1);
    assert(g_client_open_v4_calls == 0);
    assert(g_dialer_create_calls == 1);
    assert(g_dialer_set_ptr_calls == 0);
    assert(g_dialer_start_calls == 0);
}

int
main(void)
{
    test_nano_client_publish_builds_publish_message_and_sends_nonblocking();
    test_nano_client_sets_up_v4_connection_and_callbacks();
    test_nano_client_uses_v5_open_and_returns_dialer_error();
    return 0;
}

#else

int
main(void)
{
    return 0;
}

#endif
