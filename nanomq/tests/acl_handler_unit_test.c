#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "nng/protocol/mqtt/mqtt_parser.h"
#include "../acl_handler.c"

static acl_rule *
make_rule(acl_rule_type type, acl_action_type action, acl_permit permit)
{
    acl_rule *rule = calloc(1, sizeof(*rule));
    assert(rule != NULL);
    rule->rule_type = type;
    rule->action = action;
    rule->permit = permit;
    return rule;
}

static void
free_rule(acl_rule *rule)
{
    if (rule == NULL) {
        return;
    }
    if (rule->rule_type == ACL_AND || rule->rule_type == ACL_OR) {
        if (rule->rule_ct.array.rules != NULL) {
            for (size_t i = 0; i < rule->rule_ct.array.count; ++i) {
                free(rule->rule_ct.array.rules[i]);
            }
            free(rule->rule_ct.array.rules);
        }
    }
    if (rule->topics != NULL) {
        free(rule->topics);
    }
    free(rule);
}

static void
free_acl_rules(conf *cfg)
{
    if (cfg->acl.rules == NULL) {
        return;
    }
    for (size_t i = 0; i < cfg->acl.rule_count; ++i) {
        free_rule(cfg->acl.rules[i]);
    }
    free(cfg->acl.rules);
    cfg->acl.rules = NULL;
    cfg->acl.rule_count = 0;
}

static conn_param *
make_conn_param(const char *clientid, const char *ip)
{
    conn_param *cp = NULL;
    assert(conn_param_alloc(&cp) == 0);
    conn_param_set_clientid(cp, clientid);
    snprintf(conn_param_get_ip_addr_v4(cp), 16, "%s", ip);
    return cp;
}

static void
test_match_rule_content_str_supports_all_and_exact(void)
{
    acl_rule_ct ct = { 0 };

    ct.type = ACL_RULE_ALL;
    assert(match_rule_content_str(&ct, "anything") == true);

    ct.type = ACL_RULE_SINGLE_STRING;
    ct.value.str = "alpha";
    assert(match_rule_content_str(&ct, "alpha") == true);
    assert(match_rule_content_str(&ct, "beta") == false);
    assert(match_rule_content_str(&ct, NULL) == false);
}

static void
test_replace_topic_replaces_clientid_placeholder(void)
{
    conn_param *cp = make_conn_param("device-01", "127.0.0.1");
    char *topic = replace_topic("site/${clientid}/telemetry", cp);

    assert(topic != NULL);
    assert(strcmp(topic, "site/device-01/telemetry") == 0);
    nng_strfree(topic);
    conn_param_free(cp);
}

static void
test_auth_acl_matches_clientid_and_topic_filter(void)
{
    conf      cfg = { 0 };
    conn_param *cp = make_conn_param("client-a", "127.0.0.1");
    acl_rule  *rule = make_rule(ACL_CLIENTID, ACL_PUB, ACL_ALLOW);

    rule->rule_ct.ct.type = ACL_RULE_SINGLE_STRING;
    rule->rule_ct.ct.value.str = "client-a";
    rule->topic_count = 1;
    rule->topics = calloc(1, sizeof(char *));
    assert(rule->topics != NULL);
    rule->topics[0] = "factory/+/telemetry";

    cfg.acl_nomatch = ACL_DENY;
    cfg.acl.rule_count = 1;
    cfg.acl.rules = calloc(1, sizeof(acl_rule *));
    assert(cfg.acl.rules != NULL);
    cfg.acl.rules[0] = rule;

    assert(auth_acl(&cfg, ACL_PUB, cp, "factory/line1/telemetry") == true);
    conn_param_free(cp);
    free_acl_rules(&cfg);
}

static void
test_auth_acl_honors_exact_at_topic_match(void)
{
    conf      cfg = { 0 };
    conn_param *cp = make_conn_param("client-a", "127.0.0.1");
    acl_rule  *rule = make_rule(ACL_NONE, ACL_ALL, ACL_ALLOW);

    rule->topic_count = 1;
    rule->topics = calloc(1, sizeof(char *));
    assert(rule->topics != NULL);
    rule->topics[0] = "@/sys/exact/topic";

    cfg.acl_nomatch = ACL_DENY;
    cfg.acl.rule_count = 1;
    cfg.acl.rules = calloc(1, sizeof(acl_rule *));
    assert(cfg.acl.rules != NULL);
    cfg.acl.rules[0] = rule;

    assert(auth_acl(&cfg, ACL_SUB, cp, "/sys/exact/topic") == true);
    assert(auth_acl(&cfg, ACL_SUB, cp, "/sys/exact/topic/child") == false);
    conn_param_free(cp);
    free_acl_rules(&cfg);
}

static void
test_auth_acl_supports_placeholder_topics(void)
{
    conf      cfg = { 0 };
    conn_param *cp = make_conn_param("robot-9", "127.0.0.1");
    acl_rule  *rule = make_rule(ACL_NONE, ACL_ALL, ACL_ALLOW);

    rule->topic_count = 1;
    rule->topics = calloc(1, sizeof(char *));
    assert(rule->topics != NULL);
    rule->topics[0] = "devices/${clientid}/up";

    cfg.acl_nomatch = ACL_DENY;
    cfg.acl.rule_count = 1;
    cfg.acl.rules = calloc(1, sizeof(acl_rule *));
    assert(cfg.acl.rules != NULL);
    cfg.acl.rules[0] = rule;

    assert(auth_acl(&cfg, ACL_PUB, cp, "devices/robot-9/up") == true);
    assert(auth_acl(&cfg, ACL_PUB, cp, "devices/robot-8/up") == false);
    conn_param_free(cp);
    free_acl_rules(&cfg);
}

static void
test_auth_acl_supports_and_rule(void)
{
    conf       cfg = { 0 };
    conn_param *cp = make_conn_param("cid-77", "10.0.0.8");
    acl_rule   *rule = make_rule(ACL_AND, ACL_PUB, ACL_ALLOW);

    rule->rule_ct.array.count = 2;
    rule->rule_ct.array.rules = calloc(2, sizeof(acl_sub_rule *));
    assert(rule->rule_ct.array.rules != NULL);

    rule->rule_ct.array.rules[0] = calloc(1, sizeof(acl_sub_rule));
    rule->rule_ct.array.rules[1] = calloc(1, sizeof(acl_sub_rule));
    assert(rule->rule_ct.array.rules[0] != NULL);
    assert(rule->rule_ct.array.rules[1] != NULL);

    rule->rule_ct.array.rules[0]->rule_type = ACL_CLIENTID;
    rule->rule_ct.array.rules[0]->rule_ct.type = ACL_RULE_SINGLE_STRING;
    rule->rule_ct.array.rules[0]->rule_ct.value.str = "cid-77";

    rule->rule_ct.array.rules[1]->rule_type = ACL_IPADDR;
    rule->rule_ct.array.rules[1]->rule_ct.type = ACL_RULE_SINGLE_STRING;
    rule->rule_ct.array.rules[1]->rule_ct.value.str = "10.0.0.8";

    cfg.acl_nomatch = ACL_DENY;
    cfg.acl.rule_count = 1;
    cfg.acl.rules = calloc(1, sizeof(acl_rule *));
    assert(cfg.acl.rules != NULL);
    cfg.acl.rules[0] = rule;

    assert(auth_acl(&cfg, ACL_PUB, cp, "topic/x") == true);
    conn_param_free(cp);
    free_acl_rules(&cfg);
}

static void
test_auth_acl_supports_or_rule_and_nomatch_allow(void)
{
    conf       cfg = { 0 };
    conn_param *cp = make_conn_param("cid-10", "192.168.1.8");
    acl_rule   *rule = make_rule(ACL_OR, ACL_SUB, ACL_DENY);

    rule->rule_ct.array.count = 2;
    rule->rule_ct.array.rules = calloc(2, sizeof(acl_sub_rule *));
    assert(rule->rule_ct.array.rules != NULL);

    rule->rule_ct.array.rules[0] = calloc(1, sizeof(acl_sub_rule));
    rule->rule_ct.array.rules[1] = calloc(1, sizeof(acl_sub_rule));
    assert(rule->rule_ct.array.rules[0] != NULL);
    assert(rule->rule_ct.array.rules[1] != NULL);

    rule->rule_ct.array.rules[0]->rule_type = ACL_CLIENTID;
    rule->rule_ct.array.rules[0]->rule_ct.type = ACL_RULE_SINGLE_STRING;
    rule->rule_ct.array.rules[0]->rule_ct.value.str = "other";

    rule->rule_ct.array.rules[1]->rule_type = ACL_IPADDR;
    rule->rule_ct.array.rules[1]->rule_ct.type = ACL_RULE_SINGLE_STRING;
    rule->rule_ct.array.rules[1]->rule_ct.value.str = "192.168.1.8";

    cfg.acl_nomatch = ACL_ALLOW;
    cfg.acl.rule_count = 1;
    cfg.acl.rules = calloc(1, sizeof(acl_rule *));
    assert(cfg.acl.rules != NULL);
    cfg.acl.rules[0] = rule;

    assert(auth_acl(&cfg, ACL_SUB, cp, "topic/x") == false);

    free_acl_rules(&cfg);
    cfg.acl_nomatch = ACL_ALLOW;
    assert(auth_acl(&cfg, ACL_SUB, cp, "topic/x") == true);
    conn_param_free(cp);
}

int
main(void)
{
    test_match_rule_content_str_supports_all_and_exact();
    test_replace_topic_replaces_clientid_placeholder();
    test_auth_acl_matches_clientid_and_topic_filter();
    test_auth_acl_honors_exact_at_topic_match();
    test_auth_acl_supports_placeholder_topics();
    test_auth_acl_supports_and_rule();
    test_auth_acl_supports_or_rule_and_nomatch_allow();
    return 0;
}
