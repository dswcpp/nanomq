#include <assert.h>
#include <stdio.h>
#include <string.h>

int
get_cache_argc(void)
{
    return 0;
}

char **
get_cache_argv(void)
{
    return NULL;
}

#include "../rest_api.c"

static void
test_uri_parse_tree_marks_terminal_segments(void)
{
    size_t count = 0;
    tree **nodes = uri_parse_tree("/api/v4/clients/alpha", &count);

    assert(nodes != NULL);
    assert(count == 3);
    assert(strcmp(nodes[0]->node, "") == 0);
    assert(nodes[0]->end == false);
    assert(strcmp(nodes[1]->node, "clients") == 0);
    assert(nodes[1]->end == false);
    assert(strcmp(nodes[2]->node, "alpha") == 0);
    assert(nodes[2]->end == true);

    uri_content ct = { .sub_count = count, .sub_tree = nodes };
    uri_tree_free(&ct);
}

static void
test_uri_param_parse_filters_invalid_pairs(void)
{
    size_t count = 0;
    kv   **params = uri_param_parse("a=1&broken&b=2&c=", &count);

    assert(params != NULL);
    assert(count == 2);
    assert(strcmp(params[0]->key, "a") == 0);
    assert(strcmp(params[0]->value, "1") == 0);
    assert(strcmp(params[1]->key, "b") == 0);
    assert(strcmp(params[1]->value, "2") == 0);

    uri_content ct = { .params_count = count, .params = params };
    uri_param_free(&ct);
}

static void
test_uri_parse_splits_path_and_query(void)
{
    uri_content *ct =
        uri_parse("/api/v4/rules/detail?id=100&name=tester");

    assert(ct != NULL);
    assert(ct->sub_count == 3);
    assert(strcmp(ct->sub_tree[0]->node, "") == 0);
    assert(strcmp(ct->sub_tree[1]->node, "rules") == 0);
    assert(strcmp(ct->sub_tree[2]->node, "detail") == 0);
    assert(ct->sub_tree[2]->end == true);
    assert(ct->params_count == 2);
    assert(strcmp(ct->params[0]->key, "id") == 0);
    assert(strcmp(ct->params[0]->value, "100") == 0);
    assert(strcmp(ct->params[1]->key, "name") == 0);
    assert(strcmp(ct->params[1]->value, "tester") == 0);

    uri_free(ct);
}

static void
test_uri_parse_root_has_no_segments(void)
{
    uri_content *ct = uri_parse(REST_URI_ROOT);

    assert(ct != NULL);
    assert(ct->sub_count == 0);
    assert(ct->params_count == 0);

    uri_free(ct);
}

static void
test_put_and_destroy_http_msg_manage_owned_buffers(void)
{
    http_msg    msg = { 0 };
    const char *payload = "{\"ok\":true}";

    put_http_msg(&msg, "application/json", "POST", "/api/v4/test",
        "Bearer token", payload, strlen(payload));
    assert(msg.content_type_len == strlen("application/json"));
    assert(strcmp(msg.content_type, "application/json") == 0);
    assert(msg.method_len == strlen("POST"));
    assert(strcmp(msg.method, "POST") == 0);
    assert(msg.uri_len == strlen("/api/v4/test"));
    assert(strcmp(msg.uri, "/api/v4/test") == 0);
    assert(msg.token_len == strlen("Bearer token"));
    assert(strcmp(msg.token, "Bearer token") == 0);
    assert(msg.data_len == strlen(payload));
    assert(memcmp(msg.data, payload, strlen(payload)) == 0);

    destory_http_msg(&msg);
    assert(msg.content_type_len == 0);
    assert(msg.method_len == 0);
    assert(msg.uri_len == 0);
    assert(msg.token_len == 0);
    assert(msg.data_len == 0);
    assert(msg.uri == NULL);
}

static void
test_mk_str_joins_arguments_with_separator(void)
{
    char *parts[] = { "nanomq", "reload", "--conf" };
    char *joined = mk_str(3, parts, " ");

    assert(joined != NULL);
    assert(strcmp(joined, "nanomq reload --conf ") == 0);
    free(joined);
}

int
main(void)
{
    test_uri_parse_tree_marks_terminal_segments();
    test_uri_param_parse_filters_invalid_pairs();
    test_uri_parse_splits_path_and_query();
    test_uri_parse_root_has_no_segments();
    test_put_and_destroy_http_msg_manage_owned_buffers();
    test_mk_str_joins_arguments_with_separator();
    return 0;
}
