#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "include/conf_api.h"

static int         g_conf_init_calls = 0;
static int         g_conf_fini_calls = 0;
static int         g_conf_parse_ver2_calls = 0;
static int         g_conf_parse_calls = 0;
static int         g_reload_basic_calls = 0;
static int         g_reload_sqlite_calls = 0;
static int         g_reload_auth_calls = 0;
static int         g_reload_log_calls = 0;
static char        g_last_conf_file[256];

#include "../cmd_proc.c"

void
conf_init(conf *nanomq_conf)
{
    g_conf_init_calls++;
    memset(nanomq_conf, 0, sizeof(*nanomq_conf));
}

void
conf_fini(conf *nanomq_conf)
{
    g_conf_fini_calls++;
    if (nanomq_conf != NULL && nanomq_conf->conf_file != NULL) {
        nng_strfree(nanomq_conf->conf_file);
        nanomq_conf->conf_file = NULL;
    }
}

void
conf_parse(conf *nanomq_conf)
{
    g_conf_parse_calls++;
    snprintf(g_last_conf_file, sizeof(g_last_conf_file), "%s",
        nanomq_conf->conf_file != NULL ? nanomq_conf->conf_file : "");
}

void
conf_parse_ver2(conf *nanomq_conf)
{
    g_conf_parse_ver2_calls++;
    snprintf(g_last_conf_file, sizeof(g_last_conf_file), "%s",
        nanomq_conf->conf_file != NULL ? nanomq_conf->conf_file : "");
}

void
reload_basic_config(conf *cur_conf, conf *new_conf)
{
    (void) cur_conf;
    (void) new_conf;
    g_reload_basic_calls++;
}

void
reload_sqlite_config(conf_sqlite *cur_conf, conf_sqlite *new_conf)
{
    (void) cur_conf;
    (void) new_conf;
    g_reload_sqlite_calls++;
}

void
reload_auth_config(conf_auth *cur_conf, conf_auth *new_conf)
{
    (void) cur_conf;
    (void) new_conf;
    g_reload_auth_calls++;
}

void
reload_log_config(conf *cur_conf, conf *new_conf)
{
    (void) cur_conf;
    (void) new_conf;
    g_reload_log_calls++;
}

static void
reset_stubs(void)
{
    g_conf_init_calls = 0;
    g_conf_fini_calls = 0;
    g_conf_parse_ver2_calls = 0;
    g_conf_parse_calls = 0;
    g_reload_basic_calls = 0;
    g_reload_sqlite_calls = 0;
    g_reload_auth_calls = 0;
    g_reload_log_calls = 0;
    memset(g_last_conf_file, 0, sizeof(g_last_conf_file));
}

static void
write_temp_file(const char *path)
{
    FILE *fp = fopen(path, "w");
    assert(fp != NULL);
    fputs("# test\n", fp);
    fclose(fp);
}

static void
test_handle_recv_rejects_invalid_command(void)
{
    conf  cfg = { 0 };
    char *err = NULL;
    const char *msg = "{\"cmd\":\"stop\",\"conf_file\":\"/tmp/nanomq.conf\"}";

    reset_stubs();
    assert(handle_recv(msg, strlen(msg), &cfg, &err) == -1);
    assert(err != NULL);
    assert(strcmp(err, "reload failed, invalid command!") == 0);
    nng_strfree(err);
}

static void
test_handle_recv_requires_conf_file_when_config_missing_default(void)
{
    conf  cfg = { 0 };
    char *err = NULL;
    const char *msg = "{\"cmd\":\"reload\"}";

    reset_stubs();
    assert(handle_recv(msg, strlen(msg), &cfg, &err) == -1);
    assert(err != NULL);
    assert(strcmp(err, "reload failed, conf_file is not specified!") == 0);
    nng_strfree(err);
}

static void
test_handle_recv_rejects_missing_input_file(void)
{
    conf  cfg = { 0 };
    char *err = NULL;
    const char *msg = "{\"cmd\":\"reload\",\"conf_file\":\"/tmp/missing.conf\"}";

    reset_stubs();
    assert(handle_recv(msg, strlen(msg), &cfg, &err) == -1);
    assert(err != NULL);
    assert(strcmp(err, "reload failed, conf_file does not exist!") == 0);
    nng_strfree(err);
}

static void
test_handle_recv_rejects_wrong_conf_type(void)
{
    conf  cfg = { 0 };
    char *err = NULL;
    const char *path = "/tmp/nanomq_cmd_proc_wrong_type.conf";
    const char *msg =
        "{\"cmd\":\"reload\",\"conf_file\":\"/tmp/nanomq_cmd_proc_wrong_type.conf\",\"conf_type\":9}";

    reset_stubs();
    write_temp_file(path);
    cfg.conf_file = (char *) "/tmp/default.conf";
    assert(handle_recv(msg, strlen(msg), &cfg, &err) == -1);
    assert(err != NULL);
    assert(strcmp(err, "reload failed, wrong conf type!") == 0);
    assert(g_conf_init_calls == 1);
    assert(g_conf_fini_calls == 1);
    assert(g_conf_parse_ver2_calls == 0);
    assert(g_conf_parse_calls == 0);
    nng_strfree(err);
    unlink(path);
}

static void
test_handle_recv_uses_hocon_reload_path(void)
{
    conf        cfg = { 0 };
    char       *err = NULL;
    const char *path = "/tmp/nanomq_cmd_proc_hocon.conf";
    const char *msg =
        "{\"cmd\":\"reload\",\"conf_file\":\"/tmp/nanomq_cmd_proc_hocon.conf\",\"conf_type\":2}";

    reset_stubs();
    write_temp_file(path);
    cfg.conf_file = (char *) "/tmp/default.conf";
    assert(handle_recv(msg, strlen(msg), &cfg, &err) == 0);
    assert(err == NULL);
    assert(g_conf_init_calls == 1);
    assert(g_conf_fini_calls == 1);
    assert(g_conf_parse_ver2_calls == 1);
    assert(g_conf_parse_calls == 0);
    assert(g_reload_basic_calls == 1);
    assert(g_reload_sqlite_calls == 1);
    assert(g_reload_auth_calls == 1);
    assert(g_reload_log_calls == 1);
    assert(strcmp(g_last_conf_file, path) == 0);
    unlink(path);
}

static void
test_handle_recv_falls_back_to_existing_conf_file_for_legacy_parse(void)
{
    conf        cfg = { 0 };
    char       *err = NULL;
    const char *path = "/tmp/nanomq_cmd_proc_legacy.conf";
    const char *msg = "{\"cmd\":\"reload\",\"conf_type\":3}";

    reset_stubs();
    write_temp_file(path);
    cfg.conf_file = (char *) path;
    assert(handle_recv(msg, strlen(msg), &cfg, &err) == 0);
    assert(err == NULL);
    assert(g_conf_parse_ver2_calls == 0);
    assert(g_conf_parse_calls == 1);
    assert(strcmp(g_last_conf_file, path) == 0);
    unlink(path);
}

static void
test_encode_client_cmd_serializes_reload_request(void)
{
    char  *cmd = encode_client_cmd("/tmp/cmd.conf", 3);
    cJSON *obj = NULL;
    cJSON *item = NULL;
    char  *str = NULL;
    int    number = 0;
    int    rv = -1;

    assert(cmd != NULL);
    obj = cJSON_Parse(cmd);
    assert(obj != NULL);

    getStringValue(obj, item, "cmd", str, rv);
    assert(rv == 0);
    assert(strcmp(str, "reload") == 0);
    getStringValue(obj, item, "conf_file", str, rv);
    assert(rv == 0);
    assert(strcmp(str, "/tmp/cmd.conf") == 0);
    getNumberValue(obj, item, "conf_type", number, rv);
    assert(rv == 0);
    assert(number == 3);

    cJSON_Delete(obj);
    cJSON_free(cmd);
}

int
main(void)
{
    test_handle_recv_rejects_invalid_command();
    test_handle_recv_requires_conf_file_when_config_missing_default();
    test_handle_recv_rejects_missing_input_file();
    test_handle_recv_uses_hocon_reload_path();
    test_handle_recv_falls_back_to_existing_conf_file_for_legacy_parse();
    test_encode_client_cmd_serializes_reload_request();
    return 0;
}
