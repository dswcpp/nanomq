#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define dlopen fake_dlopen
#define dlsym fake_dlsym
#include "../plugin/plugin.c"
#undef dlopen
#undef dlsym

static int   g_fake_plugin_init_calls = 0;
static int   g_fake_plugin_init_rc = 0;
static bool  g_fake_dlopen_fail = false;
static bool  g_fake_dlsym_fail = false;
static void *g_fake_handle = (void *) 0x1234;

static int   g_cb1_calls = 0;
static int   g_cb2_calls = 0;
static int   g_last_cb_value = 0;

static int fake_plugin_entry(void);

void *
fake_dlopen(const char *path, int mode)
{
    (void) mode;
    if (g_fake_dlopen_fail || path == NULL) {
        return NULL;
    }
    return g_fake_handle;
}

void *
fake_dlsym(void *handle, const char *symbol)
{
    (void) handle;
    if (g_fake_dlsym_fail || strcmp(symbol, "nano_plugin_init") != 0) {
        return NULL;
    }
    return (void *) fake_plugin_entry;
}

static int
fake_plugin_entry(void)
{
    g_fake_plugin_init_calls++;
    return g_fake_plugin_init_rc;
}

static int
hook_cb1(void *data)
{
    g_cb1_calls++;
    g_last_cb_value = *(int *) data;
    return 11;
}

static int
hook_cb2(void *data)
{
    g_cb2_calls++;
    g_last_cb_value = *(int *) data;
    return 22;
}

static void
free_hooks(void)
{
    for (int i = 0; i < cvector_size(g_hooks); ++i) {
        nng_free(g_hooks[i], sizeof(struct plugin_hook));
    }
    cvector_free(g_hooks);
    g_hooks = NULL;
}

static void
reset_plugin_state(void)
{
    if (g_plugins != NULL) {
        plugins_clear();
        g_plugins = NULL;
    }
    if (g_hooks != NULL) {
        free_hooks();
    }

    g_fake_plugin_init_calls = 0;
    g_fake_plugin_init_rc = 0;
    g_fake_dlopen_fail = false;
    g_fake_dlsym_fail = false;
    g_cb1_calls = 0;
    g_cb2_calls = 0;
    g_last_cb_value = 0;
}

static void
test_plugin_hook_register_replaces_existing_callback(void)
{
    int value = 7;

    reset_plugin_state();
    assert(plugin_hook_register(HOOK_USER_PROPERTY, hook_cb1) == 0);
    assert(cvector_size(g_hooks) == 1);
    assert(plugin_hook_call(HOOK_USER_PROPERTY, &value) == 11);
    assert(g_cb1_calls == 1);
    assert(g_last_cb_value == 7);

    value = 9;
    assert(plugin_hook_register(HOOK_USER_PROPERTY, hook_cb2) == 0);
    assert(cvector_size(g_hooks) == 1);
    assert(plugin_hook_call(HOOK_USER_PROPERTY, &value) == 22);
    assert(g_cb1_calls == 1);
    assert(g_cb2_calls == 1);
    assert(g_last_cb_value == 9);
}

static void
test_plugin_init_handles_dlopen_and_dlsym_failures(void)
{
    struct nano_plugin plugin = { 0 };

    reset_plugin_state();
    plugin.path = "missing.so";
    g_fake_dlopen_fail = true;
    assert(plugin_init(&plugin) == NNG_EINVAL);

    reset_plugin_state();
    plugin.path = "missing-init.so";
    g_fake_dlsym_fail = true;
    assert(plugin_init(&plugin) == NNG_EINVAL);
}

static void
test_plugin_init_and_register_success_and_failure_paths(void)
{
    struct nano_plugin plugin = { 0 };

    reset_plugin_state();
    plugin.path = "ok.so";
    assert(plugin_init(&plugin) == 0);
    assert(g_fake_plugin_init_calls == 1);
    assert(plugin.init != NULL);

    reset_plugin_state();
    g_fake_plugin_init_rc = -1;
    plugin.path = "bad-init.so";
    assert(plugin_init(&plugin) == NNG_EINVAL);
    assert(g_fake_plugin_init_calls == 1);

    reset_plugin_state();
    assert(plugin_register(NULL) == NNG_EINVAL);

    reset_plugin_state();
    assert(plugin_register("plugin-a.so") == 0);
    assert(cvector_size(g_plugins) == 1);
    assert(strcmp(g_plugins[0]->path, "plugin-a.so") == 0);
    assert(g_fake_plugin_init_calls == 1);

    plugins_clear();
    g_plugins = NULL;

    reset_plugin_state();
    g_fake_plugin_init_rc = -1;
    assert(plugin_register("plugin-bad.so") == NNG_EINVAL);
    assert(cvector_size(g_plugins) == 0);
}

int
main(void)
{
    test_plugin_hook_register_replaces_existing_callback();
    test_plugin_init_handles_dlopen_and_dlsym_failures();
    test_plugin_init_and_register_success_and_failure_paths();
    reset_plugin_state();
    return 0;
}
