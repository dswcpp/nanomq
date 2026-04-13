#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "include/plugin.h"

static unsigned int g_registered_point = UINT32_MAX;
static int (*g_registered_cb)(void *data) = NULL;
static int g_register_calls = 0;

#include "../plugin/plugin_user_property.c"

int
plugin_hook_register(unsigned int point, int (*cb)(void *data))
{
    g_registered_point = point;
    g_registered_cb = cb;
    g_register_calls++;
    return 0;
}

static void
reset_hook_stub(void)
{
    g_registered_point = UINT32_MAX;
    g_registered_cb = NULL;
    g_register_calls = 0;
}

static void
test_cb_populates_two_user_properties(void)
{
    char *property[2] = { 0 };

    assert(cb(property) == 0);
    assert(strcmp(property[0], "alpha") == 0);
    assert(strcmp(property[1], "beta") == 0);

    free(property[0]);
    free(property[1]);
}

static void
test_cb_ignores_null_pointer(void)
{
    assert(cb(NULL) == 0);
}

static void
test_nano_plugin_init_registers_user_property_hook(void)
{
    reset_hook_stub();
    assert(nano_plugin_init() == 0);
    assert(g_register_calls == 1);
    assert(g_registered_point == HOOK_USER_PROPERTY);
    assert(g_registered_cb == cb);
}

int
main(void)
{
    test_cb_populates_two_user_properties();
    test_cb_ignores_null_pointer();
    test_nano_plugin_init_registers_user_property_hook();
    return 0;
}
