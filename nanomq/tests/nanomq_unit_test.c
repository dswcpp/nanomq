#include <assert.h>
#include <stdio.h>
#include <string.h>

#ifdef SUPP_NANO_LIB
#undef SUPP_NANO_LIB
#endif

#define main nanomq_main_impl
#include "../nanomq.c"
#undef main

static int g_start_calls = 0;
static int g_stop_calls = 0;
static int g_restart_calls = 0;
static int g_reload_calls = 0;
static int g_dflt_calls = 0;
static int g_last_argc = 0;
static char g_last_command[32];

int
broker_start(int argc, char **argv)
{
    g_start_calls++;
    g_last_argc = argc;
    snprintf(g_last_command, sizeof(g_last_command), "%s", argv[1]);
    return 101;
}

int
broker_stop(int argc, char **argv)
{
    g_stop_calls++;
    g_last_argc = argc;
    snprintf(g_last_command, sizeof(g_last_command), "%s", argv[1]);
    return 102;
}

int
broker_restart(int argc, char **argv)
{
    g_restart_calls++;
    g_last_argc = argc;
    snprintf(g_last_command, sizeof(g_last_command), "%s", argv[1]);
    return 103;
}

int
broker_reload(int argc, char **argv)
{
    g_reload_calls++;
    g_last_argc = argc;
    snprintf(g_last_command, sizeof(g_last_command), "%s", argv[1]);
    return 104;
}

int
broker_dflt(int argc, char **argv)
{
    g_dflt_calls++;
    g_last_argc = argc;
    snprintf(g_last_command, sizeof(g_last_command), "%s", argv[1]);
    return 105;
}

static void
reset_dispatch_state(void)
{
    g_start_calls = 0;
    g_stop_calls = 0;
    g_restart_calls = 0;
    g_reload_calls = 0;
    g_dflt_calls = 0;
    g_last_argc = 0;
    memset(g_last_command, 0, sizeof(g_last_command));
}

static void
test_get_cache_args_reflect_main_inputs(void)
{
    char *argv[] = { "nanomq", "start", "--conf", "/tmp/nanomq.conf", NULL };

    reset_dispatch_state();
    assert(nanomq_main_impl(4, argv) == 101);
    assert(g_start_calls == 1);
    assert(g_last_argc == 4);
    assert(strcmp(g_last_command, "start") == 0);
    assert(get_cache_argc() == 4);
    assert(get_cache_argv() == argv);
    assert(strcmp(get_cache_argv()[2], "--conf") == 0);
}

static void
test_main_dispatches_all_supported_commands(void)
{
    char *stop_argv[] = { "nanomq", "stop", NULL };
    char *restart_argv[] = { "nanomq", "restart", NULL };
    char *reload_argv[] = { "nanomq", "reload", NULL };
    char *help_argv[] = { "nanomq", "--help", NULL };

    reset_dispatch_state();
    assert(nanomq_main_impl(2, stop_argv) == 102);
    assert(g_stop_calls == 1);

    reset_dispatch_state();
    assert(nanomq_main_impl(2, restart_argv) == 103);
    assert(g_restart_calls == 1);

    reset_dispatch_state();
    assert(nanomq_main_impl(2, reload_argv) == 104);
    assert(g_reload_calls == 1);

    reset_dispatch_state();
    assert(nanomq_main_impl(2, help_argv) == 105);
    assert(g_dflt_calls == 1);
}

static void
test_main_rejects_missing_or_unknown_command(void)
{
    char *noarg_argv[] = { "nanomq", NULL };
    char *bad_argv[] = { "nanomq", "unknown", NULL };

    reset_dispatch_state();
    assert(nanomq_main_impl(1, noarg_argv) == EXIT_FAILURE);
    assert(g_start_calls == 0);
    assert(g_stop_calls == 0);
    assert(g_dflt_calls == 0);

    reset_dispatch_state();
    assert(nanomq_main_impl(2, bad_argv) == EXIT_FAILURE);
    assert(g_start_calls == 0);
    assert(g_stop_calls == 0);
    assert(g_restart_calls == 0);
    assert(g_reload_calls == 0);
    assert(g_dflt_calls == 0);
}

int
main(void)
{
    test_get_cache_args_reflect_main_inputs();
    test_main_dispatches_all_supported_commands();
    test_main_rejects_missing_or_unknown_command();
    return 0;
}
