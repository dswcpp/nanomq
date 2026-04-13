#include <assert.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#if defined(SUPP_RULE_ENGINE)

#define DB_CLI_H
typedef int fdb_error_t;
typedef struct FDBDatabase {
    int marker;
} FDBDatabase;

fdb_error_t  fdb_run_network(void);
fdb_error_t  fdb_setup_network(void);
fdb_error_t  fdb_create_database(const char *cluster_file, FDBDatabase **out_db);
const char * fdb_get_error(fdb_error_t err);
int          fake_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
             void *(*start_routine)(void *), void *arg);
void         fake_exit(int code);

#define pthread_create fake_pthread_create
#define exit fake_exit
#include "../db_cli.c"
#undef pthread_create
#undef exit

static int         g_fdb_run_network_rc = 0;
static int         g_fdb_setup_network_rc = 0;
static int         g_fdb_create_database_rc = 0;
static int         g_fdb_get_error_calls = 0;
static int         g_pthread_create_calls = 0;
static void *    (*g_last_thread_start)(void *) = NULL;
static void       *g_last_thread_arg = NULL;
static int         g_exit_code = -1;
static jmp_buf     g_exit_jmp;
static FDBDatabase g_fake_db = { .marker = 42 };

fdb_error_t
fdb_run_network(void)
{
    return g_fdb_run_network_rc;
}

fdb_error_t
fdb_setup_network(void)
{
    return g_fdb_setup_network_rc;
}

fdb_error_t
fdb_create_database(const char *cluster_file, FDBDatabase **out_db)
{
    assert(cluster_file == NULL);
    if (g_fdb_create_database_rc == 0) {
        *out_db = &g_fake_db;
    }
    return g_fdb_create_database_rc;
}

const char *
fdb_get_error(fdb_error_t err)
{
    g_fdb_get_error_calls++;
    return err == 0 ? "ok" : "err";
}

int
fake_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
    void *(*start_routine)(void *), void *arg)
{
    (void) attr;
    g_pthread_create_calls++;
    g_last_thread_start = start_routine;
    g_last_thread_arg = arg;
    if (thread != NULL) {
        *thread = (pthread_t) 123;
    }
    return 0;
}

void
fake_exit(int code)
{
    g_exit_code = code;
    longjmp(g_exit_jmp, 1);
}

static void
reset_db_cli_stubs(void)
{
    g_fdb_run_network_rc = 0;
    g_fdb_setup_network_rc = 0;
    g_fdb_create_database_rc = 0;
    g_fdb_get_error_calls = 0;
    g_pthread_create_calls = 0;
    g_last_thread_start = NULL;
    g_last_thread_arg = NULL;
    g_exit_code = -1;
}

static void
test_run_network_success_and_error_exit(void)
{
    reset_db_cli_stubs();
    assert(runNetwork(NULL) == NULL);
    assert(g_fdb_get_error_calls == 0);

    reset_db_cli_stubs();
    g_fdb_run_network_rc = 7;
    if (setjmp(g_exit_jmp) == 0) {
        (void) runNetwork(NULL);
        assert(!"runNetwork should exit on FDB failure");
    }
    assert(g_exit_code == 1);
    assert(g_fdb_get_error_calls == 1);
}

static void
test_open_database_success_path_starts_network_thread(void)
{
    pthread_t     thread = 0;
    FDBDatabase * db = NULL;

    reset_db_cli_stubs();
    db = openDatabase(&thread);
    assert(db == &g_fake_db);
    assert(g_pthread_create_calls == 1);
    assert(g_last_thread_start == runNetwork);
    assert(g_last_thread_arg == NULL);
}

static void
test_open_database_exits_on_setup_or_create_failure(void)
{
    pthread_t thread = 0;

    reset_db_cli_stubs();
    g_fdb_setup_network_rc = 3;
    if (setjmp(g_exit_jmp) == 0) {
        (void) openDatabase(&thread);
        assert(!"openDatabase should exit when setup_network fails");
    }
    assert(g_exit_code == 1);
    assert(g_fdb_get_error_calls == 1);
    assert(g_pthread_create_calls == 0);

    reset_db_cli_stubs();
    g_fdb_create_database_rc = 9;
    if (setjmp(g_exit_jmp) == 0) {
        (void) openDatabase(&thread);
        assert(!"openDatabase should exit when create_database fails");
    }
    assert(g_exit_code == 1);
    assert(g_fdb_get_error_calls == 1);
    assert(g_pthread_create_calls == 1);
}

int
main(void)
{
    test_run_network_success_and_error_exit();
    test_open_database_success_path_starts_network_thread();
    test_open_database_exits_on_setup_or_create_failure();
    return 0;
}

#else

int
main(void)
{
    return 0;
}

#endif
