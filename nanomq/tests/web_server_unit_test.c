#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define nng_id_map_alloc fake_nng_id_map_alloc
#define nng_id_map_free fake_nng_id_map_free
#define nng_id_map_foreach2 fake_nng_id_map_foreach2
#define nng_id_count fake_nng_id_count
#define nng_id_remove fake_nng_id_remove
#define nng_sleep_aio fake_nng_sleep_aio
#define nng_aio_alloc fake_nng_aio_alloc
#define nng_aio_stop fake_nng_aio_stop
#define nng_aio_free fake_nng_aio_free
#define nng_http_res_free fake_nng_http_res_free
#define nng_msg_free fake_nng_msg_free
#define nng_ctx_close fake_nng_ctx_close
#define nng_ctx_open fake_nng_ctx_open
#include "../web_server.c"
#undef nng_id_map_alloc
#undef nng_id_map_free
#undef nng_id_map_foreach2
#undef nng_id_count
#undef nng_id_remove
#undef nng_sleep_aio
#undef nng_aio_alloc
#undef nng_aio_stop
#undef nng_aio_free
#undef nng_http_res_free
#undef nng_msg_free
#undef nng_ctx_close
#undef nng_ctx_open

static int          g_id_map_alloc_calls = 0;
static int          g_id_map_free_calls = 0;
static int          g_id_map_foreach2_calls = 0;
static int          g_id_remove_calls = 0;
static uint64_t     g_last_removed_id = 0;
static int          g_fake_id_count = 0;
static int          g_aio_alloc_calls = 0;
static int          g_aio_stop_calls = 0;
static int          g_aio_free_calls = 0;
static int          g_http_res_free_calls = 0;
static int          g_msg_free_calls = 0;
static int          g_ctx_close_calls = 0;
static int          g_ctx_open_calls = 0;
static nng_duration g_last_sleep_duration = 0;
static int          g_sleep_calls = 0;
static int          g_fake_aio_alloc_rc = 0;
static int          g_fake_id_map_alloc_rc = 0;
static int          g_fake_ctx_open_rc = 0;
static nng_id_map  *g_fake_map_ptr = (nng_id_map *) 0xabc1;
static nng_aio     *g_fake_aio_ptr = (nng_aio *) 0xabc2;

void
put_http_msg(http_msg *msg, const char *content_type, const char *method,
    const char *uri, const char *token, const char *data, size_t data_sz)
{
    (void) msg;
    (void) content_type;
    (void) method;
    (void) uri;
    (void) token;
    (void) data;
    (void) data_sz;
}

void
destory_http_msg(http_msg *msg)
{
    (void) msg;
}

http_msg
process_request(http_msg *msg, conf_http_server *config, nng_socket *sock)
{
    (void) msg;
    (void) config;
    (void) sock;
    http_msg out = { 0 };
    return out;
}

int
fake_nng_id_map_alloc(nng_id_map **map, uint64_t lo, uint64_t hi, int flags)
{
    (void) lo;
    (void) hi;
    (void) flags;
    g_id_map_alloc_calls++;
    if (g_fake_id_map_alloc_rc == 0) {
        *map = g_fake_map_ptr;
    }
    return g_fake_id_map_alloc_rc;
}

void
fake_nng_id_map_free(nng_id_map *map)
{
    assert(map == g_fake_map_ptr);
    g_id_map_free_calls++;
}

void
fake_nng_id_map_foreach2(
    nng_id_map *map, void (*cb)(void *, void *, void *), void *arg)
{
    (void) cb;
    (void) arg;
    assert(map == g_fake_map_ptr);
    g_id_map_foreach2_calls++;
}

uint32_t
fake_nng_id_count(nng_id_map *map)
{
    assert(map == g_fake_map_ptr);
    return (uint32_t) g_fake_id_count;
}

int
fake_nng_id_remove(nng_id_map *map, uint64_t id)
{
    assert(map == g_fake_map_ptr);
    g_id_remove_calls++;
    g_last_removed_id = id;
    return 0;
}

void
fake_nng_sleep_aio(nng_duration duration, nng_aio *aio)
{
    assert(aio == g_fake_aio_ptr);
    g_sleep_calls++;
    g_last_sleep_duration = duration;
}

int
fake_nng_aio_alloc(nng_aio **aio, void (*cb)(void *), void *arg)
{
    (void) cb;
    (void) arg;
    g_aio_alloc_calls++;
    if (g_fake_aio_alloc_rc == 0) {
        *aio = g_fake_aio_ptr;
    }
    return g_fake_aio_alloc_rc;
}

void
fake_nng_aio_stop(nng_aio *aio)
{
    assert(aio == g_fake_aio_ptr);
    g_aio_stop_calls++;
}

void
fake_nng_aio_free(nng_aio *aio)
{
    assert(aio == g_fake_aio_ptr);
    g_aio_free_calls++;
}

void
fake_nng_http_res_free(nng_http_res *res)
{
    (void) res;
    g_http_res_free_calls++;
}

void
fake_nng_msg_free(nng_msg *msg)
{
    (void) msg;
    g_msg_free_calls++;
}

int
fake_nng_ctx_close(nng_ctx ctx)
{
    assert(ctx.id != 0);
    g_ctx_close_calls++;
    return 0;
}

int
fake_nng_ctx_open(nng_ctx *ctx, nng_socket sock)
{
    (void) sock;
    g_ctx_open_calls++;
    if (g_fake_ctx_open_rc == 0) {
        ctx->id = 321;
    }
    return g_fake_ctx_open_rc;
}

static void
reset_web_server_stubs(void)
{
    g_id_map_alloc_calls = 0;
    g_id_map_free_calls = 0;
    g_id_map_foreach2_calls = 0;
    g_id_remove_calls = 0;
    g_last_removed_id = 0;
    g_fake_id_count = 0;
    g_aio_alloc_calls = 0;
    g_aio_stop_calls = 0;
    g_aio_free_calls = 0;
    g_http_res_free_calls = 0;
    g_msg_free_calls = 0;
    g_ctx_close_calls = 0;
    g_ctx_open_calls = 0;
    g_last_sleep_duration = 0;
    g_sleep_calls = 0;
    g_fake_aio_alloc_rc = 0;
    g_fake_id_map_alloc_rc = 0;
    g_fake_ctx_open_rc = 0;
    job_freelist = NULL;
    req_sock.id = 0;
    http_server_conf = NULL;
    global_config = NULL;
    boot_time = 0;
}

static void
free_job_lock_if_needed(void)
{
    if (job_lock != NULL) {
        nng_mtx_free(job_lock);
        job_lock = NULL;
    }
}

static void
test_global_and_http_server_config_helpers_apply_defaults(void)
{
    conf            cfg = { 0 };
    conf_http_server http = { 0 };

    reset_web_server_stubs();
    set_global_conf(&cfg);
    assert(get_global_conf() == &cfg);

    set_http_server_conf(&http);
    assert(get_http_server_conf() == &http);
    assert(strcmp(http.username, HTTP_DEFAULT_USER) == 0);
    assert(strcmp(http.password, HTTP_DEFAULT_PASSWORD) == 0);

    http.username = "custom-user";
    http.password = "custom-pass";
    set_http_server_conf(&http);
    assert(strcmp(http.username, "custom-user") == 0);
    assert(strcmp(http.password, "custom-pass") == 0);

    boot_time = 123456;
    assert(get_boot_time() == 123456);
}

static void
test_acl_cache_init_success_and_failure_paths(void)
{
    conf_auth_http auth = { 0 };

    reset_web_server_stubs();
    auth.cache_ttl = 6;
    assert(nmq_acl_cache_init(&auth) == 0);
    assert(auth.acl_cache_map == g_fake_map_ptr);
    assert(auth.acl_cache_reset_aio == g_fake_aio_ptr);
    assert(g_id_map_alloc_calls == 1);
    assert(g_aio_alloc_calls == 1);
    assert(g_sleep_calls == 1);
    assert(g_last_sleep_duration == 6000);

    reset_web_server_stubs();
    auth.acl_cache_map = NULL;
    auth.acl_cache_reset_aio = NULL;
    auth.cache_ttl = 3;
    g_fake_aio_alloc_rc = NNG_ENOMEM;
    assert(nmq_acl_cache_init(&auth) == NNG_ENOMEM);
    assert(g_id_map_alloc_calls == 1);
    assert(g_aio_alloc_calls == 1);
    assert(g_id_map_free_calls == 1);
    assert(auth.acl_cache_map == NULL);
}

static void
test_acl_cache_reset_helpers_remove_entries_and_reschedule(void)
{
    conf_auth_http auth = { 0 };
    uint64_t       key = 42;

    reset_web_server_stubs();
    auth.acl_cache_map = g_fake_map_ptr;
    auth.acl_cache_reset_aio = g_fake_aio_ptr;
    auth.cache_ttl = 8;
    assert(nng_mtx_alloc(&auth.acl_cache_mtx) == 0);

    nmq_acl_cache_reset_cb(&key, NULL, &auth);
    assert(g_id_remove_calls == 1);
    assert(g_last_removed_id == 42);

    g_fake_id_count = 2;
    nmq_acl_cache_reset_timer_cb(&auth);
    assert(g_id_map_foreach2_calls == 1);
    assert(g_sleep_calls == 1);
    assert(g_last_sleep_duration == 8000);

    g_fake_id_count = 0;
    nmq_acl_cache_reset_timer_cb(&auth);
    assert(g_id_map_foreach2_calls == 1);
    assert(g_sleep_calls == 2);

    nng_mtx_free(auth.acl_cache_mtx);
}

static void
test_acl_cache_finit_stops_aio_and_frees_map(void)
{
    conf_auth_http auth = { 0 };

    reset_web_server_stubs();
    auth.acl_cache_map = g_fake_map_ptr;
    auth.acl_cache_reset_aio = g_fake_aio_ptr;
    assert(nng_mtx_alloc(&auth.acl_cache_mtx) == 0);

    g_fake_id_count = 1;
    nmq_acl_cache_finit(&auth);
    assert(g_aio_stop_calls == 1);
    assert(g_id_map_foreach2_calls == 1);
    assert(g_id_map_free_calls == 1);
    assert(g_aio_free_calls == 1);

    nng_mtx_free(auth.acl_cache_mtx);
}

static void
test_rest_job_recycle_returns_job_to_freelist(void)
{
    rest_job *job1;
    rest_job *job2;

    reset_web_server_stubs();
    assert(nng_mtx_alloc(&job_lock) == 0);

    job1 = rest_get_job();
    assert(job1 != NULL);
    assert(g_aio_alloc_calls == 1);

    job1->http_res = (nng_http_res *) 0x11;
    job1->msg = (nng_msg *) 0x22;
    job1->ctx.id = 99;
    rest_recycle_job(job1);
    assert(g_http_res_free_calls == 1);
    assert(g_msg_free_calls == 1);
    assert(g_ctx_close_calls == 1);
    assert(job_freelist == job1);

    job2 = rest_get_job();
    assert(job2 == job1);
    assert(g_aio_alloc_calls == 1);

    free(job2);
    free_job_lock_if_needed();
}

static void
test_alloc_work_initializes_state_and_context(void)
{
    struct rest_work *work;
    conf_http_server  http = { 0 };
    nng_socket        sock = { .id = 77 };

    reset_web_server_stubs();
    work = alloc_work(sock, &http);
    assert(work != NULL);
    assert(work->conf == &http);
    assert(work->state == SRV_INIT);
    assert(work->aio == g_fake_aio_ptr);
    assert(work->ctx.id == 321);
    assert(g_aio_alloc_calls == 1);
    assert(g_ctx_open_calls == 1);

    free(work);
}

int
main(void)
{
    test_global_and_http_server_config_helpers_apply_defaults();
    test_acl_cache_init_success_and_failure_paths();
    test_acl_cache_reset_helpers_remove_entries_and_reschedule();
    test_acl_cache_finit_stops_aio_and_frees_map();
    test_rest_job_recycle_returns_job_to_freelist();
    test_alloc_work_initializes_state_and_context();
    free_job_lock_if_needed();
    return 0;
}
