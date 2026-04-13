#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define nng_aio_alloc fake_nng_aio_alloc
#define nng_mtx_alloc fake_nng_mtx_alloc
#define nng_lmq_alloc fake_nng_lmq_alloc
#define nng_url_parse fake_nng_url_parse
#define nng_thread_create fake_nng_thread_create
#define nng_thread_destroy fake_nng_thread_destroy
#define nng_msleep fake_nng_msleep
#include "../webhook_inproc.c"
#undef nng_aio_alloc
#undef nng_mtx_alloc
#undef nng_lmq_alloc
#undef nng_url_parse
#undef nng_thread_create
#undef nng_thread_destroy
#undef nng_msleep

static int           g_aio_alloc_calls = 0;
static int           g_mtx_alloc_calls = 0;
static int           g_lmq_alloc_calls = 0;
static int           g_url_parse_calls = 0;
static int           g_thread_create_calls = 0;
static int           g_thread_destroy_calls = 0;
static int           g_msleep_calls = 0;
static nng_duration  g_last_msleep = 0;
static void        (*g_last_thread_cb)(void *) = NULL;
static void         *g_last_thread_arg = NULL;
static char          g_last_url[256];
static nng_thread   *g_last_thread = NULL;

int
fake_nng_aio_alloc(nng_aio **aio, void (*cb)(void *), void *arg)
{
    (void) cb;
    (void) arg;
    *aio = malloc(1);
    assert(*aio != NULL);
    g_aio_alloc_calls++;
    return 0;
}

int
fake_nng_mtx_alloc(nng_mtx **mtx)
{
    *mtx = malloc(1);
    assert(*mtx != NULL);
    g_mtx_alloc_calls++;
    return 0;
}

int
fake_nng_lmq_alloc(nng_lmq **lmq, size_t cap)
{
    assert(cap == NANO_LMQ_INIT_CAP);
    *lmq = malloc(1);
    assert(*lmq != NULL);
    g_lmq_alloc_calls++;
    return 0;
}

int
fake_nng_url_parse(nng_url **url, const char *raw)
{
    *url = malloc(1);
    assert(*url != NULL);
    g_url_parse_calls++;
    snprintf(g_last_url, sizeof(g_last_url), "%s", raw);
    return 0;
}

int
fake_nng_thread_create(nng_thread **thr, void (*cb)(void *), void *arg)
{
    *thr = malloc(1);
    assert(*thr != NULL);
    g_thread_create_calls++;
    g_last_thread_cb = cb;
    g_last_thread_arg = arg;
    g_last_thread = *thr;
    return 0;
}

void
fake_nng_thread_destroy(nng_thread *thr)
{
    g_thread_destroy_calls++;
    assert(thr == g_last_thread);
    free(thr);
    g_last_thread = NULL;
}

void
fake_nng_msleep(nng_duration duration)
{
    g_msleep_calls++;
    g_last_msleep = duration;
}

static void
reset_webhook_stubs(void)
{
    g_aio_alloc_calls = 0;
    g_mtx_alloc_calls = 0;
    g_lmq_alloc_calls = 0;
    g_url_parse_calls = 0;
    g_thread_create_calls = 0;
    g_thread_destroy_calls = 0;
    g_msleep_calls = 0;
    g_last_msleep = 0;
    g_last_thread_cb = NULL;
    g_last_thread_arg = NULL;
    g_last_thread = NULL;
    memset(g_last_url, 0, sizeof(g_last_url));
}

static void
free_alloc_work_result(struct hook_work *work, bool has_http)
{
    free(work->aio);
    free(work->mtx);
    free(work->lmq);
    if (has_http) {
        free(work->http_aio);
        free(work->url);
    }
    nng_free(work, sizeof(*work));
}

static void
test_alloc_work_without_http_mode_skips_http_specific_allocations(void)
{
    conf_web_hook    conf = { 0 };
    nng_socket       sock = { 0 };
    struct hook_work *work = NULL;

    reset_webhook_stubs();
    conf.enable = false;
    work = alloc_work(sock, &conf);

    assert(work != NULL);
    assert(work->conf == &conf);
    assert(work->sock.id == sock.id);
    assert(work->state == HOOK_INIT);
    assert(work->aio != NULL);
    assert(work->mtx != NULL);
    assert(work->lmq != NULL);
    assert(work->http_aio == NULL);
    assert(work->url == NULL);
    assert(g_aio_alloc_calls == 1);
    assert(g_mtx_alloc_calls == 1);
    assert(g_lmq_alloc_calls == 1);
    assert(g_url_parse_calls == 0);

    free_alloc_work_result(work, false);
}

static void
test_alloc_work_with_http_mode_allocates_http_aio_and_url(void)
{
    conf_web_hook    conf = { 0 };
    nng_socket       sock = { 0 };
    struct hook_work *work = NULL;

    reset_webhook_stubs();
    conf.enable = true;
    conf.url = "http://127.0.0.1:9000/hook";
    work = alloc_work(sock, &conf);

    assert(work != NULL);
    assert(work->http_aio != NULL);
    assert(work->url != NULL);
    assert(g_aio_alloc_calls == 2);
    assert(g_url_parse_calls == 1);
    assert(strcmp(g_last_url, "http://127.0.0.1:9000/hook") == 0);

    free_alloc_work_result(work, true);
}

static void
test_start_and_stop_hook_service_use_thread_lifecycle_helpers(void)
{
    conf cfg = { 0 };

    reset_webhook_stubs();
    assert(start_hook_service(&cfg) == 0);
    assert(g_thread_create_calls == 1);
    assert(g_last_thread_cb == hook_cb);
    assert(g_last_thread_arg == &cfg);
    assert(g_msleep_calls == 1);
    assert(g_last_msleep == 500);

    assert(stop_hook_service() == 0);
    assert(g_thread_destroy_calls == 1);
}

int
main(void)
{
    test_alloc_work_without_http_mode_skips_http_specific_allocations();
    test_alloc_work_with_http_mode_allocates_http_aio_and_url();
    test_start_and_stop_hook_service_use_thread_lifecycle_helpers();
    return 0;
}
