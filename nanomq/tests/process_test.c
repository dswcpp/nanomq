#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include "include/process.h"

static int
child_exit_with_code(void *arg)
{
    return *(int *) arg;
}

static int
child_wait_for_signal(void *arg)
{
    int create_session = *(int *) arg;
    if (create_session) {
        if (setsid() == -1) {
            return 90;
        }
    }
    for (;;) {
        pause();
    }
}

static void
wait_for_pgid(pid_t pid, pid_t expected_pgid)
{
    for (int i = 0; i < 100; ++i) {
        pid_t pgid = getpgid(pid);
        if (pgid == expected_pgid) {
            return;
        }
        usleep(10000);
    }
    assert(!"child process group was not ready");
}

static void
test_invalid_pid_is_rejected(void)
{
    assert(process_is_alive(0) == 0);
    assert(process_is_alive(-1) == 0);
    assert(process_send_signal(0, SIGTERM) == 0);
    assert(process_send_signal(-1, SIGTERM) == 0);
    assert(pidgrp_send_signal(0, SIGTERM) == 0);
    assert(pidgrp_send_signal(-1, SIGTERM) == 0);
}

static void
test_process_is_alive_for_self(void)
{
    assert(process_is_alive(getpid()) == 1);
}

static void
test_process_create_child_returns_exit_code(void)
{
    int code = 7;
    pid_t pid = process_create_child(child_exit_with_code, &code);
    int status = 0;

    assert(pid > 0);
    assert(waitpid(pid, &status, 0) == pid);
    assert(WIFEXITED(status));
    assert(WEXITSTATUS(status) == 7);
    assert(process_is_alive(pid) == 0);
}

static void
test_process_send_signal_terminates_child(void)
{
    int create_session = 0;
    pid_t pid = process_create_child(child_wait_for_signal, &create_session);
    int status = 0;

    assert(pid > 0);
    assert(process_is_alive(pid) == 1);
    assert(process_send_signal(pid, SIGTERM) == 0);
    assert(waitpid(pid, &status, 0) == pid);
    assert(WIFSIGNALED(status));
    assert(WTERMSIG(status) == SIGTERM);
}

static void
test_pidgrp_send_signal_terminates_child_process_group(void)
{
    int create_session = 1;
    pid_t pid = process_create_child(child_wait_for_signal, &create_session);
    int status = 0;

    assert(pid > 0);
    wait_for_pgid(pid, pid);
    assert(pidgrp_send_signal(pid, SIGTERM) == 0);
    assert(waitpid(pid, &status, 0) == pid);
    assert(WIFSIGNALED(status));
    assert(WTERMSIG(status) == SIGTERM);
}

int
main(void)
{
    test_invalid_pid_is_rejected();
    test_process_is_alive_for_self();
    test_process_create_child_returns_exit_code();
    test_process_send_signal_terminates_child();
    test_pidgrp_send_signal_terminates_child_process_group();
    return 0;
}
