#include <syncio/syncio.h>
#include "../src/ctx.h"
#include <cstring>
#include <signal.h>
#include <unistd.h>

void segvHandler(int, siginfo_t* si, void*)
{
    ::_exit(si->si_code != SEGV_ACCERR);
}

// Disable tail recursion optimization
#ifdef __clang__
void f() __attribute__(( optnone ));
#else
# ifdef __GNUC__
void f() __attribute__(( optimize("no-optimize-sibling-calls") ));
# endif
#endif

void f() { f(); }

int main()
{
    io::engine engine;
    engine.doSpawn(io::impl::OneTimeFunction<void>(f), 1024).detach();

    io::impl::Stack sigStack(SIGSTKSZ);
    stack_t ss;
    ::bzero(&ss, sizeof(ss));
    ss.ss_sp = sigStack.ptr();
    ss.ss_size = sigStack.size();
    ::sigaltstack(&ss, nullptr);

    struct sigaction sa;
    ::bzero(&sa, sizeof(sa));
    sa.sa_sigaction = &segvHandler;
    sa.sa_flags = SA_SIGINFO | SA_ONSTACK;
    ::sigaction(SIGSEGV, &sa, nullptr);

    engine.run();
    // Should not reach here
    return 1;
}
