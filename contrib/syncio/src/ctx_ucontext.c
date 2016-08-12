/**
 * ctx_ucontext.c -- context-switching implementation based on swapcontext(3)
 *
 * This file is part of mongoz, a more sound implementation
 * of mongodb sharding server.
 *
 * Copyright (c) 2014 Dmitry Prokoptsev <dprokoptsev@yandex-team.ru>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <ucontext.h>

struct Context {
    ucontext_t ctx;
    int on_cpu;
};

size_t __io_ctx_size()
{
    return sizeof(struct Context);
}

void __io_ctx_init(char* pctx, void (*start)(void*), void* arg, void* stack, size_t stackSize)
{
    struct Context* ctx = (struct Context*) pctx;
    ctx->on_cpu = 0;
    
    getcontext(&ctx->ctx);
    ctx->ctx.uc_link = 0;
    ctx->ctx.uc_stack.ss_sp = stack;
    ctx->ctx.uc_stack.ss_size = stackSize;
    makecontext(&ctx->ctx, (void(*)(void)) start, 2, arg, 0);
}

int __io_ctx_swap(char* oldpctx, char* newpctx)
{
    struct Context* oldctx = (struct Context*) oldpctx;
    struct Context* newctx = (struct Context*) newpctx;
    
    --oldctx->on_cpu;
    ++newctx->on_cpu;
    swapcontext(&oldctx->ctx, &newctx->ctx);
    return 0;
}

int __io_ctx_on_cpu(const char* ctx)
{
    return ((struct Context*) ctx)->on_cpu;
}
