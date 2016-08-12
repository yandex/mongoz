/**
 * ctx_x86-64.s -- context-switching implementation for x86-64
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

.text
        .global __io_ctx_size
        .global __io_ctx_init
        .global __io_ctx_swap
        .global __io_ctx_on_cpu


__io_ctx_size:
        movq $72, %rax
        retq


io_ctx_trampoline_start:
        pushq $0
        pushq %rbp
        movq %rsp, %rbp
        
        movq 0x18(%rbp), %rdi
        movq 0x10(%rbp), %rax
        call *%rax

        movq 0x18(%rbp), %rdi
        movq 0x20(%rbp), %rax
        call *%rax

        movq %rbp, %rsp
        retq

__io_ctx_init:
        addq %r8, %rcx
        subq $0x28, %rcx

        movq %rsi, 0x08(%rcx)
        movq %rdx, 0x10(%rcx)
        movq $0, 0x18(%rcx)

        lea (%rip), %rax
next_instruction:
        subq $(next_instruction - io_ctx_trampoline_start), %rax
        movq %rax, (%rdi)
        movq %rcx, 0x08(%rdi)
        movq $-1, 0x40(%rdi)
        retq


__io_ctx_swap:
        lock
        incq 0x40(%rsi)
        jnz io_ctx_switch_busy

        testq %rdi, %rdi
        jz do_load
        
        movq (%rsp), %rax
        movq %rax, (%rdi)
        movq %rsp, 0x08(%rdi)
        movq %rbp, 0x10(%rdi)
        movq %rbx, 0x18(%rdi)
        movq %r12, 0x20(%rdi)
        movq %r13, 0x28(%rdi)
        movq %r14, 0x30(%rdi)
        movq %r15, 0x38(%rdi)

        lock
        decq 0x40(%rdi)

do_load:
        movq 0x08(%rsi), %rsp
        movq 0x10(%rsi), %rbp
        movq 0x18(%rsi), %rbx
        movq 0x20(%rsi), %r12
        movq 0x28(%rsi), %r13
        movq 0x30(%rsi), %r14
        movq 0x38(%rsi), %r15
        movq $0xA1A2A3A4B4B3B2B1, %rax
        xchgq (%rsi), %rax
        movq %rax, (%rsp)

        xorq %rax, %rax
        retq

io_ctx_switch_busy:
        lock
        decq 0x40(%rsi)
        xorq %rax, %rax
        incq %rax
        retq


__io_ctx_on_cpu:
        movq 0x40(%rdi), %rax
        incq %rax
        retq

