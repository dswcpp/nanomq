#pragma once
#ifndef _INC_PROCESS
#define _INC_PROCESS

#if defined(_WIN32)
#include <corecrt.h>
#include <corecrt_startup.h>
#include <corecrt_wprocess.h>

_CRT_BEGIN_C_HEADER

typedef void     (__cdecl*   _beginthread_proc_type)(void *);
typedef unsigned (__stdcall* _beginthreadex_proc_type)(void *);

_ACRTIMP uintptr_t __cdecl _beginthread(
    _In_     _beginthread_proc_type _StartAddress,
    _In_     unsigned               _StackSize,
    _In_opt_ void *                 _ArgList);

_ACRTIMP void __cdecl _endthread(void);

_Success_(return != 0)
_ACRTIMP uintptr_t __cdecl _beginthreadex(
    _In_opt_  void *                    _Security,
    _In_      unsigned                  _StackSize,
    _In_      _beginthreadex_proc_type  _StartAddress,
    _In_opt_  void *                    _ArgList,
    _In_      unsigned                  _InitFlag,
    _Out_opt_ unsigned *                _ThrdAddr);

_ACRTIMP void __cdecl _endthreadex(_In_ unsigned _ReturnCode);

_CRT_END_C_HEADER
#endif

#include "nmq_process.h"

#endif
