/*
 * Copyright (C) 2011, xp@renzhi.ca
 * All rights reserved.
 */

#pragma once

#if __WIN32__ || _MSC_VER
#include <windows.h>
#elif __unix__
#include <pthread.h>
#endif

/**
 * A simple mutex class that is portable.
 */
class XMutex
{
public:
    XMutex();
    ~XMutex();

    void lock();
    void unlock();

private:
#if __WIN32__ || _MSC_VER
   CRITICAL_SECTION crit_section;
#elif __unix__
   pthread_mutex_t  mutex;
#endif

};
