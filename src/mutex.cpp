/*
 * Copyright (C) 2011, xp@renzhi.ca
 * All rights reserved.
 */

#include "mutex.hpp"

XMutex::XMutex()
{
#if __WIN32__ || _MSC_VER
    InitializeCriticalSection(&crit_section);
#elif __unix__
    pthread_mutex_init(&mutex, NULL);
#endif
}

XMutex::~XMutex()
{
#if __WIN32__ || _MSC_VER
    DeleteCriticalSection(&crit_section);
#elif __unix__
    pthread_mutex_destroy(&mutex);
#endif
}

void XMutex::lock()
{
#if __WIN32__ || _MSC_VER
    EnterCriticalSection(&crit_section);
#elif __unix__
    pthread_mutex_lock(&mutex);
#endif
}

void XMutex::unlock()
{
#if __WIN32__ || _MSC_VER
    LeaveCriticalSection(&crit_section);
#elif __unix__
    pthread_mutex_unlock(&mutex);
#endif
}
