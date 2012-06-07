/*
 * Copyright (C) 2011, xp@renzhi.ca
 * All rights reserved.
 */

#if __WIN32__ || _MSC_VER
#include <windows.h>
#include <time.h>
#include <stdio.h>
#elif __unix__
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>       // Weird, it's supposed to be included in sys/time.h...
#include <sys/time.h>
#endif

#include "tmutil.hpp"

void TMUtil::getCurrentTime(struct XTime& currTime)
{
#if __WIN32__ || _MSC_VER
    static SYSTEMTIME st;
    GetSystemTime(&st);
    currTime.weekday = st.wDayOfWeek;
    currTime.month = st.wMonth;
    currTime.day = st.wDay;
    currTime.year = st.wYear;
    currTime.hour = st.wHour;
    currTime.min = st.wMinute;
    currTime.second = st.wSecond;
    currime.millisecond = st.wMilliseconds;
#elif __unix__
    static struct timeval tv;
    static struct tm* ptm;
    static int milliseconds;

    // obtain time of day and convert to a tm struct
    gettimeofday(&tv, NULL);
    ptm = localtime((const time_t*)&tv.tv_sec);
    // Convert microseconds to milliseconds
    milliseconds = tv.tv_usec / 1000;

    currTime.weekday = ptm->tm_wday;
    currTime.month = ptm->tm_mon;
    currTime.day = ptm->tm_mday;
    currTime.year = 1900 + ptm->tm_year;
    currTime.hour = ptm->tm_hour;
    currTime.min = ptm->tm_min;
    currTime.second = ptm->tm_sec;
    currTime.millisecond = milliseconds;
#endif
}
