/*
 * Copyright (C) 2011, xp@renzhi.ca
 * All rights reserved.
 */

#pragma once

#include <stdio.h>

/**
 * Portable struct to reprensent time information
 */
struct XTime {
    int weekday;        /**< weekday 0..6 */
    int month;          /**< month 0..11 */
    int day;            /**< day 0..31 */
    int year;
    int hour;
    int min;
    int second;
    int millisecond;
};

/**
 * Time-related utilities
 */
class TMUtil
{
public:
    static void getCurrentTime(struct XTime& currTime);
};
