/*
 * Copyright (C) 2011, xp@renzhi.ca
 * All rights reserved.
 */

#pragma once

#include <sys/types.h>
#include <sys/stat.h>

#ifdef _WIN32
#define FILE_PATH_SEPARATOR "\\"
#else
#define FILE_PATH_SEPARATOR "/"
#endif

/**
 * Filesystem-related utilities
 */
class FSUtil
{
public:
    /**
     * Create a directory.
     */
    static int mkdir(const char* dir, mode_t mode);
    /**
     * Create a path (directory and all sub-directories)
     */
    static int mkpath(const char* path, mode_t mode);
};
