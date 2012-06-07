/*
 * Copyright (C) 2011, xp@renzhi.ca
 * All rights reserved.
 */

#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "fsutil.hpp"

#if __WIN32__ || _MSC_VER
#define PATH_SEPARATOR '\\'
#elif __unix__
#define PATH_SEPARATOR '/'
#endif

int FSUtil::mkdir(const char* dir, mode_t mode)
{
    struct stat st;
    int status = 0;

    if (stat(dir, &st) != 0) {
        /* Directory does not exist */
        if (::mkdir(dir, mode) != 0)
            status = -1;
    }
    else if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        status = -1;
    }

    return(status);
}

int FSUtil::mkpath(const char* path, mode_t mode)
{
    char *pp;
    char *sp;
    int status;
    char *copypath = strdup(path);

    status = 0;
    pp = copypath;
    while (status == 0 && (sp = strchr(pp, PATH_SEPARATOR)) != 0)
    {
        if (sp != pp)
        {
            /* Neither root nor double slash in path */
            *sp = '\0';
            status = FSUtil::mkdir(copypath, mode);
            *sp = PATH_SEPARATOR;
        }
        pp = sp + 1;
    }
    if (status == 0)
        status = FSUtil::mkdir(path, mode);
    free(copypath);
    return (status);
}

