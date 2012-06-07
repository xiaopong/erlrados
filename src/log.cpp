/*
 * Copyright (C) 2011, xp@renzhi.ca
 * All rights reserved.
 */

#include <algorithm>

#if __linux
#include <syslog.h>
#endif

#include <typeinfo>
#include <cxxabi.h>

#include "tmutil.hpp"
#include "log.hpp"

/********************************************************************************
 * XLog
 ********************************************************************************/

XLog::XLog()
{
    curr_level = NONE;
}

XLog::XLog(const string& name)
{
    curr_level = NONE;
    logger_name = name;
}

void XLog::setLevel(LogLevel level)
{
    curr_level = level;
    set<XLogHandler*>::iterator it = log_handlers.begin();
    while (it != log_handlers.end()) {
        (*it)->setLevel(level);
        it++;
    }
}

void XLog::addHandler(XLogHandler& handler)
{
    log_handlers.insert(&handler);
}

void XLog::removeHandler(XLogHandler* const handler)
{
    set<XLogHandler*>::iterator it = log_handlers.find(handler);
    if (it != log_handlers.end())
        log_handlers.erase(it);
}

void XLog::fatal(const char* origin, const char* func, const char* format, ...)
{
    if (FATAL > curr_level)
        return;
    va_list ap;
    va_start(ap, format);
    log(origin, func, FATAL, format, ap);
    va_end(ap);
}

void XLog::error(const char* origin, const char* func, const char* format, ...)
{
    if (ERROR > curr_level)
        return;
    va_list ap;
    va_start(ap, format);
    log(origin, func, ERROR, format, ap);
    va_end(ap);
}

void XLog::warning(const char* origin, const char* func, const char* format, ...)
{
    if (WARNING > curr_level)
        return;
    va_list ap;
    va_start(ap, format);
    log(origin, func, WARNING, format, ap);
    va_end(ap);
}

void XLog::info(const char* origin, const char* func, const char* format, ...)
{
    if (INFO > curr_level)
        return;
    va_list ap;
    va_start(ap, format);
    log(origin, func, INFO, format, ap);
    va_end(ap);
}

void XLog::debug(const char* origin, const char* func, const char* format, ...)
{
    if (DEBUG > curr_level)
        return;
    va_list ap;
    va_start(ap, format);
    log(origin, func, DEBUG, format, ap);
    va_end(ap);
}

void XLog::log(const char* origin, const char* func, LogLevel level, const char* format, va_list& arglist)
{
    set<XLogHandler*>::iterator it = log_handlers.begin();
    while (it != log_handlers.end()) {
        XLogHandler * handler = *it;

        // int status;
        // char * demangled = abi::__cxa_demangle(typeid(*handler).name(), 0, 0, &status);
        // printf("XLog:log() - handler type: %s\n", demangled);
        // //free(demangled);

        handler->log(origin, func, level, format, arglist);
        it++;
    }
}

void XLog::flush()
{
    set<XLogHandler*>::iterator it = log_handlers.begin();
    while (it != log_handlers.end()) {
        XLogHandler * handler = *it;
        (*handler).flush();
        it++;
    }
}

/********************************************************************************
 * XLogManager
 ********************************************************************************/

XLogManager::XLogManager()
{
}

XLogManager::~XLogManager()
{
    map<string, XLog*>::const_iterator it = loggers.begin();
    while (it != loggers.end()) {
        delete it->second;
        it++;
    }
}

XLogManager& XLogManager::instance()
{
#ifdef _WIN32
    /*
     * According to the MSDN documentation, the C++ compiler should
     * generate mutex when the keyword volatile is specified here.
     */
    static volatile XLogManager log_manager;
#elif  defined(__GNUC__) && (__GNUC__ > 3)
    /*
     * G++ explicitly adds code to make sure that this is thread-safe,
     * so we are ok here.
     */
    static XLogManager log_manager;
#else
#error Add critical section for your platform here
#endif

    return log_manager;
}

XLogRef XLogManager::getLog(const char* name)
{
    mutex.lock();
    if (loggers.find(name) == loggers.end())
        loggers[name] = new XLog(name);
    mutex.unlock();
    return *(loggers[name]);
}

void XLogManager::setDirectory(const char* dir)
{
    log_dir = dir;

    /*
     * TODO: check if dir exists, if not, create (all sub-dirs)
     */
}

/********************************************************************************
 * XLogHandler
 ********************************************************************************/

XLogHandler::XLogHandler() : 
    level(NONE)
{
}

XLogHandler::~XLogHandler()
{
}

const char* XLogHandler::getLevelStr(const int& level)
{
    static const char* const log_level[] = { "NONE", "FATAL", "ERROR", "WARNING", "INFO", "DEBUG" };
    if (level >=0 && level < 6)
        return log_level[level];
    else
        return "INVALID_LEVEL";
}

void XLogHandler::setLevel(LogLevel l)
{
    level = l;
}

/********************************************************************************
 * XLogStderrHandler
 ********************************************************************************/

XLogStderrHandler::XLogStderrHandler()
{
}

XLogStderrHandler::~XLogStderrHandler()
{
    flush();
}

void XLogStderrHandler::flush()
{
    fflush(stderr);
}

void XLogStderrHandler::log(const char* origin, const char* func, LogLevel level, const char* format, va_list& arglist)
{
    mutex.lock();
    XTime tm;
    TMUtil::getCurrentTime(tm);
    fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d ", 
            tm.year, tm.month, tm.day, tm.hour, tm.min, tm.second, tm.millisecond);
    fprintf(stderr, "%-8s ", getLevelStr(level));
    if (origin != NULL) {
        fprintf(stderr, "[%s %s] ", origin, func);
    }
    vfprintf(stderr, format, arglist);
    fprintf(stderr, "\n");
    mutex.unlock();
}

/********************************************************************************
 * XLogFileHandlerBase
 ********************************************************************************/

XMutex XLogFileHandlerBase::mutex;
vector<string> XLogFileHandlerBase::log_files;

XLogFileHandlerBase::XLogFileHandlerBase(const char* filename) :
    XLogHandler(),
    log_file_size(0),
    log_file(NULL)
{
    vector<string>::iterator it = find(log_files.begin(), 
                                       log_files.end(), 
                                       filename);
    if (it != log_files.end()) {
        /*
         * Another log handler is writing to the same log file.
         * TODO: throw exception
         */
    }
    else {
        log_files.push_back(filename);
    }

    log_filename = filename;

    // TODO:
    //   Determine directory
    //   Split log file name and extension
}

XLogFileHandlerBase::~XLogFileHandlerBase()
{
    flush();
    if (log_file) {
        fclose(log_file);
        log_file = NULL;
    }
}

void XLogFileHandlerBase::flush()
{
    if (log_file)
        fflush(log_file);
}

void XLogFileHandlerBase::log(const char* origin, const char* func, LogLevel level, const char* format, va_list& arglist)
{
    if (!log_file)
        openLogFile();

    if (log_file) {
        mutex.lock();
        XTime tm;
        TMUtil::getCurrentTime(tm);
        fprintf(log_file, "%d-%02d-%02d %02d:%02d:%02d.%03d ", 
                tm.year, tm.month, tm.day, tm.hour, tm.min, tm.second, tm.millisecond);
        fprintf(log_file, "%-8s ", getLevelStr(level));
        if (origin != NULL) {
            fprintf(log_file, "[%s %s] ", origin, func);
        }
        vfprintf(log_file, format, arglist);
        fprintf(log_file, "\n");
        mutex.unlock();
    }
}

void XLogFileHandlerBase::openLogFile()
{
    if (!log_file) {
        log_file = fopen(log_filename.c_str(), "w");
    }
}

void XLogFileHandlerBase::openLogFile(const char* filename)
{
    if (log_file) {
        flush();
        fclose(log_file);
        log_file = NULL;
    }
    log_filename = filename;
    openLogFile();
}

/********************************************************************************
 * XLogFileHandler
 ********************************************************************************/

XLogFileHandler::XLogFileHandler(const char* filename) :
    XLogFileHandlerBase(filename)
{
}

XLogFileHandler::~XLogFileHandler()
{
}


/********************************************************************************
 * XLogRollingFileHandler
 ********************************************************************************/


/********************************************************************************
 * XLogSyslogHandler
 ********************************************************************************/

XLogSyslogHandler::XLogSyslogHandler() :
    is_open(0),
    id("XLogHandler")
{
    openLog();
}

XLogSyslogHandler::XLogSyslogHandler(const char* ident) :
    is_open(0),
    id(ident)
{
    openLog();
}

XLogSyslogHandler::~XLogSyslogHandler()
{
    if (is_open) {
#if __linux
        closelog();
#elif (_WIN32 || _WIN64)
#error Windows version not implemented yet.
#else
#error Not implemented yet.
#endif
        is_open = 0;
    }
}

void XLogSyslogHandler::openLog()
{
    if (!is_open) {
#if __linux
        openlog(id.c_str(), LOG_NDELAY, LOG_USER);
#elif (_WIN32 || _WIN64)
#error Windows version not implemented yet.
#else
#error Not implemented yet.
#endif
        is_open = 1;
    }
}

void XLogSyslogHandler::flush()
{
    // Do nothing
}

int XLogSyslogHandler::mapLevel(LogLevel level)
{
    switch(level) {
#if __linux
    case FATAL:
        return LOG_CRIT;
    case ERROR:
        return LOG_ERR;
    case WARNING:
        return LOG_WARNING;
    case INFO:
        return LOG_INFO;
    default:
        return LOG_DEBUG;
#elif (_WIN32 || _WIN64)
#error Windows version not implemented yet.
#else
#error Not implemented yet.
#endif
    }
}

void XLogSyslogHandler::log(const char* origin, const char* func, LogLevel level, const char* format, va_list& arglist)
{
    if (!is_open)
        openLog();

    if (is_open) {
        int priority = mapLevel(level);

        string s;
        if (origin != NULL) {
            s.append("[");
            s.append(origin);
            s.append(" ");
            s.append(func);
            s.append("] ");
            s.append(format);
        }

#if __linux
        vsyslog(priority, s.c_str(), arglist);
#elif (_WIN32 || _WIN64)
#error Windows version not implemented yet.
#else
#error Not implemented yet.
#endif
    }
}

void XLogSyslogHandler::setLevel(LogLevel l)
{
    level = l;
    int priority = mapLevel(level);

#if __linux
    setlogmask(LOG_UPTO(priority));
#elif (_WIN32 || _WIN64)
#error Windows version not implemented yet.
#else
#error Not implemented yet.
#endif
}
