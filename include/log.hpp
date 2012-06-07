/*
 * Copyright (C) 2011, xp@renzhi.ca
 * All rights reserved.
 */

#pragma once

#include <string>
#include <set>
#include <map>
#include <vector>
#include <stdio.h>
#include <stdarg.h>

#include "mutex.hpp"
#include "fsutil.hpp"

#ifndef NULL
#define NULL 0
#endif

using namespace std;

class XLog;
class XLogManager;
class XLogHandler;
class XLogNullHandler;
class XLogStderrHandler;
class XLogFileHandler;
class XLogSyslogHandler;

typedef XLog& XLogRef;
 
enum LogLevel {
    NONE, FATAL, ERROR, WARNING, INFO, DEBUG
};

/********************************************************************************
 * XLog
 ********************************************************************************/

/**
 * Logger class. All log messages that a client application wants to log
 * will go through this class. However, client application is not supposed
 * to instantiate this class directly. Instead, you should get a logger
 * instance through the XLogManager:
 *
 *    XLogManager mgr = XLogManager::instance();
 *    XLog logger = mgr.getLog("NameHere");
 *
 * Each logger is assigned a name so that the same logger instance
 * can be shared among client applications.
 */
class XLog
{
public:
    XLog();
    XLog(const string& name);

    /**
     * Set logging level. All log entries above this level will be output,
     * all logs below this level will be ignored.
     */
    void setLevel(LogLevel level);
    /**
     * Add a log handler.
     */
    void addHandler(XLogHandler& handler);
    /**
     * Remove a log handler.
     */
    void removeHandler(XLogHandler* const handler);

    /**
     * Log a fatal message.
     *
     * @param origin   Origin of the log message. This is usually set to the
     *                 class name or source code file name.
     * @param func     Function or method where the log message is generated.
     * @param format   Formatted log message.
     */
    void fatal(const char* origin, const char* func, const char* format, ...);
    /**
     * Log an error message.
     */
    void error(const char* origin, const char* func, const char* format, ...);
    /**
     * Log a warning message.
     */
    void warning(const char* origin, const char* func, const char* format, ...);
    /**
     * Log an error message.
     */
    void info(const char* origin, const char* func, const char* format, ...);
    void debug(const char* origin, const char* func, const char* format, ...);

    /**
     * Flush all log entries.
     */
    void flush();
protected:
    /**
     * Generic log method.
     */
    void log(const char* origin, const char* func, LogLevel level, const char* format, va_list& arglist);

private:
    friend class XLogManager;

    string logger_name;
    LogLevel curr_level;
    set<XLogHandler*> log_handlers;
};

/********************************************************************************
 * XLogManager
 ********************************************************************************/

/**
 * Log manager class. The Log Manager is a singleton, responsible for
 * managing all loggers used by client applications.
 */
class XLogManager
{
public:
    static XLogManager& instance();
    XLogRef getLog(const char* name);

    ~XLogManager();

    /**
     * Set the default log file directory. This will be useful when
     * using file handlers.
     */
    void setDirectory(const char* dir);

private:
    XLogManager();
    XLogManager& operator=(const XLogManager& copy);

    XMutex mutex;
    map<string, XLog*> loggers;
    string log_dir;
};

/********************************************************************************
 * XLogHandler
 ********************************************************************************/

/**
 * Base class for all logging handlers. Log handlers are specialized
 * classes that handle log entries. 
 *
 * When a log message is sent through the logger, it is actually handled
 * by the log handler, e.g. display on the console, write to a file, etc.
 * Each logger can have more than one handler.
 *
 * Example of using a file log handler:
 *
 *   XLogManager mgr = XLogManager::instance();
 *   XLog logger = mgr.getLog("TestFile");
 *   XLogFileHandler loghandler("test.log");
 *   logger.addHandler(loghandler);
 *
 */
class XLogHandler
{
public:
    XLogHandler();
    virtual ~XLogHandler();

    /**
     * Flush log data to output destination.
     */
    virtual void flush() = 0;

    /**
     * Write log entry to output.
     *
     * @param origin   Origin of the log message. This is usually set to the
     *                 class name, source code file name, or line number.
     * @param func     Function or method where the log message is generated.
     * @param level    Log level
     * @param format   Format string for output
     * @param arglist  Argument list for output
     */
    virtual void log(const char* origin, const char* func, LogLevel level, const char* format, va_list& arglist) = 0;

    const char* getLevelStr(const int& level);

    virtual void setLevel(LogLevel l);

protected:
    LogLevel level;
};

/********************************************************************************
 * XLogNullHandler
 ********************************************************************************/

/**
 * Null log handler, for testing only. This handler does nothing.
 */
class XLogNullHandler : public XLogHandler
{
public:
    XLogNullHandler() {};
    ~XLogNullHandler() {};

    virtual void flush() {};
    virtual void log(const char* origin, const char* func, LogLevel level, const char* format, va_list& arglist) {};
};

/********************************************************************************
 * XLogStderrHandler
 ********************************************************************************/

/**
 * Log handler that outputs to stderr.
 */
class XLogStderrHandler : public XLogHandler
{
public:
    XLogStderrHandler();
    ~XLogStderrHandler();

    virtual void flush();
    virtual void log(const char* origin, const char* func, LogLevel level, const char* format, va_list& arglist); 

private:
    XMutex mutex;
};

/********************************************************************************
 * XLogFileHandlerBase
 ********************************************************************************/

/**
 * Abstract class for file log handler.
 */
class XLogFileHandlerBase : public XLogHandler
{
public:
    XLogFileHandlerBase(const char* filename);
    ~XLogFileHandlerBase();

    virtual void flush();
    virtual void log(const char* origin, const char* func, LogLevel level, const char* format, va_list& arglist); 

protected:
    void openLogFile();
    void openLogFile(const char* filename);

    static XMutex mutex;
    static vector<string> log_files;

    string log_filename;
    long log_file_size;
    FILE * log_file;
};

/********************************************************************************
 * XLogFileHandler
 ********************************************************************************/

/**
 * File log handler, which will write all log messages to the
 * specified file.
 */
class XLogFileHandler : public XLogFileHandlerBase
{
public:
    /**
     * Constructor.
     *
     * @param filename   Path and file name of the log file.
     */
    XLogFileHandler(const char* filename);
    ~XLogFileHandler();
};


/********************************************************************************
 * XLogRollingFileHandler
 ********************************************************************************/

// class XLogRollingFileHandler : public XLogFileHandlerBase
// {
// public:
//     XLogRollingFileHandler();
//     ~XLogRollingFileHandler();
//    virtual const string getNextFilename() = 0;
//    virtual bool reachFileLimit() = 0;

// private:
//     const string getNextFilename();
//     bool reachFileLimit();

//     int curr_index;      // Current log file 1 .. num_files
//     int num_files;       // Number of log files
//     long max_file_size;  // Log file size limit
//     int curr_file_num;   // Current log file number
// };

/********************************************************************************
 * XLogSyslogHandler
 ********************************************************************************/

/**
 * Log handler that will write log messages to the system log. On Unix
 * system, this log handler will write log messages to syslog. On Windows,
 * it will write to system event log (the Windows version is not implemented
 * yet).
 */
class XLogSyslogHandler : public XLogHandler
{
public:
    /**
     * Default constructor
     */
    XLogSyslogHandler();
    /**
     * Constructor
     *
     * @param ident   Identity string, which will be prepended to every
     *                log message. This is typically set to the program name.
     */
    XLogSyslogHandler(const char* ident);
    ~XLogSyslogHandler();

    virtual void flush();
    virtual void log(const char* origin, const char* func, LogLevel level, const char* format, va_list& arglist); 

    virtual void setLevel(LogLevel l);

private:
    void openLog();
    int mapLevel(LogLevel level);

    short is_open;
    string id;

/*
 * TODO:
 *   - Implement Windows event log handler
 */

};

