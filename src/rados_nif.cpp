/*
 * Copyright (C) 2012, xp@renzhi.ca
 * All rights reserved.
 */


#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include <map>
#include <rados/librados.h>
#include <erl_nif.h>

#include "rados_nif.h"

using namespace std;

#define _UINT64_C(c)            c ## UL
#define _UINT64_MAX             (_UINT64_C(18446744073709551615))

/*
 * Note: not used for now.
 */
static void dtor_cluster_type(ErlNifEnv* env, void* obj);
static void dtor_ioctx_type(ErlNifEnv* env, void* obj);

static ErlNifResourceType * cluster_type_resource = NULL;
static ErlNifResourceType * ioctx_type_resource = NULL;

/*
 * Map of rados cluster handles. We keep a local map of rados cluster
 * handles here, mapping to long integers. The handles are created
 * by librados api, and it's not possible to pass between Erlang and C.
 * Therefore, we pass a long integer, which is mapped to the handle here.
 */
map<uint64_t, rados_t> map_cluster;
/*
 * Map of IO context. Same mechanism as the cluster handles.
 */
map<uint64_t, rados_ioctx_t> map_ioctx;

/*
 * Map of list context. Same mechanism as the cluster handles.
 */
map<uint64_t, rados_list_ctx_t> map_list_ctx;

/*
 * Map of xattr iterators.
 */
map<uint64_t, rados_xattrs_iter_t> map_xattr_iter;

static uint64_t id_index = 0;
static XMutex id_mutex;

XLog logger = XLogManager::instance().getLog("RadosLog");

int load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
    erlrados_atoms.ok = enif_make_atom(env, "ok");
    erlrados_atoms.error = enif_make_atom(env, "error");
    erlrados_atoms.eof = enif_make_atom(env, "eof");
    erlrados_atoms.end = enif_make_atom(env, "end");
    erlrados_atoms.key = enif_make_atom(env, "key");
    erlrados_atoms.entry = enif_make_atom(env, "entry");
    erlrados_atoms.num_objects = enif_make_atom(env, "num_objects");
    erlrados_atoms.kb_avail = enif_make_atom(env, "kb_avail");
    erlrados_atoms.kb_used = enif_make_atom(env, "kb_used");
    erlrados_atoms.kb = enif_make_atom(env, "kb");
    erlrados_atoms.num_wr_kb = enif_make_atom(env, "num_wr_kb");
    erlrados_atoms.num_wr = enif_make_atom(env, "num_wr");
    erlrados_atoms.num_rd_kb = enif_make_atom(env, "num_rd_kb");
    erlrados_atoms.num_rd = enif_make_atom(env, "num_rd");
    erlrados_atoms.num_objects_degraded = enif_make_atom(env, "num_objects_degraded");
    erlrados_atoms.num_objects_unfound = enif_make_atom(env, "num_objects_unfound");
    erlrados_atoms.num_objects_missing_on_primary = enif_make_atom(env, "num_objects_missing_on_primary");
    erlrados_atoms.num_object_copies = enif_make_atom(env, "num_object_copies");
    erlrados_atoms.num_object_clones = enif_make_atom(env, "num_object_clones");
    erlrados_atoms.num_kb = enif_make_atom(env, "num_kb");
    erlrados_atoms.num_bytes = enif_make_atom(env, "num_bytes");
    erlrados_atoms.mtime = enif_make_atom(env, "mtime");
    erlrados_atoms.size = enif_make_atom(env, "size");
    erlrados_atoms.value = enif_make_atom(env, "value");
    erlrados_atoms.xattr = enif_make_atom(env, "xattr");

    ErlNifResourceType * rt = enif_open_resource_type(
        env, NULL, "cluster_type_resource", dtor_cluster_type, ERL_NIF_RT_CREATE, NULL);
    if (rt == NULL)
        return -1;
    cluster_type_resource = rt;

    rt = enif_open_resource_type(
        env, NULL, "ioctx_type_resource", dtor_ioctx_type, ERL_NIF_RT_CREATE, NULL);
    if (rt == NULL)
        return -1;
    ioctx_type_resource = rt;

    id_index = 0;

    return 0;
}

static int reload(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
    return 0;
}

static int upgrade(ErlNifEnv* env, 
                   void** priv, void** old_priv, 
                   ERL_NIF_TERM load_info)
{
    return 0;
}

static void unload(ErlNifEnv* env, void* priv)
{
    return;
}

static void dtor_cluster_type(ErlNifEnv* env, void* obj)
{
}

static void dtor_ioctx_type(ErlNifEnv* env, void* obj)
{
}

uint64_t new_id()
{
    id_mutex.lock();
    if (id_index == _UINT64_MAX)
        id_index = 0;
    id_index++;
    id_mutex.unlock();
    return id_index;
}

ERL_NIF_TERM make_error_tuple(ErlNifEnv* env, int err)
{
    ERL_NIF_TERM atom = erlrados_atoms.error;
    ERL_NIF_TERM reason = enif_make_string(env, strerror(err), ERL_NIF_LATIN1);
    return enif_make_tuple2(env, atom, reason);
}

ERL_NIF_TERM x_add_stderr_log_handler(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    XLogStderrHandler *log_handler = new XLogStderrHandler();
    logger.addHandler(*log_handler);
    return erlrados_atoms.ok;
}

ERL_NIF_TERM x_add_sys_log_handler(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    XLogSyslogHandler *log_handler = new XLogSyslogHandler();
    logger.addHandler(*log_handler);
    return erlrados_atoms.ok;
}

ERL_NIF_TERM x_add_file_log_handler(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char log_file[MAX_FILE_NAME_LEN];
    memset(log_file, 0, MAX_FILE_NAME_LEN);
    if (!enif_get_string(env, argv[0], log_file, MAX_FILE_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }
    XLogFileHandler *log_handler = new XLogFileHandler(log_file);
    logger.addHandler(*log_handler);
    return erlrados_atoms.ok;
}

static LogLevel atom_to_level(char * level)
{
    if (strcmp(level, "fatal") == 0)
        return FATAL;
    else if (strcmp(level, "error") == 0)
        return ERROR;
    else if (strcmp(level, "warning") == 0)
        return WARNING;
    else if (strcmp(level, "info") == 0)
        return INFO;
    else if (strcmp(level, "debug") == 0)
        return DEBUG;
    else
        return NONE;
}

ERL_NIF_TERM x_set_log_level(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char level[32];
    memset(level, 0, 32);
    if (!enif_get_atom(env, argv[0], level, 32, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }
    LogLevel l = atom_to_level(level);
    logger.setLevel(l);
    return erlrados_atoms.ok;
}

ErlNifFunc nif_funcs[] =
{
    {"add_stderr_log_handler", 0, x_add_stderr_log_handler},
    {"add_sys_log_handler", 0, x_add_sys_log_handler},
    {"add_file_log_handler", 1, x_add_file_log_handler},
    {"set_log_level", 1, x_set_log_level},
    {"create", 0, x_create},
    {"create", 1, x_create_with_user},
    {"conf_read_file", 1, x_conf_read_file},
    {"conf_read_file", 2, x_conf_read_file2},
    {"conf_set", 3, x_conf_set},
    {"connect", 1, x_connect},
    {"shutdown", 1, x_shutdown},
    {"get_instance_id", 1, x_get_instance_id},
    {"pool_list", 1, x_pool_list},
    {"cluster_stat", 1, x_cluster_stat},
    {"pool_lookup", 2, x_pool_lookup},
    {"pool_create", 2, x_pool_create},
    {"pool_create", 3, x_pool_create_for_user},
    {"pool_delete", 2, x_pool_delete},
    {"ioctx_create", 2, x_ioctx_create},
    {"ioctx_destroy", 1, x_ioctx_destroy},
    {"ioctx_pool_stat", 1, x_ioctx_pool_stat},
    {"ioctx_pool_set_auid", 2, x_ioctx_pool_set_auid},
    {"ioctx_pool_get_auid", 1, x_ioctx_pool_get_auid},
    {"ioctx_get_id", 1, x_ioctx_get_id},
    {"ioctx_get_pool_name", 1, x_ioctx_get_pool_name},
    {"ioctx_snap_create", 2, x_ioctx_snap_create},
    {"ioctx_snap_remove", 2, x_ioctx_snap_remove},
    {"rollback", 3, x_rollback},
    {"ioctx_snap_list", 1, x_ioctx_snap_list},
    {"ioctx_snap_lookup", 2, x_ioctx_snap_lookup},
    {"ioctx_snap_get_name", 2, x_ioctx_snap_get_name},
    {"ioctx_snap_get_stamp", 2, x_ioctx_snap_get_stamp},
    {"aio_flush", 1, x_aio_flush},
    {"write", 4, x_write},
    {"write_full", 3, x_write_full},
    {"append", 3, x_append},
    {"read", 4, x_read},
    {"remove", 2, x_remove},
    {"trunc", 3, x_trunc},
    {"stat", 2, x_stat},
    {"objects_list_open", 1, x_objects_list_open},
    {"objects_list_next", 1, x_objects_list_next},
    {"objects_list_close", 1, x_objects_list_close},
    {"getxattr", 3, x_getxattr},
    {"setxattr", 4, x_setxattr},
    {"rmxattr", 3, x_rmxattr},
    {"getxattrs", 2, x_getxattrs},
    {"getxattrs_next", 1, x_getxattrs_next},
    {"getxattrs_end", 1, x_getxattrs_end},
};

ERL_NIF_INIT(rados, nif_funcs, load, reload, upgrade, unload)
