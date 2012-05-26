/*
 * Copyright (C) 2012, xp@renzhi.ca
 * All rights reserved.
 */


#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <map>
#include <rados/librados.h>
#include <erl_nif.h>

#include "rados_nif.h"

/*
 * TODO:
 *   - Add logging
 */


using namespace std;


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
map<long, rados_t> map_cluster;
/*
 * Map of IO context. Same mechanism as the cluster handles.
 */
map<long, rados_ioctx_t> map_ioctx;

/*
 * Map of list context. Same mechanism as the cluster handles.
 */
map<long, rados_list_ctx_t> map_list_ctx;

/*
 * Map of xattr iterators.
 */
map<long, rados_xattrs_iter_t> map_xattr_iter;

static int load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
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

    unsigned int iseed = (unsigned int)time(NULL);
    srandom(iseed);

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

ERL_NIF_TERM make_error_tuple(ErlNifEnv* env, int err)
{
    ERL_NIF_TERM atom = enif_make_atom(env, "error");
    ERL_NIF_TERM reason = enif_make_string(env, strerror(err), ERL_NIF_LATIN1);
    return enif_make_tuple2(env, atom, reason);
}


ErlNifFunc nif_funcs[] =
{
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
