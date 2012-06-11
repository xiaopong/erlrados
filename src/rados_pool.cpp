/*
 * Copyright (C) 2012, xp@renzhi.ca
 * All rights reserved.
 */

#include "rados_nif.h"


ERL_NIF_TERM x_pool_lookup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char pool_name[MAX_NAME_LEN];
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], pool_name, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster_get(id);
    if (cluster == NULL)
    {
        return enif_make_badarg(env);
    }

    int64_t err = rados_pool_lookup(cluster, pool_name);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_int64(env, err));  // Pool ID
}

ERL_NIF_TERM x_pool_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char pool_name[MAX_NAME_LEN];
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], pool_name, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster_get(id);
    if (cluster == NULL)
    {
        return enif_make_badarg(env);
    }

    int err = rados_pool_create(cluster, pool_name);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }
    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_pool_create_for_user(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char pool_name[MAX_NAME_LEN];
    uint64_t uid;
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], pool_name, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_get_uint64(env, argv[2], &uid))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster_get(id);
    if (cluster == NULL)
    {
        return enif_make_badarg(env);
    }

    int err = rados_pool_create_with_auid(cluster, pool_name, uid);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }
    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_pool_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char pool_name[MAX_NAME_LEN];
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], pool_name, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster_get(id);
    if (cluster == NULL)
    {
        return enif_make_badarg(env);
    }

    int err = rados_pool_delete(cluster, pool_name);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }
    return enif_make_atom(env, "ok");
}
