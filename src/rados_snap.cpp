/*
 * Copyright (C) 2012, xp@renzhi.ca
 * All rights reserved.
 */

#include <stdio.h>
#include <errno.h>

#include "rados_nif.h"


ERL_NIF_TERM x_ioctx_snap_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char snap[MAX_NAME_LEN];
    memset(snap, 0, MAX_NAME_LEN);
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], snap, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    int err = rados_ioctx_snap_create(io, snap);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_ioctx_snap_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char snap[MAX_NAME_LEN];
    memset(snap, 0, MAX_NAME_LEN);
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], snap, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    int err = rados_ioctx_snap_remove(io, snap);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_rollback(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char oid[MAX_NAME_LEN];
    char snap[MAX_NAME_LEN];
    memset(snap, 0, MAX_NAME_LEN);
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_get_string(env, argv[2], snap, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    int err = rados_rollback(io, oid, snap);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_ioctx_snap_list(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    rados_snap_t snaps[2000];
    int num = rados_ioctx_snap_list(io, snaps, 2000);
    if ((num < 0) && (num != -ERANGE))
    {
        return make_error_tuple(env, -num);
    }

    if (num > 0)
    {
        ERL_NIF_TERM term_list = enif_make_list(env, 0);
        for (int i = num - 1; i >= 0; i--)
        {
            ERL_NIF_TERM t = enif_make_uint64(env, snaps[i]);
            term_list = enif_make_list_cell(env, t, term_list);
        }

        return enif_make_tuple2(env,
                                enif_make_atom(env, "ok"),
                                term_list);
    }
    else
    {
        return enif_make_tuple2(env,
                                enif_make_atom(env, "ok"),
                                enif_make_list(env, 0));    // empty list
    }
}
ERL_NIF_TERM x_ioctx_snap_lookup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char snap[MAX_NAME_LEN];
    memset(snap, 0, MAX_NAME_LEN);
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], snap, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }
    
    rados_snap_t snapid;
    int err = rados_ioctx_snap_lookup(io, snap, &snapid);
    if (err < 0)
        return make_error_tuple(env, -err);

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_uint64(env, snapid));
}

ERL_NIF_TERM x_ioctx_snap_get_name(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    rados_snap_t snapid;
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_uint64(env, argv[1], &snapid))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    char snap[MAX_NAME_LEN];
    memset(snap, 0, MAX_NAME_LEN);
    int err = rados_ioctx_snap_get_name(io, snapid, snap, MAX_NAME_LEN);
    if (err < 0)
        return make_error_tuple(env, -err);

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_string(env, snap, ERL_NIF_LATIN1));
}
ERL_NIF_TERM x_ioctx_snap_get_stamp(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    rados_snap_t snapid;
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_uint64(env, argv[1], &snapid))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    time_t tm;
    int err = rados_ioctx_snap_get_stamp(io, snapid, &tm);
    if (err < 0)
        return make_error_tuple(env, -err);

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_uint64(env, tm));
}

