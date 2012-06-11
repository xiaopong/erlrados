/*
 * Copyright (C) 2012, xp@renzhi.ca
 * All rights reserved.
 */

#include <errno.h>

#include "rados_nif.h"


ERL_NIF_TERM x_getxattr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char oid[MAX_NAME_LEN];
    char xattr[MAX_NAME_LEN];
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_get_string(env, argv[2], xattr, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    char val[MAX_BUF_LEN];
    int err = rados_getxattr(io, oid, xattr, val, MAX_BUF_LEN);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    ErlNifBinary obin;
    enif_alloc_binary(err, &obin);  // On success, returned value from rados_getxattr() is the length
    memcpy(obin.data, val, err);
    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_binary(env, &obin));
}

ERL_NIF_TERM x_setxattr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char oid[MAX_NAME_LEN];
    char xattr[MAX_NAME_LEN];
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_get_string(env, argv[2], xattr, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_is_binary(env, argv[3]))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    ErlNifBinary ibin;
    enif_inspect_binary(env, argv[3], &ibin);

    int err = rados_setxattr(io, oid, xattr, (const char*)ibin.data, ibin.size);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }
    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_rmxattr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char oid[MAX_NAME_LEN];
    char xattr[MAX_NAME_LEN];
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_get_string(env, argv[2], xattr, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }
    
    int err = rados_rmxattr(io, oid, xattr);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_getxattrs(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char oid[MAX_NAME_LEN];
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    rados_xattrs_iter_t iter;
    int err = rados_getxattrs(io, oid, &iter);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    uint64_t iter_id = new_id();
    map_xattr_iter_add(iter_id, iter);
    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_uint64(env, iter_id));
}

ERL_NIF_TERM x_getxattrs_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char oid[MAX_NAME_LEN];
    if (!enif_get_uint64(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_xattrs_iter_t iter = map_xattr_iter_get(id);
    if (iter == NULL)
    {
        return enif_make_badarg(env);
    }

    const char * name[1];
    const char * val[1];
    size_t len;
    int err = rados_getxattrs_next(iter, name, val, &len);
    if ((err < 0) && (err != -ENOENT))
    {
        return make_error_tuple(env, -err);
    }

    if (len > 0)
    {
        ERL_NIF_TERM term_list = enif_make_list(env, 0);
        ErlNifBinary obin;
        enif_alloc_binary(len, &obin);
        memcpy(obin.data, val[0], len);
        term_list = enif_make_list_cell(env,
                                        enif_make_tuple2(env,
                                                         enif_make_atom(env, "value"),
                                                         enif_make_binary(env, &obin)),
                                        term_list);
        enif_release_binary(&obin);
        ERL_NIF_TERM t = enif_make_string(env, name[0], ERL_NIF_LATIN1);
        term_list = enif_make_list_cell(env,
                                        enif_make_tuple2(env,
                                                         enif_make_atom(env, "xattr"),
                                                         t),
                                        term_list);

        return enif_make_tuple2(env,
                                enif_make_atom(env, "ok"),
                                term_list);
    }
    else
    {
        return enif_make_atom(env, "end");
    }
}

ERL_NIF_TERM x_getxattrs_end(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint64_t id;
    char oid[MAX_NAME_LEN];
    if (!enif_get_uint64(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_xattrs_iter_t iter = map_xattr_iter_get(id);
    if (iter == NULL)
    {
        return enif_make_badarg(env);
    }

    rados_getxattrs_end(iter);
    map_xattr_iter_remove(id);

    return enif_make_atom(env, "ok");
}
