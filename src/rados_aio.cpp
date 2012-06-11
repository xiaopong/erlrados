/*
 * Copyright (C) 2012, xp@renzhi.ca
 * All rights reserved.
 */


#include "rados_nif.h"

ERL_NIF_TERM x_aio_flush(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
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

    int err = rados_aio_flush(io);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");
}
