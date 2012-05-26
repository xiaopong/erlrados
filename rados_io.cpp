/*
 * Copyright (C) 2012, xp@renzhi.ca
 * All rights reserved.
 */

#include <errno.h>

#include "rados_nif.h"

ERL_NIF_TERM x_ioctx_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    char pool_name[MAX_NAME_LEN];
    if (!enif_get_long(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], pool_name, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io;
    int err = rados_ioctx_create(cluster, pool_name, &io);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    long io_id = random();
    map_ioctx[io_id] = io;
    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_long(env, io_id));
}

ERL_NIF_TERM x_ioctx_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_destroy(io);
    map_ioctx.erase(id);

    return enif_make_atom(env, "ok");    
}

ERL_NIF_TERM x_ioctx_pool_set_auid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    uint64_t uid;
    if (!enif_get_long(env, argv[0], &id) ||
        !enif_get_uint64(env, argv[1], &uid))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    int err = rados_ioctx_pool_set_auid(io, uid);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");    
}

ERL_NIF_TERM x_ioctx_pool_get_auid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    uint64_t uid;
    int err = rados_ioctx_pool_get_auid(io, &uid);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_uint64(env, uid));
}

ERL_NIF_TERM x_ioctx_get_id(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    uint64_t uid = rados_ioctx_get_id(io);
    return enif_make_tuple2(env, 
                            enif_make_atom(env, "ok"),
                            enif_make_uint64(env, uid));
}

ERL_NIF_TERM x_ioctx_get_pool_name(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    char pool_name[MAX_NAME_LEN];
    memset(pool_name, 0, MAX_NAME_LEN);
    int err = rados_ioctx_get_pool_name(io, pool_name, MAX_NAME_LEN - 1);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    // TODO: check return length of the pool name buffer

    return enif_make_tuple2(env, 
                            enif_make_atom(env, "ok"),
                            enif_make_string(env, pool_name, ERL_NIF_LATIN1));
}


// Erlang: write(IoCtx, Oid, Data, Offset)
ERL_NIF_TERM x_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    char oid[MAX_NAME_LEN];
    uint64_t offset;
    memset(oid, 0, MAX_NAME_LEN);
    if (!enif_get_long(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_is_binary(env, argv[2]) ||
        !enif_get_uint64(env, argv[3], &offset))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    ErlNifBinary ibin;
    enif_inspect_binary(env, argv[2], &ibin);

    int err = rados_write(io, oid, (const char*)ibin.data, ibin.size, offset);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }
    enif_release_binary(&ibin);
    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_int(env, err));    // Number of bytes written
}

// Erlang: write_full(IoCtx, Oid, Data)
ERL_NIF_TERM x_write_full(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    char oid[MAX_NAME_LEN];
    memset(oid, 0, MAX_NAME_LEN);
    if (!enif_get_long(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_is_binary(env, argv[2]))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    ErlNifBinary ibin;
    enif_inspect_binary(env, argv[2], &ibin);

    int err = rados_write_full(io, oid, (const char*)ibin.data, ibin.size);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }
    enif_release_binary(&ibin);
    //return enif_make_int(env, err);    
    return enif_make_atom(env, "ok");
}

// Erlang: append(IoCtx, Oid, Data)
ERL_NIF_TERM x_append(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    char oid[MAX_NAME_LEN];
    memset(oid, 0, MAX_NAME_LEN);
    if (!enif_get_long(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_is_binary(env, argv[2]))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    ErlNifBinary ibin;
    enif_inspect_binary(env, argv[2], &ibin);

    int err = rados_append(io, oid, (const char*)ibin.data, ibin.size);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }
    enif_release_binary(&ibin);
    return enif_make_tuple2(env, 
                            enif_make_atom(env, "ok"),
                            enif_make_int(env, err));  // Number of bytes appended
}

// Erlang: read(IoCtx, Oid, Len, Offset)
ERL_NIF_TERM x_read(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    char oid[MAX_NAME_LEN];
    long len;
    uint64_t offset;
    memset(oid, 0, MAX_NAME_LEN);
    if (!enif_get_long(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_get_long(env, argv[2], &len) ||
        !enif_get_uint64(env, argv[3], &offset))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    char * buf = (char *)malloc(len);
    int err = rados_read(io, oid, buf, len, offset);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    if (err > 0)
    {
        ErlNifBinary obin;
        enif_alloc_binary(err, &obin);
        memcpy(obin.data, buf, err);
        free(buf);

        ERL_NIF_TERM ret = enif_make_tuple2(env,
                                            enif_make_atom(env, "ok"),
                                            enif_make_binary(env, &obin));
        enif_release_binary(&obin);
        return ret;
    }
    else
    {
        free(buf);
        return enif_make_atom(env, "eof");
    }
}

ERL_NIF_TERM x_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    char oid[MAX_NAME_LEN];
    if (!enif_get_long(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    int err = rados_remove(io, oid);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_trunc(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    char oid[MAX_NAME_LEN];
    uint64_t size;
    if (!enif_get_long(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_get_uint64(env, argv[2], &size))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    int err = rados_trunc(io, oid, size);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_ioctx_pool_stat(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }

    rados_pool_stat_t stat;
    int err = rados_ioctx_pool_stat(io, &stat);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    ERL_NIF_TERM term_list = enif_make_list(env, 0);
    ERL_NIF_TERM t = enif_make_uint64(env, stat.num_wr_kb);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_wr_kb"),
                                                     t),
                                    term_list);

    t = enif_make_uint64(env, stat.num_wr);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_wr"),
                                                     t),
                                    term_list);

    t = enif_make_uint64(env, stat.num_rd_kb);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_rd_kb"),
                                                     t),
                                    term_list);

    t = enif_make_uint64(env, stat.num_rd);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_rd"),
                                                     t),
                                    term_list);

    t = enif_make_uint64(env, stat.num_objects_degraded);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_objects_degraded"),
                                                     t),
                                    term_list);

    t = enif_make_uint64(env, stat.num_objects_unfound);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_objects_unfound"),
                                                     t),
                                    term_list);

    t = enif_make_uint64(env, stat.num_objects_missing_on_primary);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_objects_missing_on_primary"),
                                                     t),
                                    term_list);

    t = enif_make_uint64(env, stat.num_object_copies);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_object_copies"),
                                                     t),
                                    term_list);

    t = enif_make_uint64(env, stat.num_object_clones);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_object_clones"),
                                                     t),
                                    term_list);

    t = enif_make_uint64(env, stat.num_objects);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_objects"),
                                                     t),
                                    term_list);

    t = enif_make_uint64(env, stat.num_kb);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_kb"),
                                                     t),
                                    term_list);

    t = enif_make_uint64(env, stat.num_bytes);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_bytes"),
                                                     t),
                                    term_list);

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            term_list);
}

ERL_NIF_TERM x_stat(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    char oid[MAX_NAME_LEN];
    memset(oid, 0, MAX_NAME_LEN);
    if (!enif_get_long(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }
    
    uint64_t size;
    time_t mtime;
    int err = rados_stat(io, oid, &size, &mtime);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    ERL_NIF_TERM term_list = enif_make_list(env, 0);
    ERL_NIF_TERM t = enif_make_uint64(env, mtime);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "mtime"),
                                                     t),
                                    term_list);

    t = enif_make_uint64(env, size);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "size"),
                                                     t),
                                    term_list);

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            term_list);
}

ERL_NIF_TERM x_objects_list_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx[id];
    if (io == NULL)
    {
        return enif_make_badarg(env);
    }
    
    rados_list_ctx_t ctx;
    int err = rados_objects_list_open(io, &ctx);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    long listid = random();
    map_list_ctx[listid] = ctx;
    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_long(env, listid));
}

ERL_NIF_TERM x_objects_list_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_list_ctx_t ctx = map_list_ctx[id];
    if (ctx == NULL)
    {
        return enif_make_badarg(env);
    }

    const char * entry[1];
    const char * key[1];
    int err = rados_objects_list_next(ctx, entry, key);
    if ((err < 0) && (err != -ENOENT))
    {
        return make_error_tuple(env, -err);
    }

    if (err == -ENOENT)
    {
        return enif_make_atom(env, "end");
    }

    ERL_NIF_TERM term_list = enif_make_list(env, 0);
    ERL_NIF_TERM t;
    if (key[0] != NULL)
    {
        t = enif_make_string(env, key[0], ERL_NIF_LATIN1);
        term_list = enif_make_list_cell(env,
                                        enif_make_tuple2(env,
                                                         enif_make_atom(env, "key"),
                                                         t),
                                        term_list);
    }
    t = enif_make_string(env, entry[0], ERL_NIF_LATIN1);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "entry"),
                                                     t),
                                    term_list);

    // TODO: fix me
    // Caller must free, according to the API doc.
    // if (entry[0] != NULL)
    //     free((void*)entry[0]);
    // if (entry[0])
    //     free((void*)key[0]);

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            term_list);
}

ERL_NIF_TERM x_objects_list_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_list_ctx_t ctx = map_list_ctx[id];
    if (ctx == NULL)
    {
        return enif_make_badarg(env);
    }
    
    rados_objects_list_close(ctx);
    map_list_ctx.erase(id);

    return enif_make_atom(env, "ok");    
}
