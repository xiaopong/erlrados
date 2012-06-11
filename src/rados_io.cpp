/*
 * Copyright (C) 2012, xp@renzhi.ca
 * All rights reserved.
 */

#include <errno.h>
#include <time.h>
#include <stdio.h>

#include "rados_nif.h"

static const char* MOD_NAME = "rados_io";

ERL_NIF_TERM x_ioctx_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_ioctx_create()";

    uint64_t id;
    char pool_name[MAX_NAME_LEN];
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], pool_name, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "cluster : %ld", id);
    rados_t cluster = map_cluster_get(id);
    if (cluster == NULL)
    {
        logger.error(MOD_NAME, func_name, "cluster non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    rados_ioctx_t io;
    int err = rados_ioctx_create(cluster, pool_name, &io);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "failed to create ioctx to cluster %ld: %s", id, strerror(-err));
        return make_error_tuple(env, -err);
    }

    uint64_t io_id = new_id();
    map_ioctx_add(io_id, io);

    logger.debug(MOD_NAME, func_name, "cluster=%ld, ioctx=%ld", id, io_id);

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_uint64(env, io_id));
}

ERL_NIF_TERM x_ioctx_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_ioctx_destroy()";

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "ioctx : %ld", id);

    // Flush first to make sure that any writes are completed.
    rados_aio_flush(io);

    rados_ioctx_destroy(io);
    map_ioctx_remove(id);

    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_ioctx_pool_set_auid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_ioctx_pool_set_auid()";

    uint64_t id;
    uint64_t uid;
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_uint64(env, argv[1], &uid))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "ioctx : %ld, uid : %ld", id, uid);

    int err = rados_ioctx_pool_set_auid(io, uid);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "set auid failed: %s", strerror(-err));
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_ioctx_pool_get_auid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_ioctx_pool_get_auid()";

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "ioctx : %ld", id);

    uint64_t uid;
    int err = rados_ioctx_pool_get_auid(io, &uid);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "get auid failed: %s", strerror(-err));
        return make_error_tuple(env, -err);
    }

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_uint64(env, uid));
}

ERL_NIF_TERM x_ioctx_get_id(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_ioctx_get_id()";

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "ioctx : %ld", id);

    uint64_t uid = rados_ioctx_get_id(io);
    return enif_make_tuple2(env, 
                            enif_make_atom(env, "ok"),
                            enif_make_uint64(env, uid));
}

ERL_NIF_TERM x_ioctx_get_pool_name(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_ioctx_get_pool_name()";

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "ioctx : %ld", id);

    char pool_name[MAX_NAME_LEN];
    memset(pool_name, 0, MAX_NAME_LEN);
    int err = rados_ioctx_get_pool_name(io, pool_name, MAX_NAME_LEN - 1);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "get pool name failed: %s", strerror(-err));
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
    const char * func_name = "x_write()";

    uint64_t id;
    char oid[MAX_NAME_LEN];
    uint64_t offset;
    memset(oid, 0, MAX_NAME_LEN);
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_is_binary(env, argv[2]) ||
        !enif_get_uint64(env, argv[3], &offset))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    ErlNifBinary ibin;
    enif_inspect_binary(env, argv[2], &ibin);

    logger.debug(MOD_NAME, func_name, "id=%ld, oid=%s, len=%d, offset=%ld", id, oid, ibin.size, offset);

    int err = rados_write(io, oid, (const char*)ibin.data, ibin.size, offset);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "write failed: %s", strerror(-err));
        enif_release_binary(&ibin);
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
    const char * func_name = "x_write_full()";

    uint64_t id;
    char oid[MAX_NAME_LEN];
    memset(oid, 0, MAX_NAME_LEN);
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_is_binary(env, argv[2]))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    ErlNifBinary ibin;
    enif_inspect_binary(env, argv[2], &ibin);

    int err = rados_write_full(io, oid, (const char*)ibin.data, ibin.size);
    if (err < 0) 
    {
        enif_release_binary(&ibin);
        return make_error_tuple(env, -err);
    }
    enif_release_binary(&ibin);

    return enif_make_atom(env, "ok");
}

// Erlang: append(IoCtx, Oid, Data)
ERL_NIF_TERM x_append(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_append()";

    uint64_t id;
    char oid[MAX_NAME_LEN];
    memset(oid, 0, MAX_NAME_LEN);
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_is_binary(env, argv[2]))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    ErlNifBinary ibin;
    enif_inspect_binary(env, argv[2], &ibin);

    int err = rados_append(io, oid, (const char*)ibin.data, ibin.size);
    if (err < 0) 
    {
        enif_release_binary(&ibin);
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
    const char * func_name = "x_read()";

    uint64_t id;
    char oid[MAX_NAME_LEN];
    long len;
    uint64_t offset;
    memset(oid, 0, MAX_NAME_LEN);
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_get_long(env, argv[2], &len) ||
        !enif_get_uint64(env, argv[3], &offset))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "io=%ld, oid=%s, len=%ld, offset=%ld", id, oid, len, offset);

    char * buf = (char *)malloc(len);
    if (!buf)
    {
        logger.error(MOD_NAME, func_name, "unable to malloc for %ld", id);
        return make_error_tuple(env, ENOMEM);
    }

    int err = rados_read(io, oid, buf, len, offset);
    if (err < 0) 
    {
        free(buf);
        logger.error(MOD_NAME, func_name, "read failed %ld: %s", id, strerror(-err));
        return make_error_tuple(env, -err);
    }

    if (err > 0)
    {
        ErlNifBinary obin;
        if (!enif_alloc_binary(err, &obin))
        {
            free(buf);
            return make_error_tuple(env, ENOMEM);
        }
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
    const char * func_name = "x_remove()";

    uint64_t id;
    char oid[MAX_NAME_LEN];
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "io=%ld, oid=%s", id, oid);

    int err = rados_remove(io, oid);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "failed to remove: io=%ld, oid=%s", id, oid);
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_trunc(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_trunc()";

    uint64_t id;
    char oid[MAX_NAME_LEN];
    uint64_t size;
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_get_uint64(env, argv[2], &size))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "io=%ld, oid=%s, size=%ld", id, oid, size);

    int err = rados_trunc(io, oid, size);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "failed to truncate : io=%ld, oid=%s, size=%ld", id, oid, size);
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_ioctx_pool_stat(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_ioctx_pool_stat()";

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    rados_pool_stat_t stat;
    int err = rados_ioctx_pool_stat(io, &stat);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "unable to read pool stat for %ld: %s", id, strerror(-err));
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
    const char * func_name = "x_stat()";

    uint64_t id;
    char oid[MAX_NAME_LEN];
    memset(oid, 0, MAX_NAME_LEN);
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], oid, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "ioctx=%ld, oid=%s", id, oid);

    uint64_t size;
    time_t mtime;
    int err = rados_stat(io, oid, &size, &mtime);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "unable to read object stat for %s: %s", oid, strerror(-err));
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
    const char * func_name = "x_objects_list_open()";

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_ioctx_t io = map_ioctx_get(id);
    if (io == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx non-existing : %ld", id);
        return enif_make_badarg(env);
    }
    
    rados_list_ctx_t ctx;
    int err = rados_objects_list_open(io, &ctx);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    uint64_t listid = new_id();
    map_list_ctx_add(listid, ctx);
    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_uint64(env, listid));
}

ERL_NIF_TERM x_objects_list_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_objects_list_next()";

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_list_ctx_t ctx = map_list_ctx_get(id);
    if (ctx == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx object list : %ld", id);
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "list id: %ld", id);

    const char * entry[1];
    const char * key[1];
    int err = rados_objects_list_next(ctx, entry, key);
    if ((err < 0) && (err != -ENOENT))
    {
        logger.error(MOD_NAME, func_name, "unable to get next object in list for %ld: %s", id, strerror(-err));
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

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            term_list);
}

ERL_NIF_TERM x_objects_list_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_objects_list_close()";

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_list_ctx_t ctx = map_list_ctx_get(id);
    if (ctx == NULL)
    {
        logger.error(MOD_NAME, func_name, "ioctx object list : %ld", id);
        return enif_make_badarg(env);
    }
    
    logger.debug(MOD_NAME, func_name, "list id: %ld", id);

    rados_objects_list_close(ctx);
    map_list_ctx_remove(id);

    return enif_make_atom(env, "ok");
}
