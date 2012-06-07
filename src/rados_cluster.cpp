/*
 * Copyright (C) 2012, xp@renzhi.ca
 * All rights reserved.
 */

#include <errno.h>
#include <stdio.h>
#include <list>

#include "rados_nif.h"

static const char* MOD_NAME = "rados_cluster";

ERL_NIF_TERM x_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_create()";
    logger.debug(MOD_NAME, func_name, "Entered");
    logger.flush();

    rados_t cluster;
    int err = rados_create(&cluster, NULL);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "Unable to create cluster handle");
        return make_error_tuple(env, -err);
    }

    logger.debug(MOD_NAME, func_name, "cluster created");
    logger.flush();

    uint64_t id = new_id();
    map_cluster[id] = cluster;

    logger.debug(MOD_NAME, func_name, "cluster added to local map: %ld", id);
    logger.flush();

    return enif_make_tuple2(env, 
                            erlrados_atoms.ok,
                            enif_make_uint64(env, id));
}

ERL_NIF_TERM x_create_with_user(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_create_with_user()";
    logger.debug(MOD_NAME, func_name, "Entered");

    char name[MAX_NAME_LEN];
    memset(name, 0, MAX_NAME_LEN);
    if (!enif_get_string(env, argv[0], name, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    rados_t cluster;
    int err = rados_create(&cluster, name);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "Unable to create cluster handle with name: %s", name);
        return make_error_tuple(env, -err);
    }

    uint64_t id = new_id();
    map_cluster[id] = cluster;

    logger.debug(MOD_NAME, func_name, "cluster : %ld", id);

    return enif_make_tuple2(env,
                            erlrados_atoms.ok,
                            enif_make_uint64(env, id));
}

ERL_NIF_TERM x_conf_read_file(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_conf_read_file()";
    logger.debug(MOD_NAME, func_name, "Entered");

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "cluster : %ld", id);

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        logger.error(MOD_NAME, func_name, "cluster non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    int err = rados_conf_read_file(cluster, NULL);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "failed to read default config file for cluster: %ld", id);
        return make_error_tuple(env, -err);
    }

    return erlrados_atoms.ok;
}

ERL_NIF_TERM x_conf_read_file2(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_conf_read_file2()";
    logger.debug(MOD_NAME, func_name, "Entered");
    logger.flush();

    uint64_t id;
    char conf_file[MAX_FILE_NAME_LEN];
    memset(conf_file, 0, MAX_FILE_NAME_LEN);
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], conf_file, MAX_FILE_NAME_LEN, ERL_NIF_LATIN1))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "cluster : %ld", id);
    logger.flush();

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        logger.error(MOD_NAME, func_name, "cluster non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "cluster found: %ld", id);
    logger.flush();

    int err = rados_conf_read_file(cluster, conf_file);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "failed to read default config file %s for cluster: %ld", conf_file, id);
        return make_error_tuple(env, -err);
    }

    logger.debug(MOD_NAME, func_name, "config file read: %ld", id);
    logger.flush();

    return erlrados_atoms.ok;
}

ERL_NIF_TERM x_conf_set(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_conf_set()";
    logger.debug(MOD_NAME, func_name, "Entered");

    uint64_t id;
    char option[MAX_NAME_LEN];
    memset(option, 0, MAX_NAME_LEN);
    char value[MAX_NAME_LEN];
    memset(value, 0, MAX_NAME_LEN);
    if (!enif_get_uint64(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], option, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_get_string(env, argv[1], option, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "cluster : %ld", id);

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        logger.error(MOD_NAME, func_name, "cluster non-existing : %ld", id);
        return enif_make_badarg(env);
    }
    
    int err = rados_conf_set(cluster, option, value);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return erlrados_atoms.ok;
}

ERL_NIF_TERM x_connect(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_connect()";
    logger.debug(MOD_NAME, func_name, "Entered");

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "cluster : %ld", id);

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        logger.error(MOD_NAME, func_name, "cluster non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    int err = rados_connect(cluster);
    if (err < 0) 
    {
        logger.error(MOD_NAME, func_name, "failed to connect to cluster %ld: %s", id, strerror(-err));
        return make_error_tuple(env, -err);
    }

    return erlrados_atoms.ok;
}

ERL_NIF_TERM x_shutdown(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_shutdown()";
    logger.debug(MOD_NAME, func_name, "Entered");

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "cluster : %ld", id);
    logger.flush();

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        logger.error(MOD_NAME, func_name, "cluster non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "found cluster: %ld", id);
    logger.flush();

    rados_shutdown(cluster);

    logger.debug(MOD_NAME, func_name, "cluster shutdown: %ld", id);
    logger.flush();
    
    map_cluster.erase(id);

    logger.debug(MOD_NAME, func_name, "cluster erased: %ld", id);
    logger.flush();

    return erlrados_atoms.ok;
}

ERL_NIF_TERM x_get_instance_id(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_get_instance_id()";

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "cluster : %ld", id);

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        logger.error(MOD_NAME, func_name, "cluster non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    uint64_t inst_id = rados_get_instance_id(cluster);

    return enif_make_tuple2(env,
                            erlrados_atoms.ok,
                            enif_make_uint64(env, inst_id));
}


void scan_pool_name_list(list<int> *pool_list, char * buf, int buf_len)
{
    int done = 0;
    int curr = 0;
    while (!done)
    {
        if ((buf[curr] == 0) && (buf[curr+1] == 0))
            done = 1;
        else 
        {
            if (buf[curr] == 0)
                curr++;

            pool_list->push_back(curr);

            while (buf[curr] != 0)
            {
                curr++;
            }
        }
    }
}

ERL_NIF_TERM x_pool_list(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_pool_list()";

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "cluster : %ld", id);

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        logger.error(MOD_NAME, func_name, "cluster non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    // Call with a null buffer to get the buffer length first.
    int buf_len = rados_pool_list(cluster, NULL, 0);
    if (buf_len < 0)
    {
        return make_error_tuple(env, -buf_len);
    }

    if (buf_len > 0)
    {
        char * buf = (char *)malloc(buf_len + 10);
        if (buf == NULL)
        {
            logger.error(MOD_NAME, func_name, "unable to malloc: %ld", id);
            return make_error_tuple(env, ENOMEM);
        }
        int buf_len2 = rados_pool_list(cluster, buf, buf_len + 10);
        if (buf_len2 < 0)
        {
            logger.error(MOD_NAME, func_name, "failed to get pool list for %ld: %s", id, strerror(-buf_len2));
            return make_error_tuple(env, -buf_len2);
        }

        list<int> pool_list;
        scan_pool_name_list(&pool_list, buf, buf_len);
        list<int>::const_iterator it;
        ERL_NIF_TERM term_list = enif_make_list(env, 0);
        for (it = pool_list.begin(); it != pool_list.end(); it++)
        {
            int off = *it;
            ERL_NIF_TERM head = enif_make_string(env, buf + off, ERL_NIF_LATIN1);
            
            term_list = enif_make_list_cell(env, head, term_list);
        }

        free(buf);

        return enif_make_tuple2(env,
                                erlrados_atoms.ok,
                                term_list);
    }
    else
    {
        return enif_make_tuple2(env,
                                erlrados_atoms.ok,
                                enif_make_list(env, 0));    // empty list
    }
}


ERL_NIF_TERM x_cluster_stat(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const char * func_name = "x_cluster_stat()";

    uint64_t id;
    if (!enif_get_uint64(env, argv[0], &id))
    {
        logger.error(MOD_NAME, func_name, "enif get params failed");
        return enif_make_badarg(env);
    }

    logger.debug(MOD_NAME, func_name, "cluster : %ld", id);

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        logger.error(MOD_NAME, func_name, "cluster non-existing : %ld", id);
        return enif_make_badarg(env);
    }

    rados_cluster_stat_t stat;
    int err = rados_cluster_stat(cluster, &stat);
    if (err < 0)
    {
        logger.error(MOD_NAME, func_name, "failed to get stat for %ld: %s", id, strerror(-err));
        return make_error_tuple(env, -err);
    }

    ERL_NIF_TERM term_list = enif_make_list(env, 0);
    ERL_NIF_TERM t = enif_make_uint64(env, stat.num_objects);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     erlrados_atoms.num_objects,
                                                     t),
                                    term_list);
    t = enif_make_uint64(env, stat.kb_avail);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     erlrados_atoms.kb_avail,
                                                     t),
                                    term_list);
    t = enif_make_uint64(env, stat.kb_used);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     erlrados_atoms.kb_used,
                                                     t),
                                    term_list);
    t = enif_make_uint64(env, stat.kb);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     erlrados_atoms.kb,
                                                     t),
                                    term_list);

    return enif_make_tuple2(env,
                            erlrados_atoms.ok,
                            term_list);
}
