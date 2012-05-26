/*
 * Copyright (C) 2012, xp@renzhi.ca
 * All rights reserved.
 */

#include <errno.h>
#include <list>

#include "rados_nif.h"

ERL_NIF_TERM x_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    rados_t cluster;
    int err = rados_create(&cluster, NULL);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    long id = random();
    map_cluster[id] = cluster;
    return enif_make_tuple2(env, 
                            enif_make_atom(env, "ok"),
                            enif_make_long(env, id));
}

ERL_NIF_TERM x_create_with_user(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name[MAX_NAME_LEN];
    memset(name, 0, MAX_NAME_LEN);
    if (!enif_get_string(env, argv[0], name, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster;
    int err = rados_create(&cluster, name);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    long id = random();
    map_cluster[id] = cluster;
    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            enif_make_long(env, id));
}

ERL_NIF_TERM x_conf_read_file(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        return enif_make_badarg(env);
    }

    int err = rados_conf_read_file(cluster, NULL);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");    
}

ERL_NIF_TERM x_conf_read_file2(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    char conf_file[MAX_FILE_NAME_LEN];
    memset(conf_file, 0, MAX_FILE_NAME_LEN);
    if (!enif_get_long(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], conf_file, MAX_FILE_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        return enif_make_badarg(env);
    }

    int err = rados_conf_read_file(cluster, conf_file);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");    
}

ERL_NIF_TERM x_conf_set(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    char option[MAX_NAME_LEN];
    memset(option, 0, MAX_NAME_LEN);
    char value[MAX_NAME_LEN];
    memset(value, 0, MAX_NAME_LEN);
    if (!enif_get_long(env, argv[0], &id) ||
        !enif_get_string(env, argv[1], option, MAX_NAME_LEN, ERL_NIF_LATIN1) ||
        !enif_get_string(env, argv[1], option, MAX_NAME_LEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        return enif_make_badarg(env);
    }
    
    int err = rados_conf_set(cluster, option, value);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");    
}

ERL_NIF_TERM x_connect(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        return enif_make_badarg(env);
    }

    int err = rados_connect(cluster);
    if (err < 0) 
    {
        return make_error_tuple(env, -err);
    }

    return enif_make_atom(env, "ok");    
}

ERL_NIF_TERM x_shutdown(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        return enif_make_badarg(env);
    }

    rados_shutdown(cluster);
    map_cluster.erase(id);

    return enif_make_atom(env, "ok");
}

ERL_NIF_TERM x_get_instance_id(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        return enif_make_badarg(env);
    }

    uint64_t inst_id = rados_get_instance_id(cluster);

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
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
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
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
            return make_error_tuple(env, ENOMEM);
        }
        int buf_len2 = rados_pool_list(cluster, buf, buf_len + 10);
        if (buf_len2 < 0)
        {
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


ERL_NIF_TERM x_cluster_stat(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long id;
    if (!enif_get_long(env, argv[0], &id))
    {
        return enif_make_badarg(env);
    }

    rados_t cluster = map_cluster[id];
    if (cluster == NULL)
    {
        return enif_make_badarg(env);
    }

    rados_cluster_stat_t stat;
    int err = rados_cluster_stat(cluster, &stat);
    if (err < 0)
    {
        return make_error_tuple(env, -err);
    }

    ERL_NIF_TERM term_list = enif_make_list(env, 0);
    ERL_NIF_TERM t = enif_make_uint64(env, stat.num_objects);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "num_objects"),
                                                     t),
                                    term_list);
    t = enif_make_uint64(env, stat.kb_avail);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "kb_avail"),
                                                     t),
                                    term_list);
    t = enif_make_uint64(env, stat.kb_used);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "kb_used"),
                                                     t),
                                    term_list);
    t = enif_make_uint64(env, stat.kb);
    term_list = enif_make_list_cell(env,
                                    enif_make_tuple2(env,
                                                     enif_make_atom(env, "kb"),
                                                     t),
                                    term_list);

    return enif_make_tuple2(env,
                            enif_make_atom(env, "ok"),
                            term_list);
}
