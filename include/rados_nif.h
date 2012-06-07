/*
 * Copyright (C) 2012, xp@renzhi.ca
 * All rights reserved.
 */


#ifndef _RADOS_NIF_H_
#define _RADOS_NIF_H_

#include <map>
#include <rados/librados.h>
#include <erl_nif.h>

#include "log.hpp"

using namespace std;

#define MAX_NAME_LEN       1024
#define MAX_FILE_NAME_LEN  2048
#define MAX_BUF_LEN        4096

extern map<uint64_t, rados_t> map_cluster;
extern map<uint64_t, rados_ioctx_t> map_ioctx;
extern map<uint64_t, rados_list_ctx_t> map_list_ctx;
extern map<uint64_t, rados_xattrs_iter_t> map_xattr_iter;

extern XLog logger;

uint64_t new_id();
ERL_NIF_TERM make_error_tuple(ErlNifEnv* env, int err);

ERL_NIF_TERM x_add_stderr_log_handler(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_add_sys_log_handler(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_add_file_log_handler(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_set_log_level(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM x_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_create_with_user(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_conf_read_file(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_conf_read_file2(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_conf_set(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
// x_conf_get()
ERL_NIF_TERM x_connect(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_shutdown(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_get_instance_id(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_pool_list(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_cluster_stat(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM x_pool_lookup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_pool_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_pool_create_for_user(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
// x_pool_create_with_crush_rule()
// x_pool_create_with_all()
ERL_NIF_TERM x_pool_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM x_ioctx_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_ioctx_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_ioctx_pool_stat(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_ioctx_pool_set_auid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_ioctx_pool_get_auid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_ioctx_get_id(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_ioctx_get_pool_name(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_ioctx_snap_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_ioctx_snap_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_rollback(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_ioctx_snap_list(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_ioctx_snap_lookup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_ioctx_snap_get_name(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_ioctx_snap_get_stamp(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
// x_ioctx_snap_set_read()
// x_get_last_version
ERL_NIF_TERM x_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_write_full(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
// x_clone_range
ERL_NIF_TERM x_append(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_read(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_trunc(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
// x_ioctx_pool_set_auid()
// x_ioctx_pool_get_auid()
// x_ioctx_locator_set_key()
ERL_NIF_TERM x_objects_list_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_objects_list_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_objects_list_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM x_getxattr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_setxattr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_rmxattr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_getxattrs(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_getxattrs_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM x_getxattrs_end(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM x_stat(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM x_aio_flush(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif
