%%
%% Copyright (C) 2012, xp@renzhi.ca
%% All rights reserved.
%%

-module(rados).

-export([
         load/1,
         create/0, create/1,
         conf_read_file/1, conf_read_file/2,
         conf_set/3,
         connect/1,
         shutdown/1,
         get_instance_id/1,
         pool_list/1,
         cluster_stat/1,
         pool_lookup/2,
         pool_create/2, pool_create/3,
         pool_delete/2,
         ioctx_create/2,
         ioctx_destroy/1,
         ioctx_pool_stat/1,
         ioctx_pool_set_auid/2,
         ioctx_pool_get_auid/1,
         ioctx_get_id/1,
         ioctx_get_pool_name/1,
         ioctx_snap_create/2, ioctx_snap_remove/2,
         rollback/3,
         ioctx_snap_list/1, ioctx_snap_list_with_name/1,
         ioctx_snap_lookup/2, ioctx_snap_get_name/2, ioctx_snap_get_stamp/2,
         aio_flush/1,
         write/4,
         write_full/3,
         append/3,
         read/4,
         remove/2,
         trunc/3,
         stat/2,
         objects_list_open/1, objects_list_next/1, objects_list_close/1,
         getxattr/3, setxattr/4, rmxattr/3, 
         getxattrs/2, getxattrs_next/1, getxattrs_end/1
        ]).

-define(LIBNAME, rados_nif).

%%
%% TODO:
%%   - Get config (rados_cct())
%%   - get last version
%%

%%
%% Load the rados_nif shared library. This must called before other functions
%% can be called.
%%
%% @param File   Path of the dirctory where rados_nif.so file is
%%               located, or the absolute path of rados_nif.so.
%%
load(File) ->
    SoName = case file_type(File) of
                 regular ->
                     filename:rootname(File);
                 directory ->
                     filename:join(File, ?LIBNAME);
                 _ -> error
             end,
    erlang:load_nif(SoName, 0).

%%
%% Create a handle for communicating with a RADOS cluster.
%% 
%% @param Id     the user to connect as (i.e. admin, not client.admin)
%%
%% @returns      {ok, Handle}, a handle for communicating with a RADOS cluster,
%%               {error, Reason} on failure.
%%
create(Id) ->
    "RADOS NIF library not loaded".

create() ->
    "RADOS NIF library not loaded".

%%
%% Configure the cluster handle using a Ceph config file.
%%
%% @param Cluster The cluster handle from rados_create()..
%% @param Path    Path and name to the config file.
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
conf_read_file(Cluster, Path) when is_integer(Cluster) ->
    "RADOS NIF library not loaded".

%%
%% Configure the cluster handle using a Ceph config file.
%% Search the default locations for the config file.
%%
%% @param Cluster The cluster handle from rados_create().
%% @param Path    Path and name to the config file.
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
conf_read_file(Cluster) when is_integer(Cluster) ->
    "RADOS NIF library not loaded".

%%
%% Set a configuration option.
%%
%% @param Cluster     cluster handle to configure
%% @param Option      option to set
%% @param Value       value of the option
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
conf_set(Cluster, Option, Value) when is_integer(Cluster) ->
    "RADOS NIF library not loaded".

%%
%% Connect to the cluster.
%%
%% @param Cluster  The cluster to connect to.
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
connect(Cluster) when is_integer(Cluster) ->
    "RADOS NIF library not loaded".


%%
%% Disconnects from the cluster.
%%
%% For clean up, this is only necessary after rados_connect()has succeeded.
%%
%% @param Cluster   the cluster to shutdown
%%
%% @returns       'ok' on return.
%%
shutdown(Cluster) ->
    "RADOS NIF library not loaded".

%%
%% Get a global id for current instance
%%
%% This id is a unique representation of current connection to the cluster
%%
%% @param Cluster   the cluster handle
%%
%% @returns         {ok, Id} of instance global id, or {error, Reason} on failure
%%
get_instance_id(Cluster) when is_integer(Cluster) ->
    "RADOS NIF library not loaded".

%%
%% Get a list of pool names in a cluster.
%%
%% @param Cluster    Cluster to list the pools for.
%%
%% @returns          {ok, [pool1|pool2|...]}, or {error, Reason} on failure
%%
pool_list(Cluster) when is_integer(Cluster) ->
    "RADOS NIF library not loaded".

%%
%% Read usage info about the cluster.
%% 
%% This tells you total space, space used, space available, and number of objects. These are not 
%% updated immediately when data is written, they are eventually consistent.
%%
%% @param Cluster    cluster to get the stat info for.
%%
%% @returns          {ok, [{kb, Value}|{kb_used, Value}|{kb_avail, Value}|{num_objects, Value}]}, 
%%                   or {error, Reason} on failure
cluster_stat(Cluster) when is_integer(Cluster) ->
    "RADOS NIF library not loaded".

%%
%% Get the id of a pool.
%%
%% @param Cluster  Which cluster the pool is in
%% @param PoolName Which pool to look up
%%
%% @returns        {ok, Id} of the pool on success, {error, Reason} on error.
%%
pool_lookup(Cluster, PoolName) when is_integer(Cluster) ->
    "RADOS NIF library not loaded".

%%
%% Create a pool with default settings.
%%
%% @param Cluster   the cluster in which the pool will be created
%% @param PoolName  the name of the new pool
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
pool_create(Cluster, PoolName) when is_integer(Cluster) ->
    "RADOS NIF library not loaded".

%%
%% Create a pool owned by a specific uid.
%%
%% The auid is the authenticated user id to give ownership of the pool.
%%
%% @param Cluster   the cluster in which the pool will be created
%% @param PoolName  the name of the new pool
%% @param Uid       the id of the owner of the new pool
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
pool_create(Cluster, PoolName, Uid) when is_integer(Cluster), is_integer(Uid) ->
    "RADOS NIF library not loaded".

%%
%% Create a pool with default settings.
%%
%% The pool is removed from the cluster immediately,
%% but the actual data is deleted in the background.
%%
%% @param Cluster   the cluster in which the pool is
%% @param PoolName  which pool to delete
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
pool_delete(Cluster, PoolName) when is_integer(Cluster) ->
    "RADOS NIF library not loaded".

%%
%% Create an io context. The io context allows you to perform operations within 
%% a particular pool.
%% 
%% @param Cluster   which cluster the pool is in
%% @param PoolName  name of the pool
%%
%% @returns         {ok, Handle} to the io context, or {error, Reason} on failure.
%%
ioctx_create(Cluster, PoolName) when is_integer(Cluster) ->
    "RADOS NIF library not loaded".

%%
%% The opposite of rados_ioctx_create.
%%
%% This just tells librados that you no longer need to use the io context. It may 
%% not be freed immediately if there are pending asynchronous requests on it, but 
%% you should not use an io context again after calling this function on it.
%%
%% @param IoCtx   the io context to dispose of
%%
%% @returns       'ok' on return.
%%
ioctx_destroy(IoCtx) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Get pool usage statistics. That includes the following information:
%%
%%  num_bytes             space used in bytes
%%  num_kb                space used in KB
%%  num_objects           number of objects in the pool
%%  num_object_clones     number of clones of objects
%%  num_object_copies     num_objects * num_replicas
%%  num_objects_missing_on_primary
%%  num_objects_unfound   number of objects found on no OSDs
%%  num_objects_degraded  number of objects replicated fewer times than they should be (but found on at least one OSD)
%%  num_rd
%%  num_rd_kb
%%  num_wr
%%  num_wr_kb
%%
%% @param IoCtx   the io context of the pool
%%
%% @returns       {ok, [{key|Value}|{key|Value}|...]}
%%                {error, Reason} on failure.
%%
ioctx_pool_stat(IoCtx) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Attempt to change an io context's associated auid "owner."
%%
%% Requires that you have write permission on both the current and new
%% uid.
%%
%% @param IoCtx   reference to the pool to change.
%% @param Uid     the uid you wish the io to have.
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
ioctx_pool_set_auid(IoCtx, Uid) when is_integer(IoCtx), is_integer(Uid) ->
    "RADOS NIF library not loaded".

%%
%% Get the uid of a pool
%%
%% @param IoCtx   pool to query
%%
%% @returns       {ok, Uid} of the owner, or {error, Reason} on failure
%% 
ioctx_pool_get_auid(IoCtx) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Get the pool id of the io context
%%
%% @param IoCtx   the io context to query
%%
%% @returns       {ok, Id}, the id of the pool the io context uses, or
%%                {error, Reason} on failure.
%%
ioctx_get_id(IoCtx) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Get the pool name of the io context
%%
%% @param IoCtx   the io context to query
%% 
%% @returns       {ok, PoolName}, or {error, Reason} on failure
%%
ioctx_get_pool_name(IoCtx) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Create a pool-wide snapshot
%%
%% @param IoCtx    the pool to snapshot
%% @param SnapName the name of the snapshot
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
ioctx_snap_create(IoCtx, SnapName) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Delete a pool snapshot
%%
%% @param IoCtx    the pool to delete the snapshot from
%% @param SnapName which snapshot to delete
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
ioctx_snap_remove(IoCtx, SnapName) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Rollback an object to a pool snapshot
%%
%% The contents of the object will be the same as when the snapshot was taken.
%%
%% @param IoCtx    the pool in which the object is stored
%% @param Oid      the name of the object to rollback
%% @param SnapName which snapshot to rollback to
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
rollback(IoCtx, Oid, SnapName) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% List all the ids of pool snapshots.
%%
%% @param IoCtx    the pool to read from
%% 
%% @returns        {ok, [SnapId|SnapId|...]} on success,
%%                 the list might be empty if the pool has no snapshot
%%                 {error, Reason} on failure.
%%
ioctx_snap_list(IoCtx) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

proc_snap_list(IoCtx, []) ->
    [];
proc_snap_list(IoCtx, L) ->
    [H|T] = L,
    {ok, Name} = ioctx_snap_get_name(IoCtx, H),
    [[{id, H},{name, Name}] | proc_snap_list(IoCtx, T)].

%%
%% List all the ids and names of pool snapshots.
%%
%% @param IoCtx    the pool to read from
%% 
%% @returns        {ok, [[{id, SnapId},{name, SnapName}]|[{id, SnapId},{name, SnapName}]|...]} on success,
%%                 the list might be empty if the pool has no snapshot
%%                 {error, Reason} on failure.
%%
ioctx_snap_list_with_name(IoCtx) when is_integer(IoCtx) ->
    {ok, Ids} = ioctx_snap_list(IoCtx),
    {ok, proc_snap_list(IoCtx, Ids)}.

%%
%% Get the id of a pool snapshot.
%%
%% @param IoCtx    the pool to read from
%% @param SnapName the snapshot to find
%%
%% @returns        {ok, SnapId} on success, {error, Reason} on failure.
%%
ioctx_snap_lookup(IoCtx, SnapName) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Get the name of a pool snapshot.
%%
%% @param IoCtx    the pool to read from
%% @param SnapId   the snapshot to find
%%
%% @returns        {ok, SnapName} on success, {error, Reason} on failure.
%%
ioctx_snap_get_name(IoCtx, SnapId) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Find when a pool snapshot occurred.
%% 
%% @param IoCtx     the pool the snapshot was taken in
%% @param SnapId    the snapshot to lookup
%%
%% @returns         {ok, TimeStamp} on success, {error, Reason} on failure.
%%
ioctx_snap_get_stamp(IoCtx, SnapId) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Write data to an object.
%%
%% @param IoCtx     the io context in which the write will occur
%% @param Oid       name of the object
%% @param Data      data to write, in binary format
%% @param Offset    byte offset in the object to begin writing at
%%
%% @returns         {ok, Num} number of bytes written on success, {error, Reason} on error.
write(IoCtx, Oid, Data, Offset) when is_integer(IoCtx), is_binary(Data), is_integer(Offset) ->
    "RADOS NIF library not loaded".

%%
%% Write an entire object.
%% 
%% The object is filled with the provided data. If the object exists, it is 
%% atomically truncated and then written.
%%
%% @param IoCtx     the io context in which the write will occur
%% @param Oid       name of the object
%% @param Data      data to write, in binary format
%%
write_full(IoCtx, Oid, Data) when is_integer(IoCtx), is_binary(Data) ->
    "RADOS NIF library not loaded".

%%
%% Append data to an object.
%%
%% @param IoCtx     the io context in which the write will occur
%% @param Oid       name of the object
%% @param Data      data to append, in binary format
%%
%% @returns         {ok, Num} number of bytes written on success, {error, Reason} on failure
%%
append(IoCtx, Oid, Data) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Read data from an object. The io context determines the snapshot to read from, 
%% if any was set by 
%%
%% @param IoCtx    the context in which to perform the read
%% @param Oid      the name of the object to read from
%% @param Len      the number of bytes to read
%% @param Offset   the offset to start reading from in the object
%%
%% @returns        {ok, Data} on success and Data in binary format, eof, 
%%                 or {error, Reason} on failure.
%%                 Note the size of returned data may be smaller than Len
%%                 if there are less data than Len in the object.
%%
read(IoCtx, Oid, Len, Offset) when is_integer(IoCtx), is_integer(Len), is_integer(Offset) ->
    "RADOS NIF library not loaded".

%%
%% Delete an object.
%%
%% @param IoCtx    the pool to delete the object from
%% @param Oid      the name of the object to delete
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
remove(IoCtx, Oid) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Resize an object.
%%
%% If this enlarges the object, the new area is logically filled with zeroes. If this shrinks the object, 
%% the excess data is removed.
%%
%% @param IoCtx    the context in which to truncate
%% @param Oid      the name of the object to truncate
%% @param Size     the new size of the object in bytes
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
trunc(IoCtx, Oid, Size) when is_integer(IoCtx), is_integer(Size) ->
    "RADOS NIF library not loaded".

%%
%% Get object stats (size/mtime)
%%
%%  size   object size
%%  mtime  modification time
%%
%% @param IoCtx    the pool io context
%% @param Oid      the name of the object to get stats for
%%
%% @returns        {ok, [{size, Value}|{mtime, Value}]}
%%                 {error, Reason} on failure.
%%
stat(IoCtx, Oid) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Start listing objects in a pool.
%%
%% @param IoCtx    the pool io context
%%
%% @returns        {ok, ListCtx}, {error, Reason} on failure.
%%                 ListCtx is the list context
%%
objects_list_open(IoCtx) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Get the next object name and locator in the pool.
%% 
%% @param ListCtx    iterator marking where you are in the listing
%%
%% @returns          {ok, [{entry, Value}|{key, Value}]} if object locator key is available,
%%                   {ok, [{entry, Value}]} if object locator key is not available,
%%                   'end' when there is no more, {error, Reason} on failure.
%%
%%                   Entry   the name of the entry
%%                   Key     the object locator
%%
objects_list_next(ListCtx) when is_integer(ListCtx) ->
    "RADOS NIF library not loaded".

%%
%% Close the object listing handle.
%%
%% This should be called when the handle is no longer needed. The handle should not be used after it 
%% has been closed.
%%
%% @param ListCtx    List handle to close
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
objects_list_close(ListCtx) when is_integer(ListCtx) ->
    "RADOS NIF library not loaded".

%%
%% Get the value of an extended attribute on an object.
%%
%% @param IoCtx       the context in which the attribute is read
%% @param Oid         name of the object
%% @param XAttrName   which extended attribute to read
%%
%% @returns           {ok, XAttrValue}, {error, Reason} on failure.
%%
getxattr(IoCtx, Oid, XAttrName) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Set an extended attribute on an object.
%%
%% @param IoCtx       the context in which xattr is set
%% @param Oid         name of the object
%% @param XAttrName   which extended attribute to set
%% @param XAttrVal    extended attribute value to set
%%
%% @returns           'ok' on success, {error, Reason} on failure.
%%
setxattr(IoCtx, Oid, XAttrName, XAttrVal) when is_integer(IoCtx), is_binary(XAttrVal) ->
    "RADOS NIF library not loaded".

%%
%% Delete an extended attribute from an object.
%%
%% @param IoCtx       the context in which to delete the xattr
%% @param Oid         name of the object
%% @param XAttrName   which extended attribute to delete
%%
%% @returns           'ok' on success, {error, Reason} on failure.
%%
rmxattr(IoCtx, Oid, XAttrName) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".


%%
%% Start iterating over xattrs on an object.
%%
%% @param IoCtx       the context in which to list xattrs
%% @param Oid         name of the object
%%
%% @returns           {ok, Iterator}, {error, Reason} on failure.
getxattrs(IoCtx, Oid) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%%
%% Get the next xattr on the object.
%%
%% @param Iterator    iterator to advance
%%
%% @returns           {ok, [{xattr, XAttr}|{value, Value}]}, 
%%                    'end' if the end of the list has been reached,
%%                    {error, Reason} on failure.
%%
getxattrs_next(Iterator) when is_integer(Iterator) ->
    "RADOS NIF library not loaded".

%%
%% Close the xattr iterator.
%%
%% @param Iterator    iterator to close
%%
%% @returns           'ok' on success, {error, Reason} on failure.
%%
getxattrs_end(Iterator) when is_integer(Iterator) ->
    "RADOS NIF library not loaded".

%%
%% Block until all pending writes in an io context are safe.
%% 
%% This is not equivalent to calling rados_aio_wait_for_safe()on all write completions, 
%% since this waits for the associated callbacks to complete as well.
%%
%% @param IoCtx    the context to flush
%%
%% @returns       'ok' on success, {error, Reason} on failure.
%%
aio_flush(IoCtx) when is_integer(IoCtx) ->
    "RADOS NIF library not loaded".

%============================================================================
% Internal functions
%============================================================================

file_type(File) ->
    case file:read_file_info(File) of
        {ok, Facts} ->
            case element(3, Facts) of
                regular ->
                    regular;
                directory ->
                    directory;
                _ -> error
            end;
        _ ->
            error
    end.
