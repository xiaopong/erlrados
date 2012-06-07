%%
%% Copyright (C) 2012, xp@renzhi.ca
%% All rights reserved.
%%

-module(test).

%% -export([
%%          load_rados/0,
%%          create_and_connect_cluster/0,
%%          get_cluster_instance_id/1,
%%          lookup_pool/2,
%%          create_pool/2,
%%          delete_pool/2,
%%          create_ioctx/2,
%%          destroy_ioctx/1,
%%          get_ioctx_id/1,
%%          get_ioctx_pool_name/1,
%%          test_read_write/4
%%         ]).

-compile([export_all]).


load_rados() ->
    rados:load("../").

create_and_connect_cluster() ->
    {ok, Cluster} = rados:create(),
    rados:conf_read_file(Cluster, "./etc/ceph.conf"),
    rados:connect(Cluster),
    Cluster.

get_cluster_instance_id(Cluster) ->
    {ok, Id} = rados:get_instance_id(Cluster),
    Id.

list_pool(Cluster) ->
    {ok, List} = rados:pool_list(Cluster),
    List.

cluster_stat(Cluster) ->
    {ok, List} = rados:cluster_stat(Cluster),
    List.

lookup_pool(Cluster, Pool) ->
    {ok, Id} = rados:pool_lookup(Cluster, Pool),
    Id.

create_pool(Cluster, Pool) ->
    rados:pool_create(Cluster, Pool).

delete_pool(Cluster, Pool) ->
    rados:pool_delete(Cluster, Pool).

create_ioctx(Cluster, Pool) ->
    {ok, Ctx} = rados:ioctx_create(Cluster, Pool),
    Ctx.

destroy_ioctx(Ctx) ->
    rados:ioctx_destroy(Ctx).

pool_stat(Ctx) ->
    {ok, List} = rados:ioctx_pool_stat(Ctx),
    List.

get_ioctx_id(Ctx) ->
    {ok, Id} = rados:ioctx_get_id(Ctx),
    Id.

get_ioctx_pool_name(Ctx) ->
    {ok, Pool} = rados:ioctx_get_pool_name(Ctx),
    Pool.

open_file_for_read(Filename) ->
    {ok, Fd} = file:open(Filename, [read, raw, binary]),
    Fd.

open_file_for_write(Filename) ->
    {ok, Fd} = file:open(Filename, [write, raw, binary]),
    Fd.

write_object(Ctx, Fd, Oid, Offset) ->
    BlockSize = 4096,
    case file:read(Fd, BlockSize) of
        {ok, Data} ->
            ReadLen = size(Data),
            {ok, WriteLen} = rados:write(Ctx, Oid, Data, Offset),
            if
                ReadLen /= WriteLen ->
                    io:format("write_object() - Wrong! ReadLen=~p, WriteLen=~p~n", [ReadLen, WriteLen]);
                true ->
                    io:format("write_object() - Right! ReadLen=~p, WriteLen=~p~n", [ReadLen, WriteLen])
            end,
            write_file_to_rados(Ctx, Fd, Oid, Offset + ReadLen);
        eof ->
            ok;
        {error, Reason} ->
            io:format("write_object() - Error reading from file: ~p~n", [Reason]),
            {error, Reason}
    end.
    
read_object(Ctx, Fd, Oid, Offset) ->
    BlockSize = 4096,
    case rados:read(Ctx, Oid, BlockSize, Offset) of
        {ok, Data} ->
            ReadLen = size(Data),
            case file:write(Fd, Data) of
                ok ->
                    write_file_from_rados(Ctx, Fd, Oid, Offset + ReadLen);
                {error, Reason} ->
                    io:format("read_object() - Error writing to file: ~p~n", [Reason]),
                    {error, Reason}
            end;
        eof ->
            ok;
        {error, Reason} ->
            io:format("write_file_from_rados() - Error reading from rados: ~p~n", [Reason]),
            {error, Reason}
    end.
    

create_snap(Ctx, Snap) ->
    rados:ioctx_snap_create(Ctx, Snap).

remove_snap(Ctx, Snap) ->
    rados:ioctx_snap_remove(Ctx, Snap).

rollback_to_snap(Ctx, Oid, Snap) ->
    rados:rollback(Ctx, Oid, Snap).

remove_object(Ctx, Oid) ->
    rados:remove(Ctx, Oid).

write_file_to_rados(IoCtx, Fd, Oid, Offset) ->
    BlockSize = 64000,
    case file:read(Fd, BlockSize) of
        {ok, Data} ->
            ReadLen = size(Data),
            {ok, WriteLen} = rados:write(IoCtx, Oid, Data, Offset),
            if
                ReadLen /= WriteLen ->
                    io:format("write_file_to_rados() - Wrong! ReadLen=~p, WriteLen=~p~n", [ReadLen, WriteLen]);
                true ->
                    io:format("write_file_to_rados() - Right! ReadLen=~p, WriteLen=~p~n", [ReadLen, WriteLen])
            end,
            write_file_to_rados(IoCtx, Fd, Oid, Offset + ReadLen);
        eof ->
            ok;
        {error, Reason} ->
            io:format("write_file_to_rados() - Error reading from file: ~p~n", [Reason]),
            {error, Reason}
    end.

write_file_from_rados(IoCtx, Fd, Oid, Offset) ->
    BlockSize = 4096,
    case rados:read(IoCtx, Oid, BlockSize, Offset) of
        {ok, Data} ->
            ReadLen = size(Data),
            case file:write(Fd, Data) of
                ok ->
                    write_file_from_rados(IoCtx, Fd, Oid, Offset + ReadLen);
                {error, Reason} ->
                    io:format("write_file_from_rados() - Error writing to file: ~p~n", [Reason]),
                    {error, Reason}
            end;
        eof ->
            ok;
        {error, Reason} ->
            io:format("write_file_from_rados() - Error reading from rados: ~p~n", [Reason]),
            {error, Reason}
    end.


test_read_write(Infile, Outfile, PoolName, Oid) ->
    {ok, Cluster} = rados:create(),
    rados:conf_read_file(Cluster, "./etc/ceph.conf"),
    rados:connect(Cluster),
    IoIn = case rados:ioctx_create(Cluster, PoolName) of
                {ok, Io1} ->
                    Io1;
                {error, Reason} ->
                    rados:shutdown(Cluster),
                    exit(Reason)
            end,
    {ok, Fdin} = file:open(Infile, [read, raw, binary]),
    case write_file_to_rados(IoIn, Fdin, Oid, 0) of
        {error, R} ->
            rados:ioctx_destroy(IoIn),
            rados:shutdown(Cluster),
            exit(R);
        ok ->
            io:format("write_file_to_rados() successfull~n")
    end,
    rados:ioctx_destroy(IoIn),
    {ok, FdOut} = file:open(Outfile, [write, raw, binary]),
    IoOut = case rados:ioctx_create(Cluster, PoolName) of
                {ok, Io2} ->
                    Io2;
                {error, R2} ->
                    rados:shutdown(Cluster),
                    exit(R2)
            end,
    case write_file_from_rados(IoOut, FdOut, Oid, 0) of
        {error, R3} ->
            rados:ioctx_destroy(IoOut),
            rados:shutdown(Cluster),
            exit(R3);
        ok ->
            io:format("write_file_from_rados() successful~n")
    end,
    rados:ioctx_destroy(IoOut),
    rados:shutdown(Cluster),
    io:format("Done.~n").


create_connect(0) ->
    [];
create_connect(Num) ->
    {ok, Cluster} = rados:create(),
    rados:conf_read_file(Cluster, "./etc/ceph.conf"),
    rados:connect(Cluster),
    [Cluster | create_connect(Num - 1)].

create_cluster_list(Num) ->
    statistics(runtime),
    statistics(wall_clock),
    L = create_connect(Num),
    {_, Time1} = statistics(runtime),
    {_, Time2} = statistics(wall_clock),
    U1 = Time1 * 1000,
    U2 = Time2 * 1000,
    io:format("Total time = ~p (~p) microseconds~n", [U1, U2]),
    L.

shutdown_cluster(C) ->
    rados:shutdown(C),
    io:format("Cluster : ~p~n", [C]).

shutdown_cluster_list(L) ->
    statistics(runtime),
    statistics(wall_clock),
    [shutdown_cluster(X) || X <- L],
    {_, Time1} = statistics(runtime),
    {_, Time2} = statistics(wall_clock),
    U1 = Time1 * 1000,
    U2 = Time2 * 1000,
    io:format("Total time = ~p (~p) microseconds~n", [U1, U2]),
    ok.


process_run(ParentId, Pool, Folder, Filename) -> 
    io:format("process_run() - parent=~p, pool=~p, folder=~p, file=~p~n",
             [ParentId, Pool, Folder, Filename]),
    Cluster = create_and_connect_cluster(),
    Io = create_ioctx(Cluster, Pool),
    Path = filename:join([Folder, Filename]),
    io:format("process_run() - path=~p~n", [Path]),
    {ok, Fdin} = file:open(Path, [read, raw, binary]),
    write_file_to_rados(Io, Fdin, Filename, 0),
    rados:ioctx_destroy(Io),
    rados:shutdown(Cluster),
    ParentId ! Filename ++ " is done".
    
run(ParentId, Pool, Folder, Filename) ->
    spawn(?MODULE, process_run, [ParentId, Pool, Folder, Filename]),
    ok.

wait_for_results(0) ->
    ok;
wait_for_results(Count) ->
    receive
        Msg ->
            io:format("~p~n", [Msg]),
            wait_for_results(Count - 1)
    end.

test_load_files(Folder, Pool) ->
    Cluster = create_and_connect_cluster(),
    create_pool(Cluster, Pool),
    rados:shutdown(Cluster),
    {ok, Filenames} = file:list_dir(Folder),
    Count = lists:foldl(fun(X, Count) -> Count + 1 end, 0, Filenames),
    Pid = self(),
    [run(Pid, Pool, Folder, X) || X <- Filenames],
    wait_for_results(Count),
    ok.
        
