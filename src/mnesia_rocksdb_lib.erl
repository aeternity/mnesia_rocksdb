%%% @doc RocksDB update  wrappers, in separate module for easy tracing and mocking.
%%%
-module(mnesia_rocksdb_lib).

-export([ put/4
        , write/3
        , delete/3
        ]).

-export([ open_rocksdb/3
        , data_mountpoint/1
        , create_mountpoint/1
        , tabname/1
        ]).

-export([ keypos/1
        , encode_key/1
        , decode_key/1
        , encode_val/1
        , decode_val/1 ]).

-include("mnesia_rocksdb.hrl").

put(#{db := Ref, cf := CF}, K, V, Opts) ->
    rocksdb:put(Ref, CF, K, V, Opts);
put(Ref, K, V, Opts) ->
    rocksdb:put(Ref, K, V, Opts).

write(#{db := Ref, cf := CF}, L, Opts) ->
    write_as_batch(L, Ref, CF, Opts);    
write(Ref, L, Opts) ->
    rocksdb:write(Ref, L, Opts).

delete(Ref, K, Opts) ->
    rocksdb:delete(Ref, K, Opts).


write_as_batch(L, Ref, CF, Opts) ->
    {ok, Batch} = rocksdb:batch(),
    lists:foreach(
      fun({put, K, V}) ->
              ok = rocksdb:batch_put(Batch, CF, K, V);
         ({delete, K}) ->
              ok = rocksdb:batch_delete(Batch, CF, K)
      end, L),
    rocksdb:write_batch(Ref, Batch, Opts).




create_mountpoint(Tab) ->
    MPd = data_mountpoint(Tab),
    case filelib:is_dir(MPd) of
        false ->
            file:make_dir(MPd),
            ok;
        true ->
            Dir = mnesia_lib:dir(),
            case lists:prefix(Dir, MPd) of
                true ->
                    ok;
                false ->
                    {error, exists}
            end
    end.

data_mountpoint(Tab) ->
    Dir = mnesia_monitor:get_env(dir),
    filename:join(Dir, tabname(Tab) ++ ".extrdb").

tabname({admin, Alias}) ->
    "mnesia_rocksdb-" ++ atom_to_list(Alias) ++ "-_db";
tabname({Tab, index, {{Pos},_}}) ->
    atom_to_list(Tab) ++ "-=" ++ atom_to_list(Pos) ++ "=-_ix";
tabname({Tab, index, {Pos,_}}) ->
    atom_to_list(Tab) ++ "-" ++ integer_to_list(Pos) ++ "-_ix";
tabname({Tab, retainer, Name}) ->
    atom_to_list(Tab) ++ "-" ++ retainername(Name) ++ "-_RET";
tabname(Tab) when is_atom(Tab) ->
    atom_to_list(Tab) ++ "-_tab".

-spec encode_key(any()) -> binary().
encode_key(Key) ->
    sext:encode(Key).

-spec decode_key(binary()) -> any().
decode_key(CodedKey) ->
    case sext:partial_decode(CodedKey) of
        {full, Result, _} ->
            Result;
        _ ->
            error(badarg, CodedKey)
    end.

-spec encode_val(any()) -> binary().
encode_val(Val) ->
    term_to_binary(Val).

-spec decode_val(binary()) -> any().
decode_val(CodedVal) ->
    binary_to_term(CodedVal).

keypos({_, index, _}) ->
    1;
keypos({_, retainer, _}) ->
    2;
keypos(Tab) when is_atom(Tab) ->
    2.

%% ======================================================================
%% Private functions
%% ======================================================================

retainername(Name) when is_atom(Name) ->
    atom_to_list(Name);
retainername(Name) when is_list(Name) ->
    try binary_to_list(list_to_binary(Name))
    catch
        error:_ ->
            lists:flatten(io_lib:write(Name))
    end;
retainername(Name) ->
    lists:flatten(io_lib:write(Name)).

related_resources(Tab) ->
    TabS = atom_to_list(Tab),
    Dir = mnesia_monitor:get_env(dir),
    case file:list_dir(Dir) of
        {ok, Files} ->
            lists:flatmap(
              fun(F) ->
                      Full = filename:join(Dir, F),
                      case is_index_dir(F, TabS) of
                          false ->
                              case is_retainer_dir(F, TabS) of
                                  false ->
                                      [];
                                  {true, Name} ->
                                      [{{Tab, retainer, Name}, Full}]
                              end;
                          {true, Pos} ->
                              [{{Tab, index, {Pos,ordered}}, Full}]
                      end
              end, Files);
        _ ->
            []
    end.

is_index_dir(F, TabS) ->
    case re:run(F, TabS ++ "-([0-9]+)-_ix.extrdb", [{capture, [1], list}]) of
        nomatch ->
            false;
        {match, [P]} ->
            {true, list_to_integer(P)}
    end.

is_retainer_dir(F, TabS) ->
    case re:run(F, TabS ++ "-(.+)-_RET", [{capture, [1], list}]) of
        nomatch ->
            false;
        {match, [Name]} ->
            {true, Name}
    end.

open_rocksdb(MPd, RdbOpts, CFs) ->
    open_rocksdb(MPd, rocksdb_open_opts_(RdbOpts), CFs, get_retries()).

%% Code adapted from basho/riak_kv_eleveldb_backend.erl
open_rocksdb(MPd, Opts, CFs, Retries) ->
    open_db(MPd, Opts, CFs, max(1, Retries), undefined).

open_db(_, _, _, 0, LastError) ->
    {error, LastError};
open_db(MPd, Opts, CFs, RetriesLeft, _) ->
    case rocksdb:open(MPd, Opts, CFs) of
        {ok, _Ref, _CFRefs} = Ok ->
            ?dbg("~p: Open - Rocksdb: ~s~n  -> Ok~n",
                 [self(), MPd, Ok]),
            Ok;
        %% Check specifically for lock error, this can be caused if
        %% a crashed mnesia takes some time to flush rocksdb information
        %% out to disk. The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr}=Reason} ->
            case lists:prefix("IO error: lock ", OpenErr) of
                true ->
                    SleepFor = get_retry_delay(),
                    ?dbg("~p: Open - Rocksdb backend retrying ~p in ~p ms"
                         " after error ~s\n",
                         [self(), MPd, SleepFor, OpenErr]),
                    timer:sleep(SleepFor),
                    open_db(MPd, Opts, CFs, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

get_retries() -> 30.
get_retry_delay() -> 10000.

rocksdb_open_opts_(RdbOpts) ->
    lists:foldl(
      fun({K,_} = Item, Acc) ->
              lists:keystore(K, 1, Acc, Item)
      end, default_open_opts(), RdbOpts).

default_open_opts() ->
    [ {create_if_missing, true}
      , {cache_size,
         list_to_integer(get_env_default("ROCKSDB_CACHE_SIZE", "32212254"))}
      , {block_size, 1024}
      , {max_open_files, 100}
      , {write_buffer_size,
         list_to_integer(get_env_default(
                           "ROCKSDB_WRITE_BUFFER_SIZE", "4194304"))}
      , {compression,
         list_to_atom(get_env_default("ROCKSDB_COMPRESSION", "true"))}
      , {use_bloomfilter, true}
    ].

get_env_default(Key, Default) ->
    case os:getenv(Key) of
        false ->
            Default;
        Value ->
            Value
    end.
