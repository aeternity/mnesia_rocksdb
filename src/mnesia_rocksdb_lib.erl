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
        , encode_key/1, encode_key/2
        , decode_key/1, decode_key/2
        , encode_val/1, encode_val/2
        , decode_val/1, decode_val/3
        ]).

-include("mnesia_rocksdb.hrl").
-include("mnesia_rocksdb_int.hrl").

put(#{db := Ref, cf := CF}, K, V, Opts) ->
    rocksdb:put(Ref, CF, K, V, Opts);
put(Ref, K, V, Opts) ->
    rocksdb:put(Ref, K, V, Opts).

write(#{db := Ref, cf := CF}, L, Opts) ->
    write_as_batch(L, Ref, CF, Opts).

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
    encode_(Key, sext).

encode_(Value, sext) ->
    sext:encode(Value);
encode_(Value, raw) when is_binary(Value) ->
    Value;
encode_(Value, term) ->
    term_to_binary(Value).


encode_key(Key, #{encoding := {Enc,_}}) ->
    encode_(Key, Enc);
encode_key(Key, _) ->
    encode_(Key, sext).

-spec decode_key(binary()) -> any().
decode_key(CodedKey) ->
    decode_(CodedKey, sext).

decode_key(CodedKey, #{encoding := {Enc, _}}) ->
    decode_(CodedKey, Enc);
decode_key(CodedKey, Enc) ->
    decode_(CodedKey, Enc).

decode_(Val, sext) ->
    case sext:partial_decode(Val) of
        {full, Result, _} ->
            Result;
        _ ->
            error(badarg, Val)
    end;
decode_(Val, raw) ->
    Val;
decode_(Val, term) ->
    binary_to_term(Val).

-spec encode_val(any()) -> binary().
encode_val(Val) ->
    encode_(Val, term).

encode_val(_, #{name := {_,index,_}}) ->
    <<>>;
encode_val(Val, #{encoding := {_, Enc0}, attr_pos := AP}) ->
    {Type, Enc} = enc_type(Enc0),
    case {map_size(AP), Type} of
        {2, value} ->
            encode_(element(3, Val), Enc);
        {_, object} ->
            encode_(setelement(2, Val, []), Enc)
    end.

enc_type({T, _} = E) when T==value; T==object ->
    E;
enc_type(E) when is_atom(E) ->
    {object, E}.

-spec decode_val(binary()) -> any().
decode_val(CodedVal) ->
    binary_to_term(CodedVal).

decode_val(<<>>, K, #{name := {_,index,_}}) ->
    {K};
decode_val(CodedVal, Key, Ref) ->
    {Type, Enc} = value_encoding(Ref),
    case Type of
        object ->
            setelement(2, decode_(CodedVal, Enc), Key);
        value ->
            make_rec(Key, decode_(CodedVal, Enc), Ref)
    end.

make_rec(Key, _Val, #{name := {_, index, {_,ordered}}}) ->
    {Key};
make_rec(Key, Val, #{properties := #{record_name := Tag}}) ->
    {Tag, Key, Val}.

value_encoding(#{encoding := {_, Enc}}) ->
    enc_type(Enc);
value_encoding(#{}) ->
    {object, term};
value_encoding({Type, Enc} = E) when is_atom(Type), is_atom(Enc) ->
    E.


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
