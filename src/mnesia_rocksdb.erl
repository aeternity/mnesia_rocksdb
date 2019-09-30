%%----------------------------------------------------------------
%% Copyright (c) 2013-2016 Klarna AB
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%----------------------------------------------------------------

%% @doc rocksdb storage backend for Mnesia.

%% Initialization: register() or register(Alias)
%% Usage: mnesia:create_table(Tab, [{rocksdb_copies, Nodes}, ...]).

-module(mnesia_rocksdb).


%% ----------------------------------------------------------------------------
%% BEHAVIOURS
%% ----------------------------------------------------------------------------

-behaviour(mnesia_backend_type).
-behaviour(gen_server).


%% ----------------------------------------------------------------------------
%% EXPORTS
%% ----------------------------------------------------------------------------

%%
%% CONVENIENCE API
%%

-export([register/0,
         register/1,
         default_alias/0]).

%%
%% DEBUG API
%%

-export([show_table/1,
         show_table/2,
         show_table/3,
         fold/6]).

%%
%% BACKEND CALLBACKS
%%

%% backend management
-export([init_backend/0,
         add_aliases/1,
         remove_aliases/1]).

%% schema level callbacks
-export([semantics/2,
	 check_definition/4,
	 create_table/3,
	 load_table/4,
	 close_table/2,
	 sync_close_table/2,
	 delete_table/2,
	 info/3]).

%% table synch calls
-export([sender_init/4,
         sender_handle_info/5,
         receiver_first_message/4,
         receive_data/5,
         receive_done/4]).

%% low-level accessor callbacks.
-export([delete/3,
         first/2,
         fixtable/3,
         insert/3,
         last/2,
         lookup/3,
         match_delete/3,
         next/3,
         prev/3,
         repair_continuation/2,
         select/1,
         select/3,
         select/4,
         slot/3,
         update_counter/4]).

%% Index consistency
-export([index_is_consistent/3,
         is_index_consistent/2]).

%% record and key validation
-export([validate_key/6,
         validate_record/6]).

%% file extension callbacks
-export([real_suffixes/0,
         tmp_suffixes/0]).

%%
%% GEN SERVER CALLBACKS AND CALLS
%%

-export([start_proc/4,
         init/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         terminate/2,
         code_change/3]).

-export([ix_prefixes/3]).


%% ----------------------------------------------------------------------------
%% DEFINES
%% ----------------------------------------------------------------------------

%% Name of the Rocksdb interface module; defaults to rocksdb but can be
%% configured by passing -DROCKSDB_MODULE=<name_of_module> to erlc.
-ifdef(ROCKSDB_MODULE).
-define(rocksdb, ?ROCKSDB_MODULE).
-else.
-define(rocksdb, rocksdb). %% Name of the Rocksdb interface module
-endif.

%% Data and meta data (a.k.a. info) are stored in the same table.
%% This is a table of the first byte in data
%% 0    = before meta data
%% 1    = meta data
%% 2    = before data
%% >= 8 = data

-define(INFO_START, 0).
-define(INFO_TAG, 1).
-define(DATA_START, 2).
-define(BAG_CNT, 32).   % Number of bits used for bag object counter
-define(MAX_BAG, 16#FFFFFFFF).

%% enable debugging messages through mnesia:set_debug_level(debug)
-ifndef(MNESIA_ROCKSDB_NO_DBG).
-define(dbg(Fmt, Args),
        %% avoid evaluating Args if the message will be dropped anyway
        case mnesia_monitor:get_env(debug) of
            none -> ok;
            verbose -> ok;
            _ -> mnesia_lib:dbg_out("~p:~p: "++(Fmt),[?MODULE,?LINE|Args])
        end).
-else.
-define(dbg(Fmt, Args), ok).
-endif.

%% ----------------------------------------------------------------------------
%% RECORDS
%% ----------------------------------------------------------------------------

-record(sel, { alias                            % TODO: not used
             , tab
             , ref
             , keypat
             , ms                               % TODO: not used
             , compiled_ms
             , limit
             , key_only = false                 % TODO: not used
             , direction = forward}).           % TODO: not used

-type on_write_error() :: debug | verbose | warning | error | fatal.
-define(WRITE_ERR_DEFAULT, verbose).

-record(st, { ets
            , ref
            , alias
            , tab
            , type
            , size_warnings			% integer()
            , maintain_size			% boolean()
            , on_write_error = ?WRITE_ERR_DEFAULT :: on_write_error()
            }).

%% ----------------------------------------------------------------------------
%% CONVENIENCE API
%% ----------------------------------------------------------------------------

register() ->
    register(default_alias()).

register(Alias) ->
    Module = ?MODULE,
    case mnesia:add_backend_type(Alias, Module) of
        {atomic, ok} ->
            {ok, Alias};
        {aborted, {backend_type_already_exists, _}} ->
            {ok, Alias};
        {aborted, Reason} ->
            {error, Reason}
    end.

default_alias() ->
    rocksdb_copies.


%% ----------------------------------------------------------------------------
%% DEBUG API
%% ----------------------------------------------------------------------------

%% A debug function that shows the rocksdb table content
show_table(Tab) ->
    show_table(default_alias(), Tab).

show_table(Alias, Tab) ->
    show_table(Alias, Tab, 100).

show_table(Alias, Tab, Limit) ->
    {Ref, _Type} = get_ref(Alias, Tab),
    with_iterator(Ref, fun(I) -> i_show_table(I, first, Limit) end).

%% PRIVATE

i_show_table(_, _, 0) ->
    {error, skipped_some};
i_show_table(I, Move, Limit) ->
    case ?rocksdb:iterator_move(I, Move) of
        {ok, EncKey, EncVal} ->
            {Type,Val} =
                case EncKey of
                    << ?INFO_TAG, K/binary >> ->
                        {info,{decode_key(K),decode_val(EncVal)}};
                    _ ->
                        K = decode_key(EncKey),
                        V = decode_val(EncVal),
                        V2 = setelement(2,V,K),
                        {data,V2}
                end,
            io:fwrite("~p: ~p~n", [Type, Val]),
            i_show_table(I, next, Limit-1);
        _ ->
            ok
    end.


%% ----------------------------------------------------------------------------
%% BACKEND CALLBACKS
%% ----------------------------------------------------------------------------

%% backend management

init_backend() ->
    stick_rocksdb_dir(),
    application:ensure_all_started(mnesia_rocksdb),
    ok.

%% Prevent reloading of modules in rocksdb itself during runtime, since it
%% can lead to inconsistent state in rocksdb and silent data corruption.
stick_rocksdb_dir() ->
    case code:which(rocksdb) of
        BeamPath when is_list(BeamPath), BeamPath =/= "" ->
            Dir = filename:dirname(BeamPath),
            case code:stick_dir(Dir) of
                ok -> ok;
                error -> warn_stick_dir({error, Dir})
            end;
        Other ->
            warn_stick_dir({not_found, Other})
    end.

warn_stick_dir(Reason) ->
    mnesia_lib:warning("cannot make rocksdb directory sticky:~n~p~n",
                       [Reason]).

add_aliases(_Aliases) ->
    ok.

remove_aliases(_Aliases) ->
    ok.

%% schema level callbacks

%% This function is used to determine what the plugin supports
%% semantics(Alias, storage)   ->
%%    ram_copies | disc_copies | disc_only_copies  (mandatory)
%% semantics(Alias, types)     ->
%%    [bag | set | ordered_set]                    (mandatory)
%% semantics(Alias, index_fun) ->
%%    fun(Alias, Tab, Pos, Obj) -> [IxValue]       (optional)
%% semantics(Alias, _) ->
%%    undefined.
%%
semantics(_Alias, storage) -> disc_only_copies;
semantics(_Alias, types  ) -> [set, ordered_set, bag];
semantics(_Alias, index_types) -> [ordered];
semantics(_Alias, index_fun) -> fun index_f/4;
semantics(_Alias, _) -> undefined.

is_index_consistent(Alias, {Tab, index, PosInfo}) ->
    case info(Alias, Tab, {index_consistent, PosInfo}) of
        true -> true;
        _ -> false
    end.

index_is_consistent(Alias, {Tab, index, PosInfo}, Bool)
  when is_boolean(Bool) ->
    write_info(Alias, Tab, {index_consistent, PosInfo}, Bool).


%% PRIVATE FUN
index_f(_Alias, _Tab, Pos, Obj) ->
    [element(Pos, Obj)].

ix_prefixes(_Tab, _Pos, Obj) ->
    lists:foldl(
      fun(V, Acc) when is_list(V) ->
              try Pfxs = prefixes(list_to_binary(V)),
                   Pfxs ++ Acc
              catch
                  error:_ ->
                      Acc
              end;
         (V, Acc) when is_binary(V) ->
              Pfxs = prefixes(V),
              Pfxs ++ Acc;
         (_, Acc) ->
              Acc
      end, [], tl(tuple_to_list(Obj))).

prefixes(<<P:3/binary, _/binary>>) ->
    [P];
prefixes(_) ->
    [].

%% For now, only verify that the type is set or ordered_set.
%% set is OK as ordered_set is a kind of set.
check_definition(Alias, Tab, Nodes, Props) ->
    Id = {Alias, Nodes},
    try lists:map(
          fun({type, T} = P) ->
                  if T==set; T==ordered_set; T==bag ->
                          P;
                     true ->
                          mnesia:abort({combine_error,
                                        Tab,
                                        [Id, {type,T}]})
                  end;
             ({user_properties, UPs} = P) ->
                  RdbOpts = proplists:get_value(rocksdb_opts, UPs, []),
                  OWE = proplists:get_value(on_write_error, RdbOpts, ?WRITE_ERR_DEFAULT),
                  case valid_mnesia_op(OWE) of
                      true ->
                          P;
                      false ->
                          throw({error, {invalid, {on_write_error, OWE}}})
                  end;
             (P) -> P
          end, Props) of
        Props1 ->
            {ok, Props1}
    catch
        throw:Error ->
            Error
    end.

%% -> ok | {error, exists}
create_table(_Alias, Tab, _Props) ->
    create_mountpoint(Tab).

load_table(Alias, Tab, _LoadReason, Opts) ->
    Type = proplists:get_value(type, Opts),
    RdbUserProps = proplists:get_value(
                     rocksdb_opts, proplists:get_value(
                                     user_properties, Opts, []), []),
    StorageProps = proplists:get_value(
                     rocksdb, proplists:get_value(
                                storage_properties, Opts, []), RdbUserProps),
    RdbOpts = mnesia_rocksdb_params:lookup(Tab, StorageProps),
    ProcName = proc_name(Alias, Tab),
    case whereis(ProcName) of
        undefined ->
            load_table_(Alias, Tab, Type, RdbOpts);
        Pid ->
            gen_server:call(Pid, {load, Alias, Tab, Type, RdbOpts}, infinity)
    end.

load_table_(Alias, Tab, Type, RdbOpts) ->
    ShutdownTime = proplists:get_value(
                     owner_shutdown_time, RdbOpts, 120000),
    case mnesia_ext_sup:start_proc(
           Tab, ?MODULE, start_proc, [Alias,Tab,Type, RdbOpts],
           [{shutdown, ShutdownTime}]) of
        {ok, _Pid} ->
            ok;

        %% TODO: This reply is according to the manual, but we dont get it.
        {error, {already_started, _Pid}} ->
            %% TODO: Is it an error if the table already is
            %% loaded. This printout is triggered when running
            %% transform_table on a rocksdb_table that has indexing.
            ?dbg("ERR: table:~p already loaded pid:~p~n",
                 [Tab, _Pid]),
            ok;

        %% TODO: This reply is not according to the manual, but we get it.
        {error, {{already_started, _Pid}, _Stack}} ->
            %% TODO: Is it an error if the table already is
            %% loaded. This printout is triggered when running
            %% transform_table on a rocksdb_table that has indexing.
            ?dbg("ERR: table:~p already loaded pid:~p stack:~p~n",
                 [Tab, _Pid, _Stack]),
            ok;
        {error, Other} ->
            mnesia:abort(Other)
    end.

close_table(Alias, Tab) ->
    ?dbg("~p: close_table(~p, ~p);~n Trace: ~s~n",
         [self(), Alias, Tab, pp_stack()]),
    if is_atom(Tab) ->
            [close_table(Alias, R)
             || {R, _} <- related_resources(Tab)];
       true ->
            ok
    end,
    close_table_(Alias, Tab).

close_table_(Alias, Tab) ->
    case opt_call(Alias, Tab, close_table) of
        {error, noproc} ->
            ?dbg("~p: close_table_(~p) -> noproc~n",
                 [self(), Tab]),
            ok;
        {ok, _} ->
            ok;
        _Other ->
            ?dbg("~p: close_table_(~p) -> _Other = ~p~n",
                 [self(), Tab, _Other]),
            mnesia_ext_sup:stop_proc(Tab),
            ok
    end.

-ifndef(MNESIA_ROCKSDB_NO_DBG).
pp_stack() ->
    Trace = try throw(true)
            catch
                _:_ ->
                    case erlang:get_stacktrace() of
                        [_|T] -> T;
                        [] -> []
                    end
            end,
    pp_calls(10, Trace).

pp_calls(I, [{M,F,A,Pos} | T]) ->
    Spc = lists:duplicate(I, $\s),
    Pp = fun(Mx,Fx,Ax,Px) ->
                [atom_to_list(Mx),":",atom_to_list(Fx),"/",integer_to_list(Ax),
                 pp_pos(Px)]
        end,
    [Pp(M,F,A,Pos)|[["\n",Spc,Pp(M1,F1,A1,P1)] || {M1,F1,A1,P1} <- T]].

pp_pos([]) -> "";
pp_pos(L) when is_integer(L) ->
    [" (", integer_to_list(L), ")"];
pp_pos([{file,_},{line,L}]) ->
    [" (", integer_to_list(L), ")"].
-endif.

sync_close_table(Alias, Tab) ->
    ?dbg("~p: sync_close_table(~p, ~p);~n Trace: ~s~n",
         [self(), Alias, Tab, pp_stack()]),
    close_table(Alias, Tab).

delete_table(Alias, Tab) ->
    ?dbg("~p: delete_table(~p, ~p);~n Trace: ~s~n",
         [self(), Alias, Tab, pp_stack()]),
    delete_table(Alias, Tab, data_mountpoint(Tab)).

delete_table(Alias, Tab, MP) ->
    if is_atom(Tab) ->
            [delete_table(Alias, T, M) || {T,M} <- related_resources(Tab)];
       true ->
            ok
    end,
    case opt_call(Alias, Tab, delete_table) of
        {error, noproc} ->
            do_delete_table(Tab, MP);
        {ok, _} ->
            ok
    end.

do_delete_table(Tab, MP) ->
    assert_proper_mountpoint(Tab, MP),
    destroy_db(MP, []).


info(_Alias, Tab, memory) ->
    try ets:info(tab_name(icache, Tab), memory)
    catch
        error:_ ->
            0
    end;
info(Alias, Tab, size) ->
    case retrieve_size(Alias, Tab) of
	{ok, Size} ->
	    if Size < 10000 -> ok;
	       true -> size_warning(Alias, Tab)
	    end,
	    Size;
	Error ->
	    Error
    end;
info(_Alias, Tab, Item) ->
    case try_read_info(Tab, Item, undefined) of
        {ok, Value} ->
            Value;
        Error ->
            Error
    end.

retrieve_size(_Alias, Tab) ->
    case try_read_info(Tab, size, 0) of
	{ok, Size} ->
	    {ok, Size};
	Error ->
	    Error
    end.

try_read_info(Tab, Item, Default) ->
    try
        {ok, read_info(Item, Default, tab_name(icache, Tab))}
    catch
        error:Reason ->
            {error, Reason}
    end.

write_info(Alias, Tab, Key, Value) ->
    call(Alias, Tab, {write_info, Key, Value}).

%% table synch calls

%% ===========================================================
%% Table synch protocol
%% Callbacks are
%% Sender side:
%%  1. sender_init(Alias, Tab, RemoteStorage, ReceiverPid) ->
%%        {standard, InitFun, ChunkFun} | {InitFun, ChunkFun} when
%%        InitFun :: fun() -> {Recs, Cont} | '$end_of_table'
%%        ChunkFun :: fun(Cont) -> {Recs, Cont1} | '$end_of_table'
%%
%%       If {standard, I, C} is returned, the standard init message will be
%%       sent to the receiver. Matching on RemoteStorage can reveal if a
%%       different protocol can be used.
%%
%%  2. InitFun() is called
%%  3a. ChunkFun(Cont) is called repeatedly until done
%%  3b. sender_handle_info(Msg, Alias, Tab, ReceiverPid, Cont) ->
%%        {ChunkFun, NewCont}
%%
%% Receiver side:
%% 1. receiver_first_message(SenderPid, Msg, Alias, Tab) ->
%%        {Size::integer(), State}
%% 2. receive_data(Data, Alias, Tab, _Sender, State) ->
%%        {more, NewState} | {{more, Msg}, NewState}
%% 3. receive_done(_Alias, _Tab, _Sender, _State) ->
%%        ok
%%
%% The receiver can communicate with the Sender by returning
%% {{more, Msg}, St} from receive_data/4. The sender will be called through
%% sender_handle_info(Msg, ...), where it can adjust its ChunkFun and
%% Continuation. Note that the message from the receiver is sent once the
%% receive_data/4 function returns. This is slightly different from the
%% normal mnesia table synch, where the receiver acks immediately upon
%% reception of a new chunk, then processes the data.
%%

sender_init(Alias, Tab, _RemoteStorage, _Pid) ->
    %% Need to send a message to the receiver. It will be handled in
    %% receiver_first_message/4 below. There could be a volley of messages...
    {standard,
     fun() ->
             select(Alias, Tab, [{'_',[],['$_']}], 100)
     end,
     chunk_fun()}.

sender_handle_info(_Msg, _Alias, _Tab, _ReceiverPid, Cont) ->
    %% ignore - we don't expect any message from the receiver
    {chunk_fun(), Cont}.

receiver_first_message(_Pid, {first, Size} = _Msg, _Alias, _Tab) ->
    {Size, _State = []}.

receive_data(Data, Alias, Tab, _Sender, State) ->
    [insert(Alias, Tab, Obj) || Obj <- Data],
    {more, State}.

receive_done(_Alias, _Tab, _Sender, _State) ->
    ok.

%% End of table synch protocol
%% ===========================================================

%% PRIVATE

chunk_fun() ->
    fun(Cont) ->
            select(Cont)
    end.

%% low-level accessor callbacks.

delete(Alias, Tab, Key) ->
    opt_call(Alias, Tab, {delete, encode_key(Key)}),
    ok.

first(Alias, Tab) ->
    {Ref, _Type} = get_ref(Alias, Tab),
    with_iterator(Ref, fun i_first/1).

%% PRIVATE ITERATOR
i_first(I) ->
    case ?rocksdb:iterator_move(I, <<?DATA_START>>) of
	{ok, First, _} ->
	    decode_key(First);
	_ ->
	    '$end_of_table'
    end.

%% Not relevant for an ordered_set
fixtable(_Alias, _Tab, _Bool) ->
    true.

%% To save storage space, we avoid storing the key twice. We replace the key
%% in the record with []. It has to be put back in lookup/3.
insert(Alias, Tab, Obj) ->
    Pos = keypos(Tab),
    EncKey = encode_key(element(Pos, Obj)),
    EncVal = encode_val(setelement(Pos, Obj, [])),
    call(Alias, Tab, {insert, EncKey, EncVal}).

last(Alias, Tab) ->
    {Ref, _Type} = get_ref(Alias, Tab),
    with_iterator(Ref, fun i_last/1).

%% PRIVATE ITERATOR
i_last(I) ->
    case ?rocksdb:iterator_move(I, last) of
	{ok, << ?INFO_TAG, _/binary >>, _} ->
	    '$end_of_table';
	{ok, Last, _} ->
	    decode_key(Last);
	_ ->
	    '$end_of_table'
    end.

%% Since we replace the key with [] in the record, we have to put it back
%% into the found record.
lookup(Alias, Tab, Key) ->
    Enc = encode_key(Key),
    {Ref, Type} = call(Alias, Tab, get_ref),
    case Type of
	bag -> lookup_bag(Ref, Key, Enc, keypos(Tab));
	_ ->
	    case ?rocksdb:get(Ref, Enc, []) of
		{ok, EncVal} ->
		    [setelement(keypos(Tab), decode_val(EncVal), Key)];
		_ ->
		    []
	    end
    end.

lookup_bag(Ref, K, Enc, KP) ->
    Sz = byte_size(Enc),
    with_iterator(
      Ref, fun(I) ->
		   lookup_bag_(Sz, Enc, ?rocksdb:iterator_move(I, Enc),
			       K, I, KP)
	   end).

lookup_bag_(Sz, Enc, {ok, Enc, _}, K, I, KP) ->
    lookup_bag_(Sz, Enc, ?rocksdb:iterator_move(I, next), K, I, KP);
lookup_bag_(Sz, Enc, Res, K, I, KP) ->
    case Res of
	{ok, <<Enc:Sz/binary, _:?BAG_CNT>>, V} ->
	    [setelement(KP, decode_val(V), K)|
	     lookup_bag_(Sz, Enc, ?rocksdb:iterator_move(I, next), K, I, KP)];
	_ ->
	    []
    end.

match_delete(Alias, Tab, Pat) when is_atom(Pat) ->
    %do_match_delete(Alias, Tab, '_'),
    case is_wild(Pat) of
        true ->
            call(Alias, Tab, clear_table),
            ok;
        false ->
            %% can this happen??
            error(badarg)
    end;
match_delete(Alias, Tab, Pat) when is_tuple(Pat) ->
    KP = keypos(Tab),
    Key = element(KP, Pat),
    case is_wild(Key) of
        true ->
            call(Alias, Tab, clear_table);
        false ->
            call(Alias, Tab, {match_delete, Pat})
    end,
    ok.


next(Alias, Tab, Key) ->
    {Ref, _Type} = get_ref(Alias, Tab),
    EncKey = encode_key(Key),
    with_iterator(Ref, fun(I) -> i_next(I, EncKey, Key) end).

%% PRIVATE ITERATOR
i_next(I, EncKey, Key) ->
    case ?rocksdb:iterator_move(I, EncKey) of
        {ok, EncKey, _} ->
            i_next_loop(?rocksdb:iterator_move(I, next), I, Key);
        Other ->
            i_next_loop(Other, I, Key)
    end.

i_next_loop({ok, EncKey, _}, I, Key) ->
    case decode_key(EncKey) of
        Key ->
            i_next_loop(?rocksdb:iterator_move(I, next), I, Key);
        NextKey ->
            NextKey
    end;
i_next_loop(_, _I, _Key) ->
    '$end_of_table'.

prev(Alias, Tab, Key0) ->
    {Ref, _Type} = call(Alias, Tab, get_ref),
    Key = encode_key(Key0),
    with_iterator(Ref, fun(I) -> i_prev(I, Key) end).

%% PRIVATE ITERATOR
i_prev(I, Key) ->
    case ?rocksdb:iterator_move(I, Key) of
	{ok, _, _} ->
	    i_move_to_prev(I, Key);
	{error, invalid_iterator} ->
	    i_last(I)
    end.

%% PRIVATE ITERATOR
i_move_to_prev(I, Key) ->
    case ?rocksdb:iterator_move(I, prev) of
	{ok, << ?INFO_TAG, _/binary >>, _} ->
	    '$end_of_table';
	{ok, Prev, _} when Prev < Key ->
	    decode_key(Prev);
	{ok, _, _} ->
	    i_move_to_prev(I, Key);
	_ ->
	    '$end_of_table'
    end.

repair_continuation(Cont, _Ms) ->
    Cont.

select(Cont) ->
    %% Handle {ModOrAlias, Cont} wrappers for backwards compatibility with
    %% older versions of mnesia_ext (before OTP 20).
    case Cont of
        {_, '$end_of_table'} -> '$end_of_table';
        {_, Cont1}           -> Cont1();
        '$end_of_table'      -> '$end_of_table';
        _                    -> Cont()
    end.

select(Alias, Tab, Ms) ->
    case select(Alias, Tab, Ms, infinity) of
        {Res, '$end_of_table'} ->
            Res;
        '$end_of_table' ->
            '$end_of_table'
    end.

select(Alias, Tab, Ms, Limit) when Limit==infinity; is_integer(Limit) ->
    {Ref, Type} = get_ref(Alias, Tab),
    do_select(Ref, Tab, Type, Ms, Limit).

slot(Alias, Tab, Pos) when is_integer(Pos), Pos >= 0 ->
    {Ref, Type} = get_ref(Alias, Tab),
    First = fun(I) -> ?rocksdb:iterator_move(I, <<?DATA_START>>) end,
    F = case Type of
            bag -> fun(I) -> slot_iter_set(First(I), I, 0, Pos) end;
            _   -> fun(I) -> slot_iter_set(First(I), I, 0, Pos) end
        end,
    with_iterator(Ref, F);
slot(_, _, _) ->
    error(badarg).

%% Exactly which objects Mod:slot/2 is supposed to return is not defined,
%% so let's just use the same version for both set and bag. No one should
%% use this function anyway, as it is ridiculously inefficient.
slot_iter_set({ok, K, V}, _I, P, P) ->
    [setelement(2, decode_val(V), decode_key(K))];
slot_iter_set({ok, _, _}, I, P1, P) when P1 < P ->
    slot_iter_set(?rocksdb:iterator_move(I, next), I, P1+1, P);
slot_iter_set(Res, _, _, _) when element(1, Res) =/= ok ->
    '$end_of_table'.

update_counter(Alias, Tab, C, Val) when is_integer(Val) ->
    call(Alias, Tab, {update_counter, C, Val}).

%% server-side part
do_update_counter(C, Val, Ref, St) ->
    Enc = encode_key(C),
    case ?rocksdb:get(Ref, Enc, [{fill_cache, true}]) of
	{ok, EncVal} ->
	    case decode_val(EncVal) of
		{_, _, Old} = Rec when is_integer(Old) ->
		    Res = Old+Val,
		    return_catch(
                      fun() ->
                              db_put(Ref, Enc,
                                     encode_val(
                                       setelement(3, Rec, Res)),
                                     [], St)
                      end);
		_ ->
		    badarg
	    end;
	_ ->
	    badarg
    end.

%% PRIVATE

%% key+data iterator: iterator_move/2 returns {ok, EncKey, EncVal}
with_iterator(Ref, F) ->
    {ok, I} = ?rocksdb:iterator(Ref, []),
    try F(I)
    after
        ?rocksdb:iterator_close(I)
    end.

%% keys_only iterator: iterator_move/2 returns {ok, EncKey}
%% with_keys_only_iterator(Ref, F) ->
%%     {ok, I} = ?rocksdb:iterator(Ref, [], keys_only),
%%     try F(I)
%%     after
%%         ?rocksdb:iterator_close(I)
%%     end.

%% TODO - use with_keys_only_iterator for match_delete

%% record and key validation

validate_key(_Alias, _Tab, RecName, Arity, Type, _Key) ->
    {RecName, Arity, Type}.

validate_record(_Alias, _Tab, RecName, Arity, Type, _Obj) ->
    {RecName, Arity, Type}.

%% file extension callbacks

%% Extensions for files that are permanent. Needs to be cleaned up
%% e.g. at deleting the schema.
real_suffixes() ->
    [".extrdb"].

%% Extensions for temporary files. Can be cleaned up when mnesia
%% cleans up other temporary files.
tmp_suffixes() ->
    [].


%% ----------------------------------------------------------------------------
%% GEN SERVER CALLBACKS AND CALLS
%% ----------------------------------------------------------------------------

start_proc(Alias, Tab, Type, RdbOpts) ->
    ProcName = proc_name(Alias, Tab),
    gen_server:start_link({local, ProcName}, ?MODULE,
                          {Alias, Tab, Type, RdbOpts}, []).

init({Alias, Tab, Type, RdbOpts}) ->
    process_flag(trap_exit, true),
    try
        {ok, Ref, Ets} = do_load_table(Tab, RdbOpts),
        St = #st{ ets = Ets
                , ref = Ref
                , alias = Alias
                , tab = Tab
                , type = Type
                , size_warnings = 0
                , maintain_size = should_maintain_size(Tab)
                },
        OnWriteError = proplists:get_value(on_write_error, RdbOpts, St#st.on_write_error),
        {ok, recover_size_info(St#st{on_write_error = OnWriteError})}
    catch
        throw:badarg ->
            {error, write_error}
    end.

do_load_table(Tab, RdbOpts) ->
    MPd = data_mountpoint(Tab),
    ?dbg("** Mountpoint: ~p~n ~s~n", [MPd, os:cmd("ls " ++ MPd)]),
    Ets = ets:new(tab_name(icache,Tab), [set, protected, named_table]),
    {ok, Ref} = open_rocksdb(MPd, RdbOpts),
    rocksdb_to_ets(Ref, Ets),
    {ok, Ref, Ets}.

handle_call({load, Alias, Tab, Type, RdbOpts}, _From,
            #st{type = Type, alias = Alias, tab = Tab} = St) ->
    {ok, Ref, Ets} = do_load_table(Tab, RdbOpts),
    {reply, ok, St#st{ref = Ref, ets = Ets}};
handle_call(get_ref, _From, #st{ref = Ref, type = Type} = St) ->
    {reply, {Ref, Type}, St};
handle_call({write_info, Key, Value}, _From, #st{} = St) ->
    _ = write_info_(Key, Value, St),
    {reply, ok, St};
handle_call({update_counter, C, Incr}, _From, #st{ref = Ref} = St) ->
    {reply, do_update_counter(C, Incr, Ref, St), St};
handle_call({insert, Key, Val}, _From, St) ->
    Res = do_insert(Key, Val, St),
    {reply, Res, St};
handle_call({delete, Key}, _From, St) ->
    Res = do_delete(Key, St),
    {reply, Res, St};
handle_call(clear_table, _From, #st{ets = Ets, tab = Tab, ref = Ref} = St) ->
    MPd = data_mountpoint(Tab),
    ?dbg("Attempting clear_table(~p)~n", [Tab]),
    _ = rocksdb_close(Ref),
    {ok, NewRef} = destroy_recreate(MPd, rocksdb_open_opts(Tab)),
    ets:delete_all_objects(Ets),
    rocksdb_to_ets(NewRef, Ets),
    {reply, ok, St#st{ref = NewRef}};
handle_call({match_delete, Pat}, _From, #st{} = St) ->
    Res = do_match_delete(Pat, St),
    {reply, Res, St};
handle_call(close_table, _From, #st{ref = Ref, ets = Ets} = St) ->
    _ = rocksdb_close(Ref),
    ets:delete(Ets),
    {reply, ok, St#st{ref = undefined}};
handle_call(delete_table, _From, #st{tab = T, ref = Ref, ets = Ets} = St) ->
    _ = (catch rocksdb_close(Ref)),
    _ = (catch ets:delete(Ets)),
    do_delete_table(T, data_mountpoint(T)),
    {stop, normal, ok, St#st{ref = undefined}}.

handle_cast(size_warning, #st{tab = T, size_warnings = W} = St) when W < 10 ->
    mnesia_lib:warning("large size retrieved from table: ~p~n", [T]),
    if W =:= 9 ->
            OneHrMs = 60 * 60 * 1000,
            erlang:send_after(OneHrMs, self(), unmute_size_warnings);
       true ->
            ok
    end,
    {noreply, St#st{size_warnings = W + 1}};
handle_cast(size_warning, #st{size_warnings = W} = St) when W >= 10 ->
    {noreply, St#st{size_warnings = W + 1}};
handle_cast(_, St) ->
    {noreply, St}.

handle_info(unmute_size_warnings, #st{tab = T, size_warnings = W} = St) ->
    C = W - 10,
    if C > 0 ->
            mnesia_lib:warning("warnings suppressed~ntable: ~p, count: ~p~n",
                               [T, C]);
       true ->
            ok
    end,
    {noreply, St#st{size_warnings = 0}};
handle_info({'EXIT', _, _} = _EXIT, St) ->
    ?dbg("rocksdb owner received ~p~n", [_EXIT]),
    {noreply, St};
handle_info(_, St) ->
    {noreply, St}.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, #st{ref = Ref}) ->
    if Ref =/= undefined ->
	    ?rocksdb:close(Ref);
       true -> ok
    end,
    ok.


%% ----------------------------------------------------------------------------
%% GEN SERVER PRIVATE
%% ----------------------------------------------------------------------------

get_env_default(Key, Default) ->
    case os:getenv(Key) of
        false ->
            Default;
        Value ->
            Value
    end.

rocksdb_open_opts({Tab, index, {Pos,_}}) ->
    UserProps = mnesia_lib:val({Tab, user_properties}),
    IxOpts = proplists:get_value(rocksdb_index_opts, UserProps, []),
    PosOpts = proplists:get_value(Pos, IxOpts, []),
    rocksdb_open_opts_(PosOpts);
rocksdb_open_opts(Tab) ->
    UserProps = mnesia_lib:val({Tab, user_properties}),
    RdbOpts = proplists:get_value(rocksdb_opts, UserProps, []),
    rocksdb_open_opts_(RdbOpts).

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

destroy_recreate(MPd, RdbOpts) ->
    ok = destroy_db(MPd, []),
    open_rocksdb(MPd, RdbOpts).

open_rocksdb(MPd, RdbOpts) ->
    open_rocksdb(MPd, rocksdb_open_opts_(RdbOpts), get_retries()).

%% Code adapted from basho/riak_kv_eleveldb_backend.erl
open_rocksdb(MPd, Opts, Retries) ->
    open_db(MPd, Opts, max(1, Retries), undefined).

open_db(_, _, 0, LastError) ->
    {error, LastError};
open_db(MPd, Opts, RetriesLeft, _) ->
    case ?rocksdb:open(MPd, Opts) of
        {ok, Ref} ->
            ?dbg("~p: Open - Rocksdb: ~s~n  -> {ok, ~p}~n",
                 [self(), MPd, Ref]),
            {ok, Ref};
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
                    open_db(MPd, Opts, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% await_db_closed(Tab) ->
%%     MPd = data_mountpoint(Tab),
%%     await_db_closed_(MPd).

%% await_db_closed_(MPd) ->
%%     case filelib:is_file(filename:join(MPd, "LOCK")) of
%%      true ->
%%          SleepFor = get_retry_delay(),
%%          timer:sleep(SleepFor),
%%          await_db_closed_(MPd);
%%      false ->
%%          ok
%%     end.

rocksdb_close(undefined) ->
    ok;
rocksdb_close(Ref) ->
    Res = ?rocksdb:close(Ref),
    erlang:garbage_collect(),
    Res.

destroy_db(MPd, Opts) ->
    destroy_db(MPd, Opts, get_retries()).

%% Essentially same code as above.
destroy_db(MPd, Opts, Retries) ->
    _DRes = destroy_db(MPd, Opts, max(1, Retries), undefined),
    ?dbg("~p: Destroy ~s -> ~p~n", [self(), MPd, _DRes]),
    [_|_] = MPd, % ensure MPd is non-empty
    _RmRes = os:cmd("rm -rf " ++ MPd ++ "/*"),
    ?dbg("~p: RmRes = '~s'~n", [self(), _RmRes]),
    ok.

destroy_db(_, _, 0, LastError) ->
    {error, LastError};
destroy_db(MPd, Opts, RetriesLeft, _) ->
    case ?rocksdb:destroy(MPd, Opts) of
	ok ->
	    ok;
        %% Check specifically for lock error, this can be caused if
        %% destroy follows quickly after close.
        {error, {error_db_destroy, Err}=Reason} ->
            case lists:prefix("IO error: lock ", Err) of
                true ->
                    SleepFor = get_retry_delay(),
                    ?dbg("~p: Destroy - Rocksdb backend retrying ~p in ~p ms"
                         " after error ~s\n"
                         " children = ~p~n",
                         [self(), MPd, SleepFor, Err,
                          supervisor:which_children(mnesia_ext_sup)]),
                    timer:sleep(SleepFor),
                    destroy_db(MPd, Opts, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

get_retries() -> 30.
get_retry_delay() -> 10000.

rocksdb_to_ets(Ref, Ets) ->
    with_iterator(Ref, fun(I) ->
                               i_rocksdb_to_ets(I, Ets, <<?INFO_START>>)
                       end).

i_rocksdb_to_ets(I, Ets, Move) ->
    case ?rocksdb:iterator_move(I, Move) of
        {ok, << ?INFO_TAG, EncKey/binary >>, EncVal} ->
            Item = decode_key(EncKey),
            Val = decode_val(EncVal),
            ets:insert(Ets, {{info,Item}, Val}),
            i_rocksdb_to_ets(I, Ets, next);
        _ ->
            '$end_of_table'
    end.

opt_call(Alias, Tab, Req) ->
    ProcName = proc_name(Alias, Tab),
    case whereis(ProcName) of
        undefined ->
            ?dbg("proc_name(~p, ~p): ~p; NO PROCESS~n",
                 [Alias, Tab, ProcName]),
            {error, noproc};
        Pid when is_pid(Pid) ->
            ?dbg("proc_name(~p, ~p): ~p; Pid = ~p~n",
                 [Alias, Tab, ProcName, Pid]),
            {ok, gen_server:call(Pid, Req, infinity)}
    end.

call(Alias, Tab, Req) ->
    ProcName = proc_name(Alias, Tab),
    case gen_server:call(ProcName, Req, infinity) of
        badarg ->
            mnesia:abort(badarg);
        {abort, _} = Err ->
            mnesia:abort(Err);
        Reply ->
            Reply
    end.

size_warning(Alias, Tab) ->
    ProcName = proc_name(Alias, Tab),
    gen_server:cast(ProcName, size_warning).

%% server-side end of insert/3.
do_insert(K, V, #st{ref = Ref, type = bag, maintain_size = false} = St) ->
    return_catch(fun() -> do_insert_bag(Ref, K, V, false, St) end);
do_insert(K, V, #st{ets = Ets, ref = Ref, type = bag, maintain_size = true} = St) ->
    return_catch(
      fun() ->
              CurSz = read_info(size, 0, Ets),
              NewSz = do_insert_bag(Ref, K, V, CurSz, St),
              ets_insert_info(Ets, size, NewSz),
              ok
      end);
do_insert(K, V, #st{ref = Ref, maintain_size = false} = St) ->
    return_catch(fun() -> db_put(Ref, K, V, [], St) end);
do_insert(K, V, #st{ets = Ets, ref = Ref, maintain_size = true} = St) ->
    IsNew =
	case ?rocksdb:get(Ref, K, []) of
	    {ok, _} ->
		false;
	    _ ->
		true
	end,
    case IsNew of
	true ->
            return_catch(
              fun() ->
                      NewSz = read_info(size, 0, Ets) + 1,
                      {Ki, Vi} = info_obj(size, NewSz),
                      L = [{put, Ki, Vi}, {put, K, V}],
                      write_result(mnesia_rocksdb_lib:write(Ref, L, []),
                                   write, [Ref, L, []], St), % may throw
                      ets_insert_info(Ets, size, NewSz)
              end);
	false ->
	    return_catch(fun() -> db_put(Ref, K, V, [], St) end)
    end,
    ok.

do_insert_bag(Ref, K, V, CurSz, St) ->
    KSz = byte_size(K),
    with_iterator(
      Ref, fun(I) ->
		   do_insert_bag_(
		     KSz, K, ?rocksdb:iterator_move(I, K), I, V, 0, Ref, CurSz, St)
	   end).


%% There's a potential access pattern that would force counters to
%% creep upwards and eventually hit the limit. This could be addressed,
%% with compaction. TODO.
do_insert_bag_(Sz, K, Res, I, V, Prev, Ref, TSz, St) when Prev < ?MAX_BAG ->
    case Res of
	{ok, <<K:Sz/binary, _:?BAG_CNT>>, V} ->
	    %% object exists
	    TSz;
	{ok, <<K:Sz/binary, N:?BAG_CNT>>, _} ->
	    do_insert_bag_(
	      Sz, K, ?rocksdb:iterator_move(I, next), I, V, N, Ref, TSz, St);
	_ when TSz =:= false ->
	    Key = <<K/binary, (Prev+1):?BAG_CNT>>,
	    db_put(Ref, Key, V, [], St);
	_ ->
	    NewSz = TSz + 1,
	    {Ki, Vi} = info_obj(size, NewSz),
	    Key = <<K/binary, (Prev+1):?BAG_CNT>>,
	    db_write(Ref, [{put, Ki, Vi}, {put, Key, V}], [], St),
	    NewSz
    end.

%% server-side part
do_delete(Key, #st{ref = Ref, type = bag, maintain_size = false} = St) ->
    return_catch(fun() -> do_delete_bag(byte_size(Key), Key, Ref, false, St) end);
do_delete(Key, #st{ets = Ets, ref = Ref, type = bag, maintain_size = true} = St) ->
    return_catch(
      fun() ->
              Sz = byte_size(Key),
              CurSz = read_info(size, 0, Ets),
              NewSz = do_delete_bag(Sz, Key, Ref, CurSz, St),
              ets_insert_info(Ets, size, NewSz),
              ok
      end);
do_delete(Key, #st{ref = Ref, maintain_size = false} = St) ->
    return_catch(fun() -> db_delete(Ref, Key, [], St) end);
do_delete(Key, #st{ets = Ets, ref = Ref, maintain_size = true} = St) ->
    CurSz = read_info(size, 0, Ets),
    case ?rocksdb:get(Ref, Key, [{fill_cache,true}]) of
	{ok, _} ->
            return_catch(
              fun() ->
                      NewSz = CurSz -1,
                      {Ki, Vi} = info_obj(size, NewSz),
                      ok = db_write(Ref, [{delete, Key}, {put, Ki, Vi}], [], St),
                      ets_insert_info(Ets, size, NewSz)
              end);
	not_found ->
	    false
    end.

do_delete_bag(Sz, Key, Ref, TSz, St) ->
    Found = 
	with_iterator(
	  Ref, fun(I) ->
		       do_delete_bag_(Sz, Key, ?rocksdb:iterator_move(I, Key),
				      Ref, I)
	       end),
    case {Found, TSz} of
	{[], _} ->
	    TSz;
	{_, false} ->
	    db_write(Ref, [{delete, K} || K <- Found], [], St);
	{_, _} ->
	    N = length(Found),
	    NewSz = TSz - N,
	    {Ki, Vi} = info_obj(size, NewSz),
	    db_write(Ref, [{put, Ki, Vi} |
                           [{delete, K} || K <- Found]], [], St),
	    NewSz
    end.

do_delete_bag_(Sz, K, Res, Ref, I) ->
    case Res of
	{ok, K, _} ->
	    do_delete_bag_(Sz, K, ?rocksdb:iterator_move(I, next),
			   Ref, I);
	{ok, <<K:Sz/binary, _:?BAG_CNT>> = Key, _} ->
	    [Key |
	     do_delete_bag_(Sz, K, ?rocksdb:iterator_move(I, next),
			    Ref, I)];
	_ ->
	    []
    end.

do_match_delete(Pat, #st{ets = Ets, ref = Ref, tab = Tab, type = Type,
			 maintain_size = MaintainSize} = St) ->
    Fun = fun(_, Key, Acc) -> [Key|Acc] end,
    Keys = do_fold(Ref, Tab, Type, Fun, [], [{Pat,[],['$_']}], 30),
    case {Keys, MaintainSize} of
	{[], _} ->
	    ok;
	{_, false} ->
	    db_write(Ref, [{delete, K} || K <- Keys], [], St),
	    ok;
	{_, true} ->
	    CurSz = read_info(size, 0, Ets),
	    NewSz = max(CurSz - length(Keys), 0),
	    {Ki, Vi} = info_obj(size, NewSz),
	    db_write(Ref, [{put, Ki, Vi} |
                           [{delete, K} || K <- Keys]], [], St),
	    ets_insert_info(Ets, size, NewSz),
	    ok
    end.

recover_size_info(#st{ ref = Ref
		     , tab = Tab
		     , type = Type
		     , maintain_size = MaintainSize
		     } = St) ->
    %% TODO: shall_update_size_info is obsolete, remove
    case shall_update_size_info(Tab) of
	true ->
	    Sz = do_fold(Ref, Tab, Type, fun(_, Acc) -> Acc+1 end,
			 0, [{'_',[],['$_']}], 3),
	    write_info_(size, Sz, St);
	false ->
	    case MaintainSize of
		true ->
		    %% info initialized by rocksdb_to_ets/2
		    %% TODO: if there is no stored size, recompute it
		    ignore;
		false ->
		    %% size is not maintained, ensure it's marked accordingly
		    delete_info_(size, St)
	    end
    end,
    St.

shall_update_size_info({_, index, _}) ->
    false;
shall_update_size_info(Tab) ->
    property(Tab, update_size_info, false).

should_maintain_size(Tab) ->
    property(Tab, maintain_size, false).

property(Tab, Prop, Default) ->
    try mnesia:read_table_property(Tab, Prop) of
        {Prop, P} ->
            P
    catch
        error:_ -> Default;
        exit:_  -> Default
    end.

write_info_(Item, Val, #st{ets = Ets, ref = Ref} = St) ->
    rocksdb_insert_info(Ref, Item, Val, St),
    ets_insert_info(Ets, Item, Val).

ets_insert_info(Ets, Item, Val) ->
    ets:insert(Ets, {{info, Item}, Val}).

ets_delete_info(Ets, Item) ->
    ets:delete(Ets, {info, Item}).

rocksdb_insert_info(Ref, Item, Val, St) ->
    EncKey = info_key(Item),
    EncVal = encode_val(Val),
    db_put(Ref, EncKey, EncVal, [], St).

rocksdb_delete_info(Ref, Item, St) ->
    EncKey = info_key(Item),
    db_delete(Ref, EncKey, [], St).

info_obj(Item, Val) ->
    {info_key(Item), encode_val(Val)}.

info_key(Item) ->
    <<?INFO_TAG, (encode_key(Item))/binary>>.

delete_info_(Item, #st{ets = Ets, ref = Ref} = St) ->
    rocksdb_delete_info(Ref, Item, St),
    ets_delete_info(Ets, Item).

read_info(Item, Default, Ets) ->
    case ets:lookup(Ets, {info,Item}) of
        [] ->
            Default;
        [{_,Val}] ->
            Val
    end.

tab_name(icache, Tab) ->
    list_to_atom("mnesia_ext_icache_" ++ tabname(Tab));
tab_name(info, Tab) ->
    list_to_atom("mnesia_ext_info_" ++ tabname(Tab)).

proc_name(_Alias, Tab) ->
    list_to_atom("mnesia_ext_proc_" ++ tabname(Tab)).


%% ----------------------------------------------------------------------------
%% PRIVATE SELECT MACHINERY
%% ----------------------------------------------------------------------------

do_select(Ref, Tab, Type, MS, Limit) ->
    do_select(Ref, Tab, Type, MS, false, Limit).

do_select(Ref, Tab, _Type, MS, AccKeys, Limit) when is_boolean(AccKeys) ->
    Keypat = keypat(MS, keypos(Tab)),
    Sel = #sel{tab = Tab,
               ref = Ref,
               keypat = Keypat,
               ms = MS,
               compiled_ms = ets:match_spec_compile(MS),
               key_only = needs_key_only(MS),
               limit = Limit},
    with_iterator(Ref, fun(I) -> i_do_select(I, Sel, AccKeys, []) end).

i_do_select(I, #sel{keypat = Pfx,
                    compiled_ms = MS,
                    limit = Limit} = Sel, AccKeys, Acc) ->
    StartKey =
        case Pfx of
            <<>> ->
                <<?DATA_START>>;
            _ ->
                Pfx
	end,
    select_traverse(?rocksdb:iterator_move(I, StartKey), Limit,
		    Pfx, MS, I, Sel, AccKeys, Acc).

needs_key_only([{HP,_,Body}]) ->
    BodyVars = lists:flatmap(fun extract_vars/1, Body),
    %% Note that we express the conditions for "needs more than key" and negate.
    not(wild_in_body(BodyVars) orelse
        case bound_in_headpat(HP) of
            {all,V} -> lists:member(V, BodyVars);
            none    -> false;
            Vars    -> any_in_body(lists:keydelete(2,1,Vars), BodyVars)
        end);
needs_key_only(_) ->
    %% don't know
    false.

extract_vars([H|T]) ->
    extract_vars(H) ++ extract_vars(T);
extract_vars(T) when is_tuple(T) ->
    extract_vars(tuple_to_list(T));
extract_vars(T) when T=='$$'; T=='$_' ->
    [T];
extract_vars(T) when is_atom(T) ->
    case is_wild(T) of
        true ->
            [T];
        false ->
            []
    end;
extract_vars(_) ->
    [].

any_in_body(Vars, BodyVars) ->
    lists:any(fun({_,Vs}) ->
                      intersection(Vs, BodyVars) =/= []
              end, Vars).

intersection(A,B) when is_list(A), is_list(B) ->
    A -- (A -- B).

wild_in_body(BodyVars) ->
    intersection(BodyVars, ['$$','$_']) =/= [].

bound_in_headpat(HP) when is_atom(HP) ->
    {all, HP};
bound_in_headpat(HP) when is_tuple(HP) ->
    [_|T] = tuple_to_list(HP),
    map_vars(T, 2);
bound_in_headpat(_) ->
    %% this is not the place to throw an exception
    none.

map_vars([H|T], P) ->
    case extract_vars(H) of
        [] ->
            map_vars(T, P+1);
        Vs ->
            [{P, Vs}|map_vars(T, P+1)]
    end;
map_vars([], _) ->
    [].

select_traverse({ok, K, V}, Limit, Pfx, MS, I, #sel{tab = Tab} = Sel,
                AccKeys, Acc) ->
    case is_prefix(Pfx, K) of
	true ->
	    Rec = setelement(keypos(Tab), decode_val(V), decode_key(K)),
	    case ets:match_spec_run([Rec], MS) of
		[] ->
		    select_traverse(
		      ?rocksdb:iterator_move(I, next), Limit, Pfx, MS,
		      I, Sel, AccKeys, Acc);
		[Match] ->
                    Acc1 = if AccKeys ->
                                   [{K, Match}|Acc];
                              true ->
                                   [Match|Acc]
                           end,
                    traverse_continue(K, decr(Limit), Pfx, MS, I, Sel, AccKeys, Acc1)
            end;
        false ->
            {lists:reverse(Acc), '$end_of_table'}
    end;
select_traverse({error, _}, _, _, _, _, _, _, Acc) ->
    {lists:reverse(Acc), '$end_of_table'}.

is_prefix(A, B) when is_binary(A), is_binary(B) ->
    Sa = byte_size(A),
    case B of
        <<A:Sa/binary, _/binary>> ->
            true;
        _ ->
            false
    end.

decr(I) when is_integer(I) ->
    I-1;
decr(infinity) ->
    infinity.

traverse_continue(K, 0, Pfx, MS, _I, #sel{limit = Limit, ref = Ref} = Sel, AccKeys, Acc) ->
    {lists:reverse(Acc),
     fun() ->
	     with_iterator(Ref,
			   fun(NewI) ->
                                   select_traverse(iterator_next(NewI, K),
                                                   Limit, Pfx, MS, NewI, Sel,
                                                   AccKeys, [])
			   end)
     end};
traverse_continue(_K, Limit, Pfx, MS, I, Sel, AccKeys, Acc) ->
    select_traverse(?rocksdb:iterator_move(I, next), Limit, Pfx, MS, I, Sel, AccKeys, Acc).

iterator_next(I, K) ->
    case ?rocksdb:iterator_move(I, K) of
	{ok, K, _} ->
	    ?rocksdb:iterator_move(I, next);
	Other ->
	    Other
    end.

keypat([H|T], KeyPos) ->
    keypat(T, KeyPos, keypat_pfx(H, KeyPos)).

keypat(_, _, <<>>) -> <<>>;
keypat([H|T], KeyPos, Pfx0) ->
    Pfx = keypat_pfx(H, KeyPos),
    keypat(T, KeyPos, common_prefix(Pfx, Pfx0));
keypat([], _, Pfx) ->
    Pfx.

common_prefix(<<H, T/binary>>, <<H, T1/binary>>) ->
    <<H, (common_prefix(T, T1))/binary>>;
common_prefix(_, _) ->
    <<>>.

keypat_pfx({HeadPat,_Gs,_}, KeyPos) when is_tuple(HeadPat) ->
    KP      = element(KeyPos, HeadPat),
    sext:prefix(KP);
keypat_pfx(_, _) ->
    <<>>.


%% ----------------------------------------------------------------------------
%% Db wrappers
%% ----------------------------------------------------------------------------

return_catch(F) when is_function(F, 0) ->
    try F()
    catch
        throw:badarg ->
            badarg
    end.

db_put(Ref, K, V, Opts, St) ->
    write_result(mnesia_rocksdb_lib:put(Ref, K, V, Opts), put, [Ref, K, V, Opts], St).

db_write(Ref, List, Opts, St) ->
    write_result(mnesia_rocksdb_lib:write(Ref, List, Opts), write, [Ref, List, Opts], St).

db_delete(Ref, K, Opts, St) ->
    write_result(mnesia_rocksdb_lib:delete(Ref, K, Opts), delete, [Ref, K, Opts], St).

write_result(ok, _, _, _) ->
    ok;
write_result(Res, Op, Args, #st{on_write_error = Rpt}) ->
    RptOp = rpt_op(Rpt),
    mnesia_lib:RptOp("FAILED rocksdb:~p(" ++ rpt_fmt(Args) ++ ") -> ~p~n",
                   [Op | Args] ++ [Res]),
    if Rpt == fatal; Rpt == error ->
            throw(badarg);
       true ->
            ok
    end.

rpt_fmt([_|T]) ->
    lists:append(["~p" | [", ~p" || _ <- T]]).

rpt_op(debug) ->
    dbg_out;
rpt_op(Op) ->
    Op.

valid_mnesia_op(Op) ->
    if Op==debug
     ; Op==verbose
     ; Op==warning
     ; Op==error
     ; Op==fatal ->
            true;
       true ->
            false
    end.

%% ----------------------------------------------------------------------------
%% COMMON PRIVATE
%% ----------------------------------------------------------------------------

%% Note that since a callback can be used as an indexing backend, we
%% cannot assume that keypos will always be 2. For indexes, the tab
%% name will be {Tab, index, Pos}, and The object structure will be
%% {{IxKey,Key}} for an ordered_set index, and {IxKey,Key} for a bag
%% index.
%%
keypos({_, index, _}) ->
    1;
keypos({_, retainer, _}) ->
    2;
keypos(Tab) when is_atom(Tab) ->
    2.

encode_key(Key) ->
    sext:encode(Key).

decode_key(CodedKey) ->
    case sext:partial_decode(CodedKey) of
        {full, Result, _} ->
            Result;
        _ ->
            error(badarg, CodedKey)
    end.

encode_val(Val) ->
    term_to_binary(Val).

decode_val(CodedVal) ->
    binary_to_term(CodedVal).

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

%% delete_mountpoint(Tab) ->
%%     MPd = data_mountpoint(Tab),
%%     assert_proper_mountpoint(Tab, MPd),
%%     ok = destroy_db(MPd, []).

assert_proper_mountpoint(_Tab, _MPd) ->
    %% TODO: not yet implemented. How to verify that the MPd var points
    %% to the directory we actually want deleted?
    ok.

data_mountpoint(Tab) ->
    Dir = mnesia_monitor:get_env(dir),
    filename:join(Dir, tabname(Tab) ++ ".extrdb").

tabname({Tab, index, {{Pos},_}}) ->
    atom_to_list(Tab) ++ "-=" ++ atom_to_list(Pos) ++ "=-_ix";
tabname({Tab, index, {Pos,_}}) ->
    atom_to_list(Tab) ++ "-" ++ integer_to_list(Pos) ++ "-_ix";
tabname({Tab, retainer, Name}) ->
    atom_to_list(Tab) ++ "-" ++ retainername(Name) ++ "-_RET";
tabname(Tab) when is_atom(Tab) ->
    atom_to_list(Tab) ++ "-_tab".

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

get_ref(Alias, Tab) ->
    call(Alias, Tab, get_ref).

fold(Alias, Tab, Fun, Acc, MS, N) ->
    {Ref, Type} = get_ref(Alias, Tab),
    do_fold(Ref, Tab, Type, Fun, Acc, MS, N).

%% can be run on the server side.
do_fold(Ref, Tab, Type, Fun, Acc, MS, N) ->
    {AccKeys, F} =
        if is_function(Fun, 3) ->
                {true, fun({K,Obj}, Acc1) ->
                               Fun(Obj, K, Acc1)
                       end};
           is_function(Fun, 2) ->
                {false, Fun}
        end,
    do_fold1(do_select(Ref, Tab, Type, MS, AccKeys, N), F, Acc).

do_fold1('$end_of_table', _, Acc) ->
    Acc;
do_fold1({L, Cont}, Fun, Acc) ->
    Acc1 = lists:foldl(Fun, Acc, L),
    do_fold1(select(Cont), Fun, Acc1).

is_wild('_') ->
    true;
is_wild(A) when is_atom(A) ->
    case atom_to_list(A) of
        "\$" ++ S ->
            try begin
                    _ = list_to_integer(S),
                    true
                end
            catch
                error:_ ->
                    false
            end;
        _ ->
            false
    end;
is_wild(_) ->
    false.
