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
%%
%% This module implements a mnesia backend callback plugin.
%% It's specifically documented to try to explain the workings of
%% backend plugins.
%%
%% @end

%% Initialization: register() or register(Alias)
%% Usage: mnesia:create_table(Tab, [{rocksdb_copies, Nodes}, ...]).

-module(mnesia_rocksdb).

%% ----------------------------------------------------------------------------
%% BEHAVIOURS
%% ----------------------------------------------------------------------------

-behaviour(mnesia_backend_type).
-behaviour(gen_server).

-dialyzer(no_undefined_callbacks).

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
         show_table/2]).

%%
%% BACKEND CALLBACKS
%%

%% backend management
-export([init_backend/0,
         add_aliases/1,
         remove_aliases/1]).

%% schema level callbacks
-export([ semantics/2
        , check_definition/4
        , create_table/3
        , load_table/4
        , close_table/2
        , sync_close_table/2
        , delete_table/2
        , info/3 ]).

%% table synch calls
-export([ sender_init/4
        , sender_handle_info/5
        , receiver_first_message/4
        , receive_data/5
        , receive_done/4 ]).

%% low-level accessor callbacks.
-export([ delete/3
        , first/2
        , fixtable/3
        , insert/3
        , last/2
        , lookup/3
        , match_delete/3
        , next/3
        , prev/3
        , repair_continuation/2
        , select/1
        , select/3
        , select/4
        , slot/3
        , update_counter/4 ]).

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

-export([start_proc/6,
         init/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         terminate/2,
         code_change/3]).

-export([ ix_prefixes/3
        , ix_listvals/3 ]).

-import(mrdb, [ with_iterator/2
              ]).

-import(mnesia_rocksdb_admin, [ get_ref/1 ]).

-import(mnesia_rocksdb_lib, [ encode_key/2
                            , encode_val/3
                            , decode_key/2
                            , decode_val/3
                            ]).

-include("mnesia_rocksdb.hrl").
-include("mnesia_rocksdb_int.hrl").

%% ----------------------------------------------------------------------------
%% DEFINES
%% ----------------------------------------------------------------------------


%% ----------------------------------------------------------------------------
%% RECORDS
%% ----------------------------------------------------------------------------

-record(st, { ref
            , alias
            , tab
            , type
            }).

-type data_tab() :: atom().
-type index_pos() :: integer() | {atom()}.
-type index_type() :: ordered.
-type index_info() :: {index_pos(), index_type()}.
-type retainer_name() :: any().
-type index_tab() :: {data_tab(), index, index_info()}.
-type retainer_tab() :: {data_tab(), retainer, retainer_name()}.

-type alias() :: atom().
-type table_type() :: set | ordered_set | bag.
-type table() :: data_tab() | index_tab() | retainer_tab().

-type error() :: {error, any()}.

-export_type([alias/0,
              table/0,
              table_type/0]).

%% ----------------------------------------------------------------------------
%% CONVENIENCE API
%% ----------------------------------------------------------------------------

-spec register() -> {ok, alias()} | {error, _}.
%% @equiv register(rocksdb_copies)
register() ->
    register(default_alias()).

%% @doc Convenience function for registering a mnesia_rocksdb backend plugin
%%
%% The function used to register a plugin is `mnesia_schema:add_backend_type(Alias, Module)'
%% where `Module' implements a backend_type behavior. `Alias' is an atom, and is used
%% in the same way as `ram_copies' etc. The default alias is `rocksdb_copies'.
%% @end
-spec register(alias()) -> {ok, alias()} | error().
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
    show_table(Tab, 100).

show_table(Tab, Limit) ->
    Ref = mrdb:get_ref(Tab),
    mrdb:with_iterator(Ref, fun(I) ->
                                    i_show_table(I, first, Limit, Ref)
                            end).
%% PRIVATE

i_show_table(_, _, 0, _) ->
    {error, skipped_some};
i_show_table(I, Move, Limit, Ref) ->
    case rocksdb:iterator_move(I, Move) of
        {ok, EncKey, EncVal} ->
            {Type,Val} =
                case EncKey of
                    << ?INFO_TAG, K/binary >> ->
                        K1 = decode_key(K, Ref),
                        V = decode_val(EncVal, K1, Ref),
                        {info,V};
                    _ ->
                        K = decode_key(EncKey, Ref),
                        V = decode_val(EncVal, K, Ref),
                        {data,V}
                end,
            io:fwrite("~p: ~p~n", [Type, Val]),
            i_show_table(I, next, Limit-1, Ref);
        _ ->
            ok
    end.


%% ----------------------------------------------------------------------------
%% BACKEND CALLBACKS
%% ----------------------------------------------------------------------------

%% backend management

%% @doc Called by mnesia_schema in order to intialize the backend
%%
%% This is called when the backend is registered with the first alias, or ...
%%
%% See OTP issue #425 (16 Feb 2021). This callback is supposed to be called
%% before first use of the backend, but unfortunately, it is only called at
%% mnesia startup and when a backend module is registered MORE THAN ONCE.
%% This means we need to handle this function being called multiple times.
%%
%% The bug has been fixed as of OTP 24.0-rc3
%%
%% If processes need to be started, this can be done using
%% `mnesia_ext_sup:start_proc(Name, Mod, F, Args [, Opts])'
%% where Opts are parameters for the supervised child:
%%
%% * `restart' (default: `transient')
%% * `shutdown' (default: `120000')
%% * `type' (default: `worker')
%% * `modules' (default: `[Mod]')
%% @end
init_backend() ->
    mnesia_rocksdb_admin:ensure_started().

add_aliases(Aliases) ->
    %% Since we can't be sure that init_backend() has been called (see above),
    %% and we know that it can be called repeatedly anyway, let's call it here.
    init_backend(),
    mnesia_rocksdb_admin:add_aliases(Aliases).

remove_aliases(Aliases) ->
    mnesia_rocksdb_admin:remove_aliases(Aliases).

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
semantics(_Alias, index_types) -> [ordered, bag];  % treat bag as ordered
semantics(_Alias, index_fun) -> fun index_f/4;
semantics(_Alias, _) -> undefined.

is_index_consistent(Alias, {Tab, index, PosInfo}) ->
    case info(Alias, Tab, {index_consistent, PosInfo}) of
        true -> true;
        _ -> false
    end.

index_is_consistent(_Alias, {Tab, index, PosInfo}, Bool)
  when is_boolean(Bool) ->
    mrdb:write_info(Tab, {index_consistent, PosInfo}, Bool).


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

ix_listvals(_Tab, _Pos, Obj) ->
    lists:foldl(
      fun(V, Acc) when is_list(V) ->
              V ++ Acc;
         (_, Acc) ->
              Acc
      end, [], tl(tuple_to_list(Obj))).

%% For now, only verify that the type is set or ordered_set.
%% set is OK as ordered_set is a kind of set.
check_definition(Alias, Tab, Nodes, Props) ->
    Id = {Alias, Nodes},
    try
        Props1 = lists:map(fun(E) -> check_definition_entry(Tab, Id, E) end, Props),
        {ok, Props1}
    catch
        throw:Error ->
            Error
    end.

check_definition_entry(_Tab, _Id, {type, T} = P) when T==set; T==ordered_set; T==bag ->
    P;
check_definition_entry(Tab, Id, {type, T}) ->
    mnesia:abort({combine_error, Tab, [Id, {type, T}]});
check_definition_entry(Tab, Id, {index, Ixs} = P) ->
    case [true || {_, bag} <- Ixs] of
        [] ->
            P;
        [_|_] ->
            %% Let's not pretend-support bag indexes
            mnesia:abort({combine_error, Tab, [Id, P]})
    end;
check_definition_entry(_Tab, _Id, {user_properties, UPs} = P) ->
    case lists:keyfind(rocksdb_standalone, 1, UPs) of
        false -> ok;
        {_, Bool} when is_boolean(Bool) -> ok;
        Other ->
            throw({error, {invalid_configuration, Other}})
    end,
    P;
check_definition_entry(_Tab, _Id, P) ->
    P.

create_table(Alias, Tab, Props) ->
    {ok, Pid} = maybe_start_proc(Alias, Tab, Props),
    gen_server:call(Pid, {create_table, Tab, Props}).

load_table(Alias, Tab, LoadReason, Opts) ->
    call(Alias, Tab, {load_table, LoadReason, Opts}).

close_table(Alias, Tab) ->
    case mnesia_rocksdb_admin:get_ref(Tab, error) of
        error ->
            ok;
        _ ->
            ok = mnesia_rocksdb_admin:prep_close(Alias, Tab),
            close_table_(Alias, Tab)
    end.

close_table_(Alias, Tab) ->
    case opt_call(Alias, Tab, close_table) of
        {error, noproc} ->
            ?dbg("~p: close_table_(~p) -> noproc~n",
                 [self(), Tab]),
            ok;
        {ok, _} ->
            ok
    end.

-ifndef(MNESIA_ROCKSDB_NO_DBG).
pp_stack() ->
    Trace = try throw(true)
            catch
                _:_:ST ->
                    case ST of
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
pp_pos([{file,_},{line,L}]) ->
    [" (", integer_to_list(L), ")"].
-endif.

sync_close_table(Alias, Tab) ->
    ?dbg("~p: sync_close_table(~p, ~p);~n Trace: ~s~n",
         [self(), Alias, Tab, pp_stack()]),
    close_table(Alias, Tab).

delete_table(Alias, Tab) ->
    case whereis_proc(Alias, Tab) of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            call(Alias, Tab, delete_table)
    end.

info(_Alias, Tab, Item) ->
    mrdb:read_info(Tab, Item).

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

%% Whereas the return type, legacy | Ref, seems odd, it's a shortcut for
%% performance reasons.
access_type(Tab) ->
    case get_ref(Tab) of
        #{semantics := bag, vsn := 1} -> legacy;
        R ->
            R#{mode => mnesia}
    end.

delete(Alias, Tab, Key) ->
    case access_type(Tab) of
        legacy -> call(Alias, Tab, {delete, Key});
        R -> db_delete(R, Key, [], R)
    end.
    %% call_if_legacy(Alias, Tab, {delete, Key}, fun() -> mrdb
    %% mrdb:delete(Tab, Key).
    %% %% opt_call(Alias, Tab, {delete, Key}),
    %% %% ok.

first(_Alias, Tab) ->
    mrdb:first(Tab).

%% Not relevant for an ordered_set
fixtable(_Alias, _Tab, _Bool) ->
    true.

insert(Alias, Tab, Obj) ->
    case access_type(Tab) of
        legacy -> call(Alias, Tab, {insert, Obj});
        R -> db_insert(R, Obj, [], R)
    end.

last(_Alias, Tab) ->
    mrdb:last(Tab).

%% Since we replace the key with [] in the record, we have to put it back
%% into the found record.
lookup(_Alias, Tab, Key) ->
    mrdb:read(Tab, Key).

match_delete(Alias, Tab, Pat) ->
    case access_type(Tab) of
        legacy -> call(Alias, Tab, {match_delete, Pat});
        R -> match_delete_(R, Pat)
    end.

match_delete_(#{name := {_, index, {_,bag}}, semantics := set} = R, Pat) ->
    case Pat of
        '_' ->
            mrdb:match_delete(R, Pat);
        {V, Key} ->
            db_delete(R, {V, Key}, [], R)
    end;
match_delete_(R, Pat) ->
    mrdb:match_delete(R, Pat).

next(_Alias, Tab, Key) ->
    mrdb:next(Tab, Key).

prev(_Alias, Tab, Key) ->
    mrdb:prev(Tab, Key).

repair_continuation(Cont, _Ms) ->
    Cont.

select(Cont) ->
    mrdb:select(Cont).

select(Alias, Tab, Ms) ->
    select(Alias, Tab, Ms, infinity).

select(_Alias, {_,index,{_,bag}} = IxTab, Ms, Limit) ->
    %% We at mnesia_rocksdb do not support bag indexes, but ordered indexes
    %% have the same outward semantics (more or less). Reshape the match pattern.
    [{{IxKey,'$1'}, [], ['$1']}] = Ms,
    mrdb:select(IxTab, [{{{IxKey,'$1'}}, [], [{element, 1, '$_'}]}], Limit);
select(_Alias, Tab, Ms, Limit) when Limit==infinity; is_integer(Limit) ->
    mrdb:select(Tab, Ms, Limit).

slot(_Alias, Tab, Pos) when is_integer(Pos), Pos >= 0 ->
    #{semantics := Sem} = Ref = get_ref(Tab),
    Start = case Ref of
                #{type := standalone, vsn := 1} -> <<?DATA_START>>;
                _ -> first
            end,
    First = fun(I) -> rocksdb:iterator_move(I, Start) end,
    F = case Sem of
            bag -> fun(I) -> slot_iter_set(First(I), I, 0, Pos, Ref) end;
            _   -> fun(I) -> slot_iter_set(First(I), I, 0, Pos, Ref) end
        end,
    with_iterator(Ref, F);
slot(_, _, _) ->
    error(badarg).

%% Exactly which objects Mod:slot/2 is supposed to return is not defined,
%% so let's just use the same version for both set and bag. No one should
%% use this function anyway, as it is ridiculously inefficient.
slot_iter_set({ok, K, V}, _I, P, P, R) ->
    Kd = decode_key(K, R),
    [setelement(2, decode_val(V, Kd, R), Kd)];
slot_iter_set({ok, _, _}, I, P1, P, R) when P1 < P ->
    slot_iter_set(rocksdb:iterator_move(I, next), I, P1+1, P, R);
slot_iter_set(Res, _, _, _, _) when element(1, Res) =/= ok ->
    '$end_of_table'.

update_counter(Alias, Tab, C, Val) when is_integer(Val) ->
    case access_type(Tab) of
        legacy -> call(Alias, Tab, {update_counter, C, Val});
        R -> mrdb:update_counter(R, C, Val)
    end.

%% PRIVATE

%% keys_only iterator: iterator_move/2 returns {ok, EncKey}
%% with_keys_only_iterator(Ref, F) ->
%%     {ok, I} = rocksdb:iterator(Ref, [], keys_only),
%%     try F(I)
%%     after
%%         rocksdb:iterator_close(I)
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

maybe_start_proc(Alias, Tab, Props) ->
    ProcName = proc_name(Alias, Tab),
    case whereis(ProcName) of
        undefined ->
            Type = proplists:get_value(type, Props),
            RdbUserProps = proplists:get_value(
                             rocksdb_opts, proplists:get_value(
                                             user_properties, Props, []), []),
            StorageProps = proplists:get_value(
                             rocksdb, proplists:get_value(
                                        storage_properties, Props, []), RdbUserProps),
            RdbOpts = mnesia_rocksdb_params:lookup(Tab, StorageProps),
            ShutdownTime = proplists:get_value(
                             owner_shutdown_time, RdbOpts, 120000),
            mnesia_ext_sup:start_proc(
              Tab, ?MODULE, start_proc, [Alias, Tab, Type, ProcName, Props, RdbOpts],
              [{shutdown, ShutdownTime}]);
        Pid when is_pid(Pid) ->
            {ok, Pid}
    end.

%% Exported callback
start_proc(Alias, Tab, Type, ProcName, Props, RdbOpts) ->
    gen_server:start_link({local, ProcName}, ?MODULE,
                          {Alias, Tab, Type, Props, RdbOpts}, []).

init({Alias, Tab, Type, _Props, RdbOpts}) ->
    process_flag(trap_exit, true),
    %% In case of a process restart, we try to rebuild the state
    %% from the cf info held by the admin process.
    Ref = case mnesia_rocksdb_admin:request_ref(Alias, Tab) of
              {ok, Ref1} -> Ref1;
              {error, _} -> undefined
          end,
    {ok, update_state(Ref, Alias, Tab, Type, RdbOpts, #st{})}.

update_state(Ref, Alias, Tab, Type, _RdbOpts, St) ->
    St#st{ tab = Tab
         , alias = Alias
         , type = Type
         , ref = maybe_set_ref_mode(Ref)
         }.

maybe_set_ref_mode(Ref) when is_map(Ref) ->
    Ref#{mode => mnesia};
maybe_set_ref_mode(Ref) ->
    Ref.

handle_call({create_table, Tab, Props}, _From,
            #st{alias = Alias, tab = Tab} = St) ->
    case mnesia_rocksdb_admin:create_table(Alias, Tab, Props) of
        {ok, Ref} ->
            {reply, ok, St#st{ref = maybe_set_ref_mode(Ref)}};
        Other ->
            {reply, Other, St}
    end;
handle_call({load_table, _LoadReason, _Opts}, _From,
            #st{alias = Alias, tab = Tab} = St) ->
    ok = mnesia_rocksdb_admin:load_table(Alias, Tab),
    {reply, ok, St};
handle_call({write_info, Key, Value}, _From, #st{ref = Ref} = St) ->
    mrdb:write_info(Ref, Key, Value),
    {reply, ok, St};
handle_call({update_counter, C, Incr}, _From, #st{ref = Ref} = St) ->
    {reply, mrdb:update_counter(Ref, C, Incr), St};
handle_call({insert, Obj}, _From, St) ->
    Res = do_insert(Obj, St),
    {reply, Res, St};
handle_call({delete, Key}, _From, St) ->
    Res = do_delete(Key, St),
    {reply, Res, St};
handle_call({match_delete, Pat}, _From, #st{ref = Ref} = St) ->
    Res = mrdb:match_delete(Ref, Pat),
    {reply, Res, St};
handle_call(close_table, _From, #st{alias = Alias, tab = Tab} = St) ->
    _ = mnesia_rocksdb_admin:close_table(Alias, Tab),
    {reply, ok, St#st{ref = undefined}};
handle_call(delete_table, _From, #st{alias = Alias, tab = Tab} = St) ->
    ok = mnesia_rocksdb_admin:delete_table(Alias, Tab),
    {stop, normal, ok, St#st{ref = undefined}}.

handle_cast(_, St) ->
    {noreply, St}.

handle_info({'EXIT', _, _} = _EXIT, St) ->
    ?dbg("rocksdb owner received ~p~n", [_EXIT]),
    {noreply, St};
handle_info(_, St) ->
    {noreply, St}.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, _St) ->
    ok.


%% ----------------------------------------------------------------------------
%% GEN SERVER PRIVATE
%% ----------------------------------------------------------------------------

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

%% server-side end of insert/3.
do_insert(Obj, #st{ref = Ref} = St) ->
    return_catch(fun() -> db_insert(Ref, Obj, [], St) end).

%% server-side part
do_delete(Key, #st{ref = Ref} = St) ->
    return_catch(fun() -> db_delete(Ref, Key, [], St) end).

proc_name(_Alias, Tab) ->
    list_to_atom("mnesia_ext_proc_" ++ mnesia_rocksdb_lib:tabname(Tab)).

whereis_proc(Alias, Tab) ->
    whereis(proc_name(Alias, Tab)).

%% ----------------------------------------------------------------------------
%% Db wrappers
%% ----------------------------------------------------------------------------

return_catch(F) when is_function(F, 0) ->
    try F()
    catch
        throw:badarg ->
            badarg
    end.

db_insert(Ref, Obj, Opts, St) ->
    write_result(mrdb:insert(Ref, Obj, Opts), insert, [Ref, Obj, Opts], St).

db_delete(Ref, K, Opts, St) ->
    write_result(mrdb:delete(Ref, K, Opts), delete, [Ref, K, Opts], St).

write_result(ok, _, _, _) ->
    ok.

%% ----------------------------------------------------------------------------
%% COMMON PRIVATE
%% ----------------------------------------------------------------------------
