%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
-module(mnesia_rocksdb_admin).

-behaviour(gen_server).

-export([ ensure_started/0
        , add_aliases/1
        , remove_aliases/1
        , create_table/3           %% (Alias, Name, Props) -> {ok, Ref} | error()
        , delete_table/2           %% (Alias, Name) -> ok
        , load_table/3             %% (Alias, Name, Props) -> ok
        , related_resources/2      %% (Alias, Name) -> [RelatedTab]
        , prep_close/2             %% (Alias, Tab) -> ok
        , get_ref/1                %% (Name) -> Ref | abort()
        , get_ref/2                %% (Name, Default -> Ref | Default
        , request_ref/2            %% (Alias, Name) -> Ref
        , close_table/2
        , clear_table/1
        ]).

-export([ migrate_standalone/2
        , migrate_standalone/3 ]).

-export([ start_link/0
        , init/1
        , handle_info/2
        , handle_call/3
        , handle_cast/2
        , terminate/2
        , code_change/3 ]).

-export([ read_info/1            %% (TRec)
        , read_info/2            %% (Alias, Tab)
        , read_info/4            %% (Alias, Tab, Key, Default)
        , write_info/4           %% (Alias, Tab, Key, Value)
        , delete_info/3          %% (Alias, Tab, Key)
        , write_table_property/3 %% (Alias, Tab, Property)
        ]).

-export([ meta/0
        , get_cached_env/2
        , set_and_cache_env/2 ]).

-include("mnesia_rocksdb.hrl").
-include("mnesia_rocksdb_int.hrl").
-include_lib("hut/include/hut.hrl").

-record(st, {
              backends     = #{} :: #{ alias() => backend() }
            , standalone   = #{} :: #{{alias(), table()} := cf() }
            , default_opts = []  :: [{atom(), _}]
            }).

-type st() :: #st{}.

-type alias()   :: atom().
-type tabname() :: atom().
-type table()   :: tabname()
                 | {admin, alias()}
                 | {tabname(), index, any()}
                 | {tabname(), retainer, any()}.

-type backend() :: #{ db_ref := db_ref()
                    , cf_info := #{ table() := cf() }
                    }.
-type db_ref() :: rocksdb:db_handle().
-type properties() :: [{atom(), any()}].

-type cf() :: mrdb:db_ref().

-type rpt() :: undefined | map().

-type req() :: {create_table, table(), properties()}
             | {delete_table, table()}
             | {load_table, table(), properties()}
             | {related_resources, table()}
             | {get_ref, table()}
             | {add_aliases, [alias()]}
             | {write_table_property, tabname(), tuple()}
             | {remove_aliases, [alias()]}
             | {migrate, [{tabname(), map()}], rpt()}
             | {prep_close, table()}
             | {close_table, table()}
             | {clear_table, table() | cf() }.

-type reason() :: any().
-type reply()  :: any().
-type gen_server_reply() :: {reply, reply(), st()}
                          | {stop, reason(), reply(), st()}.

-type gen_server_noreply() :: {noreply, st()}
                            | {stop, reason(), st()}.

-define(PT_KEY, {mnesia_rocksdb, meta}).

-spec ensure_started() -> ok.
ensure_started() ->
    case whereis(?MODULE) of
        undefined ->
            do_start();
        _ ->
            ok
    end.

do_start() ->
    stick_rocksdb_dir(),
    application:ensure_all_started(mnesia_rocksdb),
    case mnesia_ext_sup:start_proc(?MODULE, ?MODULE, start_link, [],
                                   [ {restart, permanent}
                                   , {shutdown, 10000}
                                   , {type, worker}
                                   , {modules, [?MODULE]} ]) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok
    end.

put_pt(Name, Value) ->
    Meta = meta(),
    persistent_term:put(?PT_KEY, Meta#{Name => Value}).

put_pts_map(PTs) ->
    Meta = maps:merge(meta(), PTs),
    persistent_term:put(?PT_KEY, Meta).

erase_pt(Name) ->
    Meta = meta(),
    persistent_term:put(?PT_KEY, maps:remove(Name, Meta)).

%% Avoid multiple updates to persistent terms, since each will trigger
%% a gc.
erase_pt_list(Names) ->
    Meta = meta(),
    persistent_term:put(?PT_KEY, maps:without(Names, Meta)).

check_application_defaults(Meta) ->
    Value = application:get_env(mnesia_rocksdb, mnesia_compatible_aborts, false),
    Meta#{ {mnesia_compatible_aborts} => Value }.

get_cached_env(Key, Default) ->
    maps:get({Key}, meta(), Default).

set_and_cache_env_(Key, Value) when is_atom(Key) ->
    Meta = meta(),
    Prev = maps:get({Key}, Meta, undefined),
    application:set_env(mnesia_rocksdb, Key, Value),
    persistent_term:put(?PT_KEY, Meta#{{Key} => Value}),
    Prev.

maybe_initial_meta() ->
    case persistent_term:get(?PT_KEY, undefined) of
       undefined ->
           M = check_application_defaults(#{}),
           persistent_term:put(?PT_KEY, M),
           M;
       M when is_map(M) ->
           M
    end.

meta() ->
    persistent_term:get(?PT_KEY, #{}).

prep_close(Alias, Tab) when is_atom(Tab) ->
    call(Alias, {prep_close, Tab});
prep_close(_, _) ->
    ok.

get_pt(Name, Default) ->
    maps:get(Name, meta(), Default).

set_and_cache_env(Key, Value) ->
    gen_server:call(?MODULE, {set_and_cache_env, Key, Value}).

create_table(Alias, Name, Props) ->
    call(Alias, {create_table, Name, Props}).

-spec delete_table(alias(), tabname()) -> ok.
delete_table(Alias, Name) ->
    call(Alias, {delete_table, Name}).

load_table(Alias, Name, Props) ->
    call(Alias, {load_table, Name, Props}).

related_resources(Alias, Name) ->
    if is_atom(Name) ->
            call(Alias, {related_resources, Name});
       true ->
            []
    end.

get_ref(Name) ->
    case get_ref(Name, error) of
        error ->
            mnesia:abort({bad_type, Name});
        Other ->
            Other
    end.

get_ref(Name, Default) ->
    get_pt(Name, Default).

request_ref(Alias, Name) ->
    call(Alias, {get_ref, Name}).

close_table(Alias, Name) ->
    call(Alias, {close_table, Name}).

clear_table(#{alias := Alias, name := Name}) ->
    case Name of
       {admin, _} -> mnesia:abort({bad_type, Name});
       {_}        -> mnesia:abort({no_exists, Name});
       _ ->
           call(Alias, {clear_table, Name})
    end;
clear_table(Name) ->
    clear_table(get_ref(Name)).

add_aliases(Aliases) ->
    call([], {add_aliases, Aliases}).

remove_aliases(Aliases) ->
    call([], {remove_aliases, Aliases}).

read_info(Alias, Tab, K, Default) ->
    read_info_(get_ref({admin, Alias}), Tab, K, Default).

read_info(Alias, Tab) ->
    read_all_info_(get_ref({admin, Alias}), Tab).

read_info(#{alias := _, name := Tab} = TRec) ->
    read_all_info_(TRec, Tab).

read_all_info_(ARef, Tab) ->
    Pat = [{ {{info,Tab,'$1'},'$2'}, [], [{{'$1','$2'}}] }],
    mrdb_select:select(ARef, Pat, infinity).

read_info_(ARef, Tab, K, Default) ->
    EncK = mnesia_rocksdb_lib:encode_key({info, Tab, K}, sext),
    get_info_res(mrdb:rdb_get(ARef, EncK, []), Default).

get_info_res(Res, Default) ->
    case Res of
        not_found ->
            Default;
        {ok, Bin} ->
            %% no fancy tricks when encoding/decoding info values
            binary_to_term(Bin);
        {error, E} ->
            error(E)
    end.

%% Admin info: metadata written by the admin proc to keep track of
%% the derived status of tables (such as detected version and encoding
%% of existing standalone tables.)
%%
write_admin_info(K, V, Alias, Name) ->
    mrdb:rdb_put(get_ref({admin, Alias}),
                 admin_info_key(K, Name),
                 term_to_binary(V)).

read_admin_info(K, Alias, Name) ->
    EncK = admin_info_key(K, Name),
    case mrdb:rdb_get(get_ref({admin,Alias}), EncK) of
        {ok, Bin} ->
            {ok, binary_to_term(Bin)};
        _ ->
            error
    end.

delete_admin_info(K, Alias, Name) ->
    EncK = admin_info_key(K, Name),
    mrdb:rdb_delete(get_ref({admin, Alias}), EncK).

admin_info_key(K, Name) ->
    mnesia_rocksdb_lib:encode_key({admin_info, Name, K}, sext).

%% Table metadata info maintained by users
%%
write_info(Alias, Tab, K, V) ->
    write_info_(get_ref({admin, Alias}), Tab, K, V).

write_info_(Ref, Tab, K, V) ->
    write_info_encv(Ref, Tab, K, term_to_binary(V)).

write_info_encv(Ref, Tab, K, V) ->
    EncK = mnesia_rocksdb_lib:encode_key({info,Tab,K}, sext),
    maybe_write_standalone_info(Ref, K, V),
    mrdb:rdb_put(Ref, EncK, V, []).

delete_info(Alias, Tab, K) ->
    delete_info_(get_ref({admin, Alias}), Tab, K).

delete_info_(Ref, Tab, K) ->
    EncK = mnesia_rocksdb_lib:encode_key({info, Tab, K}, sext),
    maybe_delete_standalone_info(Ref, K),
    mrdb:rdb_delete(Ref, EncK, []).

maybe_write_standalone_info(Ref, K, V) ->
    case Ref of
        #{type := standalone, vsn := 1, db_ref := DbRef} ->
            EncK = mnesia_rocksdb_lib:encode_key(K, sext),
            Key = <<?INFO_TAG, EncK/binary>>,
            EncV = mnesia_rocksdb_lib:encode_val(V, term),
            rocksdb:put(DbRef, Key, EncV, []);
        _ ->
            ok
    end.

maybe_delete_standalone_info(Ref, K) ->
    case Ref of
        #{type := standalone, vsn := 1, db_ref := DbRef} ->
            EncK = mnesia_rocksdb_lib:encode_key(K, sext),
            Key = <<?INFO_TAG, EncK/binary>>,
            rocksdb:delete(DbRef, Key, []);
        _ ->
            ok
    end.

write_table_property(Alias, Tab, Prop) when is_tuple(Prop), size(Prop) >= 1 ->
    call(Alias, {write_table_property, Tab, Prop}).

migrate_standalone(Alias, Tabs) ->
    migrate_standalone(Alias, Tabs, undefined).

migrate_standalone(Alias, Tabs, Rpt0) ->
    Rpt = case Rpt0 of
              undefined -> undefined;
              To -> #{to => To, tag => migrate_standalone}
          end,
    call(Alias, {migrate, Tabs, Rpt}).

-spec call(alias() | [], req()) -> no_return() | any().
call(Alias, Req) ->
    call(Alias, Req, infinity).

call(Alias, Req, Timeout) ->
    case gen_server:call(?MODULE, {Alias, Req}, Timeout) of
        {abort, Reason} ->
            mnesia:abort(Reason);
        {error, {mrdb_abort, Reason}} ->
            mnesia:abort(Reason);
        Reply ->
            Reply
    end.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    _ = maybe_initial_meta(),  %% bootstrap pt
    Opts = default_opts(),
    process_flag(trap_exit, true),
    ensure_cf_cache(),
    mnesia:subscribe({table, schema, simple}),
    {ok, recover_state(#st{default_opts = Opts})}.

recover_state(St0) ->
    Meta = maps:to_list(meta()),
    {Admins, Tabs} = lists:partition(fun is_admin/1, Meta),
    recover_tabs(Tabs, recover_admins(Admins, St0)).

is_admin({{admin,_},_}) -> true;
is_admin(_            ) -> false.

recover_admins(Admins, St) ->
    lists:foldl(fun recover_admin/2, St, Admins).

recover_admin({{admin,Alias} = T, #{db_ref := DbRef,
                                    cf_handle := CfH,
                                    mountpoint := MP} = R},
              #st{backends = Backends} = St) ->
    case cf_is_accessible(DbRef, CfH) of
        true ->
            B = #{cf_info => #{T => R},
                  db_ref => DbRef,
                  mountpoint => MP},
            St#st{backends = Backends#{Alias => B}};
        false ->
            error({cannot_access_alias_db, Alias})
    end.

recover_tabs(Tabs, St) ->
    lists:foldl(fun recover_tab/2, St, Tabs).

recover_tab({T, #{db_ref := DbRef,
                  cf_handle := CfH,
                  alias := Alias} = R}, St) ->
    case cf_is_accessible(DbRef, CfH) of
        true ->
            update_cf(Alias, T, R, St);
        false ->
            error({cannot_access_table, T})
    end;
recover_tab(_, St) ->
    St.


%% TODO: generalize
get_aliases() ->
    %% Return a list of registered aliases paired with alias-specific options
    [{rocksdb_copies, []}].

default_opts() ->
    %% TODO: make this configurable
    [].

alias_opts(Alias) ->
    %% TODO: User should have a way to configure rocksdb options for the admin db
    %% of a user-defined alias.
    proplists:get_value(Alias, get_aliases(), []).

maybe_load_admin_db({Alias, Opts}, #st{backends = Bs} = St) ->
    case maps:find(Alias, Bs) of
        {ok, #{db_ref := _}} ->
            %% already open
            St;
        error ->
            try_load_admin_db(Alias, Opts, St)
    end.

try_load_admin_db(Alias, AliasOpts, #st{ backends = Bs
                                       , default_opts = DefaultOpts} = St) ->
    case load_admin_db(Alias, AliasOpts ++ DefaultOpts, St) of
        {ok, #{cf_info := CfI0, mountpoint := MP} = AdminDb} ->
            %% We need to store the persistent ref explicitly here,
            %% since mnesia knows nothing of our admin table.
            AdminTab = {admin, Alias},
            Stats = mrdb_stats:new(),
            CfI = update_cf_info(AdminTab, #{ status => open
                                            , name => AdminTab
                                            , vsn => ?VSN
                                            , encoding => {sext,{value,term}}
                                            , attr_pos => #{key => 1,
                                                            value => 2}
                                            , stats => Stats
                                            , mountpoint => MP
                                            , properties =>
                                                #{ attributes => [key, val]
                                                 }}, CfI0),
            PTs = maps:filter(
                    fun(T, _) ->
                            case T of
                                {admin,_}   -> true;
                                {ext, _, _} -> true;
                                _ -> false
                            end
                    end, CfI),
            put_pts_map(PTs),
            Bs1 = Bs#{Alias => AdminDb#{cf_info => CfI}},
            St#st{backends = Bs1};
        {error, _} = Error ->
            mnesia_lib:fatal("Cannot load admin db for alias ~p: ~p~n",
                             [Alias, Error])
    end.

-spec handle_call({alias(), req()}, any(), st()) -> gen_server_reply().
handle_call({set_and_cache_env, Key, Value}, _From, St) ->
    Res = set_and_cache_env_(Key, Value),
    {reply, Res, St};
handle_call({[], {add_aliases, Aliases}}, _From, St) ->
    St1 = do_add_aliases(Aliases, St),
    {reply, ok, St1};
handle_call({[], {remove_aliases, Aliases}}, _From, St) ->
    St1 = do_remove_aliases(Aliases, St),
    {reply, ok, St1};
handle_call({Alias, Req}, _From, St) ->
    handle_call_for_alias(Alias, Req, St);
handle_call(_Req, _From, St) ->
    {reply, {error, unknown_request}, St}.

-spec handle_cast(any(), st()) -> gen_server_noreply().
handle_cast(_Msg, St) ->
    {noreply, St}.

-spec handle_info(any(), st()) -> gen_server_noreply().
handle_info({mnesia_table_event, Event}, St) ->
    case Event of
        {write, {schema, Tab, Props}, _} ->
            case find_cf(Tab, St) of
                error ->
                    {noreply, St};
                #{} = Cf ->
                    case try_refresh_cf(Cf, Props, St) of
                        false ->
                            {noreply, St};
                        {true, NewCf, St1} ->
                            maybe_update_pt(Tab, NewCf),
                            {noreply, St1}
                    end
            end;
       _ ->
            {noreply, St}
    end;
handle_info(_Msg, St) ->
    {noreply, St}.

terminate(shutdown, St) ->
    close_all(St),
    ok;
terminate(_, _) ->
    ok.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

-spec handle_call_for_alias(alias(), req(), st()) -> gen_server_reply().
handle_call_for_alias(Alias, Req, #st{backends = Backends} = St) ->
    case maps:find(Alias, Backends) of
        {ok, Backend} ->
            try handle_req(Alias, Req, Backend, St)
            catch
                error:E:ST ->
                    io:fwrite(standard_io, "CAUGHT error:~p / ~p~n",
                              [E, ST]),
                    {reply, {error, E}, St}
            end;
        error ->
            {reply, {error, unknown_alias}, St}
    end.

do_add_aliases(Aliases, St) ->
    New = [{A, alias_opts(A)} || A <- Aliases,
                                 not is_open(A, St)],
    lists:foldl(fun maybe_load_admin_db/2, St, New).

do_remove_aliases(Aliases, #st{backends = Bs} = St) ->
    Known = intersection(Aliases, maps:keys(Bs)),
    lists:foldl(fun remove_admin_db/2, St, Known).

intersection(A, B) ->
    A -- (A -- B).

-spec handle_req(alias(), req(), backend(), st()) -> gen_server_reply().
handle_req(Alias, {create_table, Name, Props}, Backend, St) ->
    case find_cf(Alias, Name, Backend, St) of
        {ok, TRec} ->
            {reply, {ok, TRec}, St};
        error ->
            case create_trec(Alias, Name, Props, Backend, St) of
                {ok, NewCf} ->
                    St1 = update_cf(Alias, Name, NewCf, St),
                    {reply, {ok, NewCf}, maybe_update_main(Alias, Name, create, St1)};
                {error, _} = Error ->
                    {reply, Error, St}
            end
    end;
handle_req(Alias, {load_table, Name, Props}, Backend, St) ->
    try
    case find_cf(Alias, Name, Backend, St) of
        {ok, #{status := open} = TRec} ->
            {reply, {ok, TRec}, St};
        {ok, #{status := created} = TRec} ->
            handle_load_table_req(Alias, Name, TRec, Backend, St);
        _ ->
            case create_trec(Alias, Name, Props, Backend, St) of
                {ok, NewCf} ->
                    St1 = update_cf(Alias, Name, NewCf, St),
                    Backend1 = maps:get(Alias, St1#st.backends),
                    {ok, TRec} = find_cf(Alias, Name, Backend1, St1),
                    handle_load_table_req(Alias, Name, TRec, Backend1, St1);
                {error, _} = Error ->
                    {reply, {abort, Error}, St}
            end
    end
    catch
        error:E:ST ->
            ?log(error, "CAUGHT error:~p (Tab: ~p) / ~p", [E, Name, ST]),
            {reply, {error, E}, St}
    end;
handle_req(_Alias, {prep_close, Name}, Backend, St) ->
    ok_reply(do_prep_close(Name, Backend, St), St);
handle_req(Alias, {close_table, Name}, Backend, St) ->
    ok_reply(do_close_table(Alias, Name, Backend, St), St);
handle_req(Alias, {delete_table, Name}, Backend, St) ->
    case do_delete_table(Alias, Name, Backend, St) of
        {ok, St1} ->
            {reply, ok, maybe_update_main(Alias, Name, delete, St1)};
        {error, not_found} ->
            {reply, ok, St}
    end;
handle_req(Alias, {clear_table, Name}, Backend, #st{} = St) ->
    case find_cf(Alias, Name, Backend, St) of
        {ok, #{ status := open
              , type := column_family
              , db_ref := DbRef
              , cf_handle := CfH} = Cf} ->
            CfName = tab_to_cf_name(Name),
            ok = rocksdb:drop_column_family(DbRef, CfH),
            {ok, CfH1} = rocksdb:create_column_family(DbRef, CfName, cfopts(rocksdb_opts_from_trec(Cf))),
            ok = rocksdb:destroy_column_family(DbRef, CfH),
            Cf1 = Cf#{cf_handle := CfH1},
            St1 = update_cf(Alias, Name, Cf1, St),
            put_pt(Name, Cf1),
            {reply, ok, St1};
        _ ->
            {reply, {error, not_found}, St}
    end;
handle_req(Alias, {get_ref, Name}, Backend, #st{} = St) ->
    case find_cf(Alias, Name, Backend, St) of
        {ok, #{status := open} = Ref} ->
            {reply, {ok, Ref}, St};
        {ok, _} ->      % not open - treat as not_found
            {reply, {error, not_found}, St};
        error ->
            {reply, {error, not_found}, St}
    end;
handle_req(_Alias, {related_resources, Tab}, Backend, St) ->
    Res = get_related_resources(Tab, Backend),
    {reply, Res, St};
handle_req(Alias, {write_table_property, Tab, Prop}, Backend, St) ->
    case find_cf(Alias, Tab, Backend, St) of
        {ok, #{status := opens} = Cf0} ->
            case mnesia_schema:schema_transaction(
                   fun() ->
                           erase_pt(Tab),
                           Cf = update_user_properties(Prop, Cf0),
                           St1 = update_cf(Alias, Tab, Cf, St),
                           put_pt(Tab, Cf),
                           St1
                   end) of
                {atomic, NewSt} ->
                    {reply, ok, NewSt};
                {aborted, _} ->
                    {reply, {error, badarg}, St}
            end;
        _ ->
            {reply, {error, not_found}, St}
    end;
handle_req(Alias, {migrate, Tabs0, Rpt}, Backend, St) ->
    case prepare_migration(Alias, Tabs0, Rpt, St) of
        {ok, Tabs} ->
            {Res, St1} = do_migrate_tabs(Alias, Tabs, Backend, Rpt, St),
            {reply, Res, St1};
        {error, _} = Error ->
            {reply, Error, St}
    end.

handle_load_table_req(Alias, Name, TRec, Backend, St) ->
    case create_table_from_trec(Alias, Name, TRec, Backend, St) of
        {ok, TRec1, St1} ->
            TRec2 = TRec1#{status => open},
            St2 = update_cf(Alias, Name, TRec2, St1),
            ?log(debug, "Table loaded ~p", [Name]),
            put_pt(Name, TRec2),
            {reply, {ok, TRec2}, St2};
        {error, _} = Error ->
            {reply, Error, St}
    end.

%% if an index table has been created or deleted, make sure the main
%% ref reflects it.
maybe_update_main(Alias, {Main, index, I}, Op, St) ->
    case find_cf_from_state(Alias, Main, St) of
        {ok, #{properties := #{index := Index} = Props} = CfM} ->
            case {Op, lists:member(I, Index)} of
                {delete, true} ->
                    CfM1 = CfM#{properties => Props#{index => Index -- [I]}},
                    delete_info(Alias, Main, {index_consistent, I}),
                    maybe_update_pt(Main, CfM1),
                    update_cf(Alias, Main, CfM1, St);
                {create, _} ->
                    %% Due to a previous bug, this marker might linger
                    %% In any case, it mustn't be there for a newly created index
                    delete_info(Alias, Main, {index_consistent, I}),
                    St;
                _ ->
                    St
            end;
        _ ->
            %% Might happen, perhaps. Don't worry about it here
            St
    end;
maybe_update_main(_, _, _, St) ->
    St.


%% The pt may not have been created yet. If so, don't do it here.
maybe_update_pt(Name, Ref) ->
    case get_pt(Name, error) of
        error ->
            ok;
        _Old ->
            put_pt(Name, Ref)
    end.

ok_reply({ok, St}, _) ->
    {reply, ok, St};
ok_reply({error, _} = Error, St) ->
    {reply, {abort, Error}, St}.

get_related_resources(Tab, #{cf_info := CfInfo}) ->
    F = fun(K, _, Acc) ->
                acc_related_to(Tab, K, Acc)
        end,
    maps:fold(F, [], CfInfo).

acc_related_to(T, {T, _, _} = Tab, Acc) ->
    [Tab | Acc];
acc_related_to(_, _, Acc) ->
    Acc.

update_cf(Alias, Name, Cf, #st{backends = Bs} = St) ->
    #{cf_info := CfI} = B = maps:get(Alias, Bs),
    CfI1 = update_cf_info(Name, Cf, CfI),
    St#st{backends = Bs#{Alias => B#{cf_info => CfI1}}}.

update_cf_info(Name, Cf, CfI) ->
    Cf1 = case maps:find(Name, CfI) of
              {ok, Cf0} ->
                  maps:merge(Cf0, Cf);
              error ->
                  Cf
          end,
    CfI#{Name => Cf1}.

delete_cf(Alias, Name, #st{backends = Bs} = St) ->
    #{cf_info := CfI} = B = maps:get(Alias, Bs),
    CfI1 = maps:remove(Name, CfI),
    St#st{backends = Bs#{Alias => B#{cf_info => CfI1}}}.

find_cf_from_state(Alias, Name, #st{backends = Backends} = St) ->
    case maps:find(Alias, Backends) of
        {ok, Backend} ->
            find_cf(Alias, Name, Backend, St);
        error ->
            {error, not_found}
    end.

find_cf(Name, #st{backends = Backends}) ->
    maps:fold(
      fun(_Alias, #{cf_info := CfI}, Acc) ->
              case maps:find(Name, CfI) of
                  {ok, Cf} -> Cf;
                  error    -> Acc
              end
      end, error, Backends).

find_cf(Alias, Name, #{cf_info := CfI}, #st{standalone = Ts}) ->
    case maps:find(Name, CfI) of
        {ok, _} = Ok ->
            Ok;
        error ->
            maps:find({Alias, Name}, Ts)
    end.

get_table_mountpoint(Alias, Name, #st{standalone = Ts}) ->
    case maps:find({Alias, Name}, Ts) of
        {ok, #{mountpoint := MP}} ->
            {ok, MP};
        _ ->
            error
    end.

cf_is_accessible(DbRef, CfH) ->
    try _ = estimated_num_keys(DbRef, CfH),
        true
    catch
        error:_ -> false
    end.

-dialyzer({nowarn_function, estimated_num_keys/2}).
estimated_num_keys(DbRef, CfH) ->
    case rocksdb:get_property(DbRef, CfH, <<"rocksdb.estimate-num-keys">>) of
        {error, _} -> 0;
        {ok, Bin} ->
            %% return value mis-typed in rocksdb as string()
            binary_to_integer(Bin)
    end.

create_trec(Alias, Name, Props, Backend, St) ->
    %% io:fwrite("do_create_table(~p, ~p, ~p, ~p)~n", [Alias, Name, Backend, St]),
    %% TODO: we're doing double-checking here
    case find_cf(Alias, Name, Backend, St) of
        {ok, #{status := open}} ->
            {error, exists};
        {ok, TRec0} ->
            do_create_trec(Alias, Name, Props, TRec0, St);
        _Other ->
            do_create_trec(Alias, Name, Props, #{}, St)
    end.

do_create_trec(Alias, Name, Props, TRec0, #st{} = St) ->
    Type = case rdb_opt_standalone(Props) of
               true -> standalone;
               false -> column_family
           end,
    PMap = props_to_map(Name, Props),
    {ok, maybe_map_retainer(
           Alias, Name,
           maybe_map_index(
             Alias, Name,
             maybe_map_attrs(
               TRec0#{ semantics   => semantics(Name, PMap)
                     , name        => Name
                     , type        => Type
                     , alias       => Alias
                     , properties  => PMap
                     , status      => created })), St)}.

create_table_from_trec(Alias, Name, #{cf_handle := CfH, db_ref := DbRef} = R,
                       _Backend, St) ->
    case cf_is_accessible(DbRef, CfH) of
        true ->
            R1 = check_version_and_encoding(R),
            {ok, R1, update_cf(Alias, Name, R1, St)};
        false ->
            {stop, {cf_not_accessible, {Alias, Name}}, St}
    end;
create_table_from_trec(Alias, Name, #{type := column_family} = TRec,
                       #{db_ref := DbRef} = Backend, St) ->
    case should_we_migrate_standalone(TRec) of
        false ->
            create_table_as_cf(Alias, Name, TRec#{db_ref => DbRef}, St);
        {false, MP} ->
            create_table_as_standalone(Alias, Name, true, MP, TRec, St);
        {true, MP} ->
            ?log(debug, "will create ~p as standalone and migrate", [Name]),
            case create_table_as_standalone(Alias, Name, false, MP, TRec, St) of
                {ok, OldTRec, _} ->
                    ?log(info, "Migrating ~p to column family", [Name]),
                    create_cf_and_migrate(Alias, Name, OldTRec, TRec, Backend, undefined, St);
                _Other ->
                    create_table_as_cf(Alias, Name, TRec#{db_ref => DbRef}, St)
            end
   end;
create_table_from_trec(Alias, Name, #{type := standalone} = TRec, _, St) ->
    {Exists, MP} = table_exists_as_standalone(Name),
    create_table_as_standalone(Alias, Name, Exists, MP, TRec, St).

create_cf_and_migrate(Alias, Name, OldTRec, TRec, #{db_ref := DbRef}, Rpt, St) ->
    ?log(debug, "Migrate to cf (~p)", [Name]),
    {ok, NewCf, St1} = create_table_as_cf(
                         Alias, Name, TRec#{db_ref => DbRef}, St),
    {ok, St2} = migrate_standalone_to_cf(OldTRec, NewCf, Rpt, St1),
    {ok, NewCf, St2}.

%% Return {Migrate, MP} iff table exists standalone; just false if it doesn't
should_we_migrate_standalone(#{name := Name}) ->
    case table_exists_as_standalone(Name) of
        {true, MP} ->
            case auto_migrate_to_cf(Name) of
                true ->
                    {true, MP};
                false ->
                    {false, MP}
            end;
        {false, _} ->
            false
    end.

prepare_migration(Alias, Tabs, Rpt, St) ->
    Res = lists:map(fun(T) ->
                            prepare_migration_(Alias, T, St)
                    end, Tabs),
    Res1 = add_related_tabs(Res, maps:get(Alias, St#st.backends), Alias, St),
    case [E || {error, _} = E <- Res1] of
        [] ->
            rpt(Rpt, "Will migrate ~p~n", [[T || {T,_,_} <- Res1]]),
            {ok, Res1};
        [_|_] = Errors ->
            rpt(Rpt, "Errors encountered: ~p~n", [Errors]),
            {error, Errors}
    end.

rpt(Rpt, Fmt, Args) ->
    rpt(Rpt, erlang:system_time(millisecond), Fmt, Args).

rpt(undefined, _, _, _) -> ok;
rpt(#{to := Rpt} = R, Time, Fmt, Args) ->
    Rpt ! {mnesia_rocksdb, report, R#{time => Time, fmt => Fmt, args => Args}},
    ok.

maybe_progress(#{to := To}, C) when C rem 100000 =:= 0 ->
    To ! {mnesia_rocksdb, report, progress};
maybe_progress(_, _) ->
    ok.

add_related_tabs(Ts, Backend, Alias, St) ->
    lists:flatmap(
      fun({error,_} = E) -> [E];
         ({T, _, _} = TI) ->
              [TI | [prepare_migration_(Alias, Rel, St)
                     || Rel <- get_related_resources(T, Backend)]]
      end, Ts).

prepare_migration_(Alias, T, #st{} = St) ->
    {TName, Opts} = case T of
                        {_, Map} when is_map(Map) -> T;
                        _ -> {T, #{}}
                    end,
    case find_cf_from_state(Alias, TName, St) of
        {ok, #{type := standalone} = TRec} ->
            TRec1 = apply_migration_opts(
                      Opts,
                      maps:without([db_ref, type, cf_info, cf_handle,
                                    encoding, mountpoint, status], TRec)),
            {TName, TRec, TRec1};
        {ok, _} ->
            {error, {not_standalone, TName}};
        error ->
            {error, {no_such_table, TName}}
    end.

do_migrate_tabs(Alias, Tabs, Backend, Rpt, St) ->
    lists:mapfoldl(fun(T, St1) ->
                        do_migrate_table(Alias, T, Backend, Rpt, St1)
                end, St, Tabs).

do_migrate_table(Alias, {Name, OldTRec, TRec0}, Backend, Rpt, St) when is_map(TRec0) ->
    T0 = erlang:system_time(millisecond),
    rpt(Rpt, T0, "Migrate ~p~n", [Name]),
    TRec = maps:without([encoding, vsn], TRec0),
    maybe_write_user_props(TRec),
    {ok, CF, St1} = create_cf_and_migrate(Alias, Name, OldTRec,
                                          TRec, Backend, Rpt, St),
    put_pt(Name, CF),
    T1 = erlang:system_time(millisecond),
    rpt(Rpt, T1, "~nDone (~p)~n", [Name]),
    Time = T1 - T0,
    io:fwrite("~p migrated, ~p ms~n", [Name, Time]),
    {{Name, {ok, Time}}, St1}.

migrate_standalone_to_cf(OldTRec, #{name := T, alias := Alias} = TRec,
                         Rpt, #st{standalone = Ts} = St) ->
    ChunkSz = chunk_size(TRec),
    KeyPos = mnesia_rocksdb_lib:keypos(T),
    migrate_to_cf(mrdb:select(OldTRec, [{'_',[],['$_']}], ChunkSz),
                  TRec, OldTRec, KeyPos, set_count(0, Rpt)),
    case maps:is_key({Alias,T}, Ts)
         andalso table_is_empty(OldTRec) of
        true ->
            St1 = close_and_delete_standalone(OldTRec, St),
            {ok, St1};
        false ->
            {ok, St}
    end.

migrate_to_cf({L, Cont}, Cf, DbRec, KeyPos, Rpt) ->
    Count0 = get_count(Rpt),
    Count = mrdb:as_batch(
              Cf,
              fun(New) ->
                      mrdb:as_batch(
                        DbRec,
                        fun(Old) ->
                                lists:foldl(
                                  fun(Obj, C) ->
                                          mrdb:insert(New, Obj),
                                          mrdb:delete(Old, element(KeyPos,Obj)),
                                          maybe_progress(Rpt, C),
                                          C + 1
                                  end, Count0, L)
                        end)
              end),
    migrate_to_cf(mrdb_select:select(Cont), Cf, DbRec, KeyPos, set_count(Count, Rpt));
migrate_to_cf('$end_of_table', _, _, _, _) ->
    ok.

get_count(undefined) ->
    0;
get_count(R) when is_map(R) ->
    maps:get({count}, R, 0).

set_count(_, undefined) ->
    undefined;
set_count(Count, R) when is_map(R) ->
    R#{{count} => Count}.

chunk_size(_) ->
    300.

maybe_write_user_props(#{name := T, properties := #{user_properties := UPMap}}) ->
    %% The UP map is #{Key => Prop}, where element(1, Prop) == Key
    UPs = maps:values(UPMap),
    SchemaProps = mnesia:table_info(T, user_properties),
    WritePs = props_to_write(UPs, SchemaProps),
    DelPs = props_to_delete(UPs, SchemaProps),
    case {WritePs, DelPs} of
        {[], []} -> ok;
        _ ->
            mnesia_schema:schema_transaction(
              fun() ->
                      [mnesia_schema:do_write_table_property(T, P)
                       || P <- WritePs],
                      [mnesia_schema:do_delete_table_property(T, K)
                       || K <- DelPs]
              end)
    end;
maybe_write_user_props(#{} = TRec) ->
    TRec.

props_to_write(UPs, SchemaProps) ->
    %% Include both new and modified
    [P || P <- UPs,
          not lists:member(P, SchemaProps)].

props_to_delete(UPs, SchemaProps) ->
    lists:filtermap(
      fun(P) ->
              K = element(1, P),
              case lists:keymember(K, 1, UPs) of
                  false -> {true, K};
                  true  -> false
              end
      end, SchemaProps).

apply_migration_opts(Opts, TRec) ->
    TRec1 = trec_without_user_prop(rocksdb_standalone, TRec),
    try maps:fold(fun apply_migration_opt/3, TRec1, Opts)
    catch
        throw:Error ->
            Error
    end.

apply_migration_opt(user_properties, UPs, #{properties := Props} = TRec) ->
    lists:foreach(
      fun(P) when is_tuple(P), size(P) >= 1 -> ok;
         (P) ->
              throw({error, {invalid_user_property, {tname(TRec), P}}})
      end, UPs),
    TRec#{properties => Props#{user_properties => UPs}};
apply_migration_opt(encoding, Enc0, #{properties := Props} = TRec) ->
    case mnesia_rocksdb_lib:check_encoding(Enc0, maps:get(attributes, Props)) of
        {ok, Enc} ->
            update_user_properties({mrdb_encoding, Enc}, TRec);
        {error, _} ->
            throw({error, {invalid_encoding, {tname(TRec), Enc0}}})
    end.

trec_without_user_prop(P, #{properties := #{user_properties := UPs} = Ps} = T) ->
    T#{properties := Ps#{user_properties := maps:remove(P, UPs)}};
trec_without_user_prop(_, TRec) ->
    TRec.

maybe_map_retainer(Alias, {MainTab, retainer, _}, #{properties := Ps0} = Map, St) ->
    {ok, #{properties := #{record_name := RecName}}} =
        find_cf_from_state(Alias, MainTab, St),
    Map#{properties => Ps0#{record_name => RecName}};
maybe_map_retainer(_, _, Map, _) ->
    Map.

maybe_map_index(Alias, {MainTab, index, {Pos, _IxType}}, Map) ->
    Storage = {ext, Alias, mnesia_rocksdb},
    Map#{ ix_vals_f => mnesia_index:index_vals_f(Storage, MainTab, Pos) };
maybe_map_index(_, _, Map) ->
    Map.

maybe_map_attrs(#{name := {_,retainer,_}} = R) ->
    R#{attr_pos => #{key => 2, val => 3}};
maybe_map_attrs(#{name := Name, properties := #{attributes := Attrs}} = R)
  when is_atom(Name) ->
    {AMap, _} = lists:foldl(
                  fun(A, {M, P}) ->
                          {M#{A => P}, P+1}
                  end, {#{}, 2}, Attrs),
    R#{attr_pos => AMap};
maybe_map_attrs(R) ->
    R.

rdb_opt_standalone(Props) ->
    (os:getenv("MRDB_LEGACY") == "true") orelse
        proplists:get_bool(
          rocksdb_standalone, proplists:get_value(user_properties, Props, [])).

auto_migrate_to_cf(Name) ->
    Tabs = application:get_env(mnesia_rocksdb, auto_migrate_to_cf, []),
    lists:member(Name, Tabs).

props_to_map(TabName, Props) when is_atom(TabName) ->
    #{user_properties := UPs} = PMap = maps:without([name, cookie, version],
                                                    maps:from_list(Props)),
    %% Note that user properties can have arity >= 1
    PMap#{user_properties => maps:from_list([{element(1,P), P} || P <- UPs])};
props_to_map({Tab,_,_}, _) ->
    #{main_table => Tab, attributes => [key, val]}.

try_refresh_cf(#{alias := Alias, name := Name, properties := Ps} = Cf, Props, St) ->
    PMap = props_to_map(Name, Props),
    case PMap =:= Ps of
        true -> false;
        false ->
            NewCf = maybe_map_attrs(Cf#{properties => PMap}),
            {true, NewCf, update_cf(Alias, Name, NewCf, St)}
    end;
try_refresh_cf(_, _, _) ->
    false.

update_user_properties(Prop, #{properties := Ps} = Cf) ->
    Key = element(1, Prop),
    UserProps = case maps:find(user_properties, Ps) of
                    {ok, UPs} -> UPs#{Key => Prop};
                    error     -> #{Key => Prop}
                end,
    Cf#{properties => Ps#{user_properties => UserProps}}.

semantics({_,index,_}   , _) -> ordered_set;
semantics({_,retainer,_}, _) -> set;
semantics(T, #{type := Type}) when is_atom(T) -> Type.

table_exists_as_standalone(Name) ->
    MP = mnesia_rocksdb_lib:data_mountpoint(Name),
    Exists = case file:read_link_info(MP) of
        {ok, _}    -> true;
        {error, _} -> false
    end,
    {Exists, MP}.

create_table_as_standalone(Alias, Name, Exists, MP, TRec, St) ->
    case create_table_as_standalone_(Alias, Name, Exists, MP, TRec, St) of
        {ok, #{type := standalone, vsn := Vsn1,
               encoding := Enc1} = Cf, _St1} = Ok ->
            write_admin_info(standalone_vsn_and_enc, {Vsn1, Enc1},
                             Alias, Name),
            case Vsn1 of
                1 ->
                    load_info(Alias, Name, Cf);
                _ ->
                    skip
            end,
            Ok;
        Other ->
            Other
    end.

create_table_as_standalone_(Alias, Name, Exists, MP, TRec, St) ->
    Vsn = check_version(TRec),
    TRec1 = TRec#{vsn => Vsn, encoding => get_encoding(Vsn, TRec)},
    do_open_standalone(true, Alias, Name, Exists, MP, TRec1, St).

do_open_standalone(CreateIfMissing, Alias, Name, Exists, MP, TRec0,
                   #st{standalone = Ts} = St) ->
    Opts = rocksdb_opts_from_trec(TRec0),
    case open_db_(MP, Alias, Opts, [], CreateIfMissing, Name, St) of
        {ok, #{ cf_info := CfI }} ->
            DbRec = maps:get({ext,Alias,"default"}, CfI),
            CfNames = maps:keys(CfI),
            DbRec1 = DbRec#{ cfs => CfNames,
                             mountpoint => MP },
            TRec = maps:merge(TRec0, DbRec#{type => standalone}),
            TRec1 = guess_table_vsn_and_encoding(Exists, TRec),
            {ok, TRec1, St#st{standalone = Ts#{{Alias, Name} => DbRec1}}};
        {error, _} = Err ->
            ?log(debug, "open_db error: ~p", [Err]),
            Err
    end.

%% When opening a standalone table, chances are it's a legacy table
%% where legacy encoding is already in place. We try to read the
%% first object and apply legacy encoding. If successful, we set
%% legacy encoding in the TRec. If we migrate the data to a column
%% family, we should apply the defined encoding for the cf.
%%
%% The first object can either be an info object (in the legacy case)
%% or a data object, with a sext-encoded key, and a term_to_binary-
%% encoded object as value, where the key position is set to [].
%% The info objects would be sext-encoded key + term-encoded value.
guess_table_vsn_and_encoding(false, TRec) ->
    TRec;
guess_table_vsn_and_encoding(true, #{properties := #{attributes := As},
                                     alias := Alias, name := Name} = R) ->
    case read_admin_info(standalone_vsn_and_enc, Alias, Name) of
        {ok, {V, E}} ->
            R#{vsn => V, encoding => E};
        error ->
            R1 = set_default_guess(R),
            mrdb:with_rdb_iterator(
              R1, fun(I) ->
                          guess_table_vsn_and_encoding_(
                            mrdb:rdb_iterator_move(I, first), I, As, R1)
                  end)
    end.

set_default_guess(#{type := standalone} = R) ->
    case application:get_env(mnesia_rocksdb, standalone_default_vsn, ?VSN) of
        1 ->
            R#{vsn => 1, encoding => {sext, {object, term}}};
        V ->
            R#{vsn => V}
    end.

guess_table_vsn_and_encoding_({ok, K, V}, _I, As, R) ->
    Arity = length(As) + 1,
    case K of
        <<?INFO_TAG, EncK/binary>> ->
            try _ = {mnesia_rocksdb_lib:decode(EncK, sext),
                     mnesia_rocksdb_lib:decode(V, term)},
                %% This is a vsn 1 standalone table
                R#{vsn => 1, encoding => {sext, {object, term}}}
            catch
                error:_ ->
                    R
            end;
        _ ->
            Enc = guess_obj_encoding(K, V, Arity),
            R#{encoding => Enc}
   end;
guess_table_vsn_and_encoding_(_Other, _, _, R) ->
    R.

guess_obj_encoding(K, V, Arity) ->
    {guess_key_encoding(K), guess_val_encoding(V, Arity)}.

guess_encoding(Bin) ->
    try {sext, sext:decode(Bin)}
    catch
        error:_ ->
            try {term, binary_to_term(Bin)}
            catch
                error:_ -> raw
            end
    end.

guess_key_encoding(Bin) ->
    case guess_encoding(Bin) of
        raw -> raw;
        {Enc, _} -> Enc
    end.

guess_val_encoding(Bin, Arity) ->
    case guess_encoding(Bin) of
        raw -> {value, raw};
        {Enc, Term} ->
            if is_tuple(Term), size(Term) == Arity,
               element(2, Term) == [] ->
                   {object, Enc};
               true ->
                   {value, Enc}
            end
    end.

%% This is slightly different from `rocksdb:is_empty/1`, since it allows
%% for the presence of some metadata, and still considers it empty if there
%% is no user data.
table_is_empty(#{} = DbRec) ->
    Start = iterator_data_start(DbRec),
    mrdb:with_rdb_iterator(
      DbRec, fun(I) ->
                     case mrdb:rdb_iterator_move(I, Start) of
                         {ok, _, _} -> false;
                         _          -> true
                     end
             end).

iterator_data_start(#{vsn := 1}) -> <<?DATA_START>>;
iterator_data_start(_)           -> first.

load_info(Alias, Name, Cf) ->
    ARef = get_ref({admin, Alias}),
    mrdb:with_rdb_iterator(
      Cf, fun(I) ->
                  load_info_(rocksdb:iterator_move(I, first), I, ARef, Name)
          end).

load_info_(Res, I, ARef, Tab) ->
    case Res of
        {ok, << ?INFO_TAG, K/binary >>, V} ->
            DecK = mnesia_rocksdb_lib:decode_key(K),
            case read_info_(ARef, Tab, DecK, undefined) of
                undefined ->
                    write_info_encv(ARef, Tab, DecK, V);
                <<131,_/binary>> = Value ->
                    %% Due to a previous bug, info values could be double-encoded with binary_to_term()
                    try binary_to_term(Value) of
                        _DecVal ->
                            %% We haven't been storing erlang-term encoded data as info,
                            %% so assume this is double-encoded and correct
                            write_info_encv(ARef, Tab, DecK, Value)
                    catch
                        error:_ ->
                            skip
                    end;
                _ ->
                    skip
            end,
            load_info_(rocksdb:iterator_move(I, next), I, ARef, Tab);
        _ ->
            ok
    end.

check_version(TRec) ->
    user_property(mrdb_version, TRec, ?VSN).

check_version_and_encoding(#{} = TRec) ->
    Vsn = check_version(TRec),
    Encoding = get_encoding(Vsn, TRec),
    set_access_type(TRec#{vsn => Vsn, encoding => Encoding}).

set_access_type(R) ->
    R#{access_type => access_type_(R)}.

access_type_(#{semantics := bag}) -> legacy;
access_type_(#{properties := #{user_properties := #{rocksdb_access_type := T}}}) ->
    valid_access_type(T);
access_type_(_) ->
    valid_access_type(application:get_env(mnesia_rocksdb, default_access_type, legacy)).

valid_access_type(T) when T==legacy; T==direct -> T;
valid_access_type(T) ->
    mrdb:abort({invalid_access_type, T}).

%% This access function assumes that the requested user property is
%% a 2-tuple. Mnesia allows user properties to be any non-empty tuple.
user_property(P, #{properties := #{user_properties := UPs}}, Default) ->
    case maps:find(P, UPs) of
        {ok, {_, V}} -> V;
        error        -> Default
    end;
user_property(_, _, Default) ->
    Default.

tname(#{name := Name}) -> Name.

get_encoding(1, _) -> {sext, {object, term}};
get_encoding(?VSN, TRec) ->
    case user_property(mrdb_encoding, TRec, undefined) of
        undefined ->
            default_encoding(TRec);
        E ->
            check_encoding(E, TRec)
    end.

default_encoding(#{name := Name, semantics := Sem,
                   properties := #{attributes := As}}) ->
    mnesia_rocksdb_lib:default_encoding(Name, Sem, As).

check_encoding(E, #{properties := #{attributes := As}}) ->
    case mnesia_rocksdb_lib:check_encoding(E, As) of
        {ok, Encoding} -> Encoding;
        _Error ->
            mrdb:abort(invalid_encoding)
    end.

rocksdb_opts_from_trec(TRec) ->
    user_property(rocksdb_opts, TRec, []).

create_table_as_cf(Alias, Name, #{db_ref := DbRef} = R, St) ->
    CfName = tab_to_cf_name(Name),
    case create_column_family(DbRef, CfName, cfopts(rocksdb_opts_from_trec(R)), R) of
        {ok, CfH} ->
            R1 = check_version_and_encoding(R#{ cf_handle => CfH
                                              , type => column_family }),
            {ok, R1, update_cf(Alias, Name, R1, St)};
        {error, _} = Error ->
            Error
    end.

create_column_family(DbRef, CfName, CfOpts, R) ->
    Res = case column_family_exists(CfName, R) of
              true ->
                  case find_active_cf_handle(DbRef, CfName) of
                      error ->
                          {error, {no_handle_for_existing_cf, CfName}};
                      {ok, _} = Ok ->
                          Ok
                  end;
              false ->
                  rocksdb:create_column_family(DbRef, CfName, CfOpts)
          end,
    maybe_note_active_cf(Res, CfName),
    Res.

column_family_exists(CfName, #{mountpoint := MP}) ->
    case rocksdb:list_column_families(MP, []) of
        {ok, CFs} ->
            lists:member(CfName, CFs);
        _ ->
            false
    end;
column_family_exists(CfName, #{alias := Alias}) ->
    case get_ref({admin, Alias}, error) of
        error ->
            false;
        Adm ->
            column_family_exists(CfName, Adm)
    end.

%% Column family handle caching ======================================================
%%
%% At least as far as I can tell, there is no way to query erlang-rocksdb for currently
%% active column family handles. This can become an issue e.g. during table migration
%% from standalone to column families, where the meta structures aren't updated until
%% after completed migration. A transient error during migration should be addressable
%% by simply retrying, but if the CF has already been created, and we've lost the handle,
%% there is no easy way to get it back. Unfortunately, if we start caching CFs, we also
%% need to garbage collect them.
%%

-define(CFH_CACHE, mnesia_rocksdb_cf_handle_cache).

ensure_cf_cache() ->
    case ets:info(?CFH_CACHE, name) of
        undefined ->
            ets:new(?CFH_CACHE, [ordered_set, public, named_table]);
        _ ->
            true
    end.

maybe_note_active_cf({ok, CfH}, CfName) ->
    ets:insert(?CFH_CACHE, {{CfName, CfH}});
maybe_note_active_cf(_, _) ->
    false.

find_active_cf_handle(DbRef, CfName) ->
    Candidates = ets:select(?CFH_CACHE, [{{{CfName,'$1'}}, [], ['$1']}]),
    lists:foldl(fun(CfH, Acc) -> check_cfh(DbRef, CfH, CfName, Acc) end, error, Candidates).

check_cfh(DbRef, CfH, CfName, Acc) ->
    case rocksdb:iterator(DbRef, CfH, []) of
        {ok, I} ->
            rocksdb:iterator_close(I),
            {ok, CfH};
        {error, _} ->
            ets:delete(?CFH_CACHE, {CfName, CfH}),
            Acc
    end.

drop_cached_cf(CfName, CfH) ->
    ets:delete(?CFH_CACHE, {CfName, CfH}).

%%
%% ===================================================================================

do_prep_close(Name, Backend, St) ->
    RelTabs = get_related_resources(Name, Backend),
    erase_pt_list([Name | RelTabs]),
    {ok, St}.

close_all(#st{backends = Bs, standalone = Ts}) ->
    persistent_term:erase(?PT_KEY),
    maps:fold(fun close_backend/3, ok, Bs),
    maps:fold(fun close_standalone/3, ok, Ts).

close_backend(_Alias, #{db_ref := DbRef}, _) ->
    _ = rocksdb_close(DbRef),
    ok.

close_standalone({_Alias, _Name}, #{db_ref := DbRef}, _) ->
    _ = rocksdb_close(DbRef),
    ok.

do_close_table(Alias, Name, Backend, #st{standalone = Ts} = St) ->
    case find_cf(Alias, Name, Backend, St) of
        {ok, #{type := column_family} = Cf} ->
            %% We don't actually close column families
            erase_pt(Name),
            {ok, update_cf(Alias, Name, Cf#{status => closed}, St)};
        {ok, #{type := standalone, db_ref := DbRef}} ->
            T = {Alias, Name},
            TRec = maps:get(T, Ts),
            erase_pt(Name),
            St1 = St#st{standalone = Ts#{T => TRec#{status => closed}}},
            _ = rocksdb_close(DbRef),
            {ok, St1};
        error ->
            {error, not_found}
    end.

do_delete_table(Alias, Name, Backend, #st{} = St) ->
    case find_cf(Alias, Name, Backend, St) of
        {ok, Where} ->
            erase_pt(Name),
            case Where of
                #{db_ref := DbRef, cf_handle := CfH, type := column_family} ->
                    rocksdb:drop_column_family(DbRef, CfH),
                    drop_cached_cf(tab_to_cf_name(Name), CfH),
                    rocksdb:destroy_column_family(DbRef, CfH),
                    {ok, delete_cf(Alias, Name, St)};
                #{type := standalone} = R ->
                    St1 = close_and_delete_standalone(R, St),
                    {ok, St1}
            end;
        error ->
            {error, not_found}
    end.

load_admin_db(Alias, Opts, St) ->
    DbName = {admin, Alias},
    open_db(DbName, Alias, Opts, [DbName], true, St).

open_db(DbName, Alias, Opts, CFs, CreateIfMissing, St) ->
    MP = mnesia_rocksdb_lib:data_mountpoint(DbName),
    open_db_(MP, Alias, Opts, CFs, CreateIfMissing, DbName, St).

open_db_(MP, Alias, Opts, CFs0, CreateIfMissing, DbName, #st{backends = Backends} = St) ->
    Acc0 = #{ mountpoint => MP },
    case filelib:is_dir(MP) of
        false when CreateIfMissing ->
            %% not yet created
            CFs = cfs(CFs0, Opts),
            file:make_dir(MP),
            OpenOpts = open_opts(Opts),
            log_invalid_opts(Opts),
            OpenRes = mnesia_rocksdb_lib:open_rocksdb(MP, OpenOpts, CFs),
            map_cfs(OpenRes, CFs, Alias, Acc0);
        false ->
            {error, enoent};
        true ->
            %% Assumption: even an old rocksdb database file will have at least "default"
            {ok, CFs} = rocksdb:list_column_families(MP, Opts),
            CFsWithOpts = case map_size(Backends) of
                0 ->
                    [{CF, cfopts([])} || CF <- CFs];
                _ ->
                    {ok, Trec} = find_cf(Alias, DbName, maps:get(Alias, Backends), St),
                    StoredOpts = rocksdb_opts_from_trec(Trec),
                    [{CF, cfopts(StoredOpts)} || CF <- CFs]
            end,
            map_cfs(
              rocksdb_open(MP, open_opts(Opts), CFsWithOpts),
              CFsWithOpts,
              Alias,
              Acc0
            )
    end.

log_invalid_opts(Opts) ->
    Combined = rdb_type_extractor:open_opts_allowed() ++ rdb_type_extractor:cf_opts_allowed(),
    case lists:filter(fun({Key, _Value}) -> lists:member(Key, Combined) == false end, Opts) of
        [] ->
            ok;
        NonEmptyList ->
            ?log(warning, "The following options will be ignored as they are not supported in erlang's rocksdb:options(): ~p", [NonEmptyList])
    end.

% filter and remove duplicates.
filter_opts(Defaults, Opts, Allowed) ->
    Filtered = lists:filter(fun({Key, _Value}) -> lists:member(Key, Allowed) end, Defaults ++ Opts),
    % de-duplicates and override defaults
    lists:foldl(fun
        ({Key, Value}, Acc) ->
            [{Key, Value} | lists:keydelete(Key, 1, Acc)]
        end,
        [],
        Filtered
    ).


rocksdb_open(MP, Opts, CFs) ->
    %% rocksdb:open(MP, Opts, CFs),
    mnesia_rocksdb_lib:open_rocksdb(MP, Opts, CFs).

is_open(Alias, #st{backends = Bs}) ->
    case maps:find(Alias, Bs) of
        {ok, #{db_ref := _}} ->
            true;
        _ ->
            false
    end.

remove_admin_db(Alias, #st{backends = Bs} = St) ->
    case maps:find(Alias, Bs) of
        {ok, #{db_ref := DbRef, mountpoint := MP}} ->
            close_and_delete(DbRef, MP),
            St#st{backends = maps:remove(Alias, Bs)};
        error ->
            St
    end.

cfs(CFs, Opts) ->
    CfOpts = cfopts(Opts),
    [{"default", CfOpts}] ++ lists:flatmap(fun(Tab) -> admin_cfs(Tab, CfOpts) end, CFs).

cfopts(Opts) ->
    filter_opts([{merge_operator, erlang_merge_operator}], Opts, rdb_type_extractor:cf_opts_allowed()).

open_opts(Opts) ->
    filter_opts(
        [
            {create_if_missing, true},
            {create_missing_column_families, true}
        ],
        Opts,
        rdb_type_extractor:open_opts_allowed()
    ).


admin_cfs(Tab, CFOpts) when is_atom(Tab) -> [ {tab_to_cf_name(Tab), CFOpts} ];
admin_cfs({_, _, _} = T, CFOpts)         -> [ {tab_to_cf_name(T), CFOpts} ];
admin_cfs({admin, _Alias} = A, CFOpts)   -> [ {tab_to_cf_name(A), CFOpts} ];
admin_cfs({ext, CF}, CFOpts)             -> [ {CF, CFOpts} ];
admin_cfs({info, _} = I, CFOpts)         -> [ {tab_to_cf_name(I), CFOpts} ].


map_cfs({ok, Ref, CfHandles}, CFs, Alias, Acc) ->
    ZippedCFs = lists:zip(CFs, CfHandles),
    %% io:fwrite("ZippedCFs = ~p~n", [ZippedCFs]),
    CfInfo = maps:from_list(
               [{cf_name_to_tab(N, Alias), #{ db_ref => Ref
                                            , cf_handle => H
                                            , alias => Alias
                                            , status => pre_existing
                                            , type => column_family }}
                || {{N,_}, H} <- ZippedCFs]),
    {ok, Acc#{ db_ref => Ref
             , cf_info => CfInfo }}.

tab_to_cf_name(Tab) when is_atom(Tab) -> write_term({d, Tab});
tab_to_cf_name({admin, Alias})        -> write_term({a, Alias});
tab_to_cf_name({info, Tab})           -> write_term({n, Tab});
tab_to_cf_name({Tab, index, I})       -> write_term({i, Tab, I});
tab_to_cf_name({Tab, retainer, R})    -> write_term({r, Tab, R}).

write_term(T) ->
    lists:flatten(io_lib:fwrite("~w", [T])).

cf_name_to_tab(Cf, Alias) ->
    case read_term(Cf) of
        {ok, {d, Table}}    -> Table;
        {ok, {i, Table, I}} -> {Table, index, I};
        {ok, {r, Table, R}} -> {Table, retainer, R};
        {ok, {n, Table}}    -> {info, Table};
        {ok, {a, Alias}}    -> {admin, Alias};
        _ ->
            {ext, Alias, Cf}
    end.

read_term(Str) ->
    case erl_scan:string(Str) of
        {ok, Tokens, _} ->
            erl_parse:parse_term(Tokens ++ [{dot,1}]);
        Other ->
            Other
    end.

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

close_and_delete_standalone(#{alias := Alias,
                              name := Name,
                              type := standalone,
                              db_ref := DbRef}, St) ->
    case get_table_mountpoint(Alias, Name, St) of
        {ok, MP} ->
            close_and_delete(DbRef, MP),
            delete_admin_info(standalone_vsn_and_enc, Alias, Name),
            St#st{standalone = maps:remove({Alias,Name}, St#st.standalone)};
        error ->
            St
    end.

close_and_delete(DbRef, MP) ->
    try rocksdb_close(DbRef) catch error:_ -> ok end,
    destroy_db(MP, []).

rocksdb_close(undefined) ->
    ok;
rocksdb_close(Ref) ->
    Res = rocksdb:close(Ref),
    erlang:garbage_collect(),
    Res.

destroy_db(MPd, Opts) ->
    destroy_db(MPd, Opts, get_retries()).

%% Essentially same code as above.
destroy_db(MPd, Opts, Retries) ->
    _DRes = destroy_db(MPd, Opts, max(1, Retries), undefined),
    [_|_] = MPd, % ensure MPd is non-empty
    _RmRes = os:cmd("rm -rf " ++ MPd ++ "/*"),
    ok.

destroy_db(_, _, 0, LastError) ->
    {error, LastError};
destroy_db(MPd, Opts, RetriesLeft, _) ->
    case rocksdb:destroy(MPd, Opts) of
        ok ->
            ok;
        %% Check specifically for lock error, this can be caused if
        %% destroy follows quickly after close.
        {error, {error_db_destroy, Err}=Reason} ->
            case lists:prefix("IO error: lock ", Err) of
                true ->
                    SleepFor = get_retry_delay(),
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
