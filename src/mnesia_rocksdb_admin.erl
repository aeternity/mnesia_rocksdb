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
        ]).

-export([ migrate_standalone/2 ]).

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
        , write_table_property/3 %% (Alias, Tab, Property)
        ]).

-export([meta/0]).

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

-type req() :: {create_table, table(), properties()}
             | {delete_table, table()}
             | {load_table, table()}
             | {related_resources, table()}
             | {get_ref, table()}
             | {add_aliases, [alias()]}
             | {write_table_property, tabname(), tuple()}
             | {remove_aliases, [alias()]}
             | {migrate, [{tabname(), map()}]}
             | {prep_close, table()}
             | {close_table, table()}.

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

meta() ->
    persistent_term:get(?PT_KEY, #{}).

prep_close(Alias, Tab) when is_atom(Tab) ->
    call(Alias, {prep_close, Tab});
prep_close(_, _) ->
    ok.

get_pt(Name, Default) ->
    maps:get(Name, meta(), Default).

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
    EncK = mnesia_rocksdb_lib:encode_key({info,Tab,K}, sext),
    maybe_write_standalone_info(Ref, K, V),
    mrdb:rdb_put(Ref, EncK, term_to_binary(V), []).

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

write_table_property(Alias, Tab, Prop) when is_tuple(Prop), size(Prop) >= 1 ->
    call(Alias, {write_table_property, Tab, Prop}).

migrate_standalone(Alias, Tabs) ->
    call(Alias, {migrate, Tabs}).

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
    mrdb_mutex:ensure_tab(),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Opts = default_opts(),
    process_flag(trap_exit, true),
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
    end.

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
    case load_admin_db(Alias, AliasOpts ++ DefaultOpts) of
        {ok, #{cf_info := CfI0, mountpoint := MP} = AdminDb} ->
            %% We need to store the persistent ref explicitly here,
            %% since mnesia knows nothing of our admin table.
            AdminTab = {admin, Alias},
            CfI = update_cf_info(AdminTab, #{ status => open
                                            , name => AdminTab
                                            , vsn => ?VSN
                                            , encoding => {sext,{value,term}}
                                            , attr_pos => #{key => 1,
                                                            value => 2}
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
    ?log(debug, "Table event: ~p", [Event]),
    case Event of
        {write, {schema, Tab, Props}, _} ->
            case find_cf(Tab, St) of
                error ->
                    ?log(debug, "No Cf found (~p)", [Tab]),
                    {noreply, St};
                #{} = Cf ->
                    ?log(debug, "Located Cf: ~p", [Cf]),
                    case try_refresh_cf(Cf, Props, St) of
                        false ->
                            ?log(debug, "Nothing changed (~p)", [Tab]),
                            {noreply, St};
                        {true, NewCf, St1} ->
                            ?log(debug, "NewCf = ~p", [NewCf]),
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
    case create_trec(Alias, Name, Props, Backend, St) of
        {ok, NewCf} ->
            ?log(debug, "NewCf = ~p", [NewCf]),
            St1 = update_cf(Alias, Name, NewCf, St),
            {reply, {ok, NewCf}, St1};
        {error, _} = Error ->
            {reply, Error, St}
    end;
handle_req(Alias, {load_table, Name, Props}, Backend, St) ->
    ?log(debug, "load_table, ~p, ~p", [Name, Props]),
    try
    case find_cf(Alias, Name, Backend, St) of
        {ok, #{status := open}} ->
            ?log(info, "load_table(~p) when table already loaded", [Name]),
            {reply, ok, St};
        {ok, #{status := created} = TRec} ->
            handle_load_table_req(Alias, Name, TRec, Backend, St);
        _ ->
            ?log(debug, "load_table (~p) without preceding create_table", [Name]),
            case create_trec(Alias, Name, Props, Backend, St) of
                {ok, NewCf} ->
                    ?log(debug, "NewCf = ~p", [NewCf]),
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
handle_req(Alias, {migrate, Tabs0}, Backend, St) ->
    case prepare_migration(Alias, Tabs0, St) of
        {ok, Tabs} ->
            {Res, St1} = do_migrate_tabs(Alias, Tabs, Backend, St),
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
            {reply, ok, St2};
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
                    maybe_update_pt(Main, CfM1),
                    update_cf(Alias, Main, CfM1, St);
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
            ?log(debug, "will create ~p as standalone (no migrate)", [Name]),
            create_table_as_standalone(Alias, Name, true, MP, TRec, St);
        {true, MP} ->
            ?log(debug, "will create ~p as standalone and migrate", [Name]),
            case create_table_as_standalone(Alias, Name, false, MP, TRec, St) of
                {ok, OldTRec, _} ->
                    create_cf_and_migrate(Alias, Name, OldTRec, TRec, Backend, St);
                Other ->
                    ?log(info, "Couldn't open what seems to be a standalone table"
                         " (~p): ~p", [Name, Other]),
                    create_table_as_cf(Alias, Name, TRec#{db_ref => DbRef}, St)
            end
   end;
create_table_from_trec(Alias, Name, #{type := standalone} = TRec, _, St) ->
    {Exists, MP} = table_exists_as_standalone(Name),
    create_table_as_standalone(Alias, Name, Exists, MP, TRec, St).

create_cf_and_migrate(Alias, Name, OldTRec, TRec, #{db_ref := DbRef}, St) ->
    ?log(debug, "Migrate to cf (~p)", [Name]),
    {ok, NewCf, St1} = create_table_as_cf(
                         Alias, Name, TRec#{db_ref => DbRef}, St),
    {ok, St2} = migrate_standalone_to_cf(OldTRec, NewCf, St1),
    {ok, NewCf, St2}.

%% Return {Migrate, MP} iff table exists standalone; just false if it doesn't
should_we_migrate_standalone(#{name := Name}) ->
    case table_exists_as_standalone(Name) of
        {true, MP} ->
            ?log(debug, "table ~p exists as standalone: ~p", [Name, MP]),
            case auto_migrate_to_cf(Name) of
                true ->
                    ?log(debug, "auto_migrate(~p): true", [Name]),
                    {true, MP};
                false ->
                    ?log(debug, "auto_migrate(~p): false", [Name]),
                    {false, MP}
            end;
        {false, _} ->
            false
    end.

prepare_migration(Alias, Tabs, St) ->
    Res = lists:map(fun(T) ->
                            prepare_migration_(Alias, T, St)
                    end, Tabs),
    Res1 = add_related_tabs(Res, maps:get(Alias, St#st.backends), Alias, St),
    case [E || {error, _} = E <- Res1] of
        [] -> {ok, Res1};
        [_|_] = Errors ->
            {error, Errors}
    end.

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

do_migrate_tabs(Alias, Tabs, Backend, St) ->
    lists:mapfoldl(fun(T, St1) ->
                        do_migrate_table(Alias, T, Backend, St1)
                end, St, Tabs).

do_migrate_table(Alias, {Name, OldTRec, TRec0}, Backend, St) when is_map(TRec0) ->
    T0 = erlang:system_time(millisecond),
    TRec = maps:without([encoding, vsn], TRec0),
    maybe_write_user_props(TRec),
    {ok, CF, St1} = create_cf_and_migrate(Alias, Name, OldTRec,
                                          TRec, Backend, St),
    put_pt(Name, CF),
    T1 = erlang:system_time(millisecond),
    Time = T1 - T0,
    io:fwrite("~p migrated, ~p ms~n", [Name, Time]),
    {{Name, {ok, Time}}, St1}.

migrate_standalone_to_cf(OldTRec, #{name := T, alias := Alias} = TRec,
                         #st{standalone = Ts} = St) ->
    ChunkSz = chunk_size(TRec),
    KeyPos = mnesia_rocksdb_lib:keypos(T),
    migrate_to_cf(mrdb:select(OldTRec, [{'_',[],['$_']}], ChunkSz),
                  TRec, OldTRec, KeyPos),
    case maps:is_key({Alias,T}, Ts)
         andalso table_is_empty(OldTRec) of
        true ->
            St1 = close_and_delete_standalone(OldTRec, St),
            {ok, St1};
        false ->
            {ok, St}
    end.

migrate_to_cf({L, Cont}, Cf, DbRec, KeyPos) ->
    mrdb:as_batch(
      Cf,
      fun(New) ->
              mrdb:as_batch(
                DbRec,
                fun(Old) ->
                        lists:foreach(
                          fun(Obj) ->
                                  mrdb:insert(New, Obj),
                                  mrdb:delete(Old, element(KeyPos,Obj))
                          end, L)
                end)
      end),
    migrate_to_cf(cont(Cont), Cf, DbRec, KeyPos);
migrate_to_cf('$end_of_table', _, _, _) ->
    ok.

cont('$end_of_table' = E) -> E;
cont(F) when is_function(F,0) ->
    F().

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
    ?log(debug, "PMap = ~p", [PMap]),
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
    case open_db_(MP, Alias, Opts, [], CreateIfMissing) of
        {ok, #{ cf_info := CfI }} ->
	    DbRec = maps:get({ext,Alias,"default"}, CfI),
            ?log(debug, "successfully opened db ~p", [Name]),
            CfNames = maps:keys(CfI),
            DbRec1 = DbRec#{ cfs => CfNames,
                             mountpoint => MP },
            TRec = maps:merge(TRec0, DbRec#{type => standalone}),
            TRec1 = guess_table_vsn_and_encoding(Exists, TRec),
            ?log(debug, "TRec1 = ~p", [TRec1]),
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
    ?log(debug, "guess_vsn_and_encoding(R = ~p)", [R]),
    case read_admin_info(standalone_vsn_and_enc, Alias, Name) of
        {ok, {V, E}} ->
            ?log(debug, "admin_info exists: ~p", [{V,E}]),
            R#{vsn => V, encoding => E};
        error ->
            ?log(debug, "no admin_info; will iterate", []),
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
                ?log(debug, "Found info tag; this is a vsn 1", []),
                R#{vsn => 1, encoding => {sext, {object, term}}}
            catch
                error:_ ->
                    ?log(debug, "caught bad guess K=~p, V=~p", [K,V]),
                    R
            end;
        _ ->
            ?log(debug, "not info obj K=~p", [K]),
            Enc = guess_obj_encoding(K, V, Arity),
            ?log(debug, "guessed Enc = ~p", [Enc]),
            R#{encoding => Enc}
   end;
guess_table_vsn_and_encoding_(Other, _, _, R) ->
    ?log(debug, "Iter Other=~p", [Other]),
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
                    write_info_(ARef, Tab, DecK, V);
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
    TRec#{vsn => Vsn, encoding => Encoding}.

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
    case rocksdb:create_column_family(DbRef, CfName, cfopts()) of
        {ok, CfH} ->
            R1 = check_version_and_encoding(R#{ cf_handle => CfH
                                              , type => column_family }),
            {ok, R1, update_cf(Alias, Name, R1, St)};
        {error, _} = Error ->
            Error
    end.

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
                    rocksdb:destroy_column_family(DbRef, CfH),
                    {ok, delete_cf(Alias, Name, St)};
                #{type := standalone} = R ->
                    St1 = close_and_delete_standalone(R, St),
                    {ok, St1}
            end;
        error ->
            {error, not_found}
    end.

load_admin_db(Alias, Opts) ->
    DbName = {admin, Alias},
    open_db(DbName, Alias, Opts, [DbName], true).

open_db(DbName, Alias, Opts, CFs, CreateIfMissing) ->
    MP = mnesia_rocksdb_lib:data_mountpoint(DbName),
    open_db_(MP, Alias, Opts, CFs, CreateIfMissing).

open_db_(MP, Alias, Opts, CFs0, CreateIfMissing) ->
    Acc0 = #{ mountpoint => MP },
    case filelib:is_dir(MP) of
        false when CreateIfMissing ->
            %% not yet created
            CFs = cfs(CFs0),
            file:make_dir(MP),
            OpenOpts = [ {create_if_missing, true}
                       , {create_missing_column_families, true}
                       , {merge_operator, erlang_merge_operator}
                       | Opts ],
            OpenRes = mnesia_rocksdb_lib:open_rocksdb(MP, OpenOpts, CFs),
            map_cfs(OpenRes, CFs, Alias, Acc0);
        false ->
            {error, enoent};
        true ->
            %% Assumption: even an old rocksdb database file will have at least "default"
            {ok,CFs} = rocksdb:list_column_families(MP, Opts),
            CFs1 = [{CF,[]} || CF <- CFs], %% TODO: this really needs more checking
            map_cfs(rocksdb_open(MP, Opts, CFs1), CFs1, Alias, Acc0)
    end.

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

%% TODO: Support user provision of cf-specific options
cfs(CFs) ->
    [{"default", cfopts()}] ++ lists:flatmap(fun admin_cfs/1, CFs).

cfopts() ->
    [{merge_operator, erlang_merge_operator}].

admin_cfs(Tab) when is_atom(Tab) -> [ {tab_to_cf_name(Tab), cfopts()} ];
admin_cfs({_, _, _} = T)         -> [ {tab_to_cf_name(T), cfopts()} ];
admin_cfs({admin, _Alias} = A)   -> [ {tab_to_cf_name(A), cfopts()} ];
admin_cfs({ext, CF})             -> [ {CF, cfopts()} ];
admin_cfs({info, _} = I)         -> [ {tab_to_cf_name(I), cfopts()} ].


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
