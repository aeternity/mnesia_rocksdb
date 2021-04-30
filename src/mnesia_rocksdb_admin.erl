-module(mnesia_rocksdb_admin).

-behaviour(gen_server).

-export([ ensure_started/0
        , add_aliases/1
        , remove_aliases/1
        , create_table/3           %% (Alias, Name, Props) -> {ok, Ref} | error()
        , delete_table/2           %% (Alias, Name) -> ok
        , load_table/2             %% (Alias, Name) -> ok
        , related_resources/2      %% (Alias, Name) -> [RelatedTab]
        , prep_close/2             %% (Alias, Tab) -> ok
        , get_ref/1                %% (Name) -> Ref | abort()
        , get_ref/2                %% (Name, Default -> Ref | Default
        , request_ref/2            %% (Alias, Name) -> Ref
        , close_table/2
        ]).

-export([ start_link/0
        , init/1
        , handle_info/2
        , handle_call/3
        , handle_cast/2
        , terminate/2
        , code_change/3 ]).

-include("mnesia_rocksdb.hrl").

-record(st, {
              backends = #{}
            , standalone = #{}
            , cf_info = #{}
            , default_opts = []
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
-type cf_handle() :: rocksdb:cf_handle().
-type type() :: column_family | standalone.
-type status() :: open | closed | pre_existing.
-type properties() :: [{atom(), any()}].

-type cf() :: #{ db_ref := db_ref()
               , type   := type()
               , status := status()
               , cf_handle := cf_handle() }.

-type req() :: {create_table, table(), properties()}
             | {delete_table, table()}
             | {get_ref, table()}
             | {add_aliases, [alias()]}
             | {remove_aliases, [alias()]}.

-type reason() :: any().
-type reply()  :: any().
-type gen_server_reply() :: {reply, reply(), st()}
                          | {stop, reason(), reply(), st()}.

-type gen_server_noreply() :: {noreply, st()}
                            | {stop, reason(), st()}.

-define(PT_KEY(N), {mnesia_rocksdb, N}).

-spec ensure_started() -> pid().
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
    persistent_term:put(?PT_KEY(Name), Value).

erase_pt(Name) ->
    persistent_term:erase(?PT_KEY(Name)).

prep_close(Alias, Tab) when is_atom(Tab) ->
    call(Alias, {prep_close, Tab});
prep_close(_, _) ->
    ok.

%% get_pt(Name) ->
%%     persistent_term:get(?PT_KEY(Name)).

get_pt(Name, Default) ->
    persistent_term:get(?PT_KEY(Name), Default).

create_table(Alias, Name, Props) ->
    call(Alias, {create_table, Name, Props}).

-spec delete_table(alias(), tabname()) -> ok.
delete_table(Alias, Name) ->
    call(Alias, {delete_table, Name}).

load_table(Alias, Name) ->
    call(Alias, {load_table, Name}).

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

-spec call(alias(), req()) -> no_return() | any().
call(Alias, Req) ->
    case gen_server:call(?MODULE, {Alias, Req}) of
        {abort, Reason} ->
            mnesia:abort(Reason);
        Reply ->
            Reply
    end.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Opts = default_opts(),
    %% Aliases = get_aliases(),
    %% St = load_admin_dbs(Aliases, #st{default_opts = Opts}),
    {ok, #st{default_opts = Opts}}.

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
        {ok, #{cf_info := CfI0} = AdminDb} ->
            %% We need to store the persistent ref explicitly here,
            %% since mnesia knows nothing of our admin table.
            AdminTab = {admin, Alias},
            CfI = update_cf_info(AdminTab, #{status => open}, CfI0),
            put_pt(AdminTab, maps:get(AdminTab, CfI)),
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
    {Res, St1} = do_remove_aliases(Aliases, St),
    {reply, Res, St1};
handle_call({Alias, Req}, _From, St) ->
    handle_call_for_alias(Alias, Req, St);
handle_call(_Req, _From, St) ->
    {reply, {error, unknown_request}, St}.

-spec handle_cast(any(), st()) -> gen_server_noreply().
handle_cast(_Msg, St) ->
    {noreply, St}.

-spec handle_info(any(), st()) -> gen_server_noreply().
handle_info(_Msg, St) ->
    {noreply, St}.

terminate(_Reason, _St) ->
    ok.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

-spec handle_call_for_alias(alias(), req(), st()) -> gen_server_reply().
handle_call_for_alias(Alias, Req, #st{backends = Backends} = St) ->
    case maps:find(Alias, Backends) of
        {ok, Backend} ->
            handle_req(Alias, Req, Backend, St);
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
    case find_cf(Name, Backend) of
        {ok, #{status := open} = Cf} ->
            %% For some reason, Mod:create_table/2 can be called twice in a row
            %% by mnesia.
            {reply, {ok, Cf}, St};
        _ ->
            case do_create_table(Alias, Name, Props, Backend, St) of
                {ok, St1} ->
                    St2 = maybe_update_main(Alias, Name, create, St1),
                    {reply, find_cf_from_state(Alias, Name, St2), St2};
                {error, _} = Error ->
                    {reply, Error, St}
            end
    end;
handle_req(_Alias, {load_table, Name}, Backend, St) ->
    case find_cf(Name, Backend, St) of
        {ok, Cf} ->
            put_pt(Name, Cf),
            {reply, ok, St};
        error ->
            {reply, {abort, {bad_type, Name}}, St}
    end;
handle_req(Alias, {prep_close, Name}, Backend, St) ->
    ok_reply(do_prep_close(Alias, Name, Backend, St), St);
handle_req(Alias, {close_table, Name}, Backend, St) ->
    ok_reply(do_close_table(Alias, Name, Backend, St), St);
handle_req(Alias, {delete_table, Name}, Backend, St) ->
    case do_delete_table(Alias, Name, Backend, St) of
        {ok, St1} ->
            {reply, ok, maybe_update_main(Alias, Name, delete, St1)};
        {error, _} = Error ->
            {reply, {abort, Error}, St}
    end;
handle_req(_Alias, {get_ref, Name}, Backend, #st{} = St) ->
    case find_cf(Name, Backend) of
        {ok, #{status := open} = Ref} ->
            {reply, {ok, Ref}, St};
        {ok, #{status := pre_existing}} ->      % not open - treat as not_found
            {reply, {error, not_found}, St};
        error ->
            {reply, {error, not_found}, St}
    end;
handle_req(Alias, {related_resources, Tab}, Backend, St) ->
    Res = get_related_resources(Alias, Tab, Backend, St),
    {reply, Res, St}.

%% if an index table has been created or deleted, make sure the main
%% ref reflects it.
maybe_update_main(Alias, {Main, index, I}, Op, St) ->
    case find_cf_from_state(Alias, Main, St) of
        {ok, #{properties := #{index := Index} = Props} = CfM} ->
            case {Op, lists:member(I, Index)} of
                {create, false} ->
                    CfM1 = CfM#{properties => Props#{index => lists:sort([I|Index])}},
                    maybe_update_pt(Main, CfM1),
                    update_cf(Alias, Main, CfM1, St);
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

get_related_resources(Alias, Tab, #st{backends = Backends} = St) ->
    get_related_resources(Alias, Tab, maps:get(Alias, Backends), St).

get_related_resources(_Alias, Tab, #{cf_info := CfInfo}, #st{standalone = Ts}) ->
    F = fun(K, _, Acc) ->
                acc_related_to(Tab, K, Acc)
        end,
    Res1 = maps:fold(F, [], CfInfo),
    maps:fold(F, Res1, Ts).

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



-spec find_cf(table(), backend()) -> cf().
find_cf(Name, #{cf_info := CfI}) ->
    maps:find(Name, CfI).

find_cf_from_state(Alias, Name, #st{ backends = Backends
                                   , standalone = Standalone }) ->
    case maps:find(Alias, Backends) of
        {ok, #{cf_info := #{Name := CFi}}} ->
            {ok, CFi};
        _ ->
            case maps:find(Name, Standalone) of
                {ok, _} = Ok ->
                    Ok;
                error ->
                    {error, not_found}
            end
    end.

find_cf(Name, #{cf_info := CfI}, #st{standalone = Ts}) ->
    case maps:find(Name, CfI) of
        {ok, _} = Ok ->
            Ok;
        error ->
            maps:find(Name, Ts)
    end.

locate_open_table(Name, Backend, #st{standalone = Ts}) ->
    case find_cf(Name, Backend) of
        {ok, _} = Ok ->
            Ok;
        error ->
            case maps:find(Name, Ts) of
                {ok, _} = Ok1 ->
                    Ok1;
                error ->
                    error
            end
    end.

do_create_table(Alias, Name, Props, Backend, St) ->
    %% io:fwrite("do_create_table(~p, ~p, ~p, ~p)~n", [Alias, Name, Backend, St]),
    %% TODO: we're doing double-checking here
    case locate_open_table(Name, Backend, St) of
        {ok, #{status := open}} ->
            {error, exists};
        _ ->
            do_create_table_(Alias, Name, Props, St)
    end.

do_create_table_(Alias, Name, Props, #st{backends = Bs} = St) ->
    Opts = [],                        %% TODO: fix so this becomes a parameter
    case maps:find(Alias, Bs) of
        {ok, #{cf_info := CfI, db_ref := DbRef}} ->
            %% io:fwrite("do_create ~p: CfI = ~p~n", [Name, CfI]),
            Standalone = rdb_opt_standalone(Props),
            PMap = props_to_map(Name, Props),
            TRec0 = maybe_map_index(
                      Alias, Name, Props,
                      maybe_map_attrs(
                        #{ semantics   => semantics(Name, PMap)
                         , name        => Name
                         , alias       => Alias
                         , properties  => PMap
                         , status      => open })),
            case {table_exists_as_standalone(Name), Standalone} of
                {{true, MP}, true} ->
                    do_open_standalone(Alias, Name, MP, Opts, TRec0, St);
                {{true, MP}, false} ->
                    case auto_migrate_to_cf(Name) of
                        true ->
                            {ok, St1} = create_table_as_cf(
                                          Alias, Name, TRec0#{db_ref => DbRef},
                                          CfI, St),
                            ok = migrate_standalone_to_cf(MP, Name),
                            {ok, St1};
                        false ->
                            do_open_standalone(Alias, Name, MP, Opts, TRec0, St)
                    end;
                {false, true} ->
                    create_table_as_standalone(Alias, Name, TRec0, St);
                {false, false} ->
                    create_table_as_cf(Alias, Name, TRec0#{db_ref => DbRef}, CfI, St)
            end;
        error ->
            {error, unknown_alias}
    end.

maybe_map_index(Alias, {MainTab, index, {Pos, _IxType}}, Props, Map) ->
    Storage = {ext, Alias, mnesia_rocksdb},
    {_, Attrs} = lists:keyfind(attributes, 1, Props),
    maybe_ix_alias(
      Pos, Attrs,
      Map#{ ix_vals_f => mnesia_index:index_vals_f(Storage, MainTab, Pos) });
maybe_map_index(_, _, _, Map) ->
    Map.

maybe_ix_alias(Pos, Attrs, Map) when is_integer(Pos), Pos >= 2 ->
    Map#{alias => pos_to_attr(Pos - 2, Attrs)};
maybe_ix_alias(_, _, Map) ->
    Map.

maybe_map_attrs(#{name := Name, properties := #{attributes := Attrs}} = R) when is_atom(Name) ->
    {AMap, _} = lists:foldl(
                  fun(A, {M, P}) ->
                          {M#{A => P}, P+1}
                  end, {#{}, 2}, Attrs),
    R#{attr_pos => AMap};
maybe_map_attrs(R) ->
    R.

pos_to_attr(0, [Attr | _]) ->
    Attr;
pos_to_attr(N, [_ | Attrs]) ->
    pos_to_attr(N-1, Attrs).

rdb_opt_standalone(Props) ->
    proplists:get_bool(
      rocksdb_standalone, proplists:get_value(user_properties, Props, [])).

auto_migrate_to_cf(Name) ->
    Tabs = application:get_env(mnesia_rocksdb, auto_migrate_to_cf, []),
    lists:member(Name, Tabs).

props_to_map(TabName, Props) when is_atom(TabName) ->
    #{user_properties := UPs} = PMap = maps:without([name, cookie, version],
                                                    maps:from_list(Props)),
    PMap#{user_properties => maps:from_list(UPs)};
props_to_map({Tab,_,_}, _) ->
    #{main_table => Tab}.

semantics({_,index,_}   , _) -> set;
semantics({_,retainer,_}, _) -> set;
semantics({admin,_}     , _) -> set;
semantics(T, #{type := Type}) when is_atom(T) -> Type.

table_exists_as_standalone(Name) ->
    MP = mnesia_rocksdb_lib:data_mountpoint(Name),
    case file:read_link_info(MP) of
        {ok, _} ->
            %% mountpoint exists; assume it's the standalone db
            {true, MP};
        {error, _} ->
            false
    end.

do_open_standalone(Alias, Name, MP, Opts, TRec0, #st{standalone = Ts} = St) ->
    {ok, DbRef} = mnesia_rocksdb_lib:open_rocksdb(MP, Opts),
    TRec = TRec0#{ db_ref     => DbRef
                 , mountpoint => MP
                 , type       => standalone
                 },
    {ok, St#st{standalone = Ts#{{Alias,Name} => TRec}}}.


create_table_as_standalone(Alias, Name, TRec0, #st{standalone = Ts} = St) ->
    #{user_properties := UPs} = TRec0,
    Opts = maps:get(rocksdb_opts, UPs, []),
    case open_db(Name, Opts, [], true) of
        {ok, DbRec} ->
            TRec = maps:merge(TRec0, DbRec#{type => standalone}),
            {ok, St#st{standalone = Ts#{{Alias, Name} => TRec}}};
        {error, _} = Err ->
            Err
    end.

migrate_standalone_to_cf(DbI, MP) ->
    try migrate_standalone_to_cf_(DbI, MP) of
        {ok, #{db_ref := DbRef}} ->
            ok = rocksdb:close(DbRef),
            ok = rocksdb:destroy(MP, []);
        Error ->
            Error
    catch
        error:E ->
            {error, E}
    end.

migrate_standalone_to_cf_(DbI, MP) ->
    case open_db_(MP, [], [], [], false) of
        {ok, DbRec0} ->
            DbRec = DbRec0#{type => standalone},
            mrdb:as_batch(
              DbI,
              fun(R) ->
                      mrdb:with_iterator(
                        DbRec, fun(I) ->
                                       move_to_cf(
                                         rocksdb:iterator_move(I, ?DATA_START), R, I)
                               end)
              end),
            {ok, DbRec};
        {error, _} = Err ->
            Err
    end.

move_to_cf({ok, Key, Val}, R, I) ->
    ok = mrdb:rdb_put(R, Key, Val, []),
    move_to_cf(rocksdb:iterator_move(I, next), R, I);
move_to_cf({error, _}, _, _) ->
    ok.

create_table_as_cf(Alias, Name, #{db_ref := DbRef} = R, CfI, St) ->
    CfName = tab_to_cf_name(Name),
    case maps:find(Name, CfI) of
        {ok, #{cf_handle := _, status := pre_existing} = Cf0} ->
            %% Column family exists
            Cf1 = maps:merge(Cf0, R),
            {ok, update_cf(Alias, Name, Cf1, St)};
        error ->
            case rocksdb:create_column_family(DbRef, CfName, []) of
                {ok, CfH} ->
                    {ok, update_cf(Alias, Name, R#{ type      => column_family
                                                  , cf_handle => CfH }, St)};
                {error, _} = Error ->
                    Error
            end
    end.

do_prep_close(Alias, Name, _Backend, St) ->
    RelTabs = get_related_resources(Alias, Name, St),
    lists:foreach(fun erase_pt/1, [Name | RelTabs]),
    {ok, St}.

do_close_table(Alias, Name, Backend, #st{standalone = Ts} = St) ->
    case locate_open_table(Name, Backend, St) of
        {ok, #{type := column_family} = Cf} ->
            %% We don't actually close column families
            erase_pt(Name),
            {ok, update_cf(Alias, Name, Cf#{status => closed}, St)};
        {ok, #{type := standalone, db_ref := DbRef}} ->
            TRec = maps:get(Name, Ts),
            erase_pt(Name),
            St1 = St#st{standalone = Ts#{Name => TRec#{status => closed}}},
            _ = rocksdb_close(DbRef),
            {ok, St1};
        error ->
            {error, not_found}
    end.

do_delete_table(Alias, Name, Backend, #st{standalone = Ts} = St) ->
    case locate_open_table(Name, Backend, St) of
        {ok, Where} ->
            erase_pt(Name),
            case Where of
                #{db_ref := DbRef, cf_handle := CfH, type := column_family} ->
                    rocksdb:drop_column_family(DbRef, CfH),
                    rocksdb:destroy_column_family(DbRef, CfH),
                    {ok, delete_cf(Alias, Name, St)};
                #{db_ref := DbRef, mountpoint := MP, type := standalone} ->
                    close_and_delete(DbRef, MP),
                    {ok, St#st{standalone = maps:remove({Alias, Name}, Ts)}}
            end;
        error ->
            {error, not_found}
    end.

load_admin_db(Alias, Opts) ->
    open_db({admin, Alias}, Opts, [], true).

open_db(DbName, Opts, XtraCFs, CreateIfMissing) ->
    MP = mnesia_rocksdb_lib:data_mountpoint(DbName),
    open_db_(MP, Opts, [DbName], XtraCFs, CreateIfMissing).

open_db_(MP, Opts, AdminCFs, XtraCFs, CreateIfMissing) ->
    Acc0 = #{ mountpoint => MP },
    case filelib:is_dir(MP) of
        false when CreateIfMissing ->
            %% not yet created
            CFs = cfs(AdminCFs, XtraCFs),
            file:make_dir(MP),
            OpenOpts = [ {create_if_missing, true}
                       , {create_missing_column_families, true}
                       | Opts ],
            OpenRes = mnesia_rocksdb_lib:open_rocksdb(MP, OpenOpts, CFs),
            map_cfs(OpenRes, CFs, Acc0);
        false ->
            {error, enoent};
        true ->
            %% Assumption: even an old rocksdb database file will have at least "default"
            {ok,CFs} = rocksdb:list_column_families(MP, Opts),
            CFs1 = [{CF,[]} || CF <- CFs ++ XtraCFs],   %% TODO: this really needs more checking
            map_cfs(rocksdb:open(MP, Opts, CFs1), CFs1, Acc0)
    end.

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

cfs(AdminCFs, XtraCFs) ->
    [{"default", []}] ++ lists:flatmap(fun admin_cfs/1, AdminCFs) ++ XtraCFs.

admin_cfs(Tab) when is_atom(Tab) -> [ {tab_to_cf_name({info,Tab}), []}
                                    , {tab_to_cf_name(Tab), []} ];
admin_cfs({_, _, _} = T)         -> [ {tab_to_cf_name(T), []} ];
admin_cfs({admin, _Alias} = A)   -> [ {tab_to_cf_name(A), []} ];
admin_cfs({ext, CF})             -> [ {CF, []} ];                    %% how to get opts?
admin_cfs({info, _} = I)         -> [ {tab_to_cf_name(I), []} ].


map_cfs({ok, Ref, CfHandles}, CFs, Acc) ->
    ZippedCFs = lists:zip(CFs, CfHandles),
    %% io:fwrite("ZippedCFs = ~p~n", [ZippedCFs]),
    CfInfo = maps:from_list([{cf_name_to_tab(N), #{ db_ref => Ref
                                                  , cf_handle => H
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

cf_name_to_tab(Cf) ->
    case read_term(Cf) of
        {ok, {d, Table}}    -> Table;
        {ok, {i, Table, I}} -> {Table, index, I};
        {ok, {r, Table, R}} -> {Table, retainer, R};
        {ok, {n, Table}}    -> {info, Table};
        {ok, {a, Alias}}    -> {admin, Alias};
        _ ->
            {ext, Cf}
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
