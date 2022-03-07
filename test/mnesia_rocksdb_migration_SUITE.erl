-module(mnesia_rocksdb_migration_SUITE).

-export([
         all/0
       , suite/0
       , groups/0
       , init_per_suite/1
       , end_per_suite/1
       , init_per_group/2
       , end_per_group/2
       , init_per_testcase/2
       , end_per_testcase/2
        ]).

-export([
         manual_migration/1
       , migrate_with_encoding_change/1
       , auto_migration/1
        ]).

-include_lib("common_test/include/ct.hrl").

-define(TABS_CREATED, tables_created).

suite() ->
    [].

all() ->
    [{group, all_tests}].

groups() ->
    [
     {all_tests, [sequence], [ manual_migration
                             , migrate_with_encoding_change ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    mnesia:stop(),
    ok = mnesia_rocksdb_tlib:start_mnesia(reset),
    Config.
%%    create_migrateable_db(Config).

end_per_testcase(_, _Config) ->
    ok.

manual_migration(Config) ->
    tr_ct:with_trace(fun manual_migration_/1, Config, tr_opts()).

manual_migration_(Config) ->
    create_migrateable_db(Config),
    Tabs = tables(),
    ct:log("Analyze (before): ~p", [analyze_tabs(Tabs)]),
    Res = mnesia_rocksdb_admin:migrate_standalone(rdb, Tabs),
    ct:log("migrate_standalone(rdb, ~p) -> ~p", [Tabs, Res]),
    AnalyzeRes = analyze_tabs(Tabs),
    ct:log("AnalyzeRes = ~p", [AnalyzeRes]),
    MigRes = mnesia_rocksdb_admin:migrate_standalone(rdb, Tabs),
    ct:log("MigRes = ~p", [MigRes]),
    AnalyzeRes2 = analyze_tabs(Tabs),
    ct:log("AnalyzeRes2 = ~p", [AnalyzeRes2]),
    ct:log("Admin State = ~p", [sys:get_state(mnesia_rocksdb_admin)]),
    ok.

migrate_with_encoding_change(_Config) ->
    ok = create_tab(t, [{user_properties, [{mrdb_encoding, {sext,{object,term}}},
                                           {rocksdb_standalone, true}]}
                       ]),
    mrdb:insert(t, {t, <<"1">>, <<"a">>}),
    mrdb:insert(t, {t, <<"2">>, <<"b">>}),
    TRef = mrdb:get_ref(t),
    {ok, V1} = mrdb:rdb_get(TRef, sext:encode(<<"1">>), []),
    {ok, V2} = mrdb:rdb_get(TRef, sext:encode(<<"2">>), []),
    {t,[],<<"a">>} = binary_to_term(V1),
    {t,[],<<"b">>} = binary_to_term(V2),
    Opts = #{encoding => {raw, raw}},
    MigRes = mnesia_rocksdb_admin:migrate_standalone(rdb, [{t, Opts}]),
    ct:log("MigRes (t) = ~p", [MigRes]),
    %%
    %% Ensure that metadata reflect the migrated table
    %% (now a column family, and the rocksdb_standalone prop gone)
    %%
    TRef1 = mrdb:get_ref(t),
    ct:log("TRef1(t) = ~p", [TRef1]),
    #{type := column_family,
      properties := #{user_properties := UPs}} = TRef1,
    error = maps:find(rocksdb_standalone, UPs),
    UPsR = lists:sort(maps:values(UPs)),
    UPsM = lists:sort(mnesia:table_info(t, user_properties)),
    {UPsR,UPsM} = {UPsM,UPsR},
    ct:log("user properties (t): ~p", [UPsM]),
    [{<<"2">>, <<"b">>},
     {<<"1">>, <<"a">>}] = mrdb:rdb_fold(
                             t, fun(K,V,A) -> [{K,V}|A] end, [], <<>>),
    ct:log("All data present in new column family", []),
    ct:log("Contents of mnesia dir: ~p",
           [ok(file:list_dir(mnesia:system_info(directory)))]),
    ct:log("mnesia stopped", []),
    mnesia:stop(),
    dbg:tracer(),
    dbg:tp(mnesia_rocksdb,x),
    dbg:tpl(mnesia_rocksdb_admin,x),
    dbg:tpl(mnesia_rocksdb_lib,x),
    dbg:tp(rocksdb,x),
    dbg:p(all,[c]),
    mnesia:start(),
    ct:log("mnesia started", []),
    mnesia:info(),
    ok = mnesia:wait_for_tables([t], 3000),
    ct:log("tables loaded", []),
    [{t,<<"1">>,<<"a">>},
     {t,<<"2">>,<<"b">>}] = mrdb:select(
                              t, [{'_',[],['$_']}]),
    [{<<"2">>,<<"b">>},
     {<<"1">>,<<"a">>}] = mrdb:rdb_fold(
                            t, fun(K,V,A) -> [{K,V}|A] end, [], <<>>),
    ok.

auto_migration(_Config) ->
    ok.

ok({ok, Value}) -> Value.

tr_opts() ->
    #{ patterns => [ {mnesia_rocksdb_admin, '_', []}
                   , {mnesia_rocksdb_lib, '_', []}
                   , {rocksdb, '_', x} | trace_exports(mrdb, x) ] }.

trace_exports(M, Pat) ->
    Fs = M:module_info(exports),
    [{M, F, A, Pat} || {F, A} <- Fs].

tables() ->
    [a].

create_migrateable_db(Config) ->
    Os = [{user_properties, [{rocksdb_standalone, true}]}],
    TabNames = tables(),
    Tabs = [{T, Os} || T <- TabNames],
    create_tabs(Tabs, Config),
    verify_tabs_are_standalone(TabNames),
    fill_tabs(TabNames),
    Config.

fill_tabs(Tabs) ->
    lists:foreach(fun(Tab) ->
                          [mrdb:insert(Tab, {Tab, X, a}) || X <- lists:seq(1,3)]
                  end, Tabs).

create_tabs(Tabs, Config) ->
    Res = lists:map(fun create_tab/1, Tabs),
    tr_ct:trace_checkpoint(?TABS_CREATED, Config),
    Res.

create_tab({T, Opts}) ->
    create_tab(T, Opts).

create_tab(T, Opts) ->
    {atomic, ok} = mnesia:create_table(T, [{rdb, [node()]} | Opts]),
    ok.

verify_tabs_are_standalone(Tabs) ->
    case analyze_tabs(Tabs) of
        {_, []} ->
            ok;
        {[], NotSA} ->
            error({not_standalone, NotSA})
    end.

analyze_tabs(Tabs) ->
    Dir = mnesia:system_info(directory),
    Files = filelib:wildcard(filename:join(Dir, "*-_tab.extrdb")),
    ct:log("Files = ~p", [Files]),
    TabNames = lists:map(
                 fun(F) ->
                         {match,[TStr]} =
                            re:run(F, "^.+/([^/]+)-_tab\\.extrdb$",
                                   [{capture, [1], list}]),
                         list_to_existing_atom(TStr)
                 end, Files),
    ct:log("TabNames = ~p", [TabNames]),
    NotSA = Tabs -- TabNames,
    {TabNames -- NotSA, NotSA}.

