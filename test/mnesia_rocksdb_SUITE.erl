-module(mnesia_rocksdb_SUITE).

-export([
         all/0
        , groups/0
        , suite/0
        , init_per_suite/1
        , end_per_suite/1
        , init_per_group/2
        , end_per_group/2
        , init_per_testcase/2
        , end_per_testcase/2
        ]).

-export([ error_handling/1
        , mrdb_transactions/1]).

-include_lib("common_test/include/ct.hrl").

suite() ->
    [].

all() ->
    [{group, all_tests}].

groups() ->
    [
      {all_tests, [sequence], [ {group, mrdb}
                              , error_handling ]}
    , {mrdb, [sequence], [ mrdb_transactions ]}
    ].


error_handling(Config) ->
    mnesia_rocksdb_error_handling:run(Config).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(mrdb, _Config) ->
    %% mnesia:stop(),
    %% ok = mnesia_rocksdb_tlib:start_mnesia(reset),
    %% Config;
    {skip, "rocksdb transactions broken"};
init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

mrdb_transactions(_Config) ->
    Created = create_tabs([{tx, []}]),
    mrdb:insert(tx, {tx, a, 1}),
    [_] = mrdb:read(tx, a),
    mrdb:activity(
      tx, rdb,
      fun() ->
              [{tx,a,N}] = mrdb:read(tx, a),
              N1 = N+1,
              ok = mrdb:insert(tx, {tx,a,N1})
      end),
    [{tx,a,2}] = mrdb:read(tx,a),
    delete_tabs(Created),
    ok.

create_tabs(Tabs) ->
    lists:map(fun create_tab/1, Tabs).

create_tab({T, Opts}) ->
    {atomic, ok} = mnesia:create_table(T, [{rdb,[node()]} | Opts]),
    T.

delete_tabs(Tabs) ->
    [{atomic,ok} = mnesia:delete_table(T) || T <- Tabs],
    ok.
