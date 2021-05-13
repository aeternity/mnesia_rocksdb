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
        , indexes/1
        , mrdb_transactions/1]).

-include_lib("common_test/include/ct.hrl").

suite() ->
    [].

all() ->
    [{group, all_tests}].

groups() ->
    [
      {all_tests, [sequence], [ {group, mrdb}
                              , indexes
                              , error_handling ]}
    , {mrdb, [sequence], [ mrdb_transactions ]}
    ].


error_handling(Config) ->
    mnesia_rocksdb_error_handling:run(Config).

indexes(Config) ->
    mnesia_rocksdb_indexes:run(Config).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(mrdb, Config) ->
    mnesia:stop(),
    ok = mnesia_rocksdb_tlib:start_mnesia(reset),
    Config;
init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

mrdb_transactions(_Config) ->
    ok.
