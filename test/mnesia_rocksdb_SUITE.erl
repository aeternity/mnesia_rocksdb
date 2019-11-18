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

-export([error_handling/1]).

-include_lib("common_test/include/ct.hrl").

suite() ->
    [].

all() ->
    [{group, all_tests}].

groups() ->
    [{all_tests, [sequence], [error_handling]}].


error_handling(_Config) ->
    mnesia_rocksdb_error_handling:run().

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.
