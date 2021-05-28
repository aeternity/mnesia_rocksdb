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

-module(mnesia_rocksdb_indexes_SUITE).

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

-export([
          index_plugin_mgmt/1
        , add_indexes/1
        , create_bag_index/1
        , create_ordered_index/1
        , test_1_ram_copies/1
        , test_1_disc_copies/1
        , fail_1_disc_only/1
        , plugin_ram_copies1/1
        , plugin_ram_copies2/1
        , plugin_disc_copies/1
        , fail_plugin_disc_only/1
        , plugin_disc_copies_bag/1
        , plugin_rdb_ordered/1
        , index_iterator/1
        ]).

-include_lib("common_test/include/ct.hrl").

-export([run/0,
         run/1,
         r1/0]).

-define(TAB(T), list_to_atom(lists:flatten(io_lib:fwrite("~w_~w", [T, ?LINE])))).

run() ->
    run([]).

run(Config) ->
    mnesia:stop(),
    maybe_set_dir(Config),
    ok = mnesia_rocksdb_tlib:start_mnesia(reset),
    test(1, ram_copies, r1),
    test(1, disc_copies, d1),
    fail(test, [1, disc_only_copies, do1]),  % doesn't support ordered
    test(2, disc_only_copies, do1),
    fail(test, [1, rdb, l1]), % doesn't support bag
    test(3, rdb, l1),
    add_del_indexes(),
    {atomic,ok} = mnesia_schema:add_index_plugin(
                    {pfx},mnesia_rocksdb, ix_prefixes),
    test_index_plugin(cfg([pr1, ram_copies, ordered], Config)),
    test_index_plugin(cfg([pr2, ram_copies, bag], Config)),
    test_index_plugin(cfg([pd1, disc_copies, ordered], Config)),
    fail(test_index_plugin, [cfg([pd2, disc_only_copies, ordered], Config)]),
    test_index_plugin(cfg([pd2, disc_copies, bag], Config)),
    test_index_plugin(cfg([pl2, rdb, ordered], Config)),
    index_plugin_mgmt(Config),
    ok.

suite() ->
    [].

all() ->
    [{group, all_tests}].

groups() ->
    [
      {all_tests, [sequence], [ {group, mgmt}, {group, access}, {group, plugin} ]}
    , {mgmt, [sequence], [
                           create_bag_index
                         , create_ordered_index
                         , index_plugin_mgmt
                         , add_indexes
                         ]}
    , {access, [sequence], [
                             test_1_ram_copies
                           , test_1_disc_copies
                           , fail_1_disc_only
                           , index_iterator
                           ]}
    , {plugin, [sequence], [
                             plugin_ram_copies1
                           , plugin_ram_copies2
                           , plugin_disc_copies
                           , fail_plugin_disc_only
                           , plugin_disc_copies_bag
                           , plugin_rdb_ordered
                           ]}
    ].

%% ======================================================================

init_per_suite(Config) ->
    mnesia:stop(),
    maybe_set_dir(Config),
    Config.

end_per_suite(_) ->
    ok.

init_per_group(Grp, Config) ->
    mnesia_rocksdb_tlib:restart_reset_mnesia(),
    case Grp of
        plugin ->
            {atomic,ok} = mnesia_schema:add_index_plugin(
                            {pfx},mnesia_rocksdb, ix_prefixes);
        _ ->
            ok
    end,
    Config.

end_per_group(_, _) ->
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

%% ======================================================================

cfg([Tab, Type, IxType], Config) ->
    [{my_config, #{tab => Tab, type => Type, ixtype => IxType}} | Config];
cfg(Cfg, Config) when is_map(Cfg) -> [{my_config, Cfg} | Config].

cfg(Config) -> ?config(my_config, Config).

%% ======================================================================

r1() ->
    mnesia:stop(),
    ok = mnesia_rocksdb_tlib:start_mnesia(reset),
    {atomic,ok} = mnesia_schema:add_index_plugin(
                    {pfx},mnesia_rocksdb, ix_prefixes),
    dbg:tracer(),
    dbg:tpl(mnesia_schema,x),
    dbg:tpl(mnesia_index,x),
    dbg:p(all,[c]),
    test_index_plugin(cfg([pd2, disc_only_copies, ordered], [])).

fail(F, Args) ->
    try apply(?MODULE, F, Args),
         error(should_fail)
    catch
        error:R when R =/= should_fail ->
            io:fwrite("apply(~p, ~p, ~p) -> fails as expected~n",
                      [?MODULE, F, Args])
    end.

test_1_ram_copies( _Cfg) -> test(1, ram_copies, r1).
test_1_disc_copies(_Cfg) -> test(1, disc_copies, d1).
fail_1_disc_only(  _Cfg) -> fail(test, [1, disc_only_copies, do1]).

plugin_ram_copies1(Cfg) -> test_index_plugin(cfg([pr1, ram_copies, ordered], Cfg)).
plugin_ram_copies2(Cfg) -> test_index_plugin(cfg([pr2, ram_copies, bag], Cfg)).
plugin_disc_copies(Cfg) -> test_index_plugin(cfg([pd1, disc_copies, ordered], Cfg)).
fail_plugin_disc_only(Cfg) -> fail(test_index_plugin, [cfg([pd2, disc_only_copies, ordered], Cfg)]).
plugin_disc_copies_bag(Cfg) -> test_index_plugin(cfg([pd2, disc_copies, bag], Cfg)).
plugin_rdb_ordered(Cfg) -> test_index_plugin(cfg([pl2, rdb, ordered], Cfg)).

test(N, Type, T) ->
    {atomic, ok} = mnesia:create_table(T, [{Type,[node()]},
                                           {attributes,[k,a,b,c]},
                                           {index, indexes(N)}]),
    ok = test_index(N, T).

add_del_indexes() ->
    {atomic, ok} = mnesia:del_table_index(r1, a),
    {aborted, _} = mnesia:del_table_index(r1, a),
    {atomic, ok} = mnesia:add_table_index(r1, a),
    {aborted, _} = mnesia:add_table_index(r1, a),
    {atomic, ok} = mnesia:del_table_index(d1, a),
    {atomic, ok} = mnesia:add_table_index(d1, a),
    {atomic, ok} = mnesia:del_table_index(do1, a),
    {atomic, ok} = mnesia:add_table_index(do1, a),
    {atomic, ok} = mnesia:del_table_index(l1, a),
    {atomic, ok} = mnesia:add_table_index(l1, a),
    io:fwrite("add_del_indexes() -> ok~n", []).

test_index_plugin(Config) ->
    #{tab := Tab, type := Type, ixtype := IxType} = cfg(Config),
    {atomic, ok} = mnesia:create_table(Tab, [{Type, [node()]},
                                             {index, [{{pfx}, IxType}]}]),
    mnesia:dirty_write({Tab, "foobar", "sentence"}),
    mnesia:dirty_write({Tab, "yellow", "sensor"}),
    mnesia:dirty_write({Tab, "truth", "white"}),
    mnesia:dirty_write({Tab, "fulcrum", "white"}),
    Res1 = [{Tab, "foobar", "sentence"}, {Tab, "yellow", "sensor"}],
    Res2 = [{Tab, "fulcrum", "white"}, {Tab, "truth", "white"}],
    if IxType == bag ->
            Res1 = lists:sort(mnesia:dirty_index_read(Tab,<<"sen">>, {pfx})),
            Res2 = lists:sort(mnesia:dirty_index_read(Tab,<<"whi">>, {pfx})),
            [{Tab,"foobar","sentence"}] = mnesia:dirty_index_read(
                                            Tab, <<"foo">>, {pfx});
       IxType == ordered ->
            Res1 = lists:sort(mnesia:dirty_index_read(Tab,<<"sen">>, {pfx})),
            Res2 = lists:sort(mnesia:dirty_index_read(Tab,<<"whi">>, {pfx})),
            [{Tab,"foobar","sentence"}] = mnesia:dirty_index_read(
                                            Tab, <<"foo">>, {pfx})
    end.

create_bag_index(_Config) ->
    {aborted, {combine_error, _, _}} =
        mnesia:create_table(bi, [{rdb, [node()]}, {index, [{val, bag}]}]),
    ok.

create_ordered_index(_Config) ->
    {atomic, ok} =
        mnesia:create_table(oi, [{rdb, [node()]}, {index, [{val, ordered}]}]),
    ok.

add_indexes(_Config) ->
    T = ?TAB(t1),
    {atomic, ok} = mnesia:create_table(T, [{rdb, [node()]}, {attributes, [k, a, b, c]}]),
    {atomic, ok} = mnesia:add_table_index(T, a),
    ok.

index_plugin_mgmt(_Config) ->
    {aborted,_} = mnesia:create_table(x, [{index,[{unknown}]}]),
    {aborted,_} = mnesia:create_table(x, [{index,[{{unknown},bag}]}]),
    {aborted,_} = mnesia:create_table(x, [{index,[{{unknown},ordered}]}]),
    {atomic,ok} = mnesia_schema:add_index_plugin(
                    {t}, mnesia_rocksdb,ix_prefixes),
    {atomic,ok} = mnesia_schema:delete_index_plugin({t}),
    {aborted,{bad_type,x,_}} =
        mnesia:create_table(x, [{index,[{{t},ordered}]}]),
    %% re-add plugin
    {atomic,ok} = mnesia_schema:add_index_plugin(
                    {t}, mnesia_rocksdb,ix_prefixes),
    {atomic,ok} =
        mnesia:create_table(x, [{index,[{{t},ordered}]}]),
    {aborted,{plugin_in_use,{t}}} =
        mnesia_schema:delete_index_plugin({t}).

test_index(1, T) ->
    L2 = [{T,K,x,y,z} || K <- lists:seq(4,6)],
    L1 = [{T,K,a,b,c} || K <- lists:seq(1,3)],
    true = lists:all(fun(X) -> X == ok end,
                     [mnesia:dirty_write(Obj) || Obj <- L1 ++ L2]),
    L1 = lists:sort(mnesia:dirty_index_read(T,a,a)),
    L1 = lists:sort(mnesia:dirty_index_read(T,a,3)),
    L1 = mnesia:dirty_index_read(T,b,b),
    L1 = lists:sort(mnesia:dirty_index_read(T,c,c)),
    L2 = lists:sort(mnesia:dirty_index_read(T,x,a)),
    L2 = lists:sort(mnesia:dirty_index_read(T,x,3)),
    L2 = mnesia:dirty_index_read(T,y,b),
    L2 = lists:sort(mnesia:dirty_index_read(T,z,c)),
    io:fwrite("test_index(1, ~p) -> ok~n", [T]),
    ok;
test_index(2, T) ->
    L1 = [{T,K,a,b,c} || K <- lists:seq(1,3)],
    L2 = [{T,K,x,y,z} || K <- lists:seq(4,6)],
    true = lists:all(fun(X) -> X == ok end,
                     [mnesia:dirty_write(Obj) || Obj <- L1 ++ L2]),
    L1 = lists:sort(mnesia:dirty_index_read(T,a,a)),
    L1 = lists:sort(mnesia:dirty_index_read(T,a,3)),
    L1 = lists:sort(mnesia:dirty_index_read(T,b,b)),
    L1 = lists:sort(mnesia:dirty_index_read(T,c,c)),
    L2 = lists:sort(mnesia:dirty_index_read(T,x,a)),
    L2 = lists:sort(mnesia:dirty_index_read(T,x,3)),
    L2 = lists:sort(mnesia:dirty_index_read(T,y,b)),
    L2 = lists:sort(mnesia:dirty_index_read(T,z,c)),
    io:fwrite("test_index(1, ~p) -> ok~n", [T]),
    ok;
test_index(3, T) ->
    L2 = [{T,K,x,y,z} || K <- lists:seq(4,6)],
    L1 = [{T,K,a,b,c} || K <- lists:seq(1,3)],
    true = lists:all(fun(X) -> X == ok end,
                     [mnesia:dirty_write(Obj) || Obj <- L1 ++ L2]),
    L1 = mnesia:dirty_index_read(T,a,a),
    L1 = mnesia:dirty_index_read(T,a,3),
    L1 = mnesia:dirty_index_read(T,b,b),
    L1 = mnesia:dirty_index_read(T,c,c),
    L2 = mnesia:dirty_index_read(T,x,a),
    L2 = mnesia:dirty_index_read(T,x,3),
    L2 = mnesia:dirty_index_read(T,y,b),
    L2 = mnesia:dirty_index_read(T,z,c),
    io:fwrite("test_index(1, ~p) -> ok~n", [T]),
    ok.

index_iterator(_Cfg) ->
    T = ?TAB(it),
    {atomic, ok} = mnesia:create_table(T, [ {rocksdb_copies,[node()]}
                                          , {record_name, i}
                                          , {attributes, [k,a,b]}
                                          , {index, [a,b]} ]),
    L2 = [{i,K,a,y} || K <- lists:seq(4,6)],
    L1 = [{i,K,b,x} || K <- lists:seq(1,3)],
    true = lists:all(fun(X) -> X == ok end,
                     [mnesia:dirty_write(T, Obj) || Obj <- L1 ++ L2]),
    ResA = [{b,X} || X <- L1] ++ [{a,Y} || Y <- L2],
    ResB = [{y,X} || X <- L2] ++ [{x,Y} || Y <- L1],
    F = fun iter_all/1,
    ResA = mrdb_index:with_iterator(T, a, F),
    ResB = mrdb_index:with_iterator(T, b, F),
    ok.

iter_all(I) ->
    iter_all(mrdb_index:iterator_move(I, first), I).

iter_all({ok, IxVal, Obj}, I) ->
    [{IxVal, Obj} | iter_all(mrdb_index:iterator_move(I, next), I)];
iter_all(_, _) ->
    [].

indexes(1) ->
    [a,{b,ordered},{c,bag}];
indexes(2) ->
    [a,b,{c,bag}];
indexes(3) ->
    [a,{b,ordered},{c,ordered}].

maybe_set_dir(Config) ->
    case proplists:get_value(priv_dir, Config) of
        undefined ->
            ok;
        PDir ->
            Dir = filename:join(PDir, "mnesia_indexes"),
            application:set_env(mnesia, dir, Dir)
    end.
