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

-module(mnesia_rocksdb_indexes).

-export([run/0,
         run/1,
         r1/0]).

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
    test_index_plugin(pr1, ram_copies, ordered),
    test_index_plugin(pr2, ram_copies, bag),
    test_index_plugin(pd1, disc_copies, ordered),
    fail(test_index_plugin, [pd2, disc_only_copies, ordered]),
    test_index_plugin(pd2, disc_copies, bag),
    test_index_plugin(pl2, rdb, ordered),
    test_index_plugin_mgmt(),
    ok.

maybe_set_dir(Config) ->
    case proplists:get_value(priv_dir, Config) of
        undefined ->
            ok;
        PDir ->
            Dir = filename:join(PDir, "mnesia_indexes"),
            application:set_env(mnesia, dir, Dir)
    end.

r1() ->
    mnesia:stop(),
    ok = mnesia_rocksdb_tlib:start_mnesia(reset),
    {atomic,ok} = mnesia_schema:add_index_plugin(
                    {pfx},mnesia_rocksdb, ix_prefixes),
    dbg:tracer(),
    dbg:tpl(mnesia_schema,x),
    dbg:tpl(mnesia_index,x),
    dbg:p(all,[c]),
    test_index_plugin(pd2, disc_only_copies, ordered).

fail(F, Args) ->
    try apply(?MODULE, F, Args),
         error(should_fail)
    catch
        error:_ ->
            io:fwrite("apply(~p, ~p, ~p) -> fails as expected~n",
                      [?MODULE, F, Args])
    end.

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

test_index_plugin(Tab, Type, IxType) ->
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
    end,
    io:fwrite("test_index_plugin(~p, ~p, ~p) -> ok~n", [Tab,Type,IxType]).

test_index_plugin_mgmt() ->
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

indexes(1) ->
    [a,{b,ordered},{c,bag}];
indexes(2) ->
    [a,b,{c,bag}];
indexes(3) ->
    [a,{b,ordered},{c,ordered}].
