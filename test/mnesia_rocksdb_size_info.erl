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

-module(mnesia_rocksdb_size_info).

-export([run/0]).

-define(m(A, B), (fun(L) -> {L,A} = {L,B} end)(?LINE)).


run() ->
    initialize_mnesia(),
    test_set(),
    test_bag().

initialize_mnesia() ->
    mnesia:stop(),
    mnesia:delete_schema([node()]),
    mnesia:create_schema([node()], [{backend_types,
                                     [{rocksdb_copies, mnesia_rocksdb}]}]),
    mnesia:start(),
    {atomic,ok} = mnesia:create_table(s, [{type, set},
                                          {record_name, x},
                                          {rocksdb_copies, [node()]}]),
    {atomic,ok} = mnesia:create_table(b, [{type, bag},
                                          {record_name, x},
                                          {rocksdb_copies, [node()]}]),
    ok.

test_set() ->
    ?m(0, mnesia:table_info(s, size)),
    ?m(1, w(s, 1, a)),
    ?m(1, w(s, 1, b)),
    ?m(2, w(s, 2, c)),
    ?m(3, w(s, 3, d)),
    ?m(2, d(s, 3)),
    mnesia:stop(),
    mnesia:start(),
    await(s),
    ?m(2, mnesia:table_info(s, size)).

test_bag() ->
    ?m(0, mnesia:table_info(b, size)),
    ?m(1, w(b, 1, a)),
    ?m(2, w(b, 1, b)),
    ?m(3, w(b, 2, a)),
    ?m(4, w(b, 2, d)),
    ?m(5, w(b, 2, c)),
    ?m(4, do(b, 2, c)),
    ?m(2, d(b, 2)),
    mnesia:stop(),
    mnesia:start(),
    await(b),
    ?m(2, mnesia:table_info(b, size)).

w(T, K, V) ->
    ok = mnesia:dirty_write(T, {x, K, V}),
    mnesia:table_info(T, size).

d(T, K) ->
    mnesia:dirty_delete({T, K}),
    mnesia:table_info(T, size).

do(T, K, V) ->
    mnesia:dirty_delete_object(T, {x, K, V}),
    mnesia:table_info(T, size).

await(T) ->
    ?m(ok, mnesia:wait_for_tables([T], 10000)).
