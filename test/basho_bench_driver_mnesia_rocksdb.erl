%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
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
%%
%% -------------------------------------------------------------------

-module(basho_bench_driver_mnesia_rocksdb).

-export([new/1,
         run/4]).

-include("mnesia_rocksdb_basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    Type = basho_bench_config:get(backend, ram_copies),
    Tab = basho_bench_config:get(mnesia_table, t),
    ok = bootstrap_mnesia(Tab, Type),
    {ok, Tab}.

bootstrap_mnesia(Tab, Type) ->
    ok = mnesia:create_schema([node()],
                              [{backend_types,
                                [{rocksdb_copies, mnesia_rocksdb}]}]),
    ok = mnesia:start(),
    {atomic,ok} = mnesia:create_table(Tab, [{Type, [node()]}]),
    mnesia:wait_for_tables([Tab], 10000).

run(get, KeyGen, _ValueGen, State) ->
    Tab = State,
    Key = KeyGen(),
    case mnesia:dirty_read({Tab, Key}) of
        [] ->
            {ok, State};
        [{_, Key, _}] ->
            {ok, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    Tab = State,
    ok = mnesia:dirty_write({Tab, KeyGen(), ValueGen()}),
    {ok, State};
run(delete, KeyGen, _ValueGen, State) ->
    Tab = State,
    ok = mnesia:dirty_delete({Tab, KeyGen()}),
    {ok, State}.
