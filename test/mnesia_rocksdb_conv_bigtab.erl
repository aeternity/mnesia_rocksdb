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

-module(mnesia_rocksdb_conv_bigtab).

-export([init/0, mktab/2, run/1]).

-record(t, {k, i, v}).

run(Sz) ->
    mnesia:stop(),
    init(),
    mktab(disc_copies, Sz),
    mnesia:change_table_copy_type(t, node(), rdb).

init() ->
    mnesia_rocksdb_tlib:start_mnesia(reset).

mktab(Backend, Sz) ->
    mnesia_rocksdb_tlib:create_table(Backend, [k, i, v], [i]),
    fill_table(Sz).


fill_table(Sz) when is_integer(Sz), Sz > 0 ->
    fill_table(1, Sz).

fill_table(N, Max) when N =< Max ->
    mnesia:dirty_write(#t{k = N, i = N, v = val()}),
    fill_table(N+1, Max);
fill_table(N, _) when is_integer(N) ->
    ok.

val() ->
    {1,2,3,4,5,6,7,8,9,0,
     1,2,3,4,5,6,7,8,9,0,
     1,2,3,4,5,6,7,8,9,0,
     1,2,3,4,5,6,7,8,9,0,
     1,2,3,4,5,6,7,8,9,0,
     1,2,3,4,5,6,7,8,9,0,
     1,2,3,4,5,6,7,8,9,0,
     1,2,3,4,5,6,7,8,9,0,
     1,2,3,4,5,6,7,8,9,0,
     1,2,3,4,5,6,7,8,9,0,
     1,2,3,4,5,6,7,8,9,0}.
