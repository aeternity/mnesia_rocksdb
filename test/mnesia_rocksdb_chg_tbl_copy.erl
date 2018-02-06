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

%% @doc Run through all combinations of change_table_copy_type
%% @author Ulf Wiger <ulf.wiger@feuerlabs.com>

-module(mnesia_rocksdb_chg_tbl_copy).

%% This module implements a test (to be run manually) for iterating through
%% all table copy types on a mnesia table.

-export([full/0,
         run/0,
         run/1]).
-export([trace/0]).

full() ->
    Perms = perms(copies()),
    Res = [run(P) || P <- Perms],
    Res = [ok || _ <- Perms].

run() ->
    run([rdb,disc_copies,rdb,ram_copies,disc_only_copies,rdb]).
    %% run([rdb,disc_only_copies]).
    %% run([rdb,ram_copies]).

perms([]) ->
    [[]];
perms(L)  -> [[H|T] || H <- L, T <- perms(L--[H])].

copies() ->
    [rdb,ram_copies,disc_copies,disc_only_copies].

run([T|Types]) ->
    mnesia:stop(),
    start_mnesia(),
    ok = create_tab(T),
    ok = change_type(Types, T).

create_tab(Type) ->
    {atomic,ok} = mnesia:create_table(
                    t, [{Type, [node()]},
                        {attributes, [k,v]},
                        {index, [v]}]),
    fill_tab(),
    check_tab(),
    ok.

change_type([To|Types], From) ->
    io:fwrite("changing from ~p to ~p~n", [From, To]),
    {atomic, ok} = mnesia:change_table_copy_type(t, node(), To),
    ok = check_tab(),
    io:fwrite("...ok~n", []),
    change_type(Types, To);
change_type([], _) ->
    ok.

fill_tab() ->
    Res = [mnesia:dirty_write({t,K,V}) || {t,K,V} <- l()],
    Res = [ok || _ <- Res],
    ok.

l() -> [{t,a,1},
        {t,b,2},
        {t,c,3},
        {t,d,4}].

check_tab() ->
    L = l(),
    L = lists:append([mnesia:dirty_read({t,K}) || K <- [a,b,c,d]]),
    L = lists:append([mnesia:dirty_index_read(t,V,v) ||
                         V <- [1,2,3,4]]),
    ok.

start_mnesia() -> mnesia_rocksdb_tlib:start_mnesia(reset).

trace() ->
    dbg:tracer(),
    [tp(M) || M <- mods()],
    dbg:p(all,[c]),
    try run()
    after
        [ctp(M) || M <- mods()],
        dbg:stop()
    end.

tp({l,M}  ) -> dbg:tpl(M,x);
tp({g,M}  ) -> dbg:tp(M,x);
tp({l,M,F}) -> dbg:tpl(M,F,x);
tp({g,M,F}) -> dbg:tp(M,F,x).

ctp({l,M}  ) -> dbg:ctpl(M);
ctp({g,M}  ) -> dbg:ctp(M);
ctp({l,M,F}) -> dbg:ctpl(M,F);
ctp({g,M,F}) -> dbg:ctp(M,F).

mods() ->
    [
     %% {l, mnesia_index},
     %% {l, mnesia_lib, semantics}].
     %% {g,mnesia_monitor},
     %% {l,mnesia_dumper},
     %% {g,mnesia_loader},
     %% {g,mnesia_checkpoint},
     %% {g,mnesia_lib},
     {l,mnesia_schema,expand_index_attrs},
     {l,mnesia_schema,list2cs},
     {g,mnesia_schema,new_cs},
     {g,mnesia_schema,make_change_table_copy_type},
     {g,mnesia_schema,make_create_table},
     {g,mnesia_lib,semantics},
     {l,mnesia_dumper},
     {g,mnesia_lib,exists},
     {g,mnesia},
     {l,mnesia_schema,intersect_types},
     {g,ets,new}].
