%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
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

-module(mnesia_rocksdb_params).

-behaviour(gen_server).

-export([lookup/2,
         store/2,
         delete/1]).

-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("mnesia_rocksdb_tuning.hrl").
-include("mnesia_rocksdb_int.hrl").

-define(KB, 1024).
-define(MB, 1024 * 1024).
-define(GB, 1024 * 1024 * 1024).

lookup(Tab, Default) ->
    try ets:lookup(?MODULE, Tab) of
        [{_, Params}] ->
            Params;
        [] ->
            Default
    catch error:badarg ->
            Default
    end.

store(Tab, Params) ->
    ets:insert(?MODULE, {Tab, Params}).

delete(Tab) ->
    ets:delete(?MODULE, Tab).

start_link() ->
    case ets:info(?MODULE, name) of
        undefined ->
            ets:new(?MODULE, [ordered_set, public, named_table]),
            load_tuning_parameters();
        _ ->
            ok
    end,
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    {ok, []}.

handle_call(_, _, S) -> {reply, error, S}.
handle_cast(_, S)    -> {noreply, S}.
handle_info(_, S)    -> {noreply, S}.
terminate(_, _)      -> ok.
code_change(_, S, _) -> {ok, S}.

load_tuning_parameters() ->
    case application:get_env(mnesia_rocksdb, tuning_params) of
        {ok, Ps} ->
            case Ps of
                {consult, F} -> consult(F);
                {script, F} -> script(F);
                _ when is_list(Ps) ->
                    store_params(Ps)
            end;
        _ ->
            ok
    end.

consult(F) ->
    case file:consult(F) of
        {ok, Terms} ->
            store_params(Terms);
        {error, Reason} ->
            {error, {Reason, F}}
    end.

script(F) ->
    case file:script(F) of
        {ok, Terms} ->
            store_params(Terms);
        {error, Reason} ->
            {error, {Reason, F}}
    end.

store_params(Params) ->
    _ = lists:foreach(fun({_,S}) -> valid_size(S) end, Params),
    NTabs = length(Params),
    Env0= mnesia_rocksdb_tuning:describe_env(),
    Env = Env0#tuning{n_tabs = NTabs},
    ?log(debug, "Env = ~p~n", [Env]),
    TotalFiles = lists:sum([mnesia_rocksdb_tuning:max_files(Sz) ||
                               {_, Sz} <- Params]),
    ?log(debug, "TotalFiles = ~p~n", [TotalFiles]),
    MaxFs = Env#tuning.max_files,
    ?log(debug, "MaxFs = ~p~n", [MaxFs]),
    FsHeadroom = MaxFs * 0.6,
    ?log(debug, "FsHeadroom = ~p~n", [FsHeadroom]),
    FilesFactor = if TotalFiles =< FsHeadroom ->
                          1;  % don't have to scale down
                     true ->
                          FsHeadroom / TotalFiles
                  end,
    Env1 = Env#tuning{files_factor = FilesFactor},
    ?log(debug, "Env1 = ~p~n",            [Env1]),
    lists:foreach(
      fun({Tab, Sz}) when is_atom(Tab);
                          is_atom(element(1,Tab)),
                          is_integer(element(2,Tab)) ->
              ets:insert(?MODULE, {Tab, rdb_params(Sz, Env1, Tab)})
      end, Params).

rdb_params(Sz, Env, _Tab) ->
    MaxFiles = mnesia_rocksdb_tuning:max_files(Sz) * Env#tuning.files_factor,
    Opts = if Env#tuning.avail_ram > 100 ->  % Gigabytes
                 [{write_buffer_size, mnesia_rocksdb_tuning:write_buffer(Sz)},
                  {cache_size, mnesia_rocksdb_tuning:cache(Sz)}];
              true ->
                   []
           end,
    [{max_open_files, MaxFiles} | Opts].

valid_size({I,U}) when is_number(I) ->
    true = lists:member(U, [k,m,g]).
