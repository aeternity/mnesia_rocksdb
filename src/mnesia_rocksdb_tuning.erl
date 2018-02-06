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

-module(mnesia_rocksdb_tuning).

-export([describe_env/0,
         get_maxfiles/0, get_maxfiles/1,
         get_avail_ram/0,
         rdb_tabs/0, rdb_tabs/1,
         rdb_indexes/0, rdb_indexes/1,
         count_rdb_tabs/0, count_rdb_tabs/1,
         calc_sizes/0, calc_sizes/1,
         ideal_max_files/0, ideal_max_files/1,
         max_files/1,
         write_buffer/1,
         cache/1,
         default/1]).

-include("mnesia_rocksdb_tuning.hrl").

-define(KB, 1024).
-define(MB, 1024 * 1024).
-define(GB, 1024 * 1024 * 1024).

describe_env() ->
    #tuning{max_files = get_maxfiles(),
            avail_ram = get_avail_ram()}.

get_maxfiles() ->
    get_maxfiles(os:type()).

get_maxfiles({unix,darwin}) ->
    Limit = os:cmd("launchctl limit maxfiles"),
    [_, Soft, _] = string:tokens(Limit, " \t\n"),
    list_to_integer(Soft);
get_maxfiles({unix,linux}) ->
    [L|_] = string:tokens(os:cmd("ulimit -n"), "\n"),
    list_to_integer(L).

%% Returns installed RAM in Gigabytes
get_avail_ram() ->
    get_avail_ram(os:type()).

get_avail_ram({unix,darwin}) ->
    {match, [S]} =
        re:run(os:cmd("/usr/sbin/system_profiler SPHardwareDataType"),
               "Memory: (.+) GB", [{capture, [1], list}]),
    list_to_integer(S);
get_avail_ram({unix,linux}) ->
    {match, [S]} =
        re:run(os:cmd("free -g"), "Mem:[ ]+([0-9]+) ",[{capture,[1],list}]),
    list_to_integer(S).

rdb_tabs() ->
    rdb_tabs(mnesia_lib:dir()).

rdb_tabs(Db) ->
    rdb_tabs(list_dir(Db), Db).

rdb_tabs(Fs, _Db) ->
    lists:flatmap(
      fun(F) ->
              case re:run(F, "(.+)-_tab\\.extrdb",
                          [global,{capture,[1],list}]) of
                  {match, [Match]} ->
                      Match;
                  _ ->
                      []
              end
      end, Fs).

rdb_indexes() ->
    rdb_indexes(mnesia_lib:dir()).

rdb_indexes(Db) ->
    rdb_indexes(list_dir(Db), Db).

rdb_indexes(Fs, _Db) ->
    lists:flatmap(
      fun(F) ->
              case re:run(F, "(.+)-([0-9]+)-_ix\\.extrdb",
                          [global,{capture,[1,2],list}]) of
                  {match, [[T,P]]} ->
                      [{T,P}];
                  _ ->
                      []
              end
      end, Fs).

list_dir(D) ->
    case file:list_dir(D) of
        {ok, Fs} -> Fs;
        {error, Reason} -> erlang:error({Reason,D})
    end.

fname({Tab,IxPos}, Dir) ->
    filename:join(Dir, Tab ++ "-" ++ IxPos ++ "-_ix.extrdb");
fname(Tab, Dir) when is_list(Tab) ->
    filename:join(Dir, Tab ++ "-_tab.extrdb").

%% Number of rocksdb tables + indexes
count_rdb_tabs() ->
    count_rdb_tabs(mnesia_lib:dir()).

count_rdb_tabs(Db) ->
    Fs = list_dir(Db),
    length(rdb_tabs(Fs, Db)) + length(rdb_indexes(Fs, Db)).

calc_sizes() ->
    calc_sizes(mnesia_lib:dir()).

calc_sizes(D) ->
    lists:sort(
      fun(A,B) -> sort_size(B,A) end,  % rev sort
      [{T, dir_size(fname(T, D))} || T <- rdb_tabs(D)]
      ++ [{I, dir_size(fname(I, D))} || I <- rdb_indexes(D)]).

ideal_max_files() ->
    ideal_max_files(mnesia_lib:dir()).

ideal_max_files(D) ->
    [{T,Sz,max_files(Sz)} || {T, Sz} <- calc_sizes(D)].

max_files({I,g}) ->
    round(I * 1000) div 20;
max_files({I,m}) when I > 400 ->
    round(I) div 20;
max_files(_) ->
    default(max_open_files).

write_buffer({_,g}) ->
    120 * ?MB;
write_buffer({I,m}) when I > 400 ->
    120 * ?MB;
write_buffer(_) ->
    default(write_buffer).

cache({_,g}) ->
    8 * ?MB;
cache({I,m}) when I > 400 ->
    6 * ?MB;
cache(_) ->
    default(cache).

default(write_buffer) -> 2 * ?MB;
default(max_open_files) -> 20;
default(cache) -> 2 * ?MB.

%% open_file_memory() ->
%%     (max_open_files-10) *
%%         (184 + (average_sst_filesize/2048) *
%%              (8 + ((average_key_size+average_value_size)/2048 +1) * 0.6).

dir_size(D) ->
    R =  os:cmd("du -hc " ++ D ++ " | grep total"),
    parse_sz(hd(string:tokens(R," \t\n"))).

parse_sz(S) ->
    {match,[I,U]} = re:run(S, "([\\.0-9]+)([BKMG])", [{capture,[1,2],list}]),
    {scan_num(I), unit(U)}.

scan_num(S) ->
    case erl_scan:string(S) of
        {ok, [{integer,_,I}],_} ->
            I;
        {ok, [{float,_,F}],_} ->
            F
    end.

unit("B") -> b;
unit("K") -> k;
unit("M") -> m;
unit("G") -> g.

%% Custom sort: b < k < m < g
sort_size({_,{A,U}},{_,{B,U}}) -> A < B;
sort_size({_,{_,U1}},{_,{_,U2}}) ->
    case {U1,U2} of
        {b,_} -> true;
        {k,_} when U2 =/= b -> true;
        {m,g} -> true;
        _ -> false
    end.
