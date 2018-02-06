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

%% This module is used to test backend plugin extensions to the mnesia
%% backend. It also indirectly tests the mnesia backend plugin
%% extension machinery
%%
%% Usage: mnesia_ext_rocksdb_test:recompile(Extension).
%% Usage: mnesia_ext_rocksdb_test:recompile().
%% This command is executed in the release/tests/test_server directory
%% before running the normal tests. The command patches the test code,
%% via a parse_transform, to replace disc_only_copies with the Alias.

-module(mnesia_rocksdb_xform).

-author("roland.karlsson@erlang-solutions.com").
-author("ulf.wiger@klarna.com").

%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Exporting API
-export([recompile/0, recompile/1]).

%% Exporting parse_transform callback
-export([parse_transform/2]).

%% Exporting replacement for mnesia:create_table/2
-export([create_table/1, create_table/2, rpc/4]).

%% API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Recompiling the test code, replacing disc_only_copies with
%% Extension.
recompile() ->
    [{Module,Alias}|_] = extensions(),
    recompile(Module, Alias).

recompile(MorA) ->
    case { lists:keyfind(MorA, 1, extensions()),
           lists:keyfind(MorA, 2, extensions())
         } of
        {{Module,Alias}, _} ->
            recompile(Module, Alias);
        {false, {Module,Alias}} ->
            recompile(Module, Alias);
        {false,false} ->
            {error, cannot_find_module_or_alias}
    end.

recompile(Module, Alias) ->
    io:format("recompile(~p,~p)~n",[Module, Alias]),
    put_ext(module, Module),
    put_ext(alias, Alias),
    Modules = [ begin {M,_} = lists:split(length(F)-4, F),
                      list_to_atom(M) end ||
                  F <- begin {ok,L} = file:list_dir("."), L end,
                  lists:suffix(".erl", F),
                  F=/= atom_to_list(?MODULE) ++ ".erl" ],
    io:format("Modules = ~p~n",[Modules]),
    lists:foreach(fun(M) ->
                          c:c(M, [{parse_transform, ?MODULE}])
                  end, Modules).

%% TEST REPLACEMENT CALLBACKS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% replacement for mnesia:create_table that ensures that
create_table(Name, Parameters) ->
    create_table([{name,Name} | Parameters]).

create_table(Parameters) when is_list(Parameters) ->
    case lists:keymember(rocksdb_copies, 1, Parameters) of
        true ->
            %% case lists:member({type, bag}, Parameters) of
            %%     true ->
            %%         ct:comment("ERROR: Contains rocksdb table with bag"),
            %%         {aborted, {rocksdb_does_not_support_bag, Parameters}};
            %%     false ->
            ct:comment("INFO: Contains rocksdb table"),
            io:format("INFO: create_table(~p)~n", [Parameters]),
            mnesia:create_table(Parameters);
            %% end;
        false ->
            mnesia:create_table(Parameters)
    end;
create_table(Param) ->
    %% Probably bad input, e.g. from mnesia_evil_coverage_SUITE.erl
    mnesia:create_table(Param).


rpc(N, mnesia, start, [Opts]) ->
    case lists:keymember(schema, 1, Opts) of
        true  -> rpc:call(N, mnesia, call, [Opts]);
        false ->
            Opts1 = [{schema, [{backend_types, backends()}]}|Opts],
            rpc:call(N, mnesia, start, [Opts1])
    end;
rpc(N, M, F, A) ->
    rpc:call(N, M, F, A).


%% PARSE_TRANSFORM CALLBACK %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% The callback for c:c(Module, [{parse_transform,?MODULE}])
parse_transform(Forms, _Options) ->
    plain_transform(fun do_transform/1, Forms).

do_transform({'attribute', _, module, Module}) ->
    io:format("~n~nMODULE: ~p~n", [Module]),
    continue;
do_transform({'atom', Line, disc_only_copies}) ->
    io:format("replacing disc_only_copies with ~p~n", [get_ext(alias)]),
    {'atom', Line, get_ext(alias)};
do_transform(Form = { call, L1,
                      { remote, L2,
                        {atom, L3, mnesia},
                        {atom, L4, create_table}},
                      Arguments}) ->
    NewForm = { call, L1,
                { remote, L2,
                  {atom, L3, ?MODULE},
                  {atom, L4, create_table}},
                plain_transform(fun do_transform/1, Arguments)},
    io:format("~nConvert Form:~n~s~n~s~n", [pp_form(Form), pp_form(NewForm)]),
    NewForm;
do_transform(Form = { call, L1,
                      { remote, L2,
                        {atom, L3, rpc},
                        {atom, L4, call}},
                      [{var, _, _} = N, {atom, _, mnesia} = Mnesia,
                       {atom, _, start} = Start, Args]}) ->
    NewForm = { call, L1, { remote, L2,
                            {atom, L3, ?MODULE},
                            {atom, L4, rpc}},
                [N, Mnesia, Start, Args]},
    io:format("~nConvert Form:~n~s~n~s~n", [pp_form(Form), pp_form(NewForm)]),
    NewForm;

do_transform(Form = { call, L1,
                      { remote, L2,
                        {atom, L3, mnesia},
                        {atom, L4, create_schema}},
                      [Nodes]}) ->
    P = element(2, Nodes),
    NewForm = { call, L1,
                { remote, L2,
                  {atom, L3, mnesia},
                  {atom, L4, create_schema}},
                [Nodes, erl_parse:abstract([{backend_types, backends()}], P)]},
    io:format("~nConvert Form:~n~s~n~s~n", [pp_form(Form), pp_form(NewForm)]),
    NewForm;
do_transform(Form = { call, L1,
                      { remote, L2,
                        {atom, L3, mnesia},
                        {atom, L4, start}},
                      []}) ->
    NewForm = { call, L1,
                { remote, L2,
                  {atom, L3, mnesia},
                  {atom, L4, start}},
                [erl_parse:abstract(
                   [{schema, [{backend_types, backends()}]}], L4)]},
    io:format("~nConvert Form:~n~s~n~s~n", [pp_form(Form), pp_form(NewForm)]),
    NewForm;
do_transform(Form = { call, L1,
                      { remote, L2,
                        {atom, L3, mnesia},
                        {atom, L4, start}},
                      [Opts]}) ->
    P = element(2, Opts),
    NewForm = { call, L1,
                { remote, L2,
                  {atom, L3, mnesia},
                  {atom, L4, start}},
                [{cons, P,
                  erl_parse:abstract(
                    {schema, [{backend_types, backends()}]}, L4), Opts}]},
    io:format("~nConvert Form:~n~s~n~s~n", [pp_form(Form), pp_form(NewForm)]),
    NewForm;

 %% 1354:unsupp_user_props(doc) ->
 %% 1355:    ["Simple test of adding user props in a schema_transaction"];
 %% 1356:unsupp_user_props(suite) -> [];
 %% 1357:unsupp_user_props(Config) when is_list(Config) ->
do_transform(Form = { function, L1, F, 1, [C1, C2, C3] })
  when F == unsupp_user_props ->
    L3 = element(2, C3),
    NewForm = { function, L1, F, 1,
                [C1, C2, {clause, L3, [{var, L3, '_'}], [],
                          [{tuple, L3, [{atom, L3, skip},
                                        erl_parse:abstract(
                                          "Skipped for rocksdb test", L3)]}
                          ]} ] },
    io:format("~nConvert Form:"
              "~n=============~n~s"
              "==== To: ====~n~s"
              "=============~n",
              [cut(20, pp_form(Form)),  cut(20, pp_form(NewForm))]),
    NewForm;
do_transform(Form = { function, L1, F, 1, [C1, C2] })
  when F == storage_options ->
    L2 = element(2, C2),
    NewForm = { function, L1, F, 1,
                [C1, {clause, L2, [{var, L2, '_'}], [],
                          [{tuple, L2, [{atom, L2, skip},
                                        erl_parse:abstract(
                                          "Skipped for rocksdb test", L2)]}
                          ]} ] },
    io:format("~nConvert Form:"
              "~n=============~n~s"
              "==== To: ====~n~s"
              "=============~n",
              [cut(20, pp_form(Form)), cut(20, pp_form(NewForm))]),
    NewForm;
do_transform(_Form) ->
    continue.

pp_form(F) when element(1,F) == attribute; element(1,F) == function ->
    erl_pp:form(F);
pp_form(F) ->
    erl_pp:expr(F).

cut(Lines, S) ->
    case re:split(S, "\\v", [{return,list}]) of
        Lns when length(Lns) =< Lines ->
            S;
        Lns ->
            lists:flatten(
              add_lf(lists:sublist(Lns, 1, Lines) ++ ["...\n"]))
    end.

add_lf([H|T]) ->
    [H | ["\n" ++ L || L <- T]].

%% INTERNAL %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% A trick for doing parse transforms easier

plain_transform(Fun, Forms) when is_function(Fun, 1), is_list(Forms) ->
    plain_transform1(Fun, Forms).

plain_transform1(_, []) ->
    [];
plain_transform1(Fun, [F|Fs]) when is_atom(element(1,F)) ->
    case Fun(F) of
        continue ->
            [list_to_tuple(plain_transform1(Fun, tuple_to_list(F))) |
             plain_transform1(Fun, Fs)];
        {done, NewF} ->
            [NewF | Fs];
        {error, Reason} ->
            io:format("Error: ~p (~p)~n", [F,Reason]);
        NewF when is_tuple(NewF) ->
            [NewF | plain_transform1(Fun, Fs)]
    end;
plain_transform1(Fun, [L|Fs]) when is_list(L) ->
    [plain_transform1(Fun, L) | plain_transform1(Fun, Fs)];
plain_transform1(Fun, [F|Fs]) ->
    [F | plain_transform1(Fun, Fs)];
plain_transform1(_, F) ->
    F.

%% Existing extensions.
%% NOTE: The first is default.
extensions() ->
    [ {mnesia_rocksdb, rocksdb_copies}
    ].
    %%   {mnesia_ext_filesystem, fs_copies},
    %%   {mnesia_ext_filesystem, fstab_copies},
    %%   {mnesia_ext_filesystem, raw_fs_copies}
    %% ].

backends() ->
    [{T,M} || {M,T} <- extensions()].

%% Process global storage

put_ext(Key, Value) ->
    ets:insert(global_storage(), {Key, Value}).

global_storage() ->
    case whereis(?MODULE) of
        undefined ->
            Me = self(),
            P = spawn(fun() ->
                              T = ets:new(?MODULE, [public,named_table]),
                              init_ext(T),
                              register(?MODULE, self()),
                              Me ! {self(), done},
                              wait()
                      end),
            receive {P, done} ->
                    ok
            end;
        _ ->
            ok
    end,
    ?MODULE.

init_ext(T) ->
    [{Mod,Alias}|_] = extensions(),
    ets:insert(T, {alias, Alias}),
    ets:insert(T, {module, Mod}).

wait() ->
    receive stop ->
            ok
    end.

get_ext(Key) ->
    case catch ets:lookup(global_storage(), Key) of
        [] ->
            io:format("Data for ~p not stored~n", [Key]),
            undefined;
        {'EXIT', Reason} ->
            io:format("Get value for ~p failed (~p)~n", [Key, Reason]),
            undefined;
        [{Key,Value}] ->
            Value
    end.
