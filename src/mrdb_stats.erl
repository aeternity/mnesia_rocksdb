%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% @doc Statistics API for the mnesia_rocksdb plugin
%%
%% Some counters are maintained for each active alias. Currently, the following
%% counters are supported:
%% * inner_retries
%% * outer_retries
%%
-module(mrdb_stats).

-export([new/0]).

-export([incr/3,
         get/1,
         get/2]).

-type alias() :: mnesia_rocksdb:alias().
-type db_ref() :: mrdb:db_ref().
-type counter() :: atom().
-type increment() :: integer().

-type counters() :: #{ counter() := integer() }.

new() ->
    #{ ref => counters:new(map_size(ctr_meta()), [write_concurrency])
     , meta => ctr_meta()}.

ctr_meta() ->
    #{ inner_retries => 1
     , outer_retries => 2 }.

-spec incr(alias() | db_ref(), counter(), increment()) -> ok.
%% @doc Increment `Ctr' counter for `Alias` with increment `N'.
%%
%% Note that the first argument may also be a `db_ref()' map,
%% corresponding to `mrdb:get_ref({admin, Alias})'.
%% @end
incr(Alias, Ctr, N) when is_atom(Alias) ->
    #{stats := #{ref := Ref, meta := Meta}} = mrdb:get_ref({admin, Alias}),
    incr_(Ref, Meta, Ctr, N);
incr(#{stats := #{ref := Ref, meta := Meta}}, Ctr, N) ->
    incr_(Ref, Meta, Ctr, N).

-spec get(alias() | db_ref(), counter()) -> integer().
%% @doc Fetches the integer value of the known counter `Ctr' for `Alias'.
%% @end
get(Alias, Ctr) when is_atom(Alias) ->
    #{stats := #{ref := Ref, meta := Meta}} = mrdb:get_ref({admin, Alias}),
    get_(Ref, Meta, Ctr);
get(#{stats := #{ref := Ref, meta := Meta}}, Ctr) ->
    get_(Ref, Meta, Ctr).

-spec get(alias() | db_ref()) -> counters().
%% @doc Fetches all known counters for `Alias', in the form of a map,
%% `#{Counter => Value}'.
%% @end
get(Alias) when is_atom(Alias) ->
    get_(mrdb:get_ref({admin, Alias}));
get(Ref) when is_map(Ref) ->
    get_(Ref).

get_(#{stats := #{ref := Ref, meta := Meta}}) ->
    lists:foldl(
      fun({K, P}, M) ->
              M#{K := counters:get(Ref, P)}
      end, Meta, maps:to_list(Meta)).

get_(Ref, Meta, Attr) ->
    Ix = maps:get(Attr, Meta),
    counters:get(Ref, Ix).

incr_(Ref, Meta, Attr, N) ->
    Ix = maps:get(Attr, Meta),
    counters:add(Ref, Ix, N).
