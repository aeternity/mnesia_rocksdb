-module(mrdb_ttb).

-export([ on_nodes/2
        , stop/0
        , stop_nofetch/0
        , format/2
        , format/3 ]).

-export([ patterns/0
        , flags/0 ]).

on_nodes(Ns, File) ->
    tr_ttb:on_nodes(Ns, File, ?MODULE).

patterns() ->
    mrdb:patterns().

flags() ->
    {all, call}.

stop() ->
    tr_ttb:stop().

stop_nofetch() ->
    tr_ttb:stop_nofetch().

format(Dir, Out) ->
    tr_ttb:format(Dir, Out).

format(Dir, Out, Opts) ->
    tr_ttb:format(Dir, Out, Opts).
