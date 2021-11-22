-module(mnesia_rocksdb_SUITE).

-export([
         all/0
        , groups/0
        , suite/0
        , init_per_suite/1
        , end_per_suite/1
        , init_per_group/2
        , end_per_group/2
        , init_per_testcase/2
        , end_per_testcase/2
        ]).

-export([ mrdb_transactions/1
        , mrdb_abort/1
        , mrdb_two_procs/1
        , mrdb_two_procs_snap/1
        ]).

-include_lib("common_test/include/ct.hrl").

suite() ->
    [].

all() ->
    [{group, all_tests}].

groups() ->
    [
      {all_tests, [sequence], [ {group, mrdb} ]}
                              %% , error_handling ]}
    , {mrdb, [sequence], [ mrdb_transactions
                         , mrdb_abort
                         , mrdb_two_procs
                         , mrdb_two_procs_snap ]}
    ].


%% error_handling(Config) ->
%%     mnesia_rocksdb_error_handling:run(Config).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(mrdb, Config) ->
    mnesia:stop(),
    ok = mnesia_rocksdb_tlib:start_mnesia(reset),
    Config;

init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(mrdb_two_procs_snap, Config) ->
    %% dbg:tracer(),
    %% dbg:tpl(mrdb,x),
    %% dbg:tp(mnesia,create_table,x),
    %% dbg:p(all,[c]),
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    %% dbg:ctpl('_'),
    %% dbg:ctp('_'),
    %% dbg:stop(),
    ok.

mrdb_transactions(_Config) ->
    Created = create_tabs([{tx, []}]),
    mrdb:insert(tx, {tx, a, 1}),
    [_] = mrdb:read(tx, a),
    mrdb:activity(
      tx, rdb,
      fun() ->
              [{tx,a,N}] = mrdb:read(tx, a),
              N1 = N+1,
              ok = mrdb:insert(tx, {tx,a,N1})
      end),
    [{tx,a,2}] = mrdb:read(tx,a),
    delete_tabs(Created),
    ok.

mrdb_abort(_Config) ->
    Created = create_tabs([{tx_abort, []}]),
    mrdb:insert(tx_abort, {tx_abort, a, 1}),
    Pre = mrdb:read(tx_abort, a),
    TRes = try mrdb:activity(
                 tx, rdb,
                 fun() ->
                         [{tx_abort, a, N}] = mrdb:read(tx_abort, a),
                         error(abort_here),
                         ok = mrdb:insert(tx_abort, [{tx_abort, a, N+1}]),
                         noooo
                 end)
           catch
               error:abort_here ->
                   ok
           end,
    ok = TRes,
    Pre = mrdb:read(tx_abort, a),
    delete_tabs(Created),
    ok.

mrdb_two_procs(_Config) ->
    R = ?FUNCTION_NAME,
    Parent = self(),
    Created = create_tabs([{R, []}]),
    mrdb:insert(R, {R, a, 1}),
    Pre = mrdb:read(R, a),
    F0 = fun() ->
                 ok = mrdb:insert(R, {R, a, 17}),
                 Parent ! {self(), ready},
                 receive {Parent, cont} -> ok
                 after 1000 -> error(inner_timeout)
                 end
         end,
    {POther, MRef} = spawn_opt(
                       fun() ->
                               ok = mrdb:activity(tx, rdb, F0)
                       end, [link, monitor]),
    receive {POther, ready} -> ok after 1000 -> error(timeout) end,
    Pre = mrdb:read(R, a),
    F1 = fun() ->
                 POther ! {self(), cont},
                 Pre = mrdb:read(R, a),
                 receive {'DOWN', MRef, _, _, _} -> ok
                 after 1000 -> error(monitor_timeout)
                 end,
                 [{R, a, 17}] = mrdb:read(R, a),
                 ok = mrdb:insert(R, {R, a, 18})
         end,
    try mrdb:activity(tx, rdb, F1)
    catch
        error:{error, "Resource busy" ++ _} ->
            ok
    end,
    [{R, a, 17}] = mrdb:read(R, a),
    delete_tabs(Created),
    ok.

mrdb_two_procs_snap(_Config) ->
    R = ?FUNCTION_NAME,
    Parent = self(),
    Created = create_tabs([{R, []}]),
    mrdb:insert(R, {R, a, 1}),
    Pre = mrdb:read(R, a),
    mrdb:insert(R, {R, b, 11}),
    PreB = mrdb:read(R, b),
    F0 = fun() ->
                 ok = mrdb:insert(R, {R, a, 17}),
                 Parent ! {self(), ready},
                 receive {Parent, cont} -> ok
                 after 1000 -> error(inner_timeout)
                 end
         end,
    {POther, MRef} =
        spawn_opt(fun() ->
                          ok = mrdb:activity(tx, rdb, F0)
                  end, [link, monitor]),
    receive {POther, ready} -> ok after 1000 -> error(timeout) end,
    Pre = mrdb:read(R, a),
    F1 = fun() ->
                 POther ! {self(), cont},
                 Pre = mrdb:read(R, a),
                 receive {'DOWN', MRef, _, _, _} -> ok
                 after 1000 -> error(monitor_timeout)
                 end,
                 PreB = mrdb:read(R, b),
                 mrdb:insert(R, {R, b, 18}),
                 1477
         end,
    1477 = mrdb:activity(snap_tx, rdb, F1),
    [{R, a, 17}] = mrdb:read(R, a),
    [{R, b, 18}] = mrdb:read(R, b),
    delete_tabs(Created),
    ok.

create_tabs(Tabs) ->
    lists:map(fun create_tab/1, Tabs).

create_tab({T, Opts}) ->
    {atomic, ok} = mnesia:create_table(T, [{rdb,[node()]} | Opts]),
    T.

delete_tabs(Tabs) ->
    [{atomic,ok} = mnesia:delete_table(T) || T <- Tabs],
    ok.
