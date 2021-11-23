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
        , mrdb_three_procs/1
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
                         , mrdb_two_procs_snap
                         , mrdb_three_procs ]}
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

init_per_testcase(mrdb_three_procs, Config) ->
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
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
                 wait_for_parent(Parent, ?LINE)
         end,
    {POther, MRef} = spawn_opt(
                       fun() ->
                               ok = mrdb:activity(tx, rdb, F0)
                       end, [link, monitor]),
    F1 = fun() ->
                 go_ahead_other(POther),
                 Pre = mrdb:read(R, a),
                 await_other_down(POther, MRef),
                 [{R, a, 17}] = mrdb:read(R, a),
                 ok = mrdb:insert(R, {R, a, 18})
         end,
    try mrdb:activity({tx, 0}, rdb, F1)
    catch
        error:{error, "Resource busy" ++ _} ->
            ok
    end,
    [{R, a, 17}] = mrdb:read(R, a),
    delete_tabs(Created),
    ok.

%% For testing purposes, we use side-effects inside the transactions
%% to synchronize the concurrent transactions. If a transaction fails due
%% to "Resource busy", it can re-run, but then mustn't attempt to sync with
%% the other transaction, which is already committed.
%%
%% To achieve this, we rely on the `mrdb:current_context()` function, which gives
%% us information about which is the current attempt; we only sync on the first
%% attempt, and ignore the sync ops on retries.
%%
-define(IF_FIRST(N, Expr),
        if N == 1 ->
                Expr;
           true ->
                ok
        end).

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
                 wait_for_parent(Parent, ?LINE)
         end,
    {POther, MRef} =
        spawn_opt(fun() ->
                          ok = mrdb:activity(tx, rdb, F0)
                  end, [link, monitor]),
    F1 = fun() ->
                 Att = get_attempt(),
                 go_ahead_other(Att, POther),
                 ARes = mrdb:read(R, a),
                 ARes = case Att of
                            1 -> Pre;
                            2 -> [{R, a, 17}]
                        end,
                 await_other_down(POther, MRef),
                 PreB = mrdb:read(R, b),
                 mrdb:insert(R, {R, b, 18}),
                 1477
         end,
    1477 = mrdb:activity(snap_tx, rdb, F1),
    [{R, a, 17}] = mrdb:read(R, a),
    [{R, b, 18}] = mrdb:read(R, b),
    delete_tabs(Created),
    ok.

%% We spawn two helper processes, making it 3 transactions, with the one
%% in the parent process. P2 writes to key `a`, which the other two try to read.
%% We make sure that P2 commits before finishing the other two, and P3 and the 
%% main thread sync, so as to maximize the contention for the retry lock.
mrdb_three_procs(_Config) ->
    R = ?FUNCTION_NAME,
    Parent = self(),
    Created = create_tabs([{R, []}]),
    dbg:tpl(mnesia_rocksdb_lib,x),
    ok = mrdb:insert(R, {R, a, 1}),
    F0 = fun() ->
                 ok = mrdb:insert(R, {R, a, 47}),
                 wait_for_parent(Parent, ?LINE)
         end,
    {P2, MRef2} =
        spawn_opt(fun() ->
                          ok = mrdb:activity(snap_tx, rdb, F0)
                  end, [link, monitor]),
    F1 = fun() ->
                 Att = get_attempt(),
                 wait_for_parent(Att, Parent, ?LINE),
                 ReadRes = mrdb:read(R, a),
                 ct:log("p3(~p): ReadRes = ~p", [Att, ReadRes]),
                 ok = mrdb:insert(R, {R, b, ReadRes}),
                 wait_for_parent(Att, Parent, ?LINE)
         end,
    {P3, MRef3} =
        spawn_opt(fun() ->
                          ok = mrdb:activity(snap_tx, rdb, F1)
                  end, [link, monitor]),
    ok = mrdb:activity(snap_tx, rdb,
                       fun() ->
                               Att = get_attempt(),
                               go_ahead_other(Att, P2),
                               go_ahead_other(Att, P3),
                               ReadRes = mrdb:read(R, a),
                               ct:log("main(~p): ReadRes = ~p", [Att, ReadRes]),
                               await_other_down(P2, MRef2),
                               ok = mrdb:insert(R, {R, c, ReadRes}),
                               go_ahead_other(Att, P3)
                       end),
    await_other_down(P3, MRef3),
    ct:log("b: ~p", [mrdb:read(R, b)]),
    ct:log("c: ~p", [mrdb:read(R, c)]),
    delete_tabs(Created),
    ok.

wait_for_parent(Parent, L) ->
    wait_for_parent(get_attempt(), Parent, 1000, L).

wait_for_parent(Att, Parent, L) ->
    wait_for_parent(Att, Parent, 1000, L).

wait_for_parent(1, Parent, Timeout, L) ->
    Parent ! {self(), ready},
    receive
        {Parent, cont} -> ok
    after Timeout ->
            error({inner_timeout, L})
    end;
wait_for_parent(_, _, _, _) ->
    ok.

go_ahead_other(POther) ->
    go_ahead_other(get_attempt(), POther).

go_ahead_other(Att, POther) ->
    go_ahead_other(Att, POther, 1000).

go_ahead_other(Att, POther, Timeout) ->
    ?IF_FIRST(Att, go_ahead_other_(POther, Timeout)).

go_ahead_other_(POther, Timeout) ->
    receive
        {POther, ready} ->
            POther ! {self(), cont}
    after Timeout ->
            error(go_ahead_timeout)
    end.

await_other_down(P, MRef) ->
    case is_process_alive(P) of
        false -> ok;
        true ->
            receive {'DOWN', MRef, _, _, _} -> ok
            after 1000 -> error(monitor_timeout)
            end
    end.

get_attempt() ->
    #{attempt := Attempt} = mrdb:current_context(),
    Attempt.

create_tabs(Tabs) ->
    lists:map(fun create_tab/1, Tabs).

create_tab({T, Opts}) ->
    {atomic, ok} = mnesia:create_table(T, [{rdb,[node()]} | Opts]),
    T.

delete_tabs(Tabs) ->
    [{atomic,ok} = mnesia:delete_table(T) || T <- Tabs],
    ok.
