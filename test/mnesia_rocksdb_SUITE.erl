%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
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

-export([ encoding_sext_attrs/1
        , encoding_binary_binary/1
        , encoding_defaults/1
        ]).
-export([ mrdb_batch/1
        , mrdb_transactions/1
	, mrdb_abort_reasons/1
        , mrdb_repeated_transactions/1
        , mrdb_abort/1
        , mrdb_two_procs/1
        , mrdb_two_procs_tx_restart/1
        , mrdb_two_procs_tx_inner_restart/1
        , mrdb_two_procs_snap/1
        , mrdb_three_procs/1
        ]).

-include_lib("common_test/include/ct.hrl").

-define(TABS_CREATED, tables_created).

suite() ->
    [].

all() ->
    [{group, all_tests}].

groups() ->
    [
      {all_tests, [sequence], [ {group, checks}
                              , {group, mrdb} ]}
                              %% , error_handling ]}
    , {checks, [sequence], [ encoding_sext_attrs
                           , encoding_binary_binary
                           , encoding_defaults ]}
    , {mrdb, [sequence], [ mrdb_batch
                         , mrdb_transactions
			 , mrdb_abort_reasons
                         , mrdb_repeated_transactions
                         , mrdb_abort
                         , mrdb_two_procs
                         , mrdb_two_procs_tx_restart
                         , mrdb_two_procs_tx_inner_restart
                         , mrdb_two_procs_snap
                         , mrdb_three_procs ]}
    ].


%% error_handling(Config) ->
%%     mnesia_rocksdb_error_handling:run(Config).

init_per_suite(Config) ->
    tr_ct:set_activation_checkpoint(?TABS_CREATED, Config).

end_per_suite(_Config) ->
    ok.

init_per_group(G, Config) when G==mrdb
                             ; G==checks ->
    mnesia:stop(),
    ok = mnesia_rocksdb_tlib:start_mnesia(reset),
    Config;

init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

encoding_sext_attrs(Config) ->
    tr_ct:with_trace(fun encoding_sext_attrs_/1, Config,
                     tr_patterns(mnesia_rocksdb,
                                 [{mnesia_rocksdb,'_',x}], tr_opts())).

encoding_sext_attrs_(Config) ->
    Created = create_tabs([{t, [{attributes, [k, v]}]}], Config),
    ok = mrdb:insert(t, {t, 1, a}),
    ok = mnesia:dirty_write({t, 2, b}),
    expect_error(fun() -> mrdb:insert(t, {t, a}) end, ?LINE,
                 error, {mrdb_abort, badarg}),
    expect_error(fun() -> mnesia:dirty_write({t, a}) end, ?LINE,
                 exit, '_'),
    delete_tabs(Created),
    ok.

encoding_defaults(Config) ->
    UP = fun(T) -> mnesia:table_info(T, user_properties) end,
    Created = create_tabs([ {a, [ {attributes, [k, v]}
                                , {type, set}]}
                          , {b, [ {attributes, [k, v, w]}
                                , {type, ordered_set}]}
                          , {c, [ {attributes, [k, v]}
                                , {type, bag} ]}], Config),
    [{mrdb_encoding,{term,{value,term}}}]  = UP(a),
    [{mrdb_encoding,{sext,{object,term}}}] = UP(b),
    [{mrdb_encoding,{sext,{value,term}}}]  = UP(c),
    delete_tabs(Created),
    ok.

encoding_binary_binary(Config) ->
    Created = create_tabs([ {a, [ {attributes, [k,v]}
                                  , {user_properties,
                                     [{mrdb_encoding, {raw, raw}}]}]}
                          , {b, [ {attributes, [k, v, w]}
                                , {user_properties,
                                   [{mrdb_encoding, {raw, {object, term}}}]}]}
                          ], Config),
    expect_error(fun() ->
                         create_tab(
                           c, [ {attributes, [k, v, w]}
                              , {user_properties,
                                 [{mrdb_encoding, {raw, {value, raw}}}]}])
                 end, ?LINE, error, '_'),
    delete_tabs(Created),
    ok.

expect_error(F, Line, Type, Expected) ->
    try F() of
        Unexpected -> error({unexpected, Line, Unexpected})
    catch
        Type:Expected ->
            ct:log("Caught expected ~p:~p (Line: ~p)", [Type, Expected, Line]),
            ok;
        Type:Error when Expected == '_' ->
            ct:log("Caught expected ~p:_ (Line:~p): ~p", [Type, Line, Error]),
            ok
    end.

mrdb_batch(Config) ->
    Created = create_tabs([{b, []}], Config),
    D0 = get_dict(),
    mrdb:activity(
      batch, rdb,
      fun() ->
              [mrdb:insert(b, {b, K, K})
               || K <- lists:seq(1, 10)]
      end),
    dictionary_unchanged(D0),
    [[{b,K,K}] = mrdb:read(b, K) || K <- lists:seq(1, 10)],
    expect_error(
      fun() -> mrdb:activity(
                 batch, rdb,
                 fun() ->
                         mrdb:insert(b, {b, 11, 11}),
                         error(willful_abort)
                 end)
      end, ?LINE, error, '_'),
    dictionary_unchanged(D0),
    [] = mrdb:read(b, 11),
    TRef = mrdb:get_ref(b),
    mrdb:activity(
      batch, rdb,
      fun() ->
              mrdb:insert(TRef, {b, 12, 12})
      end),
    dictionary_unchanged(D0),
    [{b, 12, 12}] = mrdb:read(b, 12),
    mrdb:as_batch(b, fun(R) ->
                             mrdb:insert(R, {b, 13, 13})
                     end),
    dictionary_unchanged(D0),
    [{b, 13, 13}] = mrdb:read(b, 13),
    delete_tabs(Created),
    ok.

mrdb_transactions(Config) ->
    tr_ct:with_trace(fun mrdb_transactions_/1, Config,
                     tr_patterns(
                       mnesia_rocksdb_admin,
                       [{mnesia_rocksdb_admin,'_',x}], tr_opts())).

mrdb_transactions_(Config) ->
    Created = create_tabs([{tx, []}], Config),
    mrdb:insert(tx, {tx, a, 1}),
    [_] = mrdb:read(tx, a),
    D0 = get_dict(),
    mrdb:activity(
      tx, rdb,
      fun() ->
              [{tx,a,N}] = mrdb:read(tx, a),
              N1 = N+1,
              ok = mrdb:insert(tx, {tx,a,N1}),
              [{tx,a,N1}] = mrdb:read(tx, a),
              ok
      end),
    dictionary_unchanged(D0),
    [{tx,a,2}] = mrdb:read(tx,a),
    delete_tabs(Created),
    ok.

mrdb_abort_reasons(_Config) ->
    Prev = mnesia_rocksdb_admin:set_and_cache_env(mnesia_compatible_aborts, true),
    X = some_value,
    compare_txs('throw', fun() -> throw(X) end),
    compare_txs('exit' , fun() -> exit(X) end),
    compare_txs('error', fun() -> error(X) end),
    compare_txs('abort', fun() -> mnesia:abort(X) end),
    compare_txs('abort' , fun() -> mrdb:abort(X) end),
    mnesia_rocksdb_admin:set_and_cache_env(mnesia_compatible_aborts, Prev),
    ok.

compare_txs(Type, F) ->
    {caught, exit, {aborted, EMn}} = mnesia_tx(F),
    {caught, exit, {aborted, EMr}} = mrdb_tx(F),
    ct:log("Mnesia = ~p/~p", [Type, EMn]),
    ct:log("Mrdb   = ~p/~p", [Type, EMr]),
    case {Type, EMn, EMr} of
	{error, {some_value, [_|_]}, {some_value, []}} -> ok;
	{throw, {throw, some_value}, {throw, some_value}} -> ok;
	{exit, some_value, some_value} -> ok;
	{abort, some_value, some_value} -> ok
    end.

mnesia_tx(F) ->
    try
	mnesia:activity(transaction, F)
    catch
	C:E ->
	    {caught, C, E}
    end.

mrdb_tx(F) ->
    try
	mrdb:activity(transaction, rdb, F)
    catch
	C:E ->
	    {caught, C, E}
    end.

mrdb_repeated_transactions(Config) ->
    Created = create_tabs([{rtx, []}], Config),
    mrdb:insert(rtx, {rtx, a, 0}),
    [_] = mrdb:read(rtx, a),
    Fun = fun() ->
                  [{rtx, a, N}] = mrdb:read(rtx, a),
                  N1 = N+1,
                  ok = mrdb:insert(rtx, {rtx, a, N1})
          end,
    D0 = get_dict(),
    [ok = mrdb:activity(tx, rdb, Fun) || _ <- lists:seq(1,100)],
    dictionary_unchanged(D0),
    [{rtx,a,100}] = mrdb:read(rtx, a),
    delete_tabs(Created),
    ok.

mrdb_abort(Config) ->
    Created = create_tabs([{tx_abort, []}], Config),
    mrdb:insert(tx_abort, {tx_abort, a, 1}),
    Pre = mrdb:read(tx_abort, a),
    D0 = get_dict(),
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
    dictionary_unchanged(D0),
    ok = TRes,
    Pre = mrdb:read(tx_abort, a),
    delete_tabs(Created),
    ok.

mrdb_two_procs(Config) ->
    tr_ct:with_trace(fun mrdb_two_procs_/1, Config,
                     tr_flags(
                       {self(), [call, sos, p, 'receive']},
                       tr_patterns(
                         mrdb, [ {mrdb, insert, 2, x}
                               , {mrdb, read, 2, x}
                               , {mrdb, retry_activity, 3, x}
                               , {mrdb, try_f, 2, x}
                               , {mrdb, incr_attempt, 2, x}
                               , {mrdb_mutex, do, 2, x}
                               , {mrdb_mutex_serializer, do, 2, x}
                               , {?MODULE, wait_for_other, 2, x}
                               , {?MODULE, go_ahead_other, 1, x}
                               , {mrdb, activity, x}], tr_opts()))).

mrdb_two_procs_(Config) ->
    R = ?FUNCTION_NAME,
    Parent = self(),
    Created = create_tabs([{R, []}], Config),
    mrdb:insert(R, {R, a, 1}),
    Pre = mrdb:read(R, a),
    F0 = fun() ->
                 wait_for_other(Parent, ?LINE),
                 ok = mrdb:insert(R, {R, a, 17}),
                 wait_for_other(Parent, ?LINE)
         end,
    {POther, MRef} = spawn_opt(
                       fun() ->
                               D0 = get_dict(),
                               ok = mrdb:activity(tx, rdb, F0),
                               dictionary_unchanged(D0)
                       end, [monitor]),
    F1 = fun() ->
                 Pre = mrdb:read(R, a),
                 go_ahead_other(POther),
                 await_other_down(POther, MRef, ?LINE),
                 %% The test proc is still inside the transaction, and POther
                 %% has terminated/committed. If we now read 'a', we should see
                 %% the value that POther wrote (no isolation).
                 [{R, a, 17}] = mrdb:read(R, a),
                 ok = mrdb:insert(R, {R, a, 18})
         end,
    go_ahead_other(0, POther),
    Do0 = get_dict(),
    %% Our transaction should fail due to the resource conflict, and because
    %% we're not using retries.
    try mrdb:activity({tx, #{no_snapshot => true,
                             retries => 0}}, rdb, F1) of
        ok -> error(unexpected)
    catch
        error:{error, "Resource busy" ++ _} ->
            ok
    end,
    dictionary_unchanged(Do0),
    [{R, a, 17}] = mrdb:read(R, a),
    delete_tabs(Created),
    ok.

mrdb_two_procs_tx_restart(Config) ->
    tr_ct:with_trace(fun mrdb_two_procs_tx_restart_/1, Config,
                     light_tr_opts()).

mrdb_two_procs_tx_restart_(Config) ->
    R = ?FUNCTION_NAME,
    Parent = self(),
    Created = create_tabs([{R, []}], Config),
    check_stats(rdb),
    mrdb:insert(R, {R, a, 1}),
    Pre = mrdb:read(R, a),
    F0 = fun() ->
                 wait_for_other(Parent, ?LINE),
                 ok = mrdb:insert(R, {R, a, 17}),
                 wait_for_other(Parent, ?LINE)
         end,
    {POther, MRef} = spawn_opt(
                       fun() ->
                               ok = mrdb:activity(tx, rdb, F0)
                       end, [monitor]),
    F1 = fun() ->
                 OtherWrite = [{R, a, 17}],
                 Att = get_attempt(),
                 Expected = case Att of
                                0 -> Pre;
                                _ -> OtherWrite
                            end,
                 Expected = mrdb:read(R, a),
                 go_ahead_other(POther),
                 await_other_down(POther, MRef, ?LINE),
                 OtherWrite = mrdb:read(R, a),
                 ok = mrdb:insert(R, {R, a, 18})
         end,
    go_ahead_other(0, POther),
    Do0 = get_dict(),
    mrdb:activity({tx, #{no_snapshot => true}}, rdb, F1),
    dictionary_unchanged(Do0),
    check_stats(rdb),
    [{R, a, 18}] = mrdb:read(R, a),
    delete_tabs(Created),
    ok.

mrdb_two_procs_tx_inner_restart(Config) ->
    tr_ct:with_trace(fun mrdb_two_procs_tx_inner_restart_/1, Config,
                     dbg_tr_opts()).

mrdb_two_procs_tx_inner_restart_(Config) ->
    R = ?FUNCTION_NAME,
    Parent = self(),
    Created = create_tabs([{R, []}], Config),
    mrdb:insert(R, {R, a, 1}),
    mrdb:insert(R, {R, b, 1}),
    PreA = mrdb:read(R, a),
    PreB = mrdb:read(R, b),
    #{inner_retries := Ri0, outer_retries := Ro0} = check_stats(rdb),
    F0 = fun() ->
                 wait_for_other(Parent, ?LINE),
                 ok = mrdb:insert(R, {R, a, 17}),
                 wait_for_other(Parent, ?LINE)
         end,
    F1 = fun() ->
                 wait_for_other(Parent, ?LINE),
                 ok = mrdb:insert(R, {R, b, 147}),
                 wait_for_other(Parent, ?LINE)
         end,

    Spawn = fun(F) ->
                    Res = spawn_opt(
                            fun() ->
                                    ok = mrdb:activity(tx, rdb, F)
                            end, [monitor]),
                    Res
            end,
    {P1, MRef1} = Spawn(F0),
    {P2, MRef2} = Spawn(F1),
    F2 = fun() ->
                 PostA = [{R, a, 17}],  % once F0 (P1) has committed
                 PostB = [{R, b, 147}], % once F1 (P2) has committed
                 ARes = mrdb:read(R, a),  % We need to read first to establish a pre-image
                 BRes = mrdb:read(R, b),
                 Att = get_attempt(),
                 ct:log("Att = ~p", [Att]),
                 case Att of
                     0 ->                         % This is the first run (no retry yet)
                         ARes = PreA,
                         BRes = PreB,
                         go_ahead_other(0, P1),   % Having our pre-image, now let P1 write
                         go_ahead_other(0, P1),   % Let P1 commit, then await DOWN (normal)
                         await_other_down(P1, MRef1, ?LINE),
                         PostA = mrdb:read(R, a); % now, P1's write should leak through here
                     {0, 1} ->                    % This is the first (outer) retry
                         go_ahead_other(0, P2),   % Let P2 write
                         go_ahead_other(0, P2),   % Let P2 commit, then await DOWN (normal)
                         await_other_down(P2, MRef2, ?LINE),
                         PostA = mrdb:read(R, a), % now we should see writes from both P1
                         %% On OTP 23, the following step might fail due to timing, even
                         %% though the trace output looks as expected. Possibly some quirk
                         %% with leakage propagation time in rocksdb. Let's retry to be sure.
                         ok = try_until(PostB, fun() -> mrdb:read(R, b) end); % ... and P2
                     {1, 1} ->
                         PostA = mrdb:read(R, a),
                         PostB = mrdb:read(R, b),
                         ok
                 end,
                 mrdb:insert(R, {R, a, 18}),
                 mrdb:insert(R, {R, b, 18})
         end,
    Do0 = get_dict(),
    mrdb:activity({tx, #{no_snapshot => true, retries => {1,1}}}, rdb, F2),
    check_stats(rdb),
    dictionary_unchanged(Do0),
    [{R, a, 18}] = mrdb:read(R, a),
    [{R, b, 18}] = mrdb:read(R, b),
    #{inner_retries := Ri1, outer_retries := Ro1} = check_stats(rdb),
    {restarts, {1, 1}} = {restarts, {Ri1 - Ri0, Ro1 - Ro0}},
    delete_tabs(Created),
    ok.

try_until(Result, F) ->
    try_until(Result, F, 10).

try_until(Result, F, N) when N > 0 ->
    case F() of
        Result ->
            ok;
        _ ->
            receive after 100 -> ok end,
            try_until(Result, F, N-1)
    end;
try_until(Result, F, _) ->
    error({badmatch, {Result, F}}).

%
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
        if N == 0 ->
                Expr;
           true ->
                ok
        end).

mrdb_two_procs_snap(Config) ->
    %% _snap is now the default tx mode
    R = ?FUNCTION_NAME,
    Parent = self(),
    Created = create_tabs([{R, []}], Config),
    mrdb:insert(R, {R, a, 1}),
    Pre = mrdb:read(R, a),
    mrdb:insert(R, {R, b, 11}),
    PreB = mrdb:read(R, b),
    F0 = fun() ->
                 ok = mrdb:insert(R, {R, a, 17}),
                 wait_for_other(Parent, ?LINE)
         end,
    {POther, MRef} =
        spawn_opt(fun() ->
                          D0 = get_dict(),
                          ok = mrdb:activity(tx, rdb, F0),
                          dictionary_unchanged(D0)
                  end, [monitor]),
    F1 = fun() ->
                 Att = get_attempt(),
                 go_ahead_other(Att, POther),
                 ARes = mrdb:read(R, a),
                 ARes = case Att of
                            0 -> Pre;
                            _ -> [{R, a, 17}]
                        end,
                 await_other_down(POther, MRef, ?LINE),
                 PreB = mrdb:read(R, b),
                 mrdb:insert(R, {R, b, 18}),
                 1477
         end,
    Do0 = get_dict(),
    1477 = mrdb:activity(tx, rdb, F1),
    dictionary_unchanged(Do0),
    [{R, a, 17}] = mrdb:read(R, a),
    [{R, b, 18}] = mrdb:read(R, b),
    delete_tabs(Created),
    ok.

%% We spawn two helper processes, making it 3 transactions, with the one
%% in the parent process. P2 writes to key `a`, which the other two try to read.
%% We make sure that P2 commits before finishing the other two, and P3 and the 
%% main thread sync, so as to maximize the contention for the retry lock.
mrdb_three_procs(Config) ->
    tr_ct:with_trace(fun mrdb_three_procs_/1, Config, dbg_tr_opts()).

mrdb_three_procs_(Config) ->
    R = ?FUNCTION_NAME,
    Parent = self(),
    Created = create_tabs([{R, []}], Config),
    A0 = {R, a, 1},
    A1 = {R, a, 11},
    A2 = {R, a, 12},
    ok = mrdb:insert(R, A0),
    F1 = fun() ->
                 ok = mrdb:insert(R, A1),
                 ok = mrdb:insert(R, {R, p1, 1})
         end,
    {P1, MRef1} =
        spawn_opt(fun() ->
                          D0 = get_dict(),
                          do_when_p_allows(
                            0, Parent, ?LINE,
                            fun() ->
                                    ok = mrdb:activity({tx,#{retries => 0}}, rdb, F1)
                            end),
                          dictionary_unchanged(D0)
                  end, [monitor]),
    F2 = fun() ->
                 [A0] = mrdb:read(R, a),
                 Att = get_attempt(),
                 wait_for_other(Att, Parent, ?LINE),
                 do_when_p_allows(
                   Att, Parent, ?LINE,
                   fun() ->
                           [A1] = mrdb:read(R, a),
                           ok = mrdb:insert(R, A2),
                           ok = mrdb:insert(R, {R, p2, 1})
                   end)
         end,
    {P2, MRef2} =
        spawn_opt(fun() ->
                          D0 = get_dict(),
                          try mrdb:activity(
                                {tx, #{retries => 0,
                                       no_snapshot => true}}, rdb, F2) of
                              ok -> error(unexpected)
                          catch
                              error:{error, "Resource busy" ++ _} ->
                                  ok
                          end,
                          dictionary_unchanged(D0)
                  end, [monitor]),
    Do0 = get_dict(),
    ok = mrdb:activity(tx, rdb,
                       fun() ->
                               Att = get_attempt(),
                               ARes = case Att of
                                          0 -> [A0];
                                          _ -> [A1]
                                      end,
                               %% First, ensure that P2 tx is running
                               go_ahead_other(Att, P2),
                               ARes = mrdb:read(R, a),
                               allow_p(Att, P1, ?LINE),
                               ARes = mrdb:read(R, a),
                               allow_p(Att, P2, ?LINE),
                               ARes = mrdb:read(R, a),
                               await_other_down(P1, MRef1, ?LINE),
                               await_other_down(P2, MRef2, ?LINE),
                               ok = mrdb:insert(R, {R, p0, 1})
                       end),
    dictionary_unchanged(Do0),
    [{R, p1, 1}] = mrdb:read(R, p1),
    [] = mrdb:read(R, p2),
    [A1] = mrdb:read(R, a),
    [{R, p0, 1}] = mrdb:read(R, p0),
    delete_tabs(Created),
    ok.

tr_opts() ->
    #{patterns => [ {mrdb, '_', '_', x}
                  , {mrdb_lib, '_', '_', x}
                  , {tr_ttb, event, 3, []}
                  , {?MODULE, go_ahead_other, 3, x}
                  , {?MODULE, wait_for_other, 3, x}
                  , {?MODULE, await_other_down, 3, x}
                  , {?MODULE, do_when_p_allows, 4, x}
                  , {?MODULE, allow_p, 3, x}
                  , {?MODULE, check_stats, 1, x}
                  ]}.

light_tr_opts() ->
    tr_flags(
      {self(), [call, sos, p]},
      tr_patterns(
        mrdb, [ {mrdb, insert, 2, x}
              , {mrdb, read, 2, x}
              , {mrdb, activity, x} ], tr_opts())).

dbg_tr_opts() ->
    tr_flags(
      {self(), [call, sos, p, 'receive']},
      tr_patterns(
        mrdb, [ {mrdb, insert, 2, x}
              , {mrdb, read, 2, x}
              , {mrdb, retry_activity, 3, x}
              , {mrdb, try_f, 2, x}
              , {mrdb, incr_attempt, 2, x}
              , {mrdb, current_context, 0, x}
              , {mrdb_mutex, do, 2, x}
              , {mrdb_mutex_serializer, do, 2, x}
              , {?MODULE, wait_for_other, 2, x}
              , {?MODULE, go_ahead_other, 1, x}
              , {?MODULE, try_until, 3, x}
              , {mrdb, activity, x} ], tr_opts())).

tr_patterns(Mod, Ps, #{patterns := Pats} = Opts) ->
    Pats1 = [P || P <- Pats, element(1,P) =/= Mod],
    Opts#{patterns => Ps ++ Pats1}.

tr_flags(Flags, Opts) when is_map(Opts) ->
    Opts#{flags => Flags}.

wait_for_other(Parent, L) ->
    wait_for_other(get_attempt(), Parent, 1000, L).

wait_for_other(Att, Parent, L) ->
    wait_for_other(Att, Parent, 1000, L).

wait_for_other(0, Parent, Timeout, L) ->
    MRef = monitor(process, Parent),
    Parent ! {self(), ready},
    receive
        {Parent, cont} ->
            demonitor(MRef),
            Parent ! {self(), cont_ack},
            ok;
        {'DOWN', MRef, _, _, Reason} ->
            ct:log("Parent died, Reason = ~p", [Reason]),
            exit(Reason)
    after Timeout ->
              demonitor(MRef),
              error({inner_timeout, L})
    end;
wait_for_other(_, _, _, _) ->
    ok.

do_when_p_allows(Att, P, Line, F) ->
    wait_for_other(Att, P, Line),
    F(),
    %% Tell P that we're done
    go_ahead_other(Att, P, Line),
    %% Wait for P to acknowlege
    wait_for_other(Att, P, Line).

allow_p(Att, P, Line) ->
    go_ahead_other(Att, P),
    %% This is where P does its thing.
    wait_for_other(Att, P, Line),
    %% Acknowledge
    go_ahead_other(Att, P, Line).

go_ahead_other(POther) ->
    go_ahead_other(get_attempt(), POther).

go_ahead_other(Att, POther) ->
    go_ahead_other(Att, POther, 1000).

go_ahead_other(Att, POther, Timeout) ->
    ?IF_FIRST(Att, go_ahead_other_(POther, Timeout)).

go_ahead_other_(POther, Timeout) ->
    receive
        {POther, ready} ->
            POther ! {self(), cont},
            receive
                {POther, cont_ack} ->
                    ok
            after Timeout ->
                    error(cont_ack_timeout)
            end
    after Timeout ->
            error(go_ahead_timeout)
    end.

%% Due to transaction restarts, we may already have collected
%% a DOWN message. In this case, P will already be dead, and there
%% will not be a 'DOWN' messsage still in the msg queue.
%% This is fine (we assume it is), and we just make sure that the
%% process didn't die abnormally.
await_other_down(P, MRef, Line) ->
    Attempt = get_attempt(),
    ?IF_FIRST(Attempt, await_other_down_(P, MRef, Line)).

await_other_down_(P, MRef, Line) ->
    receive {'DOWN', MRef, _, _, Reason} ->
                case Reason of
                    normal -> ok;
                    _ ->
                        error({abnormal_termination,
                               [ {pid, P}
                               , {mref, MRef}
                               , {line, Line}
                               , {reason, Reason}]})
                end
    after 1000 ->
              error({monitor_timeout, Line})
    end.

get_attempt() ->
    #{activity := #{attempt := Attempt}} = mrdb:current_context(),
    Attempt.

create_tabs(Tabs, Config) ->
    Res = lists:map(fun create_tab/1, Tabs),
    tr_ct:trace_checkpoint(?TABS_CREATED, Config),
    Res.

create_tab({T, Opts}) -> create_tab(T, Opts).

create_tab(T, Opts) ->
    {atomic, ok} = mnesia:create_table(T, [{rdb,[node()]} | Opts]),
    T.

delete_tabs(Tabs) ->
    [{atomic,ok} = mnesia:delete_table(T) || T <- Tabs],
    ok.

get_dict() ->
    {dictionary, D} = process_info(self(), dictionary),
    [X || {K,_} = X <- D,
          K =/= log_timestamp].

dictionary_unchanged(Old) ->
    New = get_dict(),
    #{ deleted := []
     , added   := [] } = #{ deleted => Old -- New
                          , added   => New -- Old },
    ok.

check_stats(Alias) ->
    Stats = mrdb_stats:get(Alias),
    ct:log("Stats: ~p", [Stats]),
    Stats.
