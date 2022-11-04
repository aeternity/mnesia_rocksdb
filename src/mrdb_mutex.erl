%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
-module(mrdb_mutex).

-export([ do/2 ]).

-include_lib("eunit/include/eunit.hrl").

%% We use a gen_server-based FIFO queue (one queue per alias) to manage the
%% critical section.
%%
%% Releasing the resource is done by notifying the server.
%%
%% Previous implementations tested:
%% * A counter (in ets), incremented atomically first with 0, then 1. This lets
%%   the caller know if the 'semaphore' was 1 or zero before. If it was already
%%   1, busy-loop over the counter until it reads as a transition from 0 to 1.
%%   This is a pretty simple implementation, but it lacks ordering, and appeared
%%   to sometimes lead to starvation.
%% * A duplicate_bag ets table: These are defined such that insertion order is
%%   preserved. Thus, they can serve as a mutex with FIFO characteristics.
%%   Unfortunately, performance plummeted when tested with > 25 concurrent
%%   processes. While this is likely a very high number, given that we're talking
%%   about contention in a system using optimistic concurrency, it's never good
%%   if performance falls off a cliff.

do(Rsrc, F) when is_function(F, 0) ->
    {ok, Ref} = mrdb_mutex_serializer:wait(Rsrc),
    try F()
    after
        mrdb_mutex_serializer:done(Rsrc, Ref)
    end.

-ifdef(TEST).

mutex_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Check that all operations complete", fun swarm_do/0}
     ]}.

setup() ->
    case whereis(mrdb_mutex_serializer) of
        undefined ->
            {ok, Pid} = mrdb_mutex_serializer:start_link(),
            Pid;
        Pid ->
            Pid
    end.

cleanup(Pid) ->
    unlink(Pid),
    exit(Pid, kill).

swarm_do() ->
    Rsrc = ?LINE,
    Pid = spawn(fun() -> collect([]) end),
    L = lists:seq(1, 1000),
    Evens = [X || X <- L, is_even(X)],
    Pids = [spawn_monitor(fun() ->
                                  send_even(Rsrc, N, Pid)
                          end) || N <- lists:seq(1,1000)],
    await_pids(Pids),
    Results = fetch(Pid),
    {incorrect_results, []} = {incorrect_results, Results -- Evens},
    {missing_correct_results, []} = {missing_correct_results, Evens -- Results},
    ok.

collect(Acc) ->
    receive
        {_, result, N} ->
            collect([N|Acc]);
        {From, fetch} ->
            From ! {fetch_reply, Acc},
            done
    end.

fetch(Pid) ->
    Pid ! {self(), fetch},
    receive
        {fetch_reply, Result} ->
            Result
    end.

is_even(N) ->
    (N rem 2) =:= 0.

await_pids([{_, MRef}|Pids]) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            await_pids(Pids)
    after 10000 ->
            error(timeout)
    end;
await_pids([]) ->
    ok.

send_even(Rsrc, N, Pid) ->
    do(Rsrc, fun() ->
                     case is_even(N) of
                         true ->
                             Pid ! {self(), result, N};
                         false ->
                             exit(not_even)
                     end
             end).

-endif.
