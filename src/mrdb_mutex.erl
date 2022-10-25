%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
-module(mrdb_mutex).

-export([ do/2 ]).

-include_lib("eunit/include/eunit.hrl").

%% We use a duplicate_bag ets table as a lock queue,
%% relying on the characteristic that a lookup on a key (the resource name)
%% returns the objects in the order in which they were inserted.
%% We try to claim the lock by inserting our own pid under the Rrsc key, then
%% checking which pid is at the head of the list. If it's our pid, we have the
%% lock, and proceed with calling our fun, then delecting our table entry.
%% If another pid is at the head of the list, we busy-wait on the table.
%%
%% Releasing the resource is done by deleting the resource. If we just decrement,
%% we will end up with lingering unlocked resources, so we might as well delete.
%% Either operation is atomic, and the claim op creates the object if it's missing.

%% Another, perhaps cheaper, way of implementing a mutex would be to use a counter
%% object, but we also care about avoiding starvation, and this way, we get a form
%% of serialization of requests.

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
