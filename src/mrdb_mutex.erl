%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
-module(mrdb_mutex).

-export([ do/2 ]).

-export([ ensure_tab/0 ]).

-define(LOCK_TAB, ?MODULE).

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
    ets:insert(?LOCK_TAB, {Rsrc, self()}),
    case have_lock(Rsrc) of
        true ->
            try F()
            after
                release(Rsrc)
            end;
        false ->
            busy_wait(Rsrc, 5000)
    end.

have_lock(Rsrc) ->
    case ets:lookup(?LOCK_TAB, Rsrc) of
        [{_, P}|_] ->
            P =:= self();
        [] ->
            false
    end.

release(Rsrc) ->
    ets:delete_object(?LOCK_TAB, {Rsrc, self()}).


%% The busy-wait function makes use of the fact that we can read a timer to find out
%% if it still has time remaining. This reduces the need for selective receive, looking
%% for a timeout message. We yield, then retry the claim op. Yielding at least used to
%% also be necessary for the `read_timer/1` value to refresh.
%%
busy_wait(Rsrc, Timeout) ->
    Ref = erlang:send_after(Timeout, self(), {claim, Rsrc}),
    do_wait(Rsrc, Ref).

do_wait(Rsrc, Ref) ->
    erlang:yield(),
    case erlang:read_timer(Ref) of
        false ->
            erlang:cancel_timer(Ref),
            error(lock_wait_timeout);
        _ ->
            case have_lock(Rsrc) of
                true ->
                    erlang:cancel_timer(Ref),
                    ok;
                false ->
                    do_wait(Rsrc, Ref)
            end
    end.

%% Called by the process holding the ets table.
ensure_tab() ->
    case ets:info(?LOCK_TAB, name) of
        undefined ->
            ets:new(?LOCK_TAB, [duplicate_bag, public, named_table,
                                {read_concurrency, true},
                                {write_concurrency, true}]);
        _ ->
            true
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
    ensure_tab().

cleanup(_) ->
    ets:delete(?LOCK_TAB).

swarm_do() ->
    K = ?LINE,
    Pids = [spawn_monitor(fun() ->
                                  write_evens(my_rsrc, K, N)
                          end) || N <- lists:seq(1,25)],
    await_pids(Pids),
    Written = ets:lookup(?LOCK_TAB, K),
    true = lists:all(fun is_even/1, [X || {_, X} <- Written]).

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

write_evens(Rsrc, K, N) ->
    do(Rsrc, fun() ->
                     case is_even(N) of
                         true ->
                             ets:insert(?LOCK_TAB, {K, N});
                         false ->
                             exit(not_even)
                     end
             end).

-endif.
