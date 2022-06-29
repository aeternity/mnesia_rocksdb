%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
-module(mrdb_mutex).

-export([ do/2 ]).

-export([ ensure_tab/0 ]).

-define(LOCK_TAB, ?MODULE).

%% We use a wrapping ets counter (default: 0) as a form of semaphor.
%% The claim operation is done using an atomic list of two updates:
%% first, incrementing with 0 - this returns the previous value
%% then, incrementing with 1, but wrapping at 1, ensuring that we get 1 back,
%% regardless of previous value. This means that if [0,1] is returned, the resource
%% was not locked previously; if [1,1] is returned, it was.
%%
%% Releasing the resource is done by deleting the resource. If we just decrement,
%% we will end up with lingering unlocked resources, so we might as well delete.
%% Either operation is atomic, and the claim op creates the object if it's missing.

do(Rsrc, F) when is_function(F, 0) ->
    true = claim(Rsrc),
    try F()
    after
        release(Rsrc)
    end.

claim(Rsrc) ->
    case claim_(Rsrc) of
        true -> true;
        false -> busy_wait(Rsrc, 1000)
    end.

claim_(Rsrc) ->
    case ets:update_counter(?LOCK_TAB, Rsrc, [{2, 0},
                                              {2, 1, 1, 1}], {Rsrc, 0}) of
        [0, 1] ->
            %% have lock
            true;
        [1, 1] ->
            false
    end.

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
            case claim_(Rsrc) of
                true ->
                    erlang:cancel_timer(Ref),
                    ok;
                false ->
                    do_wait(Rsrc, Ref)
            end
    end.

release(Rsrc) ->
    ets:delete(?LOCK_TAB, Rsrc),
    ok.


%% Called by the process holding the ets table.
ensure_tab() ->
    case ets:info(?LOCK_TAB, name) of
        undefined ->
            ets:new(?LOCK_TAB, [set, public, named_table, {write_concurrency, true}]);
        _ ->
            true
    end.
