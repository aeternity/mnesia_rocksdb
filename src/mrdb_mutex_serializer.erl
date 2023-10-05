%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
%% @hidden
-module(mrdb_mutex_serializer).

-behavior(gen_server).

-export([wait/1,
         done/2]).

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(st, { queues = #{}
            , empty_q = queue:new() }).  %% perhaps silly optimization

wait(Rsrc) ->
    gen_server:call(?MODULE, {wait, Rsrc}, infinity).

done(Rsrc, Ref) ->
    gen_server:cast(?MODULE, {done, Rsrc, Ref}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #st{}}.

handle_call({wait, Rsrc}, {Pid, _} = From, #st{ queues = Queues
                                              , empty_q = NewQ } = St) ->
    MRef = erlang:monitor(process, Pid),
    Q0 = maps:get(Rsrc, Queues, NewQ),
    WasEmpty = queue:is_empty(Q0),
    Q1 = queue:in({From, MRef}, Q0),
    St1 = St#st{ queues = Queues#{Rsrc => Q1} },
    case WasEmpty of
        true ->
            {reply, {ok, MRef}, St1};
        false ->
            {noreply, St1}
    end.

handle_cast({done, Rsrc, MRef}, #st{ queues = Queues } = St) ->
    case maps:find(Rsrc, Queues) of
        {ok, Q} ->
            case queue:out(Q) of
                {{value, {_From, MRef}}, Q1} ->
                    erlang:demonitor(MRef),
                    ok = maybe_dispatch_one(Q1),
                    {noreply, St#st{ queues = Queues#{Rsrc := Q1} }};
                {_, _} ->
                    %% Not the lock holder
                    {noreply, St}
            end;
        error ->
            {noreply, St}
    end.

handle_info({'DOWN', MRef, process, _, _}, #st{queues = Queues} = St) ->
    Queues1 =
        maps:map(
          fun(_Rsrc, Q) ->
                  drop_or_filter(Q, MRef)
          end, Queues),
    {noreply, St#st{ queues = Queues1 }};
handle_info(_, St) ->
    {noreply, St}.

terminate(_, _) ->
    ok.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

%% In this function, we don't actually pop
maybe_dispatch_one(Q) ->
    case queue:peek(Q) of
        empty ->
            ok;
        {value, {From, MRef}} ->
            gen_server:reply(From, {ok, MRef}),
            ok
    end.
        
drop_or_filter(Q, MRef) ->
    case queue:peek(Q) of
        {value, {_, _MRef}} ->
            Q1 = queue:drop(Q),
            ok = maybe_dispatch_one(Q1),
            Q1;
        {value, _Other} ->
            queue:filter(fun({_,M}) -> M =/= MRef end, Q);
        empty ->
            Q
    end.
