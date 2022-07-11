%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
-module(mrdb_index).

-export([
          with_iterator/3
        , iterator_move/2
        , iterator/2
        , iterator_close/1
        ]).

-record(mrdb_ix_iter, { i    :: mrdb:iterator()
                      , type = set :: set | bag
                      , sub  :: mrdb:ref() | pid()
                      }).

-type ix_iterator() :: #mrdb_ix_iter{}.
-type index_value() :: any().
-type iterator_action() :: mrdb:iterator_action().

-type object() :: tuple().

-record(subst, { i :: mrdb:iterator()
               , vals_f
               , cur
               , mref }).

-define(TIMEOUT, 5000).

-import(mnesia_rocksdb_lib, [ encode_key/2 ]).

-export_type([ ix_iterator/0 ]).

-spec with_iterator(mrdb:ref_or_tab(), mrdb:index_position(), fun( (ix_iterator()) -> Res)) -> Res.
with_iterator(Tab, IxPos, Fun) when is_function(Fun, 1) ->
    {ok, I} = iterator(Tab, IxPos),
    try Fun(I)
    after
        iterator_close(I)
    end.

-spec iterator(mrdb:ref_or_tab(), mrdb:index_position()) -> {ok, ix_iterator()}
                                                          | {error, _}.
iterator(Tab, IxPos) ->
    #{semantics := Sem} = R = mrdb:ensure_ref(Tab),
    #{ix_vals_f := IxValsF} = IxR = ensure_index_ref(IxPos, R),
    case mrdb:iterator(IxR, []) of
        {ok, I} ->
            case Sem of
                bag ->
                    P = sub_new(R, IxValsF),
                    {ok, #mrdb_ix_iter{ i = I
                                      , sub = P }};
                _ ->
                    {ok, #mrdb_ix_iter{i = I, sub = R}}
            end;
        Err ->
            Err
    end.

-spec iterator_move(ix_iterator(), iterator_action()) -> {ok, index_value(), object()}
                                                       | {error, _}.
iterator_move(#mrdb_ix_iter{type = set} = IxI, Dir) -> iterator_move_set(IxI, Dir);
iterator_move(#mrdb_ix_iter{type = bag} = IxI, Dir) -> iterator_move_bag(IxI, Dir).

iterator_move_set(#mrdb_ix_iter{i = I, sub = Sub}, Dir) ->
    case mrdb:iterator_move(I, Dir) of
        {ok, {{FKey, PKey}}} ->
            {ok, FKey, opt_read(Sub, PKey)};
        Other ->
            Other
    end.

iterator_move_bag(#mrdb_ix_iter{i = I, sub = Sub}, Dir) ->
    case call_sub(Sub, {move_rel, Dir}) of
        not_found ->
            case mrdb:iterator_move(I, Dir) of
                {ok, {FKey, PKey}} ->
                    call_sub(Sub, {move_abs, FKey, PKey});
                Other ->
                    Other
            end;
        Other ->
            Other
    end.

opt_read(R, Key) ->
    case mrdb:read(R, Key, []) of
        [Obj] ->
            Obj;
        [] ->
            []
    end.

sub_new(R, ValsF) when is_function(ValsF, 1) ->
    Me = self(),
    {Pid, MRef} = spawn_monitor(
                    fun() ->
                            MRef = monitor(process, Me),
                            case mrdb:iterator(R) of
                                {ok, I} ->
                                    Me ! {self(), ok},
                                    sub_loop(#subst{ mref   = MRef
                                                   , i      = I
                                                   , vals_f = ValsF
                                                   , cur    = undefined});
                                Error ->
                                    Me ! {self(), Error}
                            end
                    end),
    receive
        {'DOWN', MRef, _, _, Crash} ->
            mrdb:abort({error, Crash});
        {Pid, ok} ->
            demonitor(MRef),
            Pid;
        {Pid, Error} ->
            demonitor(MRef),
            mrdb:abort(Error)
    end.

sub_loop(#subst{i = I, mref = MRef} = St) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            mrdb:iterator_close(I);
        {Pid, Ref, close} ->
            mrdb:iterator_close(I),
            Pid ! {Ref, ok};
        {Pid, Ref, cur} ->
            Pid ! {Ref, St#subst.cur},
            sub_loop(St);
        {Pid, Ref, {move, Cur, Dir}} when is_binary(Dir) ->
            {Res, St1} = sub_abs_move(Cur, Dir, St),
            Pid ! {Ref, Res},
            sub_loop(St1);
        {Pid, Ref, {move_rel, Dir}} ->
            {Res, St1} = sub_rel_move(Dir, St),
            Pid ! {Ref, Res},
            sub_loop(St1)
    end.

sub_abs_move(Cur, Dir, #subst{i = I} = St) ->
    case mrdb:iterator_move(I, Dir) of
        {ok, _} = Ok ->
            {Ok, St#subst{cur = Cur}};
        Other ->
            {Other, St#subst{cur = undefined}}
    end.

sub_rel_move(Dir, #subst{i = I, vals_f = VF, cur = Prev} = St) ->
    case mrdb:iterator_move(I, Dir) of
        {ok, Obj} = Ok ->
            case lists:member(Prev, VF(Obj)) of
                true ->
                    {Ok, St};
                false ->
                    {not_found, St#subst{cur = undefined}}
            end;
        Other ->
            {Other, St#subst{cur = undefined}}
    end.

call_sub(Pid, Req) ->
    MRef = monitor(process, Pid),
    Pid ! {self(), MRef, Req},
    receive
        {MRef, Reply} ->
            demonitor(MRef),
            Reply;
        {'DOWN', MRef, _, _, Reason} ->
            error(Reason)
    after ?TIMEOUT ->
            error(timeout)
    end.

-spec iterator_close(ix_iterator()) -> ok.
iterator_close(#mrdb_ix_iter{i = I, sub = Sub}) ->
    mrdb:iterator_close(I),
    iterator_close_sub(Sub).

iterator_close_sub(P) when is_pid(P) ->
    call_sub(P, close);
iterator_close_sub(_) ->
    ok.

ensure_index_ref(IxPos, #{name := Name, attr_pos := AP, properties := #{index := Ixs}}) ->
    {_,ordered} = Ix = lists:keyfind(index_pos(IxPos, AP), 1, Ixs),
    mrdb:get_ref({Name, index, Ix}).

index_pos(P, AP) when is_atom(P) ->
    maps:get(P, AP);
index_pos(P, _) ->
    P.
