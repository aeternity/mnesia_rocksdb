-module(mrdb_index).

-export([
          with_iterator/3
        , iterator_move/2
        , iterator/2
        , iterator_close/1
        ]).

-record(mrdb_ix_iter, { i    :: mrdb:iterator()
                      , sub  :: mrdb:ref() | pid()
                      }).

-define(TIMEOUT, 5000).

-import(mnesia_rocksdb_lib, [ encode_key/2 ]).

with_iterator(Tab, IxPos, Fun) when is_function(Fun, 1) ->
    {ok, I} = iterator(Tab, IxPos),
    try Fun(I)
    after
        iterator_close(I)
    end.

iterator(Tab, IxPos) ->
    #{semantics := Sem} = R = mrdb:ensure_ref(Tab),
    IxR = ensure_index_ref(IxPos, R),
    case mrdb:iterator(IxR, []) of
        {ok, I} ->
            case Sem of
                bag ->
                    case mrdb:iterator(R, []) of
                        {ok, SubI} ->
                            {ok, #mrdb_ix_iter{ i = I
                                              , sub = sub_new(R) }}
                        SubErr ->
                            mrdb:iterator_close(I),
                            SubErr
                    end;
                _ ->
                    {ok, #mrdb_ix_iter{i = I, sub = R}}
            end;
        Err ->
            Err
    end.

iterator_move(#mrdb_ix_iter{i = I, sub = Sub} = IxI, Dir) ->
    case mrdb:iterator_move(I, Dir) of
        {ok, {{FKey, PKey}}} ->
            case Sub of
                R when is_map(R) ->
                    {ok, FKey, opt_read(R, PKey)};
                P when is_pid(P) ->
                    case call_sub(P, ) of
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

sub_new(R) ->
    Me = self(),
    {Pid, MRef} = spawn_monitor(
                    fun() ->
                            MRef = monitor(process, Me),
                            case mrdb:iterator(R) of
                                {ok, I} ->
                                    Me ! {self(), ok},
                                    sub_loop(MRef, I, undefined);
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

sub_loop(MRef, I, Cur) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            mrdb:iterator_close(I);
        {Pid, Ref, close} ->
            mrdb:iterator_close(I),
            Pid ! {Ref, ok};
        {Pid, Ref, cur} ->
            Pid ! {Ref, Cur},
            sub_loop(MRef, I, Cur);
        {Pid, Ref, {move, Cur, Dir}} ->
            case mrdb:iterator_move(I, Dir) of
                {ok, _} = Res ->
                    Pid ! {Ref, Res},
                    sub_loop(MRef, I, Cur);
                Err ->
                    Pid ! {Ref, Err},
                    sub_loop(MRef, I, Cur)
            end
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

iterator_close(#mrdb_ix_iter{i = I, sub = Sub}) ->
    mrdb:iterator_close(I),
    iterator_close_sub(Sub).

iterator_close_sub(P) when is_pid(P) ->
    call_sub(P, close);
iterator_close_sub(none) ->
    ok.

decode_ix_entry(Bin) ->
    sext:decode(Bin).

ensure_index_ref(IxPos, #{name := Name, attr_pos := AP, properties := #{index := Ixs}}) ->
    {_,ordered} = Ix = lists:keyfind(index_pos(IxPos, AP), 1, Ixs),
    mrdb:get_ref({Name, index, Ix}).

index_pos(P, AP) when is_atom(P) ->
    maps:get(P, AP);
index_pos(P, _) ->
    P.

read_tab_raw(R, Key) ->
    EncKey = encode_key(Key, R),
    case mrdb:rdb_get(R, EncKey, []) of
        {ok, Bin} ->
            {ok, EncKey, Bin};
        Other ->
            Other
    end.
