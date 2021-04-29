-module(mrdb).

-export([ get_ref/1
        , new_tx/1       , new_tx/2
        , tx_ref/2
        , tx_commit/1
        , with_iterator/2, with_iterator/3
        , read/2         , read/3
        , index_read/3
        , insert/2       , insert/3
        , delete/2       , delete/3
        , match_delete/2
        , write/2        , write/3
        , as_batch/2     , as_batch/3
        , first/1        , first/2
        , next/2         , next/3
        , prev/2         , prev/3
        , last/1         , last/2
        , select/2       , select/3
        , select/1
        , fold/4         , fold/5
        , write_info/3
        , read_info/2
        ]).

-export([abort/1]).

%% Low-level access wrappers.
-export([ rdb_put/4
        , rdb_get/3
        , rdb_delete/3
        , rdb_iterator/2 ]).

-import(mnesia_rocksdb_lib, [ keypos/1
                            , encode_key/1
                            , decode_key/1
                            , encode_val/1
                            , decode_val/1 ]).

-include("mnesia_rocksdb.hrl").

-type tab_name()  :: atom().
-type alias()     :: atom().
-type admin_tab() :: {admin, alias()}.
-type retainer()  :: {tab_name(), retainer, any()}.
-type index()     :: {tab_name(), index, any()}.
-type table() :: atom()
               | admin_tab()
               | index()
               | retainer().

-type key() :: any().
-type obj() :: tuple().

-type db_handle()  :: rocksdb:db_handle().
-type cf_handle()  :: rocksdb:cf_handle().
-type tx_handle()  :: rocksdb:transaction_handle().
-type itr_handle() :: rocksdb:itr_handle().

-type db_ref() :: #{ db_ref := db_handle()
                   , cf_handle := cf_handle()
                   , tx_handle := tx_handle() }.

-type tx_ref() :: #{ db_ref := db_handle()
                   , cf_handle := cf_handle()
                   , tx_handle := tx_handle() }.

-type ref_or_tab() :: table() | db_ref() | tx_ref().

%% ============================================================
%% these types should be exported from rocksdb.erl
-type snapshot_handle() :: rocksdb:snapshot_handle().
-type read_options() :: [{verify_checksums, boolean()} |
                         {fill_cache, boolean()} |
                         {iterate_upper_bound, binary()} |
                         {iterate_lower_bound, binary()} |
                         {tailing, boolean()} |
                         {total_order_seek, boolean()} |
                         {prefix_same_as_start, boolean()} |
                         {snapshot, snapshot_handle()}].

-type write_options() :: [{sync, boolean()} |
                          {disable_wal, boolean()} |
                          {ignore_missing_column_families, boolean()} |
                          {no_slowdown, boolean()} |
                          {low_pri, boolean()}].
%% ============================================================

abort(Reason) ->
    erlang:error({mrdb_abort, Reason}).

-spec new_tx(table() | db_ref()) -> tx_ref().
new_tx(Tab) ->
    new_tx(Tab, []).

-spec new_tx(ref_or_tab(), write_options()) -> tx_ref().
new_tx(Tab, Opts) ->
    #{db_ref := DbRef} = R = ensure_ref(Tab),
    {ok, TxH} = rocksdb:transaction(DbRef, write_opts(R, Opts)),
    R#{tx_handle => TxH}.

-spec tx_ref(ref_or_tab() | tx_ref() | tx_ref(), tx_handle()) -> tx_ref().
tx_ref(Tab, TxH) ->
    case ensure_ref(Tab) of
        #{tx_handle := TxH} = R ->
            R;
        #{tx_handle := OtherTxH} ->
            error({tx_handle_conflict, OtherTxH});
        R ->
            R#{tx_handle => TxH}
    end.

-spec tx_commit(tx_handle() | tx_ref()) -> ok.
tx_commit(#{tx_handle := TxH}) ->
    rocksdb:transaction_commit(TxH);
tx_commit(TxH) when is_reference(TxH); is_binary(TxH) ->
    rocksdb:transaction_commit(TxH).

-spec get_ref(table()) -> db_ref().
get_ref(Tab) ->
    mnesia_rocksdb_admin:get_ref(Tab).

-spec ensure_ref(ref_or_tab()) -> db_ref() | tx_ref().
ensure_ref(Ref) when is_map(Ref) ->
    Ref;
ensure_ref(Other) ->
    get_ref(Other).

ensure_ref(Ref, R) when is_map(Ref) ->
    inherit_ctxt(Ref, R);
ensure_ref(Other, R) ->
    inherit_ctxt(get_ref(Other), R).

inherit_ctxt(Ref, R) ->
    maps:merge(Ref, maps:with([batch, tx_handle], R)).


-spec with_iterator(ref_or_tab(), fun( (itr_handle()) -> Res )) -> Res.
with_iterator(Tab, Fun) ->
    with_iterator(Tab, Fun, []).

-spec with_iterator(ref_or_tab(), fun( (itr_handle()) -> Res ), read_options()) -> Res.
with_iterator(Tab, Fun, Opts) when is_function(Fun, 1) ->
    with_iterator_(ensure_ref(Tab), Fun, Opts).

with_iterator_(Ref, Fun, Opts) ->
    {ok, I} = rdb_iterator(Ref, Opts),
    try Fun(I)
    after
        rocksdb:iterator_close(I)
    end.

-spec insert(ref_or_tab(), obj()) -> ok.
insert(Tab, Obj) ->
    insert(Tab, Obj, []).

-spec insert(ref_or_tab(), obj(), write_options()) -> ok.
insert(Tab, Obj, Opts) ->
    #{name := Name} = Ref = ensure_ref(Tab),
    Pos = keypos(Name),
    Key = element(Pos, Obj),
    EncVal = encode_val(setelement(Pos, Obj, [])),
    insert_(Ref, Key, encode_key(Key), EncVal, Obj, Opts).

insert_(#{semantics := bag} = Ref, Key, EncKey, EncVal, Obj, Opts) ->
    batch_if_index(Ref, insert, bag, fun insert_bag/4, Key, EncKey, EncVal, Obj, Opts);
    %% insert_bag(Ref, Obj, Opts);
insert_(Ref, Key, EncKey, EncVal, Obj, Opts) ->
    batch_if_index(Ref, insert, set, fun insert_set/4, Key, EncKey, EncVal, Obj, Opts).
    %% insert_set(Ref, Obj, Opts).

insert_set(Ref, EncKey, EncVal, Opts) ->
    rdb_put(Ref, EncKey, EncVal, Opts).

insert_bag(Ref, EncKey, EncVal, Opts) ->
    insert_bag_(Ref, EncKey, EncVal, Opts).

batch_if_index(#{mode := mnesia} = Ref, _, _, F, _Key, EncKey, EncVal, _Obj, Opts) ->
    F(Ref, EncKey, EncVal, Opts);
batch_if_index(#{name := Name, properties := #{index := [_|_] = Ixs}} = Ref,
               Op, SoB, F, Key, EncKey, EncVal, Obj, Opts) when is_atom(Name) ->
    IxF = fun(R) ->
                  update_index(Ixs, Op, SoB, Name, R, Key, Obj, Opts),
                  F(R, EncKey, EncVal, Opts)
          end,
    as_batch(Ref, IxF, Opts);
batch_if_index(Ref, _, _, F, _, EncKey, EncVal, _, Opts) ->
    F(Ref, EncKey, EncVal, Opts).

update_index(Ixs, insert, SoB, Name, R, Key, Obj, Opts) ->
    update_index_i(Ixs, SoB, Name, R, Key, encode_val({[]}), Obj, Opts);
update_index(Ixs, delete, _SoB, Name, R, Key, _Obj, Opts) ->
    update_index_d(Ixs, Name, R, Key, Opts).

update_index_i([{_Pos,ordered} = I|Ixs],
               SoB, Name, R, Key, EncVal, Obj, Opts) ->
    Tab = {Name, index, I},
    #{ix_vals_f := IxValsF} = IRef = ensure_ref(Tab, R),
    NewVals = IxValsF(Obj),
    case SoB of
        set ->
            OldObjs = read(R, Key, Opts),
            {Del, Put} = ix_vals_to_delete(OldObjs, IxValsF, NewVals),
            [rdb_delete(IRef, encode_key({IxVal, Key}), Opts)
             || IxVal <- Del],
            [rdb_put(IRef, encode_key({IxVal, Key}), EncVal, Opts)
             || IxVal <- Put];
        bag ->
            [rdb_put(IRef, encode_key({IxVal, Key}), EncVal, Opts)
             || IxVal <- NewVals]
    end,
    update_index_i(Ixs, SoB, Name, R, Key, EncVal, Obj, Opts);
update_index_i([], _, _, _, _, _, _, _) ->
    ok.

update_index_d([{_Pos,ordered} = I|Ixs], Name, R, Key, Opts) ->
    Tab = {Name, index, I},
    #{ix_vals_f := IxValsF} = IRef = ensure_ref(Tab, R),
    Old = read(R, Key, Opts),
    lists:foreach(
      fun(Obj) ->
              [rdb_delete(IRef, encode_key({IxVal, Key}), Opts)
               || IxVal <- IxValsF(Obj)]
      end, Old),
    update_index_d(Ixs, Name, R, Key, Opts);
update_index_d([], _, _, _, _) ->
    ok.

ix_vals_to_delete(OldObjs, IxValsF, NewVals) ->
    ix_vals_to_delete(OldObjs, IxValsF, NewVals, []).

ix_vals_to_delete([H|T], IxValsF, New, Del) ->
    Del1 = lists:foldl(
             fun(V, D) ->
                     case not lists:member(V, New)
                         andalso not lists:member(V, Del) of
                         true ->
                             [V|D];
                         false ->
                             D
                     end
             end, Del, IxValsF(H)),
    ix_vals_to_delete(T, IxValsF, New, Del1);
ix_vals_to_delete([], _, New, Del) ->
    {Del, New -- Del}.


read(Tab, Key) ->
    read(Tab, Key, []).

read(Tab, Key, Opts) ->
    read_(ensure_ref(Tab), Key, Opts).

read_(#{semantics := bag} = Ref, Key, Opts) ->
    read_bag_(Ref, Key, Opts);
read_(#{name := Name} = Ref, Key, Opts) ->
    Pos = keypos(Name),
    case rdb_get(Ref, encode_key(Key), read_opts(Ref, Opts)) of
        not_found ->
            [];
        {ok, Bin} ->
            Obj = decode_val(Bin),
            [setelement(Pos, Obj, Key)];
        {error, _} = Error ->
            mnesia:abort(Error)
    end.

read_bag_(#{name := Name} = Ref, Key, Opts) ->
    Pos = keypos(Name),
    Enc = encode_key(Key),
    Sz = byte_size(Enc),
    with_iterator(
      Ref, fun(I) ->
                   read_bag_i_(Sz, Enc, i_move(I, Enc), Key, I, Pos)
           end, read_opts(Ref, Opts)).

read_bag_i_(Sz, Enc, {ok, Enc, _}, K, I, KP) ->
    %% When exactly can this happen, and why skip? (this is from the old code)
    read_bag_i_(Sz, Enc, i_move(I, next), K, I, KP);
read_bag_i_(Sz, Enc, Res, K, I, KP) ->
    case Res of
        {ok, <<Enc:Sz/binary, _:?BAG_CNT>>, V} ->
            [setelement(KP, decode_val(V), K) |
             read_bag_i_(Sz, Enc, i_move(I, next), K, I, KP)];
        _ ->
            []
    end.

index_read(Tab, Val, Ix) ->
    index_read_(ensure_ref(Tab), Val, Ix).

index_read_(#{name := Main} = Ref, Val, Ix) ->
    I = case Ix of
            _ when is_atom(Ix) ->
                {attr_pos(Ix, Ref), ordered};
            {_} ->
                Ix;
            _ when is_integer(Ix) ->
                {Ix, ordered}
        end,
    IxRef = ensure_ref({Main, index, I}),
    IxValPfx = sext:prefix({Val,'_'}),
    Sz = byte_size(IxValPfx),
    with_iterator(
      IxRef,
      fun(It) ->
              index_read_i(rocksdb:iterator_move(It, IxValPfx),
                           It, IxValPfx, Sz, Ref)
      end).

attr_pos(A, #{attr_pos := AP}) ->
    maps:get(A, AP).

index_read_i({ok, K, _}, I, Pfx, Sz, Ref) ->
    case K of
        <<Pfx:Sz/binary, _/binary>> ->
            {_, FKey} = sext:decode(K),
            read(Ref, FKey) ++
                index_read_i(rocksdb:iterator_move(I, next),
                             I, Pfx, Sz, Ref);
        _ ->
            []
    end;
index_read_i({error, invalid_iterator}, _, _, _, _) ->
    [].

as_batch(Tab, F) ->
    as_batch(Tab, F, []).

as_batch(Tab, F, Opts) when is_function(F, 1), is_list(Opts) ->
    as_batch_(ensure_ref(Tab), F, Opts).

as_batch_(#{batch := _} = R, F, _) ->
    %% If already inside a batch, add to that batch (batches don't seem to nest)
    F(R);
as_batch_(#{db_ref := DbRef} = R, F, Opts) ->
    {ok, Batch} = rocksdb:batch(),
    try F(R#{batch => Batch}) of
        Res ->
            case rocksdb:write_batch(DbRef, Batch, write_opts(R, Opts)) of
                ok ->
                    Res;
                {error, Reason} ->
                    abort(Reason)
            end
    after
        rocksdb:release_batch(Batch)
    end.

write(Tab, L) when is_list(L) ->
    write(Tab, L, []).

write(Tab, L, Opts) ->
    write_(ensure_ref(Tab), L, Opts).

write_(Ref, L, Opts) ->
    as_batch(Ref, fun(R) -> write_as_batch(R, L) end, Opts).

write_as_batch(Ref, L) ->
    lists:foreach(
      fun({put, K, V}) ->
              rdb_put(Ref, K, V, []);
         ({delete, K}) ->
              rdb_delete(Ref, K, [])
      end, L).

-spec delete(ref_or_tab(), key()) -> ok.
delete(Tab, Key) ->
    delete(Tab, Key, []).

-spec delete(ref_or_tab(), key(), write_options()) -> ok.
delete(Tab, Key, Opts) ->
    delete_(ensure_ref(Tab), Key, encode_key(Key), Opts).

delete_(#{semantics := bag} = Ref, Key, EncKey, Opts) ->
    batch_if_index(Ref, delete, bag, fun delete_bag/4, Key, EncKey, [], [], Opts);
delete_(Ref, Key, EncKey, Opts) ->
    batch_if_index(Ref, delete, set, fun delete_set/4, Key, EncKey, [], [], Opts).

-spec first(ref_or_tab()) -> key() | '$end_of_table'.
first(Tab) ->
    first(Tab, []).

-spec first(ref_or_tab(), read_options()) -> key() | '$end_of_table'.
first(Tab, Opts) ->
    with_iterator(ensure_ref(Tab), fun i_first/1, Opts).

-spec last(ref_or_tab()) -> key() | '$end_of_table'.
last(Tab) ->
    last(Tab, []).

-spec last(ref_or_tab(), read_options()) -> key() | '$end_of_table'.
last(Tab, Opts) ->
    with_iterator(ensure_ref(Tab), fun i_last/1, Opts).

-spec next(ref_or_tab(), key()) -> key() | '$end_of_table'.
next(Tab, K) ->
    next(Tab, K, []).

-spec next(ref_or_tab(), key(), read_options()) -> key() | '$end_of_table'.
next(Tab, K, Opts) ->
    EncKey = encode_key(K),
    with_iterator(ensure_ref(Tab), fun(I) -> i_next(I, EncKey) end, Opts).

-spec prev(ref_or_tab(), key()) -> key() | '$end_of_table'.
prev(Tab, K) ->
    prev(Tab, K, []).

-spec prev(ref_or_tab(), key(), read_options()) -> key() | '$end_of_table'.
prev(Tab, K, Opts) ->
    EncKey = encode_key(K),
    with_iterator(ensure_ref(Tab), fun(I) -> i_prev(I, EncKey) end, Opts).

select(Tab, Pat) ->
    select(Tab, Pat, infinity).

select(Tab, Pat, Limit) when Limit == infinity; is_integer(Limit), Limit > 0 ->
    true = valid_limit(Limit),
    mrdb_select:select(ensure_ref(Tab), Pat, Limit).

select(Cont) ->
    mrdb_select:select(Cont).

match_delete(Tab, Pat) ->
    Ref = ensure_ref(Tab),
    MatchSpec = [{Pat, [], [true]}],
    as_batch(Ref, fun(R) ->
                          match_delete_(mrdb_select:select(Ref, MatchSpec, true, 30), R)
                  end).

match_delete_({L, Cont}, Ref) ->
    [rdb_delete(Ref, K, []) || K <- L],
    match_delete_(select(Cont), Ref);
match_delete_('$end_of_table', _) ->
    ok.

fold(Tab, Fun, Acc, MatchSpec) ->
    fold(Tab, Fun, Acc, MatchSpec, infinity).

fold(Tab, Fun, Acc, MatchSpec, Limit) ->
    true = valid_limit(Limit),
    mrdb_select:fold(ensure_ref(Tab), Fun, Acc, MatchSpec, Limit).

valid_limit(L) -> 
    case L of
        infinity ->
            true;
        _ when is_integer(L), L > 0 ->
            true;
        _ ->
            abort(badarg)
    end.

write_info(Tab, K, V) ->
    case ensure_ref(Tab) of
        #{type := standalone} = TRef ->
            write_info_standalone(TRef, K, V);
        #{alias := Alias} ->
            write_info_(ensure_ref({admin, Alias}), Tab, K, V)
    end.

write_info_(#{} = R, Tab, K, V) ->
    EncK = encode_key({info,Tab,K}),
    EncV = encode_val(V),
    rdb_put(R, EncK, EncV, write_opts(R, [])).

read_info(Tab, K) ->
    read_info(Tab, K, undefined).

read_info(Tab, K, Default) when K==size; K==memory ->
    read_direct_info_(ensure_ref(Tab), K, Default);
read_info(Tab, K, Default) ->
    case ensure_ref(Tab) of
        #{type := standalone} = TRef ->
            read_info_standalone(TRef, K, Default);
        #{alias := Alias} ->
            read_info_(ensure_ref({admin, Alias}), Tab, K, Default)
    end.

read_direct_info_(_, memory, Def) ->
    Def;
read_direct_info_(#{db_ref := _DbRef, cf_handle := _CfH}, size, Def) ->
    Def.
    %% case rocksdb:count(DbRef, CfH) of
    %%     {error, _} ->
    %%         Def;
    %%     Count ->
    %%         Count
    %% end.

read_info_(#{} = R, Tab, K, Default) ->
    EncK = encode_key({info, Tab, K}),
    get_info_res(rdb_get(R, EncK, read_opts(R, [])), Default).

get_info_res(Res, Default) ->
    case Res of
        not_found ->
            Default;
        {ok, Bin} ->
            decode_val(Bin);
        {error, E} ->
            error(E)
    end.

write_info_standalone(#{} = R, K, V) ->
    EncK = <<?INFO_TAG, (encode_key(K))/binary>>,
    EncV = encode_val(V),
    rdb_put(R, EncK, EncV, write_opts(R, [])).

read_info_standalone(#{} = R, K, Default) ->
    EncK = <<?INFO_TAG, (encode_key(K))/binary>>,
    get_info_res(rdb_get(R, EncK, read_opts(R, [])), Default).

insert_bag_(Ref, K, V, Opts) ->
    KSz = byte_size(K),
    with_iterator(
      Ref, fun(I) ->
                   do_insert_bag_(KSz, K, i_move(I, K), I, V, 0, Ref, Opts)
           end).

do_insert_bag_(Sz, K, Res, I, V, Prev, Ref, Opts) ->
    case Res of
        {ok, <<K:Sz/binary, _:?BAG_CNT>>, V} ->
            %% object exists
            ok;
        {ok, <<K:Sz/binary, N:?BAG_CNT>>, _} ->
            do_insert_bag_(Sz, K, i_move(I, next), I, V, N, Ref, Opts);
        _ ->
            Key = <<K/binary, (Prev+1):?BAG_CNT>>,
            rdb_put(Ref, Key, V, Opts)
    end.

delete_set(Ref, EncKey, _, Opts) ->
    rdb_delete(Ref, EncKey, Opts).

delete_bag(Ref, EncKey, _, Opts) ->
    Sz = byte_size(EncKey),
    Found = with_iterator(
              Ref, fun(I) ->
                           do_delete_bag_(Sz, EncKey, i_move(I, EncKey), Ref, I)
                   end),
    case Found of
        [] ->
            ok;
        _ ->
            write(Ref, [{delete, K} || K <- Found], Opts)
    end.

do_delete_bag_(Sz, K, Res, Ref, I) ->
    case Res of
        {ok, K, _} ->
            do_delete_bag_(Sz, K, i_move(I, next), Ref, I);
        {ok, <<K:Sz/binary, _:?BAG_CNT>> = Key, _} ->
            [Key | do_delete_bag_(Sz, K, i_move(I, next), Ref, I)];
        _ ->
            []
    end.

rdb_put(#{batch := Batch, cf_handle := CfH}, K, V, _Opts) ->
    rocksdb:batch_put(Batch, CfH, K, V);
rdb_put(#{tx_handle := TxH, cf_handle := CfH}, K, V, _Opts) ->
    rocksdb:transaction_put(TxH, CfH, K, V);
rdb_put(#{db_ref := DbRef, cf_handle := CfH} = R, K, V, Opts) ->
    rocksdb:put(DbRef, CfH, K, V, write_opts(R, Opts)).

rdb_get(#{tx_handle := TxH, cf_handle := CfH}, K, _Opts) ->
    rocksdb:transaction_get(TxH, CfH, K);
rdb_get(#{db_ref := DbRef, cf_handle := CfH} = R, K, Opts) ->
    rocksdb:get(DbRef, CfH, K, read_opts(R, Opts)).


rdb_delete(#{batch := Batch, cf_handle := CfH}, K, _Opts) ->
    rocksdb:batch_delete(Batch, CfH, K);
rdb_delete(#{tx_handle := TxH, cf_handle := CfH}, K, _Opts) ->
    rocksdb:transaction_delete(TxH, CfH, K);
rdb_delete(#{db_ref := DbRef, cf_handle := CfH} = R, K, Opts) ->
    rocksdb:delete(DbRef, CfH, K, write_opts(R, Opts)).

rdb_iterator(#{db_ref := DbRef, tx_handle := TxH, cf_handle := CfH} = R, Opts) ->
    rocksdb:transaction_iterator(DbRef, TxH, CfH, read_opts(R, Opts));
rdb_iterator(#{db_ref := DbRef, cf_handle := CfH} = R, Opts) ->
    rocksdb:iterator(DbRef, CfH, read_opts(R, Opts)).

write_opts(#{write_opts := Os}, Opts) -> Os ++ Opts;
write_opts(_, Opts) -> 
    Opts.

read_opts(#{read_opts := Os}, Opts) -> Os ++ Opts;
read_opts(_, Opts) -> 
    Opts.

-define(EOT, '$end_of_table').

i_first(I) ->
    case i_move(I, first) of
        {ok, First, _} ->
            decode_key(First);
        _ ->
            ?EOT
    end.

i_last(I) ->
    case i_move(I, last) of
        {ok, Last, _} ->
            decode_key(Last);
        _ ->
            ?EOT
    end.

i_move(I, Where) ->
    rocksdb:iterator_move(I, Where).

i_next(I, Key) ->
    i_move_to_next(i_move(I, Key), I, Key).

i_prev(I, Key) ->
    i_move_to_prev(i_move(I, Key), I, Key).

i_move_to_next({ok, Key, _}, I, Key) ->
    i_move_to_next(i_move(I, next), I, Key);
i_move_to_next({ok, NextKey, _}, _, _) ->
    decode_key(NextKey);
i_move_to_next(_, _, _) ->
    ?EOT.

i_move_to_prev({ok, K, _}, _I, Key) when K < Key ->
    decode_key(K);
i_move_to_prev({ok, _, _}, I, Key) ->
    i_move_to_prev(i_move(I, prev), I, Key);
i_move_to_prev(_, _, _) ->
    ?EOT.
