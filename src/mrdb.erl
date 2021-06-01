%% @doc Mid-level access API for Mnesia-managed rocksdb tables
%%
%% This module implements access functions for the mnesia_rocksdb
%% backend plugin. The functions are designed to also support
%% direct access to rocksdb with little overhead. Such direct
%% access will maintain existing indexes, but not support
%% replication.
%%
%% Each table has a metadata structure stored as a persistent
%% term for fast access. The structure of the metadata is as
%% follows:
%%
%% ```
%% #{ name       := <Logical table name>
%%  , db_ref     := <Rocksdb database Ref>
%%  , cf_handle  := <Rocksdb column family handle>
%%  , batch      := <Batch reference, if any>
%%  , tx_handle  := <Rocksdb transaction handle, if any>
%%  , attr_pos   := #{AttrName := Pos}
%%  , mode       := <Set to 'mnesia' for mnesia access flows>
%%  , properties := <Mnesia table props in map format
%%  , type       := column_family | standalone
%%  }
%% '''
%%
%% Helper functions like `as_batch(Ref, fun(R) -> ... end)' and
%% `with_iterator(Ref, fun(I) -> ... end)' add some extra
%% convenience on top of the `rocksdb' API.
%%
%% Note that no automatic provision exists to manage concurrent
%% updates via mnesia AND direct access to this API. It's advisable
%% to use ONE primary mode of access. If replication is used,
%% the mnesia API will support this, but direct `mrdb' updates will
%% not be replicated.

-module(mrdb).

-export([ get_ref/1
        , ensure_ref/1   , ensure_ref/2
        , new_tx/1       , new_tx/2
        , tx_ref/2
        , tx_commit/1
        , with_iterator/2, with_iterator/3
        , with_rdb_iterator/2, with_rdb_iterator/3
        , iterator_move/2
        , rdb_iterator_move/2
        , iterator/1     , iterator/2
        , iterator_close/1
        , read/2         , read/3
        , index_read/3
        , insert/2       , insert/3
        , delete/2       , delete/3
        , delete_object/2, delete_object/3
        , match_delete/2
        , write/2        , write/3
        , update_counter/3, update_counter/4
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

-export([activity/3]).

-export([abort/1]).

%% Low-level access wrappers.
-export([ rdb_put/4
        , rdb_get/3
        , rdb_delete/3
        , rdb_iterator/2 ]).

-import(mnesia_rocksdb_lib, [ keypos/1
                            , encode_key/1
                            , encode_key/2
                            , decode_key/2
                            , encode_val/2
                            , decode_val/3 ]).

-export_type( [ mrdb_iterator/0
              , itr_handle/0
              , iterator_action/0
              , db_ref/0
              , ref_or_tab/0
              , index_position/0
              ]).

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
-type index_position() :: atom() | pos().

-type db_handle()  :: rocksdb:db_handle().
-type cf_handle()  :: rocksdb:cf_handle().
-type tx_handle()  :: rocksdb:transaction_handle().
-type itr_handle() :: rocksdb:itr_handle().
-type batch_handle() :: rocksdb:batch_handle().

-type pos() :: non_neg_integer().

-type properties() :: #{ record_name := atom()
                       , attributes  := [atom()]
                       , index       := [{pos(), bag | ordered}]
                       }.
-type semantics()  :: bag | set.
-type key_encoding() :: 'raw' | 'sext' | 'term'.
-type val_encoding() :: {'value' | 'object', 'term' | 'raw'}
                      | 'raw'.
-type encoding()   :: 'raw' | 'sext' | 'term'
                    | {key_encoding(), val_encoding()}.
-type attr_pos() :: #{atom() := pos()}.

-type db_ref() :: #{ name       => table()
                   , alias      => atom()
                   , vsn        => non_neg_integer()
                   , db_ref     := db_handle()
                   , cf_handle  := cf_handle()
                   , semantics  := semantics()
                   , encoding   := encoding()
                   , attr_pos   := attr_pos()
                   , type       := column_family | standalone
                   , status     := open | closed | pre_existing
                   , properties := properties()
                   , mode       => mnesia
                   , ix_vals_f  => fun( (tuple()) -> [any()] )
                   , batch      => batch_handle()
                   , tx_handle  => tx_handle()
                   , _ => _}.

-type ref_or_tab() :: table() | db_ref().

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

-type iterator_action() :: first | last | next | prev | binary()
                         | {seek, binary()} | {seek_for_prev, binary()}.

%% ============================================================

-record(mrdb_iter, { i   :: itr_handle()
                   , ref :: db_ref() }).

-type mrdb_iterator() :: #mrdb_iter{}.

activity(Type, Alias, F) ->
    #{db_ref := DbRef} = ensure_ref({admin, Alias}),
    case TxType = tx_type(Type) of
        tx ->
            {ok, TxH} = rocksdb:transaction(DbRef, []),
            push_ctxt(#{ type   => TxType
                       , alias  => Alias
                       , db_ref => DbRef
                       , handle => TxH });
        batch ->
            {ok, Batch} = rocksdb:batch(),
            push_ctxt(#{ type   => TxType
                       , alias  => Alias
                       , db_ref => DbRef
                       , handle => Batch })
    end,
    try F() of
        Res ->
            commit_and_pop(Res)
    catch
        Cat:Err when Cat==error;
                     Cat==exit ->
            abort_and_pop(Cat, Err)
    end.

ctxt() -> {?MODULE, ctxt}.

push_ctxt(C) ->
    K = ctxt(),
    C1 = case get(K) of
             undefined -> [C];
             C0        -> [C|C0]
         end,
    put(K, C1),
    ok.

pop_ctxt() ->
    K = ctxt(),
    case get(K) of
        undefined -> error(no_ctxt);
        [C]       -> erase(K) , C;
        [H|T]     -> put(K, T), H
    end.

tx_type(T) when T==tx; T==batch -> T;
tx_type(T) ->
    case T of
        transaction      -> tx;
        {transaction,_}  -> tx;
        sync_transaction -> tx;
        async_dirty      -> batch;
        sync_dirty       -> batch;
        _ -> abort(invalid_activity_type)
    end.

commit_and_pop(Res) ->
    #{type := Type, handle := H, db_ref := DbRef} = pop_ctxt(),
    case Type of
        tx ->
            ok = rocksdb:transaction_commit(H),
            Res;
        batch ->
            case rocksdb:write_batch(DbRef, H, []) of
                ok -> Res;
                Other ->
                    Other
            end
    end.

abort_and_pop(Cat, Err) ->
    #{type := Type, handle := H} = pop_ctxt(),
    case Type of
        %% tx    -> ok = rocksdb:transaction_abort(H);  %% doesn't yet exist
        tx    -> error(nyi);
        batch -> ok = rocksdb:release_batch(H)
    end,
    case Cat of
        error -> error(Err);
        exit  -> exit(Err)
        %% throw -> throw(Err)
    end.

abort(Reason) ->
    erlang:error({mrdb_abort, Reason}).

-spec new_tx(table() | db_ref()) -> db_ref().
new_tx(Tab) ->
    new_tx(Tab, []).

-spec new_tx(ref_or_tab(), write_options()) -> db_ref().
new_tx(Tab, Opts) ->
    #{db_ref := DbRef} = R = ensure_ref(Tab),
    {ok, TxH} = rocksdb:transaction(DbRef, write_opts(R, Opts)),
    R#{tx_handle => TxH}.

-spec tx_ref(ref_or_tab() | db_ref() | db_ref(), tx_handle()) -> db_ref().
tx_ref(Tab, TxH) ->
    case ensure_ref(Tab) of
        #{tx_handle := TxH} = R ->
            R;
        #{tx_handle := OtherTxH} ->
            error({tx_handle_conflict, OtherTxH});
        R ->
            R#{tx_handle => TxH}
    end.

-spec tx_commit(tx_handle() | db_ref()) -> ok.
tx_commit(#{tx_handle := TxH}) ->
    rocksdb:transaction_commit(TxH);
tx_commit(TxH) ->
    rocksdb:transaction_commit(TxH).

-spec get_ref(table()) -> db_ref().
get_ref(Tab) ->
    mnesia_rocksdb_admin:get_ref(Tab).

-spec ensure_ref(ref_or_tab()) -> db_ref().
ensure_ref(Ref) when is_map(Ref) ->
    Ref;
ensure_ref(Other) ->
    maybe_tx_ctxt(get(ctxt()), get_ref(Other)).

ensure_ref(Ref, R) when is_map(Ref) ->
    inherit_ctxt(Ref, R);
ensure_ref(Other, R) ->
    inherit_ctxt(get_ref(Other), R).

maybe_tx_ctxt(undefined,             R) -> R;
maybe_tx_ctxt(_, #{batch     := _} = R) -> R;
maybe_tx_ctxt(_, #{tx_handle := _} = R) -> R;
maybe_tx_ctxt([#{type := Type, handle := H}|_], R) ->
    case Type of
        tx    -> R#{tx_handle => H};
        batch -> R#{batch     => H}
    end.

inherit_ctxt(Ref, R) ->
    maps:merge(Ref, maps:with([batch, tx_handle], R)).

-spec with_iterator(ref_or_tab(), fun( (mrdb_iterator()) -> Res )) -> Res.
with_iterator(Tab, Fun) ->
    with_iterator(Tab, Fun, []).

-spec with_iterator(ref_or_tab(), fun( (mrdb_iterator()) -> Res ), read_options()) -> Res.
with_iterator(Tab, Fun, Opts) ->
    R = ensure_ref(Tab),
    with_iterator_(R, Fun, Opts).

-spec with_rdb_iterator(ref_or_tab(), fun( (itr_handle()) -> Res )) -> Res.
with_rdb_iterator(Tab, Fun) ->
    with_rdb_iterator(Tab, Fun, []).

-spec with_rdb_iterator(ref_or_tab(), fun( (itr_handle()) -> Res ), read_options()) -> Res.
with_rdb_iterator(Tab, Fun, Opts) when is_function(Fun, 1) ->
    R = ensure_ref(Tab),
    with_rdb_iterator_(R, Fun, read_opts(R, Opts)).

with_iterator_(R, Fun, Opts) ->
    {ok, I} = rdb_iterator_(R, Opts),
    try Fun(#mrdb_iter{ i   = I
                      , ref = R })
    after
        rocksdb:iterator_close(I)
    end.

with_rdb_iterator_(Ref, Fun, ROpts) ->
    {ok, I} = rdb_iterator_(Ref, ROpts),
    try Fun(I)
    after
        rocksdb:iterator_close(I)
    end.

-spec iterator_move(mrdb_iterator(), iterator_action()) ->
          {ok, tuple()} | {error, any()}.
iterator_move(#mrdb_iter{i = I, ref = Ref}, Dir) ->
    case i_move(I, Dir) of
        {ok, EncK, EncV} ->
            K = decode_key(EncK, Ref),
            Obj = decode_val(EncV, K, Ref),
            {ok, Obj};
        Other ->
            Other
    end.

-spec iterator(ref_or_tab()) -> {ok, mrdb_iterator()} | {error, _}.
iterator(Tab) ->
    iterator(Tab, []).

-spec iterator(ref_or_tab(), read_options()) -> {ok, mrdb_iterator()} | {error, _}.
iterator(Tab, Opts) ->
    Ref = ensure_ref(Tab),
    case rdb_iterator(Ref, Opts) of
        {ok, I} ->
            {ok, #mrdb_iter{ i = I
                           , ref = Ref }};
        Other ->
            Other
    end.

-spec iterator_close(mrdb_iterator()) -> ok.
iterator_close(#mrdb_iter{i = I}) ->
    rocksdb:iterator_close(I).

rdb_iterator_move(I, Dir) ->
    i_move(I, Dir).

-spec insert(ref_or_tab(), obj()) -> ok.
insert(Tab, Obj) ->
    insert(Tab, Obj, []).

-spec insert(ref_or_tab(), obj(), write_options()) -> ok.
insert(Tab, Obj0, Opts) ->
    #{name := Name} = Ref = ensure_ref(Tab),
    Obj = validate_obj(Obj0, Ref),
    Pos = keypos(Name),
    Key = element(Pos, Obj),
    EncVal = encode_val(Obj, Ref),
    insert_(Ref, Key, encode_key(Key, Ref), EncVal, Obj, Opts).

validate_obj(Obj, #{mode := mnesia}) ->
    Obj;
validate_obj(Obj, #{attr_pos := AP, properties := #{record_name := RN}}) when is_tuple(Obj) ->
    Arity = map_size(AP) + 1,
    case {element(1, Obj), tuple_size(Obj)} of
        {RN, Arity} ->
            Obj;
        _ ->
            abort(badarg)
    end;
validate_obj(_, _) ->
    abort(badarg).

insert_(#{semantics := bag} = Ref, Key, EncKey, EncVal, Obj, Opts) ->
    batch_if_index(Ref, insert, bag, fun insert_bag/5, Key, EncKey, EncVal, Obj, Opts);
    %% insert_bag(Ref, Obj, Opts);
insert_(Ref, Key, EncKey, EncVal, Obj, Opts) ->
    batch_if_index(Ref, insert, set, fun insert_set/5, Key, EncKey, EncVal, Obj, Opts).
    %% insert_set(Ref, Obj, Opts).

insert_set(Ref, EncKey, EncVal, _, Opts) ->
    rdb_put(Ref, EncKey, EncVal, Opts).

insert_bag(Ref, EncKey, EncVal, _, Opts) ->
    %% case Ref of
    %%     #{vsn := 1} ->
    insert_bag_v1(Ref, EncKey, EncVal, Opts).

batch_if_index(#{mode := mnesia} = Ref, _, _, F, _Key, EncKey, Data, _Obj, Opts) ->
    F(Ref, EncKey, Data, undefined, Opts);
batch_if_index(#{name := Name, properties := #{index := [_|_] = Ixs}} = Ref,
               Op, SoB, F, Key, EncKey, Data, Obj, Opts) when is_atom(Name) ->
    IxF = fun(R) ->
                  IxRes = update_index(Ixs, Op, SoB, Name, R, Key, Obj, Opts),
                  F(R, EncKey, Data, IxRes, Opts)
          end,
    as_batch(Ref, IxF, Opts);
batch_if_index(Ref, _, _, F, _, EncKey, Data, _, Opts) ->
    F(Ref, EncKey, Data, undefined, Opts).

update_index(Ixs, insert, SoB, Name, R, Key, Obj, Opts) ->
    update_index_i(Ixs, SoB, Name, R, Key, Obj, Opts);
update_index(Ixs, delete, set, Name, R, Key, _Obj, Opts) ->
    update_index_d(Ixs, Name, R, Key, Opts);
update_index(Ixs, delete_obj, set, Name, R, Key, Obj, Opts) ->
    update_index_do_set(Ixs, Name, R, Key, Obj, Opts);
update_index(Ixs, delete_obj, bag, Name, R, Key, Obj, Opts) ->
    update_index_do_bag(Ixs, Name, R, Key, Obj, Opts).

update_index_i([{_Pos,ordered} = I|Ixs],
               SoB, Name, R, Key, Obj, Opts) ->
    Tab = {Name, index, I},
    #{ix_vals_f := IxValsF} = IRef = ensure_ref(Tab, R),
    EncVal = <<>>,
    NewVals = IxValsF(Obj),
    case SoB of
        set ->
            OldObjs = read(R, Key, Opts),
            {Del, Put} = ix_vals_to_delete(OldObjs, IxValsF, NewVals),
            [rdb_delete(IRef, encode_key({IxVal, Key}, IRef), Opts)
             || IxVal <- Del],
            [rdb_put(IRef, encode_key({IxVal, Key}, IRef), EncVal, Opts)
             || IxVal <- Put];
        bag ->
            [rdb_put(IRef, encode_key({IxVal, Key}, IRef), EncVal, Opts)
             || IxVal <- NewVals]
    end,
    update_index_i(Ixs, SoB, Name, R, Key, Obj, Opts);
update_index_i([], _, _, _, _, _, _) ->
    ok.

update_index_d(Ixs, Name, R, Key, Opts) ->
    Found = read(R, Key, Opts),
    update_index_d_(Ixs, Name, R, Key, Found, Opts).

update_index_d_([{_Pos,ordered} = I|Ixs], Name, R, Key, Found, Opts) ->
    Tab = {Name, index, I},
    #{ix_vals_f := IxValsF} = IRef = ensure_ref(Tab, R),
    IxVals =
        lists:foldl(
          fun(Obj, Acc) ->
                  ordsets:union(Acc, ordsets:from_list(IxValsF(Obj)))
          end, ordsets:new(), Found),
    [rdb_delete(IRef, encode_key({IxVal, Key}, IRef), Opts) || IxVal <- IxVals],
    update_index_d_(Ixs, Name, R, Key, Found, Opts);
update_index_d_([], _, _, _, _, _) ->
    undefined.

update_index_do_set(Ixs, Name, R, Key, Obj, Opts) ->
    EncKey = encode_key(Key, R),
    case read_raw(R, EncKey, Key, Opts) of
        [Obj] ->
            update_index_do_set_(Ixs, Name, R, Key, Obj, Opts),
            EncKey;
        _ ->
            not_found
    end.

update_index_do_set_([{_Pos,ordered} = I|Ixs], Name, R, Key, Obj, Opts) ->
    Tab = {Name, index, I},
    #{ix_vals_f := IxValsF} = IRef = ensure_ref(Tab, R),
    IxVals = IxValsF(Obj),
    [rdb_delete(IRef, encode_key({IxVal, Key}, IRef), Opts) || IxVal <- IxVals],
    update_index_do_set_(Ixs, Name, R, Key, Obj, Opts);
update_index_do_set_([], _, _, _, _, _) ->
    ok.

%% TODO: make IxRefs for all ix positions, traverse the main tab once
update_index_do_bag(Ixs, Name, R, Key, Obj, Opts) ->
    case read_bag_ret_raw_keys(R, Key, Opts) of
        [] ->
            not_found;
        Found ->
            case lists:keytake(Obj, 2, Found) of
                {value, {RawK, _}, Rest} ->
                    update_index_do(Ixs, Name, R, Key, Obj, Rest, Opts),
                    RawK;
                false ->
                    not_found
            end
    end.
    
update_index_do([{_Pos,ordered} = Ix|Ixs], Name, R, Key, Obj, Rest, Opts) ->
    Tab = {Name, index, Ix},
    #{ix_vals_f := IxValsF} = IRef = ensure_ref(Tab, R),
    IxVals = IxValsF(Obj),
    IxVals1 = lists:foldl(fun({_,O}, Acc) -> Acc -- IxValsF(O) end, IxVals, Rest),
    [rdb_delete(IRef, encode_key({IxVal, Key}, IRef), Opts) || IxVal <- IxVals1],
    update_index_do(Ixs, Name, R, Key, Obj, Rest, Opts);
update_index_do([], _, _, _, _, _, _) ->
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
read_(Ref, Key, Opts) ->
    read_raw(Ref, encode_key(Key, Ref), Key, read_opts(Ref, Opts)).

read_raw(Ref, EncKey, Key, Opts) ->
    case rdb_get(Ref, EncKey, read_opts(Ref, Opts)) of
        not_found ->
            [];
        {ok, Bin} ->
            Obj = decode_val(Bin, Key, Ref),
            [Obj];
        {error, _} = Error ->
            mnesia:abort(Error)
    end.

read_bag_(Ref, Key, Opts) ->
    read_bag_(Ref, Key, false, Opts).

read_bag_ret_raw_keys(Ref, Key, Opts) ->
    read_bag_(Ref, Key, true, Opts).

read_bag_(#{name := Name} = Ref, Key, RetRaw, Opts) ->
    Pos = keypos(Name),
    Enc = encode_key(Key, Ref),
    Sz = byte_size(Enc),
    with_rdb_iterator_(
      Ref, fun(I) ->
                   read_bag_i_(Sz, Enc, i_move(I, Enc), Key, I, Pos, Ref, RetRaw)
           end, read_opts(Ref, Opts)).

read_bag_i_(Sz, Enc, {ok, Enc, _}, K, I, KP, Ref, RetRaw) ->
    %% When exactly can this happen, and why skip? (this is from the old code)
    read_bag_i_(Sz, Enc, i_move(I, next), K, I, KP, Ref, RetRaw);
read_bag_i_(Sz, Enc, Res, K, I, KP, Ref, RetRaw) ->
    case Res of
        {ok, <<Enc:Sz/binary, _:?BAG_CNT>> = RawK, V} ->
            Obj = decode_val(V, K, Ref),
            [maybe_wrap(RetRaw, Obj, RawK) |
             read_bag_i_(Sz, Enc, i_move(I, next), K, I, KP, Ref, RetRaw)];
        _ ->
            []
    end.

maybe_wrap(false, Obj, _   ) -> Obj;
maybe_wrap(true , Obj, RawK) -> {RawK, Obj}.

index_read(Tab, Val, Ix) ->
    index_read_(ensure_ref(Tab), Val, Ix).

index_read_(#{name := Main, semantics := Sem} = Ref, Val, Ix) ->
    I = case Ix of
            _ when is_atom(Ix) ->
                {attr_pos(Ix, Ref), ordered};
            {_} ->
                Ix;
            _ when is_integer(Ix) ->
                {Ix, ordered}
        end,
    #{ix_vals_f := IxValF} = IxRef = ensure_ref({Main, index, I}),
    IxValPfx = sext:prefix({Val,'_'}),
    Sz = byte_size(IxValPfx),
    Fun = case Sem of
              bag -> fun(It) ->
                             index_read_i_bag(rocksdb:iterator_move(It, IxValPfx),
                                              It, IxValPfx, Sz, Val, IxValF, Ref)
                     end;
              _   -> fun(It) ->
                             index_read_i(rocksdb:iterator_move(It, IxValPfx),
                                          It, IxValPfx, Sz, Ref)
                     end
          end,
    with_rdb_iterator(IxRef, Fun).
      %% IxRef,
      %% fun(It) ->
      %%         index_read_i(rocksdb:iterator_move(It, IxValPfx),
      %%                      It, IxValPfx, Sz, Ref)
      %% end).

attr_pos(A, #{attr_pos := AP}) ->
    maps:get(A, AP).

index_read_i({ok, K, _}, I, Pfx, Sz, Ref) ->
    case K of
        <<Pfx:Sz/binary, _/binary>> ->
            {_, Key} = sext:decode(K),
            read(Ref, Key, []) ++
                index_read_i(rocksdb:iterator_move(I, next),
                             I, Pfx, Sz, Ref);
        _ ->
            []
    end;
index_read_i({error, invalid_iterator}, _, _, _, _) ->
    [].

index_read_i_bag({ok, K, _}, I, Pfx, Sz, Val, ValsF, Ref) ->
    case K of
        <<Pfx:Sz/binary, _/binary>> ->
            {_, Key} = sext:decode(K),
            filter_objs(read(Ref, Key, []), Val, ValsF) ++
                index_read_i_bag(rocksdb:iterator_move(I, next),
                                 I, Pfx, Sz, Val, ValsF, Ref);
        _ ->
            []
    end;
index_read_i_bag({error, invalid_iterator}, _, _, _, _, _, _) ->
    [].

filter_objs([], _, _) ->
    [];
filter_objs([H|T], Val, ValsF) ->
    case lists:member(Val, ValsF(H)) of
        true  -> [H | filter_objs(T, Val, ValsF)];
        false -> filter_objs(T, Val, ValsF)
    end.

%% decode_raw_key(RK, #{semantics := bag, vsn := V}) when V =< 2 ->
%%     KSz = byte_size(RK) - (?BAG_CNT div 8),
%%     <<K:KSz/binary, _:?BAG_CNT>> = RK,
%%     decode_key(K, sext);
%% decode_raw_key(RK, Ref) ->
%%     decode_key(RK, Ref).

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

update_counter(Tab, C, Val) ->
    update_counter(Tab, C, Val, []).

update_counter(Tab, C, Val, Opts) ->
    Ref = ensure_ref(Tab),
    case Ref of
        #{encoding := {_, {value, term}}} ->
            update_counter_(Ref, encode_key(C, Ref), Val, Opts);
        _ ->
            abort(badarg)
    end.

update_counter_(Ref, EncKey, Val, Opts) ->
    rdb_merge(Ref, EncKey, {int_add, Val}, Opts).

-spec delete(ref_or_tab(), key()) -> ok.
delete(Tab, Key) ->
    delete(Tab, Key, []).

-spec delete(ref_or_tab(), key(), write_options()) -> ok.
delete(Tab, Key, Opts) ->
    Ref = ensure_ref(Tab),
    delete_(Ref, Key, encode_key(Key, Ref), Opts).

delete_(#{semantics := bag} = Ref, Key, EncKey, Opts) ->
    batch_if_index(Ref, delete, bag, fun delete_bag/5, Key, EncKey, [], [], Opts);
delete_(Ref, Key, EncKey, Opts) ->
    batch_if_index(Ref, delete, set, fun delete_set/5, Key, EncKey, [], [], Opts).

delete_object(Tab, Obj) ->
    delete_object(Tab, Obj, []).

delete_object(Tab, Obj, Opts) ->
    #{name := Name} = Ref = ensure_ref(Tab),
    Key = element(keypos(Name), Obj),
    EncKey = encode_key(Key, Ref),
    delete_object_(Ref, Key, EncKey, Obj, Opts).

delete_object_(#{semantics := bag} = Ref, Key, EncKey, Obj, Opts) ->
    batch_if_index(Ref, delete_obj, bag, fun delete_obj_bag/5, Key,
                   EncKey, Obj, Obj, Opts);
delete_object_(Ref, Key, EncKey, Obj, Opts) ->
    batch_if_index(Ref, delete_obj, set, fun delete_obj_set/5, Key,
                   EncKey, Obj, Obj, Opts).

-spec first(ref_or_tab()) -> key() | '$end_of_table'.
first(Tab) ->
    first(Tab, []).

-spec first(ref_or_tab(), read_options()) -> key() | '$end_of_table'.
first(Tab, Opts) ->
    Ref = ensure_ref(Tab),
    with_rdb_iterator(Ref, fun(I) -> i_first(I, Ref) end, Opts).

-spec last(ref_or_tab()) -> key() | '$end_of_table'.
last(Tab) ->
    last(Tab, []).

-spec last(ref_or_tab(), read_options()) -> key() | '$end_of_table'.
last(Tab, Opts) ->
    Ref = ensure_ref(Tab),
    with_rdb_iterator(Ref, fun(I) -> i_last(I, Ref) end, Opts).

-spec next(ref_or_tab(), key()) -> key() | '$end_of_table'.
next(Tab, K) ->
    next(Tab, K, []).

-spec next(ref_or_tab(), key(), read_options()) -> key() | '$end_of_table'.
next(Tab, K, Opts) ->
    Ref = ensure_ref(Tab),
    EncKey = encode_key(K, Ref),
    with_rdb_iterator(Ref, fun(I) -> i_next(I, EncKey, Ref) end, Opts).

-spec prev(ref_or_tab(), key()) -> key() | '$end_of_table'.
prev(Tab, K) ->
    prev(Tab, K, []).

-spec prev(ref_or_tab(), key(), read_options()) -> key() | '$end_of_table'.
prev(Tab, K, Opts) ->
    Ref = ensure_ref(Tab),
    EncKey = encode_key(K, Ref),
    with_rdb_iterator(Ref, fun(I) -> i_prev(I, EncKey, Ref) end, Opts).

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
                          %% call select() with AccKeys=true, returning [{Key, _}]
                          match_delete_(mrdb_select:select(Ref, MatchSpec, true, 30), R)
                  end).

match_delete_({L, Cont}, Ref) ->
    [rdb_delete(Ref, K, []) || {K,_} <- L],
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
    R = ensure_ref(Tab),
    Alias = case R of
                #{type := standalone, vsn := 1, alias := A} = TRef ->
                    %% Also write on legacy info format
                    write_info_standalone(TRef, K, V),
                    A;
                #{alias := A} ->
                    A
            end,
    write_info_(ensure_ref({admin, Alias}), Tab, K, V).

write_info_(#{} = R, Tab, K, V) ->
    EncK = encode_key({info,Tab,K}, sext),
    EncV = term_to_binary(V),
    rdb_put(R, EncK, EncV, write_opts(R, [])).

read_info(Tab, K) ->
    read_info(Tab, K, undefined).

read_info(Tab, K, Default) when K==size; K==memory ->
    read_direct_info_(ensure_ref(Tab), K, Default);
read_info(Tab, K, Default) ->
    #{alias := Alias} = R = ensure_ref(Tab),
    case R of
        #{type := standalone, vsn := 1} = TRef ->
            read_info_standalone(TRef, K, Default);
        #{alias := Alias} ->
            mnesia_rocksdb_admin:read_info(Alias, Tab, K, Default)
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

write_info_standalone(#{} = R, K, V) ->
    EncK = <<?INFO_TAG, (encode_key(K, sext))/binary>>,
    EncV = term_to_binary(V),
    rdb_put(R, EncK, EncV, write_opts(R, [])).

read_info_standalone(#{} = R, K, Default) ->
    EncK = <<?INFO_TAG, (encode_key(K, sext))/binary>>,
    get_info_res(rdb_get(R, EncK, read_opts(R, [])), Default).

get_info_res(Res, Default) ->
    case Res of
        not_found ->
            Default;
        {ok, Bin} ->
            %% no fancy tricks when encoding/decoding info values
            binary_to_term(Bin);
        {error, E} ->
            error(E)
    end.

%% insert_bag_v2(Ref, K, V, Opts) ->
%%     rdb_merge(Ref, K, {list_append, [V]}

insert_bag_v1(Ref, K, V, Opts) ->
    KSz = byte_size(K),
    with_rdb_iterator(
      Ref, fun(I) ->
                   do_insert_bag_v1(KSz, K, i_move(I, K), I, V, 0, Ref, Opts)
           end).

do_insert_bag_v1(Sz, K, Res, I, V, Prev, Ref, Opts) ->
    case Res of
        {ok, <<K:Sz/binary, _:?BAG_CNT>>, V} ->
            %% object exists
            ok;
        {ok, <<K:Sz/binary, N:?BAG_CNT>>, _} ->
            do_insert_bag_v1(Sz, K, i_move(I, next), I, V, N, Ref, Opts);
        _ ->
            Key = <<K/binary, (Prev+1):?BAG_CNT>>,
            rdb_put(Ref, Key, V, Opts)
    end.

delete_set(Ref, EncKey, _Data, _IxRes, Opts) ->
    rdb_delete(Ref, EncKey, Opts).

delete_bag(_, _, _, not_found, _) ->
    %% Indexing function already tried reading the object, and didn't find it
    ok;
delete_bag(Ref, _, _, RawKey, Opts) when is_binary(RawKey) ->
    rdb_delete(Ref, RawKey, Opts);
delete_bag(Ref, EncKey, _, _, Opts) ->
    Sz = byte_size(EncKey),
    Found = with_rdb_iterator(
              Ref, fun(I) ->
                           do_delete_bag_(Sz, EncKey, i_move(I, EncKey), Ref, I)
                   end),
    case Found of
        [] ->
            ok;
        _ ->
            write(Ref, [{delete, K} || K <- Found], Opts)
    end.

delete_obj_bag(_, _, _, not_found, _) ->
    ok;
delete_obj_bag(Ref, _EncKey, _Obj, RawKey, Opts) when is_binary(RawKey) ->
    rdb_delete(Ref, RawKey, Opts);
delete_obj_bag(Ref, EncKey, Obj, _, Opts) ->
    Sz = byte_size(EncKey),
    with_rdb_iterator(
      Ref, fun(I) ->
                   do_del_obj_bag_(Sz, EncKey, i_move(I, EncKey), Obj, Ref, I, Opts)
           end).

do_delete_bag_(Sz, K, Res, Ref, I) ->
    case Res of
        {ok, K, _} ->
            do_delete_bag_(Sz, K, i_move(I, next), Ref, I);
        {ok, <<K:Sz/binary, _:?BAG_CNT>> = Key, _} ->
            [Key | do_delete_bag_(Sz, K, i_move(I, next), Ref, I)];
        _ ->
            []
    end.

do_del_obj_bag_(Sz, K, Res, Obj, #{name := Name} = Ref, I, Opts) ->
    case Res of
        {ok, <<K:Sz/binary, _:?BAG_CNT>> = RawKey, V} ->
            Key = element(keypos(Name), Obj),
            case decode_val(V, Key, Ref) of
                Obj ->
                    rdb_delete(Ref, RawKey, Opts);
                _ ->
                    do_del_obj_bag_(Sz, K, i_move(I, next), Obj, Ref, I, Opts)
            end;
        _ ->
            ok
    end.

delete_obj_set(_, _, _, not_found, _) ->
    ok;
delete_obj_set(Ref, _, _, RawKey, Opts) when is_binary(RawKey) ->
    rdb_delete(Ref, RawKey, Opts);
delete_obj_set(#{name := Name} = Ref, EncKey, Obj, _, Opts) ->
    case rdb_get(Ref, EncKey, []) of
        {ok, Bin} ->
            Key = element(keypos(Name), Obj),
            case decode_val(Bin, Key, Ref) of
                Obj ->
                    rdb_delete(Ref, EncKey, Opts);
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

rdb_put(R, K, V, Opts) ->
    rdb_put_(R, K, V, write_opts(R, Opts)).

rdb_put_(#{batch := Batch, cf_handle := CfH}, K, V, _Opts) ->
    rocksdb:batch_put(Batch, CfH, K, V);
rdb_put_(#{tx_handle := TxH, cf_handle := CfH}, K, V, _Opts) ->
    rocksdb:transaction_put(TxH, CfH, K, V);
rdb_put_(#{db_ref := DbRef, cf_handle := CfH}, K, V, WOpts) ->
    rocksdb:put(DbRef, CfH, K, V, WOpts).

rdb_get(R, K, Opts) ->
    rdb_get_(R, K, read_opts(R, Opts)).

rdb_get_(#{tx_handle := TxH, cf_handle := CfH}, K, _Opts) ->
    rocksdb:transaction_get(TxH, CfH, K);
rdb_get_(#{db_ref := DbRef, cf_handle := CfH}, K, ROpts) ->
    rocksdb:get(DbRef, CfH, K, ROpts).

rdb_delete(R, K, Opts) ->
    rdb_delete_(R, K, write_opts(R, Opts)).

rdb_delete_(#{batch := Batch, cf_handle := CfH}, K, _Opts) ->
    rocksdb:batch_delete(Batch, CfH, K);
rdb_delete_(#{tx_handle := TxH, cf_handle := CfH}, K, _Opts) ->
    rocksdb:transaction_delete(TxH, CfH, K);
rdb_delete_(#{db_ref := DbRef, cf_handle := CfH}, K, WOpts) ->
    rocksdb:delete(DbRef, CfH, K, WOpts).

rdb_iterator(R, Opts) ->
    rdb_iterator_(R, read_opts(R, Opts)).

rdb_iterator_(#{db_ref := DbRef, tx_handle := TxH, cf_handle := CfH}, ROpts) ->
    rocksdb:transaction_iterator(DbRef, TxH, CfH, ROpts);
rdb_iterator_(#{db_ref := DbRef, cf_handle := CfH}, ROpts) ->
    rocksdb:iterator(DbRef, CfH, ROpts).

rdb_merge(R, K, Op, Opts) ->
    rdb_merge_(R, K, term_to_binary(Op), write_opts(R, Opts)).

rdb_merge_(#{db_ref := DbRef, cf_handle := CfH}, K, Op, WOpts) ->
    rocksdb:merge(DbRef, CfH, K, Op, WOpts).

write_opts(#{write_opts := Os}, Opts) -> Os ++ Opts;
write_opts(_, Opts) -> 
    Opts.

read_opts(#{read_opts := Os}, Opts) -> Os ++ Opts;
read_opts(_, Opts) -> 
    Opts.

-define(EOT, '$end_of_table').

i_first(I, Ref) ->
    case i_move(I, first) of
        {ok, First, _} ->
            decode_key(First, Ref);
        _ ->
            ?EOT
    end.

i_last(I, Ref) ->
    case i_move(I, last) of
        {ok, Last, _} ->
            decode_key(Last, Ref);
        _ ->
            ?EOT
    end.

i_move(I, Where) ->
    rocksdb:iterator_move(I, Where).

i_next(I, Key, Ref) ->
    i_move_to_next(i_move(I, Key), I, Key, Ref).

i_prev(I, Key, Ref) ->
    case i_move(I, Key) of
        {ok, _, _} ->
            i_move_to_prev(i_move(I, prev), I, Key, Ref);
        {error, invalid_iterator} ->
            i_move_to_prev(i_move(I, last), I, Key, Ref)
    end.

i_move_to_next({ok, Key, _}, I, Key, Ref) ->
    i_move_to_next(i_move(I, next), I, Key, Ref);
i_move_to_next({ok, NextKey, _}, _, _, Ref) ->
    decode_key(NextKey, Ref);
i_move_to_next(_, _, _, _) ->
    ?EOT.

i_move_to_prev({ok, K, _}, _I, Key, Ref) when K < Key ->
    decode_key(K, Ref);
i_move_to_prev({ok, _, _}, I, Key, Ref) ->
    i_move_to_prev(i_move(I, prev), I, Key, Ref);
i_move_to_prev(_, _, _, _) ->
    ?EOT.
