%% -*- mode: erlang; erlang-indent-level: 4; indent-tabs-mode: nil -*-
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
%%  , activity   := Ongoing batch or transaction, if any (map())
%%  , attr_pos   := #{AttrName := Pos}
%%  , mode       := <Set to 'mnesia' for mnesia access flows>
%%  , properties := <Mnesia table props in map format>
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
        , alias_of/1
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
	, clear_table/1
        , batch_write/2  , batch_write/3
        , update_counter/3, update_counter/4
        , as_batch/2     , as_batch/3
        , get_batch/1
        , snapshot/1
        , release_snapshot/1
        , first/1        , first/2
        , next/2         , next/3
        , prev/2         , prev/3
        , last/1         , last/2
        , select/2       , select/3
        , select/1
        , fold/3         , fold/4      , fold/5
        , rdb_fold/4     , rdb_fold/5
        , write_info/3
        , read_info/2
        , read_info/1
        ]).

-export([ activity/3
        , current_context/0]).

-export([abort/1]).

%% Low-level access wrappers.
-export([ rdb_put/3,      rdb_put/4
        , rdb_get/2,      rdb_get/3
        , rdb_delete/2,   rdb_delete/3
        , rdb_iterator/1, rdb_iterator/2 ]).

%% For use of trace_runner
-export([ patterns/0 ]).

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
-include("mnesia_rocksdb_int.hrl").

-define(BATCH_REF_DUMMY, '$mrdb_batch_ref_dummy').

-type tab_name()  :: atom().
-type alias()     :: atom().
-type admin_tab() :: {admin, alias()}.
-type retainer()  :: {tab_name(), retainer, any()}.
-type index()     :: {tab_name(), index, any()}.
-type table() :: atom()
               | admin_tab()
               | index()
               | retainer().

-type inner() :: non_neg_integer().
-type outer() :: non_neg_integer().
-type retries() :: outer() | {inner(), outer()}.

%% activity type 'ets' makes no sense in this context
-type mnesia_activity_type() :: transaction
                              | sync_transaction
                              | async_dirty
                              | sync_dirty.

-type tx_options() :: #{ retries => retries()
                       , no_snapshot => boolean() }.
-type mrdb_activity_type() :: tx | {tx, tx_options()} | batch.

-type activity_type() :: mrdb_activity_type() | mnesia_activity_type().

-type key() :: any().
-type obj() :: tuple().
-type index_position() :: atom() | pos().

-type db_handle()  :: rocksdb:db_handle().
-type cf_handle()  :: rocksdb:cf_handle().
-type tx_handle()  :: rocksdb:transaction_handle().
-type itr_handle() :: rocksdb:itr_handle().
-type batch_handle() :: rocksdb:batch_handle().

-type tx_activity() :: #{ type    := 'tx'
                        , handle  := tx_handle()
                        , attempt := 'undefined' | retries() }.
-type batch_activity() :: #{ type   := 'batch'
                           , handle := batch_handle() }.
-type activity() :: tx_activity() | batch_activity().

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
                   , activity   => activity()
                   , _ => _}.

-type error() :: {error, any()}.

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
%% @private
%% Used by `trace_runner' to set up trace patterns.
%%
patterns() ->
    [{?MODULE, F, A, []} || {F, A} <- ?MODULE:module_info(exports),
                            F =/= module_info andalso
                            F =/= patterns].

%% @doc Create a snapshot of the database instance associated with the
%% table reference, table name or alias.
%%
%% Snapshots provide consistent read-only views over the entire state of the key-value store.
%% @end
-spec snapshot(alias() | ref_or_tab()) -> {ok, snapshot_handle()} | error().
snapshot(Name) when is_atom(Name) ->
    case mnesia_rocksdb_admin:get_ref(Name, error) of
        error ->
            snapshot(get_ref({admin, Name}));
        Ref ->
            snapshot(Ref)
    end;
snapshot(#{db_ref := DbRef}) ->
    rocksdb:snapshot(DbRef);
snapshot(_) ->
    {error, unknown}.

%% @doc release a snapshot created by {@link snapshot/1}.
-spec release_snapshot(snapshot_handle()) -> ok | error().
release_snapshot(SHandle) ->
    rocksdb:release_snapshot(SHandle).

%% @doc Run an activity (similar to {@link //mnesia/mnesia:activity/2}).
%%
%% Supported activity types are:
%% <ul>
%% <li> `transaction' - An optimistic `rocksdb' transaction</li>
%% <li> `{tx, TxOpts}' - A `rocksdb' transaction with sligth modifications</li>
%% <li> `batch' - A `rocksdb' batch operation</li>
%% </ul>
%% 
%% By default, transactions are combined with a snapshot with 1 retry.
%% The snapshot ensures that writes from concurrent transactions don't leak into the transaction context.
%% A transaction will be retried if it detects that the commit set conflicts with recent changes.
%% A mutex is used to ensure that only one of potentially conflicting `mrdb' transactions is run at a time.
%% The re-run transaction may still fail, if new transactions, or non-transaction writes interfere with
%% the commit set. It will then be re-run again, until the retry count is exhausted.
%%
%% For finer-grained retries, it's possible to set `retries => {Inner, Outer}'. Setting the retries to a
%% single number, `Retries', is analogous to `{0, Retries}`. Each outer retry requests a 'mutex lock' by
%% waiting in a FIFO queue. Once it receives the lock, it will try the activity once + as many retries
%% as specified by `Inner'. If these fail, the activity again goes to the FIFO queue (ending up last
%% in line) if there are outer retries remaining. When all retries are exhaused, the activity aborts
%% with `retry_limit'. Note that transactions, being optimistic, do not request a lock on the first
%% attempt, but only on outer retries (the first retry is always an outer retry).
%%
%% Valid `TxOpts' are `#{no_snapshot => boolean(), retries => retries()}'.
%%
%% To simplify code adaptation, `tx | transaction | sync_transaction' are synonyms, and
%% `batch | async_dirty | sync_dirty' are synonyms.
%% @end
-spec activity(activity_type(), alias(), fun( () -> Res )) -> Res.
activity(Type, Alias, F) ->
    #{db_ref := DbRef} = ensure_ref({admin, Alias}),
    Ctxt = case tx_type(Type) of
               {tx, TxOpts} ->
                   TxCtxt = new_tx_context(TxOpts, DbRef),
                   maps:merge(
                     #{ alias  => Alias
                      , db_ref => DbRef }, TxCtxt);
               batch ->
                   Batch = init_batch_ref(DbRef),
                   #{ activity => #{ type   => batch
                                   , handle => Batch }
                    , alias  => Alias
                    , db_ref => DbRef }
           end,
    do_activity(F, Alias, Ctxt).

do_activity(F, Alias, Ctxt) ->
    try try_f(F, Ctxt)
    catch
        throw:{?MODULE, busy} ->
            retry_activity(F, Alias, Ctxt)
    end.

try_f(F, Ctxt) ->
    try run_f(F, Ctxt) of
        Res ->
            commit_and_pop(Res)
    catch
        Cat:Err ->
            abort_and_pop(Cat, Err)
    end.

run_f(F, Ctxt) ->
    push_ctxt(Ctxt),
    F().

incr_attempt(0, {_,O}) when O > 0 ->
    {outer, {0,1}};
incr_attempt({I,O}, {Ri,Ro}) when is_integer(I), is_integer(O),
                                  is_integer(Ri), is_integer(Ro) ->
    if I < Ri -> {inner, {I+1, O}};
       O < Ro -> {outer, {0, O+1}};
       true ->
            error
    end;
incr_attempt(_, _) ->
    error.
                    
retry_activity(F, Alias, #{activity := #{ type := Type
                                        , attempt := A
                                        , retries := R} = Act} = Ctxt) ->
    case incr_attempt(A, R) of
        {RetryCtxt, A1} ->
            Act1 = Act#{attempt := A1},
            Ctxt1 = Ctxt#{activity := Act1},
            try retry_activity_(RetryCtxt, F, Alias, restart_ctxt(Ctxt1))
            catch
                throw:{?MODULE, busy} ->
                    retry_activity(F, Alias, Ctxt1)
            end;
        error ->
            return_abort(Type, error, retry_limit)
    end.

retry_activity_(inner, F, Alias, Ctxt) ->
    mrdb_stats:incr(Alias, inner_retries, 1),
    try_f(F, Ctxt);
retry_activity_(outer, F, Alias, Ctxt) ->
    mrdb_stats:incr(Alias, outer_retries, 1),
    mrdb_mutex:do(Alias, fun() -> try_f(F, Ctxt) end).

restart_ctxt(#{ activity := #{type := tx} = Act, db_ref := DbRef } = C) ->
    {ok, TxH} = rdb_transaction(DbRef, []),
    Act1 = Act#{handle := TxH},
    C1 = C#{ activity := Act1 },
    case maps:is_key(snapshot, C) of
        true ->
            {ok, SH} = rocksdb:snapshot(DbRef),
            C1#{snapshot := SH};
        false ->
            C1
    end.

ctxt() -> {?MODULE, ctxt}.

push_ctxt(C) ->
    C1 = case get_ctxt() of
             undefined -> [C];
             C0        -> [C|C0]
         end,
    put_ctxt(C1),
    ok.

pop_ctxt() ->
    case get_ctxt() of
        undefined -> error(no_ctxt);
        [C]       -> erase_ctxt() , maybe_release_snapshot(C);
        [H|T]     -> put_ctxt(T), maybe_release_snapshot(H)
    end.

maybe_release_snapshot(#{snapshot := SH} = C) ->
    try rocksdb:release_snapshot(SH)
    catch
        error:_ ->
            ok
    end,
    C;
maybe_release_snapshot(C) ->
    C.

current_context() ->
    case pdict_get(ctxt()) of
        [C|_] ->
            C;
        undefined ->
            undefined
    end.

tx_type(T) ->
    case T of
        _ when T==batch;
               T==async_dirty;
               T==sync_dirty         -> batch;
        _ when T==tx;
               T==transaction;
               T==sync_transaction   -> {tx, apply_tx_opts(#{})};
        {tx, Opts} when is_map(Opts) -> {tx, apply_tx_opts(Opts)};
        _ -> abort(invalid_activity_type)
    end.

default_tx_opts() ->
    #{ retries => ?DEFAULT_RETRIES
     , no_snapshot => false }.

apply_tx_opts(Opts0) when is_map(Opts0) ->
    check_tx_opts(maps:merge(default_tx_opts(), Opts0)).

check_tx_opts(Opts) ->
    case maps:without([no_snapshot, retries], Opts) of
        Other when map_size(Other) > 0 ->
            abort({invalid_tx_opts, maps:keys(Other)});
        _ ->
            check_retries(check_nosnap(Opts))
    end.

check_retries(#{retries := Retries} = Opts) ->
    case Retries of
        _ when is_integer(Retries), Retries >= 0 ->
            Opts#{retries := {0, Retries}};
        {Inner, Outer} when is_integer(Inner), is_integer(Outer),
                            Inner >= 0, Outer >= 0 ->
            Opts;
        _ ->
            error({invalid_tx_option, {retries, Retries}})
    end.

check_nosnap(#{no_snapshot := NoSnap} = Opts) ->
    if is_boolean(NoSnap) -> Opts;
       true -> error({invalid_tx_option, {no_snapshot, NoSnap}})
    end.

new_tx_context(Opts, DbRef) ->
    maybe_snapshot(create_tx(Opts, DbRef), DbRef).

create_tx(Opts, DbRef) ->
    {ok, TxH} = rdb_transaction(DbRef, []),
    Opts#{activity => maps:merge(Opts, #{ type    => tx
                                        , handle  => TxH
                                        , attempt => 0 })}.

maybe_snapshot(#{no_snapshot := NoSnap} = Opts, DbRef) ->
    case NoSnap of
        false ->
            {ok, SH} = rocksdb:snapshot(DbRef),
            Opts#{snapshot => SH};
        _ ->
            Opts
    end.

commit_and_pop(Res) ->
    #{activity := #{type := Type, handle := H} = A} = C = current_context(),
    case Type of
        tx ->
            case rdb_transaction_commit_and_pop(H) of
                ok ->
                    Res;
                {error, {error, "Resource busy" ++ _ = Busy}} ->
                    case A of
                        #{retries := {I,O}} when I > 0; O > 0 ->
                            throw({?MODULE, busy});
                        _ ->
                            error({error, Busy})
                    end;
                {error, Reason} ->
                    error(Reason)
            end;
        batch ->
            case rdb_write_batch_and_pop(H, C) of
                ok -> Res;
                Other ->
                    Other
            end
    end.

-spec abort_and_pop(atom(), any()) -> no_return().
abort_and_pop(Cat, Err) ->
    %% We can pop the context right away, since there is no
    %% complex failure handling (like retry-on-busy) for rollback.
    #{activity := #{type := Type, handle := H}} = pop_ctxt(),
    case Type of
        tx    -> ok = rdb_transaction_rollback(H);
        batch -> ok = release_batches(H)
    end,
    return_abort(Type, Cat, Err).

-spec return_abort(batch | tx, atom(), any()) -> no_return().
return_abort(batch, Cat, Err) ->
    re_throw(Cat, Err);
return_abort(tx, Cat, Err) ->
    case mnesia_compatible_aborts() of
	true ->
	    %% Mnesia always captures stack traces, but this could actually become a
	    %% performance issue in some cases (generally, it's better not to lazily
	    %% produce stack traces.) Since we want to pushe the option checking for
	    %% mnesia-abort-style compatibility to AFTER detecting an abort, we don't
	    %% order a stack trace initially, and instead insert an empty list.
	    %% (The exact stack trace wouldn't be the same anyway.)
	    Err1 =
		case Cat of
		    error -> {fix_error(Err), []};
		    exit  -> fix_error(Err);
		    throw -> {throw, Err}
		end,
	    exit({aborted, Err1});
	false ->
	    re_throw(Cat, Err)
    end.

-spec re_throw(atom(), any()) -> no_return().
re_throw(Cat, Err) ->
    case Cat of
	error -> error(Err);
	exit  -> exit(Err);
	throw -> throw(Err)
    end.


mnesia_compatible_aborts() ->
    mnesia_rocksdb_admin:get_cached_env(mnesia_compatible_aborts, false).

fix_error({aborted, Err}) ->
    Err;
fix_error(Err) ->
    Err.

rdb_transaction(DbRef, Opts) ->
    rocksdb:transaction(DbRef, Opts).

rdb_transaction_commit_and_pop(H) ->
    try rdb_transaction_commit(H) 
    after
        pop_ctxt()
    end.

rdb_transaction_commit(H) ->
    rocksdb:transaction_commit(H).

rdb_transaction_rollback(H) ->
    rocksdb:transaction_rollback(H).

rdb_batch() ->
    rocksdb:batch().

rdb_write_batch_and_pop(BatchRef, C) ->
    %% TODO: derive write_opts(R)
    try write_batches(BatchRef, write_opts(C, []))
    after
        pop_ctxt()
    end.

rdb_release_batch(?BATCH_REF_DUMMY) ->
    ok;
rdb_release_batch(H) ->
    rocksdb:release_batch(H).

%% @doc Aborts an ongoing {@link activity/2}
abort(Reason) ->
    case mnesia_compatible_aborts() of
	true ->
	    mnesia:abort(Reason);
	false ->
	    erlang:error({mrdb_abort, Reason})
    end.

-spec new_tx(table() | db_ref()) -> db_ref().
new_tx(Tab) ->
    new_tx(Tab, []).

-spec new_tx(ref_or_tab(), write_options()) -> db_ref().
new_tx(#{activity := _}, _) ->
    abort(nested_context);
new_tx(Tab, Opts) ->
    #{db_ref := DbRef} = R = ensure_ref(Tab),
    {ok, TxH} = rdb_transaction(DbRef, write_opts(R, Opts)),
    R#{activity => #{type => tx, handle => TxH, attempt => 0}}.

-spec tx_ref(ref_or_tab() | db_ref() | db_ref(), tx_handle()) -> db_ref().
tx_ref(Tab, TxH) ->
    case ensure_ref(Tab) of
        #{activity := #{type := tx, handle := TxH}} = R ->
            R;
        #{activity := #{type := tx, handle := OtherTxH}} ->
            error({tx_handle_conflict, OtherTxH});
        R ->
            R#{activity => #{type => tx, handle => TxH, attempt => 0}}
    end.

-spec tx_commit(tx_handle() | db_ref()) -> ok.
tx_commit(#{activity := #{type := tx, handle := TxH}}) ->
    rdb_transaction_commit(TxH);
tx_commit(TxH) ->
    rdb_transaction_commit(TxH).

-spec get_ref(table()) -> db_ref().
get_ref(Tab) ->
    mnesia_rocksdb_admin:get_ref(Tab).

-spec ensure_ref(ref_or_tab()) -> db_ref().
ensure_ref(#{activity := _} = R) -> R;
ensure_ref(Ref) when is_map(Ref) ->
    maybe_tx_ctxt(get_ctxt(), Ref);
ensure_ref(Other) ->
    maybe_tx_ctxt(get_ctxt(), get_ref(Other)).

get_ctxt() ->
    pdict_get(ctxt()).

put_ctxt(C) ->
    pdict_put(ctxt(), C).

erase_ctxt() ->
    pdict_erase(ctxt()).

ensure_ref(Ref, R) when is_map(Ref) ->
    inherit_ctxt(Ref, R);
ensure_ref(Other, R) ->
    inherit_ctxt(get_ref(Other), R).

maybe_tx_ctxt(undefined,            R) -> R;
maybe_tx_ctxt(_, #{activity := _} = R) -> R;
maybe_tx_ctxt([#{activity := #{type := Type} = A} = C|_], R) ->
    case Type of
        tx    ->
            maps:merge(maps:with([snapshot], C), R#{activity => A});
        _ ->
            R#{activity => A}
    end.

inherit_ctxt(Ref, R) ->
    maps:merge(Ref, maps:with([snapshot, activity], R)).

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
validate_obj(Obj, #{attr_pos := AP,
                    properties := #{record_name := RN}} = Ref)
  when is_tuple(Obj) ->
    Arity = map_size(AP) + 1,
    case {element(1, Obj), tuple_size(Obj)} of
        {RN, Arity} ->
            validate_obj_type(Obj, Ref);
        _ ->
            abort(badarg)
    end;
validate_obj({{_,_}} = Obj, #{name := {_,index,{_,ordered}}}) ->
    Obj;
validate_obj(_, _) ->
    abort(badarg).

validate_obj_type(Obj, Ref) ->
    case mnesia_rocksdb_lib:valid_obj_type(Ref, Obj) of
        true -> Obj;
        false ->
            abort({bad_type, Obj})
    end.

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

%% @doc Returns the alias of a given table or table reference.
-spec alias_of(ref_or_tab()) -> alias().
alias_of(Tab) ->
    #{alias := Alias} = ensure_ref(Tab),
    Alias.

%% @doc Creates a `rocksdb' batch context and executes the fun `F' in it.
%%
%% %% Rocksdb batches aren't tied to a specific DbRef until written.
%% This can cause surprising problems if we're juggling multiple
%% rocksdb instances (as we do if we have standalone tables).
%% At the time of writing, all objects end up in the DbRef the batch
%% is written to, albeit not necessarily in the intended column family.
%% This will probably change, but no failure mode is really acceptable.
%% The code below ensures that separate batches are created for each
%% DbRef, under a unique reference stored in the pdict. When writing,
%% all batches are written separately to the corresponding DbRef,
%% and when releasing, all batches are released. This will not ensure
%% atomicity, but there is no way in rocksdb to achieve atomicity
%% across db instances. At least, data should end up where you expect.
%% 
%% @end
-spec as_batch(ref_or_tab(), fun( (db_ref()) -> Res )) -> Res.
as_batch(Tab, F) ->
    as_batch(Tab, F, []).

%% @doc as {@link as_batch/2}, but with the ability to pass `Opts' to `rocksdb:write_batch/2'
as_batch(Tab, F, Opts) when is_function(F, 1), is_list(Opts) ->
    as_batch_(ensure_ref(Tab), F, Opts).

as_batch_(#{activity := #{type := batch}} = R, F, _) ->
    %% If already inside a batch, add to that batch (batches don't seem to nest)
    F(R);
as_batch_(#{activity := #{type := tx}} = R, F, _) ->
    %% If already inside a tx, we need to respect the tx context
    F(R);
as_batch_(#{db_ref := DbRef} = R, F, Opts) ->
    BatchRef = get_batch_(DbRef),
    try F(R#{activity => #{type => batch, handle => BatchRef}}) of
        Res ->
            case write_batches(BatchRef, write_opts(R, Opts)) of
                ok ->
                    Res;
                {error, Reason} ->
                    abort(Reason)
            end
    after
        release_batches(BatchRef)
    end.


get_batch(#{db_ref := DbRef, batch := BatchRef}) ->
    try {ok, get_batch_(DbRef, BatchRef)}
    catch
        error:Reason ->
            {error, Reason}
    end;
get_batch(_) ->
    {error, badarg}.

init_batch_ref(DbRef) ->
    Ref = make_ref(),
    pdict_put({mrdb_batch, Ref}, #{DbRef => ?BATCH_REF_DUMMY}),
    Ref.

get_batch_(DbRef) -> Ref = make_ref(), {ok, Batch} = rdb_batch(),
    pdict_put({mrdb_batch, Ref}, #{DbRef => Batch}), Ref.

get_batch_(DbRef, BatchRef) ->
    Key = batch_ref_key(BatchRef),
    case pdict_get(Key) of
        undefined ->
            error(stale_batch_ref);
        #{DbRef := Batch} when Batch =/= ?BATCH_REF_DUMMY ->
            Batch;
        Map ->
            {ok, Batch} = rdb_batch(),
            pdict_put(Key, Map#{DbRef => Batch}),
            Batch
    end.


pdict_get(K) -> get(K).

pdict_put(K, V) -> put(K, V).

pdict_erase(K) -> erase(K).

batch_ref_key(BatchRef) ->
    {mrdb_batch, BatchRef}.

write_batches(BatchRef, Opts) ->
    Key = batch_ref_key(BatchRef),
    case pdict_get(Key) of
        undefined ->
            error(stale_batch_ref);
        Map ->
            %% Some added complication since we deal with potentially
            %% multiple DbRefs, and will want to return errors.
            pdict_erase(Key),
            ret_batch_write_acc(
              maps:fold(
                fun(_, ?BATCH_REF_DUMMY, Acc) ->
                        Acc;
                    (DbRef, Batch, Acc) ->
                        case rocksdb:write_batch(DbRef, Batch, Opts) of
                            ok ->
                                Acc;
                            {error,E} ->
                                acc_batch_write_error(E, DbRef, Acc)
                        end
                end, ok, Map))
    end.

ret_batch_write_acc(ok) ->
    ok;
ret_batch_write_acc(Es) when is_list(Es) ->
    {error, lists:reverse(Es)}.

acc_batch_write_error(E, DbRef, ok) ->
    [{DbRef, E}];

acc_batch_write_error(E, DbRef, Es) when is_list(Es) ->
    [{DbRef, E}|Es].

release_batches(BatchRef) ->
    Key = batch_ref_key(BatchRef),
    case pdict_get(Key) of
        undefined ->
            ok;
        Map ->
            pdict_erase(Key),
            maps_foreach(
              fun(_, Batch) ->
                      rdb_release_batch(Batch)
              end, Map),
            ok
    end.

%% maps:foreach/2 doesn't exist in OTP 22 ...
maps_foreach(F, Map) ->
    I = maps:iterator(Map),
    maps_foreach_(F, maps:next(I)).

maps_foreach_(F, {K, V, I}) ->
    F(K, V),
    maps_foreach_(F, maps:next(I));
maps_foreach_(_, none) ->
    ok.

%% Corresponding to `rocksdb:write()`, renamed to avoid confusion with
%% `mnesia:write()`, which has entirely different semantics.
%%
batch_write(Tab, L) when is_list(L) ->
    batch_write(Tab, L, []).

batch_write(Tab, L, Opts) ->
    batch_write_(ensure_ref(Tab), L, Opts).

batch_write_(Ref, L, Opts) ->
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

clear_table(Tab) ->
    match_delete(Tab, '_').

match_delete(Tab, Pat) ->
    Ref = ensure_ref(Tab),
    case Pat of
	'_' ->
	    delete_whole_table(Ref);
	_ ->
	    MatchSpec = [{Pat, [], [true]}],
	    as_batch(Ref, fun(R) ->
				  %% call select() with AccKeys=true, returning [{Key, _}]
				  match_delete_(mrdb_select:select(Ref, MatchSpec, true, 30), R)
			  end)
    end.

match_delete_({L, Cont}, Ref) ->
    [rdb_delete(Ref, K, []) || {K,_} <- L],
    match_delete_(select(Cont), Ref);
match_delete_('$end_of_table', _) ->
    ok.

delete_whole_table(Ref) ->
    mnesia_rocksdb_admin:clear_table(Ref).
%%     with_rdb_iterator(
%%       Ref,
%%       fun(I) ->
%% 	      case i_move(I, first) of
%% 		  {ok, First, _} ->
%% 		      {ok, Last, _} = i_move(I, last),
%% 		      %% Pad Last, since delete_range excludes EndKey
%% 		      delete_range(Ref, First, <<Last/binary, 0>>);
%% 		  _ ->
%% 		      ok
%% 	      end
%%       end).

%% N.B. rocksdb:delete_range/5 isn't implemented yet
%% delete_range(#{db_ref := DbRef, cf_handle := CfH} = R, Start, End) ->
%%     rocksdb:delete_range(DbRef, CfH, Start, End, write_opts(R, [])).

fold(Tab, Fun, Acc) ->
    fold(Tab, Fun, Acc, [{'_', [], ['$_']}]).

fold(Tab, Fun, Acc, MatchSpec) ->
    fold(Tab, Fun, Acc, MatchSpec, infinity).

fold(Tab, Fun, Acc, MatchSpec, Limit) ->
    true = valid_limit(Limit),
    mrdb_select:fold(ensure_ref(Tab), Fun, Acc, MatchSpec, Limit).

rdb_fold(Tab, Fun, Acc, Prefix) when is_function(Fun, 3)
                                   , is_binary(Prefix) ->
    rdb_fold(Tab, Fun, Acc, Prefix, infinity).

rdb_fold(Tab, Fun, Acc, Prefix, Limit) when is_function(Fun, 3)
                                          , is_binary(Prefix) ->
    true = valid_limit(Limit),
    mrdb_select:rdb_fold(ensure_ref(Tab), Fun, Acc, Prefix, Limit).

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

read_info(Tab) ->
    mnesia_rocksdb_admin:read_info(ensure_ref(Tab)).

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

read_direct_info_(R, memory, _Def) ->
    get_property(R, <<"rocksdb.total-sst-files-size">>, integer, 0);
read_direct_info_(R, size, _Def) ->
    get_property(R, <<"rocksdb.estimate-num-keys">>, integer, 0).

-dialyzer({nowarn_function, get_property/4}).
get_property(#{db_ref := R, cf_handle := CfH}, Prop, Type, Default) ->
    case rocksdb:get_property(R, CfH, Prop) of
        {error, _} ->
            Default;
        {ok, Res} ->
              case Type of
%%                  boolean -> rocksdb_boolean(Res);
%%                  string  -> Res;
                  %% get_property/3 is incorrectly typed as returning string()
                  integer -> binary_to_integer(Res)
              end
   end.

%%rocksdb_boolean(<<"1">>) -> true;
%%rocksdb_boolean(<<"0">>) -> false.

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
            batch_write(Ref, [{delete, K} || K <- Found], Opts)
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

rdb_put(R, K, V) -> rdb_put(R, K, V, []).
rdb_put(R, K, V, Opts) ->
    rdb_put_(R, K, V, write_opts(R, Opts)).

rdb_put_(#{activity  := #{type := batch, handle := BatchRef},
           db_ref    := DbRef,
           cf_handle := CfH}, K, V, _Opts) ->
    Batch = get_batch_(DbRef, BatchRef),
    rocksdb:batch_put(Batch, CfH, K, V);
rdb_put_(#{activity := #{type := tx, handle := TxH}, cf_handle := CfH}, K, V, _Opts) ->
    rocksdb:transaction_put(TxH, CfH, K, V);
rdb_put_(#{db_ref := DbRef, cf_handle := CfH}, K, V, WOpts) ->
    rocksdb:put(DbRef, CfH, K, V, WOpts).

rdb_get(R, K) -> rdb_get(R, K, []).
rdb_get(R, K, Opts) ->
    rdb_get_(R, K, read_opts(R, Opts)).

rdb_get_(#{activity := #{type := tx, handle := TxH}, cf_handle := CfH, snapshot := SH}, K, _Opts) ->
    rocksdb:transaction_get(TxH, CfH, K, [{snapshot, SH}]);
rdb_get_(#{activity := #{type := tx, handle := TxH}, cf_handle := CfH}, K, _Opts) ->
    rocksdb:transaction_get(TxH, CfH, K, []);
rdb_get_(#{db_ref := DbRef, cf_handle := CfH}, K, ROpts) ->
    rocksdb:get(DbRef, CfH, K, ROpts).

rdb_delete(R, K) -> rdb_delete(R, K, []).
rdb_delete(R, K, Opts) ->
    rdb_delete_(R, K, write_opts(R, Opts)).

rdb_delete_(#{activity  := #{type := batch, handle := BatchRef},
              db_ref    := DbRef,
              cf_handle := CfH}, K, _Opts) ->
    Batch = get_batch_(DbRef, BatchRef),
    rocksdb:batch_delete(Batch, CfH, K);
rdb_delete_(#{activity := #{type := tx, handle := TxH}, cf_handle := CfH}, K, _Opts) ->
    rocksdb:transaction_delete(TxH, CfH, K);
rdb_delete_(#{db_ref := DbRef, cf_handle := CfH}, K, WOpts) ->
    rocksdb:delete(DbRef, CfH, K, WOpts).

rdb_iterator(R) -> rdb_iterator(R, []).
rdb_iterator(R, Opts) ->
    rdb_iterator_(R, read_opts(R, Opts)).

rdb_iterator_(#{db_ref := DbRef, cf_handle := CfH, activity := #{type := tx, handle := TxH}}, ROpts) ->
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
