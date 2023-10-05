

# Module mrdb #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

Mid-level access API for Mnesia-managed rocksdb tables.

<a name="description"></a>

## Description ##

This module implements access functions for the mnesia_rocksdb
backend plugin. The functions are designed to also support
direct access to rocksdb with little overhead. Such direct
access will maintain existing indexes, but not support
replication.

Each table has a metadata structure stored as a persistent
term for fast access. The structure of the metadata is as
follows:

```
  #{ name       := <Logical table name>
   , db_ref     := <Rocksdb database Ref>
   , cf_handle  := <Rocksdb column family handle>
   , activity   := Ongoing batch or transaction, if any (map())
   , attr_pos   := #{AttrName := Pos}
   , mode       := <Set to 'mnesia' for mnesia access flows>
   , properties := <Mnesia table props in map format>
   , type       := column_family | standalone
   }
```

Helper functions like `as_batch(Ref, fun(R) -> ... end)` and
`with_iterator(Ref, fun(I) -> ... end)` add some extra
convenience on top of the `rocksdb` API.

Note that no automatic provision exists to manage concurrent
updates via mnesia AND direct access to this API. It's advisable
to use ONE primary mode of access. If replication is used,
the mnesia API will support this, but direct `mrdb` updates will
not be replicated.
<a name="types"></a>

## Data Types ##




### <a name="type-activity">activity()</a> ###


<pre><code>
activity() = <a href="#type-tx_activity">tx_activity()</a> | <a href="#type-batch_activity">batch_activity()</a>
</code></pre>




### <a name="type-activity_type">activity_type()</a> ###


<pre><code>
activity_type() = <a href="#type-mrdb_activity_type">mrdb_activity_type()</a> | <a href="#type-mnesia_activity_type">mnesia_activity_type()</a>
</code></pre>




### <a name="type-admin_tab">admin_tab()</a> ###


<pre><code>
admin_tab() = {admin, <a href="#type-alias">alias()</a>}
</code></pre>




### <a name="type-alias">alias()</a> ###


<pre><code>
alias() = atom()
</code></pre>




### <a name="type-attr_pos">attr_pos()</a> ###


<pre><code>
attr_pos() = #{atom() =&gt; <a href="#type-pos">pos()</a>}
</code></pre>




### <a name="type-batch_activity">batch_activity()</a> ###


<pre><code>
batch_activity() = #{type =&gt; batch, handle =&gt; <a href="#type-batch_handle">batch_handle()</a>}
</code></pre>




### <a name="type-batch_handle">batch_handle()</a> ###


<pre><code>
batch_handle() = <a href="/home/uwiger/ae/mnesia_rocksdb/_build/default/lib/rocksdb/doc/rocksdb.md#type-batch_handle">rocksdb:batch_handle()</a>
</code></pre>




### <a name="type-cf_handle">cf_handle()</a> ###


<pre><code>
cf_handle() = <a href="/home/uwiger/ae/mnesia_rocksdb/_build/default/lib/rocksdb/doc/rocksdb.md#type-cf_handle">rocksdb:cf_handle()</a>
</code></pre>




### <a name="type-db_handle">db_handle()</a> ###


<pre><code>
db_handle() = <a href="/home/uwiger/ae/mnesia_rocksdb/_build/default/lib/rocksdb/doc/rocksdb.md#type-db_handle">rocksdb:db_handle()</a>
</code></pre>




### <a name="type-db_ref">db_ref()</a> ###


<pre><code>
db_ref() = #{name =&gt; <a href="#type-table">table()</a>, alias =&gt; atom(), vsn =&gt; non_neg_integer(), db_ref =&gt; <a href="#type-db_handle">db_handle()</a>, cf_handle =&gt; <a href="#type-cf_handle">cf_handle()</a>, semantics =&gt; <a href="#type-semantics">semantics()</a>, encoding =&gt; <a href="#type-encoding">encoding()</a>, attr_pos =&gt; <a href="#type-attr_pos">attr_pos()</a>, type =&gt; column_family | standalone, status =&gt; open | closed | pre_existing, properties =&gt; <a href="#type-properties">properties()</a>, mode =&gt; mnesia, ix_vals_f =&gt; fun((tuple()) -&gt; [any()]), activity =&gt; <a href="#type-activity">activity()</a>, term() =&gt; term()}
</code></pre>




### <a name="type-encoding">encoding()</a> ###


<pre><code>
encoding() = raw | sext | term | {<a href="#type-key_encoding">key_encoding()</a>, <a href="#type-val_encoding">val_encoding()</a>}
</code></pre>




### <a name="type-error">error()</a> ###


<pre><code>
error() = {error, any()}
</code></pre>




### <a name="type-index">index()</a> ###


<pre><code>
index() = {<a href="#type-tab_name">tab_name()</a>, index, any()}
</code></pre>




### <a name="type-index_position">index_position()</a> ###


<pre><code>
index_position() = atom() | <a href="#type-pos">pos()</a>
</code></pre>




### <a name="type-inner">inner()</a> ###


<pre><code>
inner() = non_neg_integer()
</code></pre>




### <a name="type-iterator_action">iterator_action()</a> ###


<pre><code>
iterator_action() = first | last | next | prev | binary() | {seek, binary()} | {seek_for_prev, binary()}
</code></pre>




### <a name="type-itr_handle">itr_handle()</a> ###


<pre><code>
itr_handle() = <a href="/home/uwiger/ae/mnesia_rocksdb/_build/default/lib/rocksdb/doc/rocksdb.md#type-itr_handle">rocksdb:itr_handle()</a>
</code></pre>




### <a name="type-key">key()</a> ###


<pre><code>
key() = any()
</code></pre>




### <a name="type-key_encoding">key_encoding()</a> ###


<pre><code>
key_encoding() = raw | sext | term
</code></pre>




### <a name="type-mnesia_activity_type">mnesia_activity_type()</a> ###


<pre><code>
mnesia_activity_type() = transaction | sync_transaction | async_dirty | sync_dirty
</code></pre>




### <a name="type-mrdb_activity_type">mrdb_activity_type()</a> ###


<pre><code>
mrdb_activity_type() = tx | {tx, <a href="#type-tx_options">tx_options()</a>} | batch
</code></pre>




### <a name="type-mrdb_iterator">mrdb_iterator()</a> ###


<pre><code>
mrdb_iterator() = #mrdb_iter{i = <a href="#type-itr_handle">itr_handle()</a>, ref = <a href="#type-db_ref">db_ref()</a>}
</code></pre>




### <a name="type-obj">obj()</a> ###


<pre><code>
obj() = tuple()
</code></pre>




### <a name="type-outer">outer()</a> ###


<pre><code>
outer() = non_neg_integer()
</code></pre>




### <a name="type-pos">pos()</a> ###


<pre><code>
pos() = non_neg_integer()
</code></pre>




### <a name="type-properties">properties()</a> ###


<pre><code>
properties() = #{record_name =&gt; atom(), attributes =&gt; [atom()], index =&gt; [{<a href="#type-pos">pos()</a>, bag | ordered}]}
</code></pre>




### <a name="type-read_options">read_options()</a> ###


<pre><code>
read_options() = [{verify_checksums, boolean()} | {fill_cache, boolean()} | {iterate_upper_bound, binary()} | {iterate_lower_bound, binary()} | {tailing, boolean()} | {total_order_seek, boolean()} | {prefix_same_as_start, boolean()} | {snapshot, <a href="#type-snapshot_handle">snapshot_handle()</a>}]
</code></pre>




### <a name="type-ref_or_tab">ref_or_tab()</a> ###


<pre><code>
ref_or_tab() = <a href="#type-table">table()</a> | <a href="#type-db_ref">db_ref()</a>
</code></pre>




### <a name="type-retainer">retainer()</a> ###


<pre><code>
retainer() = {<a href="#type-tab_name">tab_name()</a>, retainer, any()}
</code></pre>




### <a name="type-retries">retries()</a> ###


<pre><code>
retries() = <a href="#type-outer">outer()</a> | {<a href="#type-inner">inner()</a>, <a href="#type-outer">outer()</a>}
</code></pre>




### <a name="type-semantics">semantics()</a> ###


<pre><code>
semantics() = bag | set
</code></pre>




### <a name="type-snapshot_handle">snapshot_handle()</a> ###


<pre><code>
snapshot_handle() = <a href="/home/uwiger/ae/mnesia_rocksdb/_build/default/lib/rocksdb/doc/rocksdb.md#type-snapshot_handle">rocksdb:snapshot_handle()</a>
</code></pre>




### <a name="type-tab_name">tab_name()</a> ###


<pre><code>
tab_name() = atom()
</code></pre>




### <a name="type-table">table()</a> ###


<pre><code>
table() = atom() | <a href="#type-admin_tab">admin_tab()</a> | <a href="#type-index">index()</a> | <a href="#type-retainer">retainer()</a>
</code></pre>




### <a name="type-tx_activity">tx_activity()</a> ###


<pre><code>
tx_activity() = #{type =&gt; tx, handle =&gt; <a href="#type-tx_handle">tx_handle()</a>, attempt =&gt; undefined | <a href="#type-retries">retries()</a>}
</code></pre>




### <a name="type-tx_handle">tx_handle()</a> ###


<pre><code>
tx_handle() = <a href="/home/uwiger/ae/mnesia_rocksdb/_build/default/lib/rocksdb/doc/rocksdb.md#type-transaction_handle">rocksdb:transaction_handle()</a>
</code></pre>




### <a name="type-tx_options">tx_options()</a> ###


<pre><code>
tx_options() = #{retries =&gt; <a href="#type-retries">retries()</a>, no_snapshot =&gt; boolean()}
</code></pre>




### <a name="type-val_encoding">val_encoding()</a> ###


<pre><code>
val_encoding() = {value | object, term | raw} | raw
</code></pre>




### <a name="type-write_options">write_options()</a> ###


<pre><code>
write_options() = [{sync, boolean()} | {disable_wal, boolean()} | {ignore_missing_column_families, boolean()} | {no_slowdown, boolean()} | {low_pri, boolean()}]
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#abort-1">abort/1</a></td><td>Aborts an ongoing <a docgen-rel="seemfa" docgen-href="#activity/2" href="#activity-2"><code>activity/2</code></a></td></tr><tr><td valign="top"><a href="#activity-3">activity/3</a></td><td>Run an activity (similar to <a docgen-rel="seemfa" docgen-href="mnesia:mnesia#activity/2" href="http://www.erlang.org/doc/man/mnesia.html#activity-2"><code>//mnesia/mnesia:activity/2</code></a>).</td></tr><tr><td valign="top"><a href="#alias_of-1">alias_of/1</a></td><td>Returns the alias of a given table or table reference.</td></tr><tr><td valign="top"><a href="#as_batch-2">as_batch/2</a></td><td>Creates a <code>rocksdb</code> batch context and executes the fun <code>F</code> in it.</td></tr><tr><td valign="top"><a href="#as_batch-3">as_batch/3</a></td><td>as <a docgen-rel="seemfa" docgen-href="#as_batch/2" href="#as_batch-2"><code>as_batch/2</code></a>, but with the ability to pass <code>Opts</code> to <code>rocksdb:write_batch/2</code></td></tr><tr><td valign="top"><a href="#batch_write-2">batch_write/2</a></td><td></td></tr><tr><td valign="top"><a href="#batch_write-3">batch_write/3</a></td><td></td></tr><tr><td valign="top"><a href="#clear_table-1">clear_table/1</a></td><td></td></tr><tr><td valign="top"><a href="#current_context-0">current_context/0</a></td><td></td></tr><tr><td valign="top"><a href="#delete-2">delete/2</a></td><td></td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete_object-2">delete_object/2</a></td><td></td></tr><tr><td valign="top"><a href="#delete_object-3">delete_object/3</a></td><td></td></tr><tr><td valign="top"><a href="#ensure_ref-1">ensure_ref/1</a></td><td></td></tr><tr><td valign="top"><a href="#ensure_ref-2">ensure_ref/2</a></td><td></td></tr><tr><td valign="top"><a href="#first-1">first/1</a></td><td></td></tr><tr><td valign="top"><a href="#first-2">first/2</a></td><td></td></tr><tr><td valign="top"><a href="#fold-3">fold/3</a></td><td></td></tr><tr><td valign="top"><a href="#fold-4">fold/4</a></td><td></td></tr><tr><td valign="top"><a href="#fold-5">fold/5</a></td><td></td></tr><tr><td valign="top"><a href="#get_batch-1">get_batch/1</a></td><td></td></tr><tr><td valign="top"><a href="#get_ref-1">get_ref/1</a></td><td></td></tr><tr><td valign="top"><a href="#index_read-3">index_read/3</a></td><td></td></tr><tr><td valign="top"><a href="#insert-2">insert/2</a></td><td></td></tr><tr><td valign="top"><a href="#insert-3">insert/3</a></td><td></td></tr><tr><td valign="top"><a href="#iterator-1">iterator/1</a></td><td></td></tr><tr><td valign="top"><a href="#iterator-2">iterator/2</a></td><td></td></tr><tr><td valign="top"><a href="#iterator_close-1">iterator_close/1</a></td><td></td></tr><tr><td valign="top"><a href="#iterator_move-2">iterator_move/2</a></td><td></td></tr><tr><td valign="top"><a href="#last-1">last/1</a></td><td></td></tr><tr><td valign="top"><a href="#last-2">last/2</a></td><td></td></tr><tr><td valign="top"><a href="#match_delete-2">match_delete/2</a></td><td></td></tr><tr><td valign="top"><a href="#new_tx-1">new_tx/1</a></td><td></td></tr><tr><td valign="top"><a href="#new_tx-2">new_tx/2</a></td><td></td></tr><tr><td valign="top"><a href="#next-2">next/2</a></td><td></td></tr><tr><td valign="top"><a href="#next-3">next/3</a></td><td></td></tr><tr><td valign="top"><a href="#prev-2">prev/2</a></td><td></td></tr><tr><td valign="top"><a href="#prev-3">prev/3</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_delete-2">rdb_delete/2</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_delete-3">rdb_delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_fold-4">rdb_fold/4</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_fold-5">rdb_fold/5</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_get-2">rdb_get/2</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_get-3">rdb_get/3</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_iterator-1">rdb_iterator/1</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_iterator-2">rdb_iterator/2</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_iterator_move-2">rdb_iterator_move/2</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_put-3">rdb_put/3</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_put-4">rdb_put/4</a></td><td></td></tr><tr><td valign="top"><a href="#read-2">read/2</a></td><td></td></tr><tr><td valign="top"><a href="#read-3">read/3</a></td><td></td></tr><tr><td valign="top"><a href="#read_info-1">read_info/1</a></td><td></td></tr><tr><td valign="top"><a href="#read_info-2">read_info/2</a></td><td></td></tr><tr><td valign="top"><a href="#release_snapshot-1">release_snapshot/1</a></td><td>release a snapshot created by <a docgen-rel="seemfa" docgen-href="#snapshot/1" href="#snapshot-1"><code>snapshot/1</code></a>.</td></tr><tr><td valign="top"><a href="#select-1">select/1</a></td><td></td></tr><tr><td valign="top"><a href="#select-2">select/2</a></td><td></td></tr><tr><td valign="top"><a href="#select-3">select/3</a></td><td></td></tr><tr><td valign="top"><a href="#snapshot-1">snapshot/1</a></td><td>Create a snapshot of the database instance associated with the
table reference, table name or alias.</td></tr><tr><td valign="top"><a href="#tx_commit-1">tx_commit/1</a></td><td></td></tr><tr><td valign="top"><a href="#tx_ref-2">tx_ref/2</a></td><td></td></tr><tr><td valign="top"><a href="#update_counter-3">update_counter/3</a></td><td></td></tr><tr><td valign="top"><a href="#update_counter-4">update_counter/4</a></td><td></td></tr><tr><td valign="top"><a href="#with_iterator-2">with_iterator/2</a></td><td></td></tr><tr><td valign="top"><a href="#with_iterator-3">with_iterator/3</a></td><td></td></tr><tr><td valign="top"><a href="#with_rdb_iterator-2">with_rdb_iterator/2</a></td><td></td></tr><tr><td valign="top"><a href="#with_rdb_iterator-3">with_rdb_iterator/3</a></td><td></td></tr><tr><td valign="top"><a href="#write_info-3">write_info/3</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="abort-1"></a>

### abort/1 ###

`abort(Reason) -> any()`

Aborts an ongoing [`activity/2`](#activity-2)

<a name="activity-3"></a>

### activity/3 ###

<pre><code>
activity(Type::<a href="#type-activity_type">activity_type()</a>, Alias::<a href="#type-alias">alias()</a>, F::fun(() -&gt; Res)) -&gt; Res
</code></pre>
<br />

Run an activity (similar to [`//mnesia/mnesia:activity/2`](http://www.erlang.org/doc/man/mnesia.html#activity-2)).

Supported activity types are:

* `transaction` - An optimistic `rocksdb` transaction

* `{tx, TxOpts}` - A `rocksdb` transaction with sligth modifications

* `batch` - A `rocksdb` batch operation


By default, transactions are combined with a snapshot with 1 retry.
The snapshot ensures that writes from concurrent transactions don't leak into the transaction context.
A transaction will be retried if it detects that the commit set conflicts with recent changes.
A mutex is used to ensure that only one of potentially conflicting `mrdb` transactions is run at a time.
The re-run transaction may still fail, if new transactions, or non-transaction writes interfere with
the commit set. It will then be re-run again, until the retry count is exhausted.

For finer-grained retries, it's possible to set `retries => {Inner, Outer}`. Setting the retries to a
single number, `Retries`, is analogous to `{0, Retries}`. Each outer retry requests a`mutex lock' by
waiting in a FIFO queue. Once it receives the lock, it will try the activity once + as many retries
as specified by `Inner`. If these fail, the activity again goes to the FIFO queue (ending up last
in line) if there are outer retries remaining. When all retries are exhaused, the activity aborts
with `retry_limit`. Note that transactions, being optimistic, do not request a lock on the first
attempt, but only on outer retries (the first retry is always an outer retry).

Valid `TxOpts` are `#{no_snapshot => boolean(), retries => retries()}`.

To simplify code adaptation, `tx | transaction | sync_transaction` are synonyms, and
`batch | async_dirty | sync_dirty` are synonyms.

<a name="alias_of-1"></a>

### alias_of/1 ###

<pre><code>
alias_of(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>) -&gt; <a href="#type-alias">alias()</a>
</code></pre>
<br />

Returns the alias of a given table or table reference.

<a name="as_batch-2"></a>

### as_batch/2 ###

<pre><code>
as_batch(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, F::fun((<a href="#type-db_ref">db_ref()</a>) -&gt; Res)) -&gt; Res
</code></pre>
<br />

Creates a `rocksdb` batch context and executes the fun `F` in it.

%% Rocksdb batches aren't tied to a specific DbRef until written.
This can cause surprising problems if we're juggling multiple
rocksdb instances (as we do if we have standalone tables).
At the time of writing, all objects end up in the DbRef the batch
is written to, albeit not necessarily in the intended column family.
This will probably change, but no failure mode is really acceptable.
The code below ensures that separate batches are created for each
DbRef, under a unique reference stored in the pdict. When writing,
all batches are written separately to the corresponding DbRef,
and when releasing, all batches are released. This will not ensure
atomicity, but there is no way in rocksdb to achieve atomicity
across db instances. At least, data should end up where you expect.

<a name="as_batch-3"></a>

### as_batch/3 ###

`as_batch(Tab, F, Opts) -> any()`

as [`as_batch/2`](#as_batch-2), but with the ability to pass `Opts` to `rocksdb:write_batch/2`

<a name="batch_write-2"></a>

### batch_write/2 ###

`batch_write(Tab, L) -> any()`

<a name="batch_write-3"></a>

### batch_write/3 ###

`batch_write(Tab, L, Opts) -> any()`

<a name="clear_table-1"></a>

### clear_table/1 ###

`clear_table(Tab) -> any()`

<a name="current_context-0"></a>

### current_context/0 ###

`current_context() -> any()`

<a name="delete-2"></a>

### delete/2 ###

<pre><code>
delete(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Key::<a href="#type-key">key()</a>) -&gt; ok
</code></pre>
<br />

<a name="delete-3"></a>

### delete/3 ###

<pre><code>
delete(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Key::<a href="#type-key">key()</a>, Opts::<a href="#type-write_options">write_options()</a>) -&gt; ok
</code></pre>
<br />

<a name="delete_object-2"></a>

### delete_object/2 ###

`delete_object(Tab, Obj) -> any()`

<a name="delete_object-3"></a>

### delete_object/3 ###

`delete_object(Tab, Obj, Opts) -> any()`

<a name="ensure_ref-1"></a>

### ensure_ref/1 ###

<pre><code>
ensure_ref(R::<a href="#type-ref_or_tab">ref_or_tab()</a>) -&gt; <a href="#type-db_ref">db_ref()</a>
</code></pre>
<br />

<a name="ensure_ref-2"></a>

### ensure_ref/2 ###

`ensure_ref(Ref, R) -> any()`

<a name="first-1"></a>

### first/1 ###

<pre><code>
first(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>) -&gt; <a href="#type-key">key()</a> | $end_of_table
</code></pre>
<br />

<a name="first-2"></a>

### first/2 ###

<pre><code>
first(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Opts::<a href="#type-read_options">read_options()</a>) -&gt; <a href="#type-key">key()</a> | $end_of_table
</code></pre>
<br />

<a name="fold-3"></a>

### fold/3 ###

`fold(Tab, Fun, Acc) -> any()`

<a name="fold-4"></a>

### fold/4 ###

`fold(Tab, Fun, Acc, MatchSpec) -> any()`

<a name="fold-5"></a>

### fold/5 ###

`fold(Tab, Fun, Acc, MatchSpec, Limit) -> any()`

<a name="get_batch-1"></a>

### get_batch/1 ###

`get_batch(X1) -> any()`

<a name="get_ref-1"></a>

### get_ref/1 ###

<pre><code>
get_ref(Tab::<a href="#type-table">table()</a>) -&gt; <a href="#type-db_ref">db_ref()</a>
</code></pre>
<br />

<a name="index_read-3"></a>

### index_read/3 ###

`index_read(Tab, Val, Ix) -> any()`

<a name="insert-2"></a>

### insert/2 ###

<pre><code>
insert(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Obj::<a href="#type-obj">obj()</a>) -&gt; ok
</code></pre>
<br />

<a name="insert-3"></a>

### insert/3 ###

<pre><code>
insert(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Obj0::<a href="#type-obj">obj()</a>, Opts::<a href="#type-write_options">write_options()</a>) -&gt; ok
</code></pre>
<br />

<a name="iterator-1"></a>

### iterator/1 ###

<pre><code>
iterator(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>) -&gt; {ok, <a href="#type-mrdb_iterator">mrdb_iterator()</a>} | {error, term()}
</code></pre>
<br />

<a name="iterator-2"></a>

### iterator/2 ###

<pre><code>
iterator(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Opts::<a href="#type-read_options">read_options()</a>) -&gt; {ok, <a href="#type-mrdb_iterator">mrdb_iterator()</a>} | {error, term()}
</code></pre>
<br />

<a name="iterator_close-1"></a>

### iterator_close/1 ###

<pre><code>
iterator_close(Mrdb_iter::<a href="#type-mrdb_iterator">mrdb_iterator()</a>) -&gt; ok
</code></pre>
<br />

<a name="iterator_move-2"></a>

### iterator_move/2 ###

<pre><code>
iterator_move(Mrdb_iter::<a href="#type-mrdb_iterator">mrdb_iterator()</a>, Dir::<a href="#type-iterator_action">iterator_action()</a>) -&gt; {ok, tuple()} | {error, any()}
</code></pre>
<br />

<a name="last-1"></a>

### last/1 ###

<pre><code>
last(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>) -&gt; <a href="#type-key">key()</a> | $end_of_table
</code></pre>
<br />

<a name="last-2"></a>

### last/2 ###

<pre><code>
last(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Opts::<a href="#type-read_options">read_options()</a>) -&gt; <a href="#type-key">key()</a> | $end_of_table
</code></pre>
<br />

<a name="match_delete-2"></a>

### match_delete/2 ###

`match_delete(Tab, Pat) -> any()`

<a name="new_tx-1"></a>

### new_tx/1 ###

<pre><code>
new_tx(Tab::<a href="#type-table">table()</a> | <a href="#type-db_ref">db_ref()</a>) -&gt; <a href="#type-db_ref">db_ref()</a>
</code></pre>
<br />

<a name="new_tx-2"></a>

### new_tx/2 ###

<pre><code>
new_tx(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Opts::<a href="#type-write_options">write_options()</a>) -&gt; <a href="#type-db_ref">db_ref()</a>
</code></pre>
<br />

<a name="next-2"></a>

### next/2 ###

<pre><code>
next(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, K::<a href="#type-key">key()</a>) -&gt; <a href="#type-key">key()</a> | $end_of_table
</code></pre>
<br />

<a name="next-3"></a>

### next/3 ###

<pre><code>
next(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, K::<a href="#type-key">key()</a>, Opts::<a href="#type-read_options">read_options()</a>) -&gt; <a href="#type-key">key()</a> | $end_of_table
</code></pre>
<br />

<a name="prev-2"></a>

### prev/2 ###

<pre><code>
prev(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, K::<a href="#type-key">key()</a>) -&gt; <a href="#type-key">key()</a> | $end_of_table
</code></pre>
<br />

<a name="prev-3"></a>

### prev/3 ###

<pre><code>
prev(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, K::<a href="#type-key">key()</a>, Opts::<a href="#type-read_options">read_options()</a>) -&gt; <a href="#type-key">key()</a> | $end_of_table
</code></pre>
<br />

<a name="rdb_delete-2"></a>

### rdb_delete/2 ###

`rdb_delete(R, K) -> any()`

<a name="rdb_delete-3"></a>

### rdb_delete/3 ###

`rdb_delete(R, K, Opts) -> any()`

<a name="rdb_fold-4"></a>

### rdb_fold/4 ###

`rdb_fold(Tab, Fun, Acc, Prefix) -> any()`

<a name="rdb_fold-5"></a>

### rdb_fold/5 ###

`rdb_fold(Tab, Fun, Acc, Prefix, Limit) -> any()`

<a name="rdb_get-2"></a>

### rdb_get/2 ###

`rdb_get(R, K) -> any()`

<a name="rdb_get-3"></a>

### rdb_get/3 ###

`rdb_get(R, K, Opts) -> any()`

<a name="rdb_iterator-1"></a>

### rdb_iterator/1 ###

`rdb_iterator(R) -> any()`

<a name="rdb_iterator-2"></a>

### rdb_iterator/2 ###

`rdb_iterator(R, Opts) -> any()`

<a name="rdb_iterator_move-2"></a>

### rdb_iterator_move/2 ###

`rdb_iterator_move(I, Dir) -> any()`

<a name="rdb_put-3"></a>

### rdb_put/3 ###

`rdb_put(R, K, V) -> any()`

<a name="rdb_put-4"></a>

### rdb_put/4 ###

`rdb_put(R, K, V, Opts) -> any()`

<a name="read-2"></a>

### read/2 ###

`read(Tab, Key) -> any()`

<a name="read-3"></a>

### read/3 ###

`read(Tab, Key, Opts) -> any()`

<a name="read_info-1"></a>

### read_info/1 ###

`read_info(Tab) -> any()`

<a name="read_info-2"></a>

### read_info/2 ###

`read_info(Tab, K) -> any()`

<a name="release_snapshot-1"></a>

### release_snapshot/1 ###

<pre><code>
release_snapshot(SHandle::<a href="#type-snapshot_handle">snapshot_handle()</a>) -&gt; ok | <a href="#type-error">error()</a>
</code></pre>
<br />

release a snapshot created by [`snapshot/1`](#snapshot-1).

<a name="select-1"></a>

### select/1 ###

`select(Cont) -> any()`

<a name="select-2"></a>

### select/2 ###

`select(Tab, Pat) -> any()`

<a name="select-3"></a>

### select/3 ###

`select(Tab, Pat, Limit) -> any()`

<a name="snapshot-1"></a>

### snapshot/1 ###

<pre><code>
snapshot(Name::<a href="#type-alias">alias()</a> | <a href="#type-ref_or_tab">ref_or_tab()</a>) -&gt; {ok, <a href="#type-snapshot_handle">snapshot_handle()</a>} | <a href="#type-error">error()</a>
</code></pre>
<br />

Create a snapshot of the database instance associated with the
table reference, table name or alias.

Snapshots provide consistent read-only views over the entire state of the key-value store.

<a name="tx_commit-1"></a>

### tx_commit/1 ###

<pre><code>
tx_commit(TxH::<a href="#type-tx_handle">tx_handle()</a> | <a href="#type-db_ref">db_ref()</a>) -&gt; ok
</code></pre>
<br />

<a name="tx_ref-2"></a>

### tx_ref/2 ###

<pre><code>
tx_ref(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a> | <a href="#type-db_ref">db_ref()</a> | <a href="#type-db_ref">db_ref()</a>, TxH::<a href="#type-tx_handle">tx_handle()</a>) -&gt; <a href="#type-db_ref">db_ref()</a>
</code></pre>
<br />

<a name="update_counter-3"></a>

### update_counter/3 ###

`update_counter(Tab, C, Val) -> any()`

<a name="update_counter-4"></a>

### update_counter/4 ###

`update_counter(Tab, C, Val, Opts) -> any()`

<a name="with_iterator-2"></a>

### with_iterator/2 ###

<pre><code>
with_iterator(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Fun::fun((<a href="#type-mrdb_iterator">mrdb_iterator()</a>) -&gt; Res)) -&gt; Res
</code></pre>
<br />

<a name="with_iterator-3"></a>

### with_iterator/3 ###

<pre><code>
with_iterator(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Fun::fun((<a href="#type-mrdb_iterator">mrdb_iterator()</a>) -&gt; Res), Opts::<a href="#type-read_options">read_options()</a>) -&gt; Res
</code></pre>
<br />

<a name="with_rdb_iterator-2"></a>

### with_rdb_iterator/2 ###

<pre><code>
with_rdb_iterator(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Fun::fun((<a href="#type-itr_handle">itr_handle()</a>) -&gt; Res)) -&gt; Res
</code></pre>
<br />

<a name="with_rdb_iterator-3"></a>

### with_rdb_iterator/3 ###

<pre><code>
with_rdb_iterator(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Fun::fun((<a href="#type-itr_handle">itr_handle()</a>) -&gt; Res), Opts::<a href="#type-read_options">read_options()</a>) -&gt; Res
</code></pre>
<br />

<a name="write_info-3"></a>

### write_info/3 ###

`write_info(Tab, K, V) -> any()`

