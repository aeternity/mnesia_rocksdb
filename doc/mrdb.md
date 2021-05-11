

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
   , batch      := <Batch reference, if any>
   , tx_handle  := <Rocksdb transaction handle, if any>
   , attr_pos   := #{AttrName := Pos}
   , mode       := <Set to 'mnesia' for mnesia access flows>
   , properties := <Mnesia table props in map format
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




### <a name="type-admin_tab">admin_tab()</a> ###


<pre><code>
admin_tab() = {admin, <a href="#type-alias">alias()</a>}
</code></pre>




### <a name="type-alias">alias()</a> ###


<pre><code>
alias() = atom()
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
db_ref() = #{db_ref =&gt; <a href="#type-db_handle">db_handle()</a>, cf_handle =&gt; <a href="#type-cf_handle">cf_handle()</a>, tx_handle =&gt; <a href="#type-tx_handle">tx_handle()</a>}
</code></pre>




### <a name="type-index">index()</a> ###


<pre><code>
index() = {<a href="#type-tab_name">tab_name()</a>, index, any()}
</code></pre>




### <a name="type-itr_handle">itr_handle()</a> ###


<pre><code>
itr_handle() = <a href="/home/uwiger/ae/mnesia_rocksdb/_build/default/lib/rocksdb/doc/rocksdb.md#type-itr_handle">rocksdb:itr_handle()</a>
</code></pre>




### <a name="type-key">key()</a> ###


<pre><code>
key() = any()
</code></pre>




### <a name="type-obj">obj()</a> ###


<pre><code>
obj() = tuple()
</code></pre>




### <a name="type-read_options">read_options()</a> ###


<pre><code>
read_options() = [{verify_checksums, boolean()} | {fill_cache, boolean()} | {iterate_upper_bound, binary()} | {iterate_lower_bound, binary()} | {tailing, boolean()} | {total_order_seek, boolean()} | {prefix_same_as_start, boolean()} | {snapshot, <a href="#type-snapshot_handle">snapshot_handle()</a>}]
</code></pre>




### <a name="type-ref_or_tab">ref_or_tab()</a> ###


<pre><code>
ref_or_tab() = <a href="#type-table">table()</a> | <a href="#type-db_ref">db_ref()</a> | <a href="#type-tx_ref">tx_ref()</a>
</code></pre>




### <a name="type-retainer">retainer()</a> ###


<pre><code>
retainer() = {<a href="#type-tab_name">tab_name()</a>, retainer, any()}
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




### <a name="type-tx_handle">tx_handle()</a> ###


<pre><code>
tx_handle() = <a href="/home/uwiger/ae/mnesia_rocksdb/_build/default/lib/rocksdb/doc/rocksdb.md#type-transaction_handle">rocksdb:transaction_handle()</a>
</code></pre>




### <a name="type-tx_ref">tx_ref()</a> ###


<pre><code>
tx_ref() = #{db_ref =&gt; <a href="#type-db_handle">db_handle()</a>, cf_handle =&gt; <a href="#type-cf_handle">cf_handle()</a>, tx_handle =&gt; <a href="#type-tx_handle">tx_handle()</a>}
</code></pre>




### <a name="type-write_options">write_options()</a> ###


<pre><code>
write_options() = [{sync, boolean()} | {disable_wal, boolean()} | {ignore_missing_column_families, boolean()} | {no_slowdown, boolean()} | {low_pri, boolean()}]
</code></pre>

 ============================================================

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#abort-1">abort/1</a></td><td></td></tr><tr><td valign="top"><a href="#as_batch-2">as_batch/2</a></td><td></td></tr><tr><td valign="top"><a href="#as_batch-3">as_batch/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete-2">delete/2</a></td><td></td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#first-1">first/1</a></td><td></td></tr><tr><td valign="top"><a href="#first-2">first/2</a></td><td></td></tr><tr><td valign="top"><a href="#fold-4">fold/4</a></td><td></td></tr><tr><td valign="top"><a href="#fold-5">fold/5</a></td><td></td></tr><tr><td valign="top"><a href="#get_ref-1">get_ref/1</a></td><td></td></tr><tr><td valign="top"><a href="#index_read-3">index_read/3</a></td><td></td></tr><tr><td valign="top"><a href="#insert-2">insert/2</a></td><td></td></tr><tr><td valign="top"><a href="#insert-3">insert/3</a></td><td></td></tr><tr><td valign="top"><a href="#last-1">last/1</a></td><td></td></tr><tr><td valign="top"><a href="#last-2">last/2</a></td><td></td></tr><tr><td valign="top"><a href="#match_delete-2">match_delete/2</a></td><td></td></tr><tr><td valign="top"><a href="#new_tx-1">new_tx/1</a></td><td></td></tr><tr><td valign="top"><a href="#new_tx-2">new_tx/2</a></td><td></td></tr><tr><td valign="top"><a href="#next-2">next/2</a></td><td></td></tr><tr><td valign="top"><a href="#next-3">next/3</a></td><td></td></tr><tr><td valign="top"><a href="#prev-2">prev/2</a></td><td></td></tr><tr><td valign="top"><a href="#prev-3">prev/3</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_delete-3">rdb_delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_get-3">rdb_get/3</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_iterator-2">rdb_iterator/2</a></td><td></td></tr><tr><td valign="top"><a href="#rdb_put-4">rdb_put/4</a></td><td></td></tr><tr><td valign="top"><a href="#read-2">read/2</a></td><td></td></tr><tr><td valign="top"><a href="#read-3">read/3</a></td><td></td></tr><tr><td valign="top"><a href="#read_info-2">read_info/2</a></td><td></td></tr><tr><td valign="top"><a href="#select-1">select/1</a></td><td></td></tr><tr><td valign="top"><a href="#select-2">select/2</a></td><td></td></tr><tr><td valign="top"><a href="#select-3">select/3</a></td><td></td></tr><tr><td valign="top"><a href="#tx_commit-1">tx_commit/1</a></td><td></td></tr><tr><td valign="top"><a href="#tx_ref-2">tx_ref/2</a></td><td></td></tr><tr><td valign="top"><a href="#with_iterator-2">with_iterator/2</a></td><td></td></tr><tr><td valign="top"><a href="#with_iterator-3">with_iterator/3</a></td><td></td></tr><tr><td valign="top"><a href="#write-2">write/2</a></td><td></td></tr><tr><td valign="top"><a href="#write-3">write/3</a></td><td></td></tr><tr><td valign="top"><a href="#write_info-3">write_info/3</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="abort-1"></a>

### abort/1 ###

`abort(Reason) -> any()`

<a name="as_batch-2"></a>

### as_batch/2 ###

`as_batch(Tab, F) -> any()`

<a name="as_batch-3"></a>

### as_batch/3 ###

`as_batch(Tab, F, Opts) -> any()`

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

<a name="fold-4"></a>

### fold/4 ###

`fold(Tab, Fun, Acc, MatchSpec) -> any()`

<a name="fold-5"></a>

### fold/5 ###

`fold(Tab, Fun, Acc, MatchSpec, Limit) -> any()`

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
insert(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Obj::<a href="#type-obj">obj()</a>, Opts::<a href="#type-write_options">write_options()</a>) -&gt; ok
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
new_tx(Tab::<a href="#type-table">table()</a> | <a href="#type-db_ref">db_ref()</a>) -&gt; <a href="#type-tx_ref">tx_ref()</a>
</code></pre>
<br />

<a name="new_tx-2"></a>

### new_tx/2 ###

<pre><code>
new_tx(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Opts::<a href="#type-write_options">write_options()</a>) -&gt; <a href="#type-tx_ref">tx_ref()</a>
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

<a name="rdb_delete-3"></a>

### rdb_delete/3 ###

`rdb_delete(R, K, Opts) -> any()`

<a name="rdb_get-3"></a>

### rdb_get/3 ###

`rdb_get(R, K, Opts) -> any()`

<a name="rdb_iterator-2"></a>

### rdb_iterator/2 ###

`rdb_iterator(R, Opts) -> any()`

<a name="rdb_put-4"></a>

### rdb_put/4 ###

`rdb_put(R, K, V, Opts) -> any()`

<a name="read-2"></a>

### read/2 ###

`read(Tab, Key) -> any()`

<a name="read-3"></a>

### read/3 ###

`read(Tab, Key, Opts) -> any()`

<a name="read_info-2"></a>

### read_info/2 ###

`read_info(Tab, K) -> any()`

<a name="select-1"></a>

### select/1 ###

`select(Cont) -> any()`

<a name="select-2"></a>

### select/2 ###

`select(Tab, Pat) -> any()`

<a name="select-3"></a>

### select/3 ###

`select(Tab, Pat, Limit) -> any()`

<a name="tx_commit-1"></a>

### tx_commit/1 ###

<pre><code>
tx_commit(TxH::<a href="#type-tx_handle">tx_handle()</a> | <a href="#type-tx_ref">tx_ref()</a>) -&gt; ok
</code></pre>
<br />

<a name="tx_ref-2"></a>

### tx_ref/2 ###

<pre><code>
tx_ref(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a> | <a href="#type-tx_ref">tx_ref()</a> | <a href="#type-tx_ref">tx_ref()</a>, TxH::<a href="#type-tx_handle">tx_handle()</a>) -&gt; <a href="#type-tx_ref">tx_ref()</a>
</code></pre>
<br />

<a name="with_iterator-2"></a>

### with_iterator/2 ###

<pre><code>
with_iterator(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Fun::fun((<a href="#type-itr_handle">itr_handle()</a>) -&gt; Res)) -&gt; Res
</code></pre>
<br />

<a name="with_iterator-3"></a>

### with_iterator/3 ###

<pre><code>
with_iterator(Tab::<a href="#type-ref_or_tab">ref_or_tab()</a>, Fun::fun((<a href="#type-itr_handle">itr_handle()</a>) -&gt; Res), Opts::<a href="#type-read_options">read_options()</a>) -&gt; Res
</code></pre>
<br />

<a name="write-2"></a>

### write/2 ###

`write(Tab, L) -> any()`

<a name="write-3"></a>

### write/3 ###

`write(Tab, L, Opts) -> any()`

<a name="write_info-3"></a>

### write_info/3 ###

`write_info(Tab, K, V) -> any()`

