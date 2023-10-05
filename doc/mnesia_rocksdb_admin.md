

# Module mnesia_rocksdb_admin #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

__Behaviours:__ [`gen_server`](gen_server.md).

<a name="types"></a>

## Data Types ##




### <a name="type-alias">alias()</a> ###


<pre><code>
alias() = atom()
</code></pre>




### <a name="type-backend">backend()</a> ###


<pre><code>
backend() = #{db_ref =&gt; <a href="#type-db_ref">db_ref()</a>, cf_info =&gt; #{<a href="#type-table">table()</a> =&gt; <a href="#type-cf">cf()</a>}}
</code></pre>




### <a name="type-cf">cf()</a> ###


<pre><code>
cf() = <a href="http://www.erlang.org/doc/man/mrdb.html#type-db_ref">mrdb:db_ref()</a>
</code></pre>




### <a name="type-db_ref">db_ref()</a> ###


<pre><code>
db_ref() = <a href="/home/uwiger/ae/mnesia_rocksdb/_build/default/lib/rocksdb/doc/rocksdb.md#type-db_handle">rocksdb:db_handle()</a>
</code></pre>




### <a name="type-gen_server_noreply">gen_server_noreply()</a> ###


<pre><code>
gen_server_noreply() = {noreply, <a href="#type-st">st()</a>} | {stop, <a href="#type-reason">reason()</a>, <a href="#type-st">st()</a>}
</code></pre>




### <a name="type-gen_server_reply">gen_server_reply()</a> ###


<pre><code>
gen_server_reply() = {reply, <a href="#type-reply">reply()</a>, <a href="#type-st">st()</a>} | {stop, <a href="#type-reason">reason()</a>, <a href="#type-reply">reply()</a>, <a href="#type-st">st()</a>}
</code></pre>




### <a name="type-properties">properties()</a> ###


<pre><code>
properties() = [{atom(), any()}]
</code></pre>




### <a name="type-reason">reason()</a> ###


<pre><code>
reason() = any()
</code></pre>




### <a name="type-reply">reply()</a> ###


<pre><code>
reply() = any()
</code></pre>




### <a name="type-req">req()</a> ###


<pre><code>
req() = {create_table, <a href="#type-table">table()</a>, <a href="#type-properties">properties()</a>} | {delete_table, <a href="#type-table">table()</a>} | {load_table, <a href="#type-table">table()</a>, <a href="#type-properties">properties()</a>} | {related_resources, <a href="#type-table">table()</a>} | {get_ref, <a href="#type-table">table()</a>} | {add_aliases, [<a href="#type-alias">alias()</a>]} | {write_table_property, <a href="#type-tabname">tabname()</a>, tuple()} | {remove_aliases, [<a href="#type-alias">alias()</a>]} | {migrate, [{<a href="#type-tabname">tabname()</a>, map()}], <a href="#type-rpt">rpt()</a>} | {prep_close, <a href="#type-table">table()</a>} | {close_table, <a href="#type-table">table()</a>} | {clear_table, <a href="#type-table">table()</a> | <a href="#type-cf">cf()</a>}
</code></pre>




### <a name="type-rpt">rpt()</a> ###


<pre><code>
rpt() = undefined | map()
</code></pre>




### <a name="type-st">st()</a> ###


<pre><code>
st() = #st{backends = #{<a href="#type-alias">alias()</a> =&gt; <a href="#type-backend">backend()</a>}, standalone = #{{<a href="#type-alias">alias()</a>, <a href="#type-table">table()</a>} =&gt; <a href="#type-cf">cf()</a>}, default_opts = [{atom(), term()}]}
</code></pre>




### <a name="type-table">table()</a> ###


<pre><code>
table() = <a href="#type-tabname">tabname()</a> | {admin, <a href="#type-alias">alias()</a>} | {<a href="#type-tabname">tabname()</a>, index, any()} | {<a href="#type-tabname">tabname()</a>, retainer, any()}
</code></pre>




### <a name="type-tabname">tabname()</a> ###


<pre><code>
tabname() = atom()
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_aliases-1">add_aliases/1</a></td><td></td></tr><tr><td valign="top"><a href="#clear_table-1">clear_table/1</a></td><td></td></tr><tr><td valign="top"><a href="#close_table-2">close_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#code_change-3">code_change/3</a></td><td></td></tr><tr><td valign="top"><a href="#create_table-3">create_table/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete_table-2">delete_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#ensure_started-0">ensure_started/0</a></td><td></td></tr><tr><td valign="top"><a href="#get_cached_env-2">get_cached_env/2</a></td><td></td></tr><tr><td valign="top"><a href="#get_ref-1">get_ref/1</a></td><td></td></tr><tr><td valign="top"><a href="#get_ref-2">get_ref/2</a></td><td></td></tr><tr><td valign="top"><a href="#handle_call-3">handle_call/3</a></td><td></td></tr><tr><td valign="top"><a href="#handle_cast-2">handle_cast/2</a></td><td></td></tr><tr><td valign="top"><a href="#handle_info-2">handle_info/2</a></td><td></td></tr><tr><td valign="top"><a href="#init-1">init/1</a></td><td></td></tr><tr><td valign="top"><a href="#load_table-3">load_table/3</a></td><td></td></tr><tr><td valign="top"><a href="#meta-0">meta/0</a></td><td></td></tr><tr><td valign="top"><a href="#migrate_standalone-2">migrate_standalone/2</a></td><td></td></tr><tr><td valign="top"><a href="#migrate_standalone-3">migrate_standalone/3</a></td><td></td></tr><tr><td valign="top"><a href="#prep_close-2">prep_close/2</a></td><td></td></tr><tr><td valign="top"><a href="#read_info-1">read_info/1</a></td><td></td></tr><tr><td valign="top"><a href="#read_info-2">read_info/2</a></td><td></td></tr><tr><td valign="top"><a href="#read_info-4">read_info/4</a></td><td></td></tr><tr><td valign="top"><a href="#related_resources-2">related_resources/2</a></td><td></td></tr><tr><td valign="top"><a href="#remove_aliases-1">remove_aliases/1</a></td><td></td></tr><tr><td valign="top"><a href="#request_ref-2">request_ref/2</a></td><td></td></tr><tr><td valign="top"><a href="#set_and_cache_env-2">set_and_cache_env/2</a></td><td></td></tr><tr><td valign="top"><a href="#start_link-0">start_link/0</a></td><td></td></tr><tr><td valign="top"><a href="#terminate-2">terminate/2</a></td><td></td></tr><tr><td valign="top"><a href="#write_info-4">write_info/4</a></td><td></td></tr><tr><td valign="top"><a href="#write_table_property-3">write_table_property/3</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="add_aliases-1"></a>

### add_aliases/1 ###

`add_aliases(Aliases) -> any()`

<a name="clear_table-1"></a>

### clear_table/1 ###

`clear_table(Name) -> any()`

<a name="close_table-2"></a>

### close_table/2 ###

`close_table(Alias, Name) -> any()`

<a name="code_change-3"></a>

### code_change/3 ###

`code_change(FromVsn, St, Extra) -> any()`

<a name="create_table-3"></a>

### create_table/3 ###

`create_table(Alias, Name, Props) -> any()`

<a name="delete_table-2"></a>

### delete_table/2 ###

<pre><code>
delete_table(Alias::<a href="#type-alias">alias()</a>, Name::<a href="#type-tabname">tabname()</a>) -&gt; ok
</code></pre>
<br />

<a name="ensure_started-0"></a>

### ensure_started/0 ###

<pre><code>
ensure_started() -&gt; ok
</code></pre>
<br />

<a name="get_cached_env-2"></a>

### get_cached_env/2 ###

`get_cached_env(Key, Default) -> any()`

<a name="get_ref-1"></a>

### get_ref/1 ###

`get_ref(Name) -> any()`

<a name="get_ref-2"></a>

### get_ref/2 ###

`get_ref(Name, Default) -> any()`

<a name="handle_call-3"></a>

### handle_call/3 ###

<pre><code>
handle_call(Req::{<a href="#type-alias">alias()</a>, <a href="#type-req">req()</a>}, From::any(), St::<a href="#type-st">st()</a>) -&gt; <a href="#type-gen_server_reply">gen_server_reply()</a>
</code></pre>
<br />

<a name="handle_cast-2"></a>

### handle_cast/2 ###

<pre><code>
handle_cast(Msg::any(), St::<a href="#type-st">st()</a>) -&gt; <a href="#type-gen_server_noreply">gen_server_noreply()</a>
</code></pre>
<br />

<a name="handle_info-2"></a>

### handle_info/2 ###

<pre><code>
handle_info(Msg::any(), St::<a href="#type-st">st()</a>) -&gt; <a href="#type-gen_server_noreply">gen_server_noreply()</a>
</code></pre>
<br />

<a name="init-1"></a>

### init/1 ###

`init(X1) -> any()`

<a name="load_table-3"></a>

### load_table/3 ###

`load_table(Alias, Name, Props) -> any()`

<a name="meta-0"></a>

### meta/0 ###

`meta() -> any()`

<a name="migrate_standalone-2"></a>

### migrate_standalone/2 ###

`migrate_standalone(Alias, Tabs) -> any()`

<a name="migrate_standalone-3"></a>

### migrate_standalone/3 ###

`migrate_standalone(Alias, Tabs, Rpt0) -> any()`

<a name="prep_close-2"></a>

### prep_close/2 ###

`prep_close(Alias, Tab) -> any()`

<a name="read_info-1"></a>

### read_info/1 ###

`read_info(TRec) -> any()`

<a name="read_info-2"></a>

### read_info/2 ###

`read_info(Alias, Tab) -> any()`

<a name="read_info-4"></a>

### read_info/4 ###

`read_info(Alias, Tab, K, Default) -> any()`

<a name="related_resources-2"></a>

### related_resources/2 ###

`related_resources(Alias, Name) -> any()`

<a name="remove_aliases-1"></a>

### remove_aliases/1 ###

`remove_aliases(Aliases) -> any()`

<a name="request_ref-2"></a>

### request_ref/2 ###

`request_ref(Alias, Name) -> any()`

<a name="set_and_cache_env-2"></a>

### set_and_cache_env/2 ###

`set_and_cache_env(Key, Value) -> any()`

<a name="start_link-0"></a>

### start_link/0 ###

`start_link() -> any()`

<a name="terminate-2"></a>

### terminate/2 ###

`terminate(X1, St) -> any()`

<a name="write_info-4"></a>

### write_info/4 ###

`write_info(Alias, Tab, K, V) -> any()`

<a name="write_table_property-3"></a>

### write_table_property/3 ###

`write_table_property(Alias, Tab, Prop) -> any()`

