

# Module mnesia_rocksdb #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

rocksdb storage backend for Mnesia.

__Behaviours:__ [`gen_server`](gen_server.md), [`mnesia_backend_type`](mnesia_backend_type.md).

<a name="types"></a>

## Data Types ##




### <a name="type-alias">alias()</a> ###


<pre><code>
alias() = atom()
</code></pre>




### <a name="type-data_tab">data_tab()</a> ###


<pre><code>
data_tab() = atom()
</code></pre>




### <a name="type-index_info">index_info()</a> ###


<pre><code>
index_info() = {<a href="#type-index_pos">index_pos()</a>, <a href="#type-index_type">index_type()</a>}
</code></pre>




### <a name="type-index_pos">index_pos()</a> ###


<pre><code>
index_pos() = integer() | {atom()}
</code></pre>




### <a name="type-index_tab">index_tab()</a> ###


<pre><code>
index_tab() = {<a href="#type-data_tab">data_tab()</a>, index, <a href="#type-index_info">index_info()</a>}
</code></pre>




### <a name="type-index_type">index_type()</a> ###


<pre><code>
index_type() = ordered
</code></pre>




### <a name="type-retainer_name">retainer_name()</a> ###


<pre><code>
retainer_name() = any()
</code></pre>




### <a name="type-retainer_tab">retainer_tab()</a> ###


<pre><code>
retainer_tab() = {<a href="#type-data_tab">data_tab()</a>, retainer, <a href="#type-retainer_name">retainer_name()</a>}
</code></pre>




### <a name="type-table">table()</a> ###


<pre><code>
table() = <a href="#type-data_tab">data_tab()</a> | <a href="#type-index_tab">index_tab()</a> | <a href="#type-retainer_tab">retainer_tab()</a>
</code></pre>




### <a name="type-table_type">table_type()</a> ###


<pre><code>
table_type() = set | ordered_set | bag
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_aliases-1">add_aliases/1</a></td><td></td></tr><tr><td valign="top"><a href="#check_definition-4">check_definition/4</a></td><td></td></tr><tr><td valign="top"><a href="#close_table-2">close_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#code_change-3">code_change/3</a></td><td></td></tr><tr><td valign="top"><a href="#create_table-3">create_table/3</a></td><td></td></tr><tr><td valign="top"><a href="#default_alias-0">default_alias/0</a></td><td></td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete_table-2">delete_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#first-2">first/2</a></td><td></td></tr><tr><td valign="top"><a href="#fixtable-3">fixtable/3</a></td><td></td></tr><tr><td valign="top"><a href="#handle_call-3">handle_call/3</a></td><td></td></tr><tr><td valign="top"><a href="#handle_cast-2">handle_cast/2</a></td><td></td></tr><tr><td valign="top"><a href="#handle_info-2">handle_info/2</a></td><td></td></tr><tr><td valign="top"><a href="#index_is_consistent-3">index_is_consistent/3</a></td><td></td></tr><tr><td valign="top"><a href="#info-3">info/3</a></td><td></td></tr><tr><td valign="top"><a href="#init-1">init/1</a></td><td></td></tr><tr><td valign="top"><a href="#init_backend-0">init_backend/0</a></td><td></td></tr><tr><td valign="top"><a href="#insert-3">insert/3</a></td><td></td></tr><tr><td valign="top"><a href="#is_index_consistent-2">is_index_consistent/2</a></td><td></td></tr><tr><td valign="top"><a href="#ix_prefixes-3">ix_prefixes/3</a></td><td></td></tr><tr><td valign="top"><a href="#last-2">last/2</a></td><td></td></tr><tr><td valign="top"><a href="#load_table-4">load_table/4</a></td><td></td></tr><tr><td valign="top"><a href="#lookup-3">lookup/3</a></td><td></td></tr><tr><td valign="top"><a href="#match_delete-3">match_delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#next-3">next/3</a></td><td></td></tr><tr><td valign="top"><a href="#prev-3">prev/3</a></td><td></td></tr><tr><td valign="top"><a href="#real_suffixes-0">real_suffixes/0</a></td><td></td></tr><tr><td valign="top"><a href="#receive_data-5">receive_data/5</a></td><td></td></tr><tr><td valign="top"><a href="#receive_done-4">receive_done/4</a></td><td></td></tr><tr><td valign="top"><a href="#receiver_first_message-4">receiver_first_message/4</a></td><td></td></tr><tr><td valign="top"><a href="#register-0">register/0</a></td><td></td></tr><tr><td valign="top"><a href="#register-1">register/1</a></td><td></td></tr><tr><td valign="top"><a href="#remove_aliases-1">remove_aliases/1</a></td><td></td></tr><tr><td valign="top"><a href="#repair_continuation-2">repair_continuation/2</a></td><td></td></tr><tr><td valign="top"><a href="#select-1">select/1</a></td><td></td></tr><tr><td valign="top"><a href="#select-3">select/3</a></td><td></td></tr><tr><td valign="top"><a href="#select-4">select/4</a></td><td></td></tr><tr><td valign="top"><a href="#semantics-2">semantics/2</a></td><td></td></tr><tr><td valign="top"><a href="#sender_handle_info-5">sender_handle_info/5</a></td><td></td></tr><tr><td valign="top"><a href="#sender_init-4">sender_init/4</a></td><td></td></tr><tr><td valign="top"><a href="#show_table-1">show_table/1</a></td><td></td></tr><tr><td valign="top"><a href="#show_table-2">show_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#slot-3">slot/3</a></td><td></td></tr><tr><td valign="top"><a href="#start_proc-6">start_proc/6</a></td><td></td></tr><tr><td valign="top"><a href="#sync_close_table-2">sync_close_table/2</a></td><td></td></tr><tr><td valign="top"><a href="#terminate-2">terminate/2</a></td><td></td></tr><tr><td valign="top"><a href="#tmp_suffixes-0">tmp_suffixes/0</a></td><td></td></tr><tr><td valign="top"><a href="#update_counter-4">update_counter/4</a></td><td></td></tr><tr><td valign="top"><a href="#validate_key-6">validate_key/6</a></td><td></td></tr><tr><td valign="top"><a href="#validate_record-6">validate_record/6</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="add_aliases-1"></a>

### add_aliases/1 ###

`add_aliases(Aliases) -> any()`

<a name="check_definition-4"></a>

### check_definition/4 ###

`check_definition(Alias, Tab, Nodes, Props) -> any()`

<a name="close_table-2"></a>

### close_table/2 ###

`close_table(Alias, Tab) -> any()`

<a name="code_change-3"></a>

### code_change/3 ###

`code_change(FromVsn, St, Extra) -> any()`

<a name="create_table-3"></a>

### create_table/3 ###

`create_table(Alias, Tab, Props) -> any()`

<a name="default_alias-0"></a>

### default_alias/0 ###

`default_alias() -> any()`

<a name="delete-3"></a>

### delete/3 ###

`delete(Alias, Tab, Key) -> any()`

<a name="delete_table-2"></a>

### delete_table/2 ###

`delete_table(Alias, Tab) -> any()`

<a name="first-2"></a>

### first/2 ###

`first(Alias, Tab) -> any()`

<a name="fixtable-3"></a>

### fixtable/3 ###

`fixtable(Alias, Tab, Bool) -> any()`

<a name="handle_call-3"></a>

### handle_call/3 ###

`handle_call(X1, From, St) -> any()`

<a name="handle_cast-2"></a>

### handle_cast/2 ###

`handle_cast(X1, St) -> any()`

<a name="handle_info-2"></a>

### handle_info/2 ###

`handle_info(EXIT, St) -> any()`

<a name="index_is_consistent-3"></a>

### index_is_consistent/3 ###

`index_is_consistent(Alias, X2, Bool) -> any()`

<a name="info-3"></a>

### info/3 ###

`info(Alias, Tab, Item) -> any()`

<a name="init-1"></a>

### init/1 ###

`init(X1) -> any()`

<a name="init_backend-0"></a>

### init_backend/0 ###

`init_backend() -> any()`

<a name="insert-3"></a>

### insert/3 ###

`insert(Alias, Tab, Obj) -> any()`

<a name="is_index_consistent-2"></a>

### is_index_consistent/2 ###

`is_index_consistent(Alias, X2) -> any()`

<a name="ix_prefixes-3"></a>

### ix_prefixes/3 ###

`ix_prefixes(Tab, Pos, Obj) -> any()`

<a name="last-2"></a>

### last/2 ###

`last(Alias, Tab) -> any()`

<a name="load_table-4"></a>

### load_table/4 ###

`load_table(Alias, Tab, LoadReason, Opts) -> any()`

<a name="lookup-3"></a>

### lookup/3 ###

`lookup(Alias, Tab, Key) -> any()`

<a name="match_delete-3"></a>

### match_delete/3 ###

`match_delete(Alias, Tab, Pat) -> any()`

<a name="next-3"></a>

### next/3 ###

`next(Alias, Tab, Key) -> any()`

<a name="prev-3"></a>

### prev/3 ###

`prev(Alias, Tab, Key0) -> any()`

<a name="real_suffixes-0"></a>

### real_suffixes/0 ###

`real_suffixes() -> any()`

<a name="receive_data-5"></a>

### receive_data/5 ###

`receive_data(Data, Alias, Tab, Sender, State) -> any()`

<a name="receive_done-4"></a>

### receive_done/4 ###

`receive_done(Alias, Tab, Sender, State) -> any()`

<a name="receiver_first_message-4"></a>

### receiver_first_message/4 ###

`receiver_first_message(Pid, Msg, Alias, Tab) -> any()`

<a name="register-0"></a>

### register/0 ###

`register() -> any()`

<a name="register-1"></a>

### register/1 ###

`register(Alias) -> any()`

<a name="remove_aliases-1"></a>

### remove_aliases/1 ###

`remove_aliases(Aliases) -> any()`

<a name="repair_continuation-2"></a>

### repair_continuation/2 ###

`repair_continuation(Cont, Ms) -> any()`

<a name="select-1"></a>

### select/1 ###

`select(Cont) -> any()`

<a name="select-3"></a>

### select/3 ###

`select(Alias, Tab, Ms) -> any()`

<a name="select-4"></a>

### select/4 ###

`select(Alias, Tab, Ms, Limit) -> any()`

<a name="semantics-2"></a>

### semantics/2 ###

`semantics(Alias, X2) -> any()`

<a name="sender_handle_info-5"></a>

### sender_handle_info/5 ###

`sender_handle_info(Msg, Alias, Tab, ReceiverPid, Cont) -> any()`

<a name="sender_init-4"></a>

### sender_init/4 ###

`sender_init(Alias, Tab, RemoteStorage, Pid) -> any()`

<a name="show_table-1"></a>

### show_table/1 ###

`show_table(Tab) -> any()`

<a name="show_table-2"></a>

### show_table/2 ###

`show_table(Tab, Limit) -> any()`

<a name="slot-3"></a>

### slot/3 ###

`slot(Alias, Tab, Pos) -> any()`

<a name="start_proc-6"></a>

### start_proc/6 ###

`start_proc(Alias, Tab, Type, ProcName, Props, RdbOpts) -> any()`

<a name="sync_close_table-2"></a>

### sync_close_table/2 ###

`sync_close_table(Alias, Tab) -> any()`

<a name="terminate-2"></a>

### terminate/2 ###

`terminate(Reason, St) -> any()`

<a name="tmp_suffixes-0"></a>

### tmp_suffixes/0 ###

`tmp_suffixes() -> any()`

<a name="update_counter-4"></a>

### update_counter/4 ###

`update_counter(Alias, Tab, C, Val) -> any()`

<a name="validate_key-6"></a>

### validate_key/6 ###

`validate_key(Alias, Tab, RecName, Arity, Type, Key) -> any()`

<a name="validate_record-6"></a>

### validate_record/6 ###

`validate_record(Alias, Tab, RecName, Arity, Type, Obj) -> any()`

