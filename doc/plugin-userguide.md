

# Using Mnesia Plugins #

Copyright (c) 2017-21 Aeternity Anstalt. All Rights Reserved.

__Authors:__ Ulf Wiger ([`ulf@wiger.net`](mailto:ulf@wiger.net)).

The Mnesia DBMS, part of Erlang/OTP, supports 'backend plugins', making
it possible to utilize more capable key-value stores than the `dets`
module (limited to 2 GB per table). Unfortunately, this support is
undocumented. Below, some informal documentation for the plugin system
is provided.

This user guide illustrates these concepts using `mnesia_rocksdb`
as an example.

We will deal with two types of plugin:
1. backend plugins
2. index plugins

A backend plugin is a module that implements a `mnesia_backend_type`
behavior. Each plugin can support any number of `aliases`, which
combined with the plugin module make up a `backend_type`.

When using `mnesia_rocksdb`, the default alias is `rocksdb_copies`,
and it is registered as a `{rocksdb_copies, mnesia_rocksdb}` pair.
Once registered, the alias can be used just like the built-in
backend types `ram_copies`, `disc_copies`, `disc_only_copies`.
Mnesia asks the plugin module which one of the built-in types'
semantics the new type is supposed to mimick: ram-only, ram+disk
or disk-only. This is mainly relevant for how Mnesia checkpoints and
backs up data.

### <a name="Table_of_Contents">Table of Contents</a> ###

1. [Usage](#Usage)
   1. [Prerequisites](#Prerequisites)
   2. [Getting started](#Getting_started)
   3. [New indexing functionality](#New_indexing_functionality)

## Usage

### Prerequisites

- rocksdb (included as dependency)
- sext (included as dependency)
- Erlang/OTP 21.0 or newer (https://github.com/erlang/otp)

### Getting started

For the purposes of this user guide, we assume an unnamed, single node
mnesia installation. The only place where plugins are affected by
distributed Mnesia, is in the table sync callbacks. The simplest way
to get all paths in order for experimentation is to check out
`mnesia_rocksdb`, building it, and then calling `rebar3 shell`. Unless
we note otherwise, this is how a node has been started for each example.

> Erlang shell interactions have been slightly beautified by eliding
> some text and breaking and indenting some lines

#### Adding a backend type to mnesia

There are three different ways, all undocumented, to register a
backend plugin in mnesia:

1. Add a `backend_types` option when creating the schema, using
   `mnesia:create_schema/2`

```erlang
Erlang/OTP 22 [erts-10.7] ...

Eshell V10.7  (abort with ^G)
1> mnesia:create_schema([node()],
                        [{backend_types,[{rocksdb_copies,mnesia_rocksdb}]}]).
ok
2> mnesia:start().
ok
3> mnesia_schema:backend_types().
[ram_copies,disc_copies,disc_only_copies,rocksdb_copies]
```

2. Add it when starting mnesia, using `mnesia:start/1` (undocumented)

```erlang
Eshell V10.7  (abort with ^G)
1> mnesia:create_schema([node()]).
ok
2> mnesia:start([{schema,[{backend_types,
                           [{rocksdb_copies,mnesia_rocksdb}]}]}]).
ok
3> mnesia_schema:backend_types().
[ram_copies,disc_copies,disc_only_copies]
```

3. Call `mnesia_schema:add_backend_type/2` when mnesia is running.

```erlang
Eshell V10.7  (abort with ^G)
1> mnesia:create_schema([node()]).
ok
2> mnesia:start().
ok
3> mnesia_schema:add_backend_type(rocksdb_copies,mnesia_rocksdb).
{atomic,ok}
4> mnesia_schema:backend_types().
[ram_copies,disc_copies,disc_only_copies,rocksdb_copies]
```

In all cases the schema is updated, and other nodes, and subsequently
added nodes, will automatically receive the information.
The function `mnesia_schema:backend_types()` shows which backend plugin
aliases are registered.

The information is also displayed when calling `mnesia:info()`:

```erlang
5> mnesia:info().
---> Processes holding locks <---
---> Processes waiting for locks <---
---> Participant transactions <---
---> Coordinator transactions <---
---> Uncertain transactions <---
---> Active tables <---
schema         : with 1        records occupying 443      words of mem
===> System info in version "4.16.3", debug level = none <===
opt_disc. Directory "/.../Mnesia.nonode@nohost" is used.
use fallback at restart = false
running db nodes   = [nonode@nohost]
stopped db nodes   = []
master node tables = []
backend types      = rocksdb_copies - mnesia_rocksdb
remote             = []
ram_copies         = []
disc_copies        = [schema]
disc_only_copies   = []
[{nonode@nohost,disc_copies}] = [schema]
2 transactions committed, 0 aborted, 0 restarted, 0 logged to disc
0 held locks, 0 in queue; 0 local transactions, 0 remote
0 transactions waits for other nodes: []
ok
```

To illustrate how mnesia persists the information in the schema:

```erlang
6> mnesia:table_info(schema,user_properties).
[{mnesia_backend_types,[{rocksdb_copies,mnesia_rocksdb}]}]
```

#### Rocksdb registration shortcut

Call `mnesia_rocksdb:register()` after starting mnesia.

#### Creating a table

Put `{rocksdb_copies, [node()]}` into the table definitions of
tables you want to be in RocksDB.

```erlang
4> mnesia:create_table(t, [{rocksdb_copies,[node()]}]).
{atomic,ok}
5> rr(mnesia).
[commit,cstruct,cyclic,decision,log_header,mnesia_select,
 tid,tidstore]
6> mnesia:table_info(t,cstruct).
#cstruct{name = t,type = set,ram_copies = [],
         disc_copies = [],disc_only_copies = [],
         external_copies = [{{rocksdb_copies,mnesia_rocksdb},
                             [nonode@nohost]}],
         load_order = 0,access_mode = read_write,majority = false,
         index = [],snmp = [],local_content = false,record_name = t,
         attributes = [key,val],
         user_properties = [],frag_properties = [],
         storage_properties = [],
         cookie = {{1621758137965715000,-576460752303423420,1},
                   nonode@nohost},
         version = {{2,0},[]}}
```

In the example above, we take a peek at the `cstruct`, which is the
internal metadata structure for mnesia tables. The attribute showing
that the table has been created with a `rocksdb_copies` instance, is
the `external_copies` attribute. It lists the alias, the callback module
and the nodes, where the instances reside.

The table works essentially like one of the built-in table types.
If we want to find out which type, we can query the callback module:

```erlang
8> mnesia_rocksdb:semantics(rocksdb_copies, storage).
disc_only_copies
```

Consult the `mnesia_rocksdb` man page for more info on the
`Mod:semantics/2` function.

### New indexing functionality

With the introduction of backend plugins, a few improvements were made
to mnesia's indexing support.

#### Persistent indexes

In the past, and still with the built-in types, indexes were always
rebuilt on startup. Since backend plugins were introduced mainly in
order to support very large tables, a couple of callback functions
were added in order to detect whether a full rebuild is needed.

> The callback functions are `Mod:is_index_consistent/2` and
> `Mod:index_is_consistent/3`.
> The first function (figuratively) always returns `false` for indexes
> on built-in table types. Backend plugin modules should always return
> `false` if they have no information. After building the index, mnesia
> calls `Mod:index_is_consistent(Alias, IxTab, true)`, and the callback
> is expected to persist this information. `IxTab`, in this case, is
> a logical name for the index 'table': `{Tab, index, PosInfo}`

#### Ordered indexes

A problem in the past with mnesia indexing has been that indexes with
very large fan-out were inefficient. Indexes were represented as `bag`
tables, and the cost of inserting a secondary key was proportional to
the number of identical secondary keys already in the index.

When adding the backend plugin support - also not least because the
first candidate LevelDb didn't do bags well - support for ordered
indexes was added. They turn out to be have much more stable performance
for indexes with large fan-out. They also work on all built-in table
types.

When creating an index, you can specify the type of index as `bag` or
`ordered`. If you omit the type, it will default to `bag` for built-in
table types, and for external types, whatever is the first type in the
list of supported index types returned by `Mod:semantics(Alias, index_types)`.

> For `mnesia_rocksdb`, only `ordered` is supported, but a bug in mnesia
> makes it ignore this, and try to create a bag index anyway. The
> `mnesia_rocksdb` plugin rejects this.
> Note that while e.g. mnesia_rocksdb supports regular bag tables, they are not
> efficiently implemented.

Mnesia currently doesn't allow specifying an index type in
`mnesia:add_table_index/2`, so simply indicate the index position,
and let the backend choose the default.

Having ordered indexes opens up for some new possibilities, but
there are currently no functions in mnesia such as index_first, index_next
etc., or performing a select in index order.

#### Index plugins

Index plugins are a great new feature, also almost entirely undocumented.

An index plugin is a registered indexing function, which can operate
on the entire object, and shall return a list of secondary keys.
When registering an index plugin, it is given an alias, a callback module,
and an function name, not unlike backend plugins. The index plugin alias
must be an atom wrapped inside a 1-tuple, i.e. `{atom()}`.

To illustrate, we use a sample indexing function implemented in
mnesia_rocksdb, which checks all non-key attributes of an object,
and for each value that is a list, makes each list element a secondary
key value.

```erlang
9> mnesia_schema:add_index_plugin({lv}, mnesia_rocksdb, ix_listvals). 
{atomic,ok}
10> mnesia:add_table_index(t,{lv}).
{atomic,ok}
11> mnesia:dirty_write({t,1,[a,b]}).
ok
12> mnesia:dirty_write({t,2,[b,c]}).
ok
13> mnesia:dirty_index_read(t,a,{lv}).
[{t,1,[a,b]}]
14> mnesia:dirty_index_read(t,b,{lv}).
[{t,1,[a,b]},{t,2,[b,c]}]
15> mnesia:dirty_index_read(t,c,{lv}).
[{t,2,[b,c]}]
```

For clarity, this is the implementation of the index callback:

```erlang
ix_listvals(_Tab, _Pos, Obj) ->
    lists:foldl(
      fun(V, Acc) when is_list(V) ->
              V ++ Acc;
         (_, Acc) ->
              Acc
      end, [], tl(tuple_to_list(Obj))).
```

Note that the index callback must be a pure function, as it
is also relied upon when deleting objects. That is, it must
always return the same values when called with a specific
set of input arguments.
