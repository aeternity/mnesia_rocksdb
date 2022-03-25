

# Mnesia Rocksdb - Rocksdb backend plugin for Mnesia #

Copyright (c) 2013-21 Klarna AB

__Authors:__ Ulf Wiger ([`ulf@wiger.net`](mailto:ulf@wiger.net)).

The Mnesia DBMS, part of Erlang/OTP, supports 'backend plugins', making
it possible to utilize more capable key-value stores than the `dets`
module (limited to 2 GB per table). Unfortunately, this support is
undocumented. Below, some informal documentation for the plugin system
is provided.


### <a name="Table_of_Contents">Table of Contents</a> ###


1. [Usage](https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/README.md#Usage)
1. [Prerequisites](https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/README.md#Prerequisites)
1. [Getting started](https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/README.md#Getting_started)
1. [Special features](https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/README.md#Special_features)
1. [Customization](https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/README.md#Customization)
1. [Handling of errors in write operations](https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/README.md#Handling_of_errors_in_write_operations)
1. [Caveats](https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/README.md#Caveats)

1. [Mnesia backend plugins](https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/README.md#Mnesia_backend_plugins)
1. [Background](https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/README.md#Background)
1. [Design](https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/README.md#Design)

1. [Mnesia index plugins](https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/README.md#Mnesia_index_plugins)

1. [Rocksdb](https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/README.md#Rocksdb)



### <a name="Usage">Usage</a> ###


#### <a name="Prerequisites">Prerequisites</a> ####

* rocksdb (included as dependency)

* sext (included as dependency)

* Erlang/OTP 21.0 or newer (https://github.com/erlang/otp)



#### <a name="Getting_started">Getting started</a> ####

Call `mnesia_rocksdb:register()` immediately after
starting mnesia.

Put `{rocksdb_copies, [node()]}` into the table definitions of
tables you want to be in RocksDB.


#### <a name="Special_features">Special features</a> ####

RocksDB tables support efficient selects on _prefix keys_.

The backend uses the `sext` module (see
[`https://github.com/uwiger/sext`](https://github.com/uwiger/sext)) for mapping between Erlang terms and the
binary data stored in the tables. This provides two useful properties:

* The records are stored in the Erlang term order of their keys.

* A prefix of a composite key is ordered just before any key for which
  it is a prefix. For example, `{x, '_'}` is a prefix for keys `{x, a}`,`{x, b}` and so on.


This means that a prefix key identifies the start of the sequence of
entries whose keys match the prefix. The backend uses this to optimize
selects on prefix keys.

### Customization

RocksDB supports a number of customization options. These can be specified
by providing a `{Key, Value}` list named `rocksdb_opts` under `user_properties`,
for example:

```
mnesia:create_table(foo, [{rocksdb_copies, [node()]},
                          ...
                          {user_properties,
                              [{rocksdb_opts, [{max_open_files, 1024}]}]
                          }])
```

Consult the [RocksDB documentation](https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
for information on configuration parameters. Also see the section below on handling write errors.

The default configuration for tables in `mnesia_rocksdb` is:

```
default_open_opts() ->
    [ {create_if_missing, true}
      , {cache_size,
         list_to_integer(get_env_default("ROCKSDB_CACHE_SIZE", "32212254"))}
      , {block_size, 1024}
      , {max_open_files, 100}
      , {write_buffer_size,
         list_to_integer(get_env_default(
                           "ROCKSDB_WRITE_BUFFER_SIZE", "4194304"))}
      , {compression,
         list_to_atom(get_env_default("ROCKSDB_COMPRESSION", "true"))}
      , {use_bloomfilter, true}
    ].
```

It is also possible, for larger databases, to produce a tuning parameter file.
This is experimental, and mostly copied from `mnesia_leveldb`. Consult the
source code in `mnesia_rocksdb_tuning.erl` and `mnesia_rocksdb_params.erl`.
Contributions are welcome.


#### <a name="Caveats">Caveats</a> ####

Avoid placing `bag` tables in RocksDB. Although they work, each write
requires additional reads, causing substantial runtime overheads. There
are better ways to represent and process bag data (see above about
_prefix keys_).

The `mnesia:table_info(T, size)` call always returns zero for RocksDB
tables. RocksDB itself does not track the number of elements in a table, and
although it is possible to make the `mnesia_rocksdb` backend maintain a size
counter, it incurs a high runtime overhead for writes and deletes since it
forces them to first do a read to check the existence of the key. If you
depend on having an up to date size count at all times, you need to maintain
it yourself. If you only need the size occasionally, you may traverse the
table to count the elements.


### <a name="Mnesia_backend_plugins">Mnesia backend plugins</a> ###


#### <a name="Background">Background</a> ####

Mnesia was initially designed to be a RAM-only DBMS, and Erlang's
`ets` tables were developed for this purpose. In order to support
persistence, e.g. for configuration data, a disk-based version of `ets`
(called `dets`) was created. The `dets` API mimicks the `ets` API,
and `dets` is quite convenient and fast for (nowadays) small datasets.
However, using a 32-bit bucket system, it is limited to 2GB of data.
It also doesn't support ordered sets. When used in Mnesia, dets-based
tables are called `disc_only_copies`.

To circumvent these limitations, another table type, called `disc_copies`
was added. This is a combination of `ets` and `disk_log`, where Mnesia
periodically snapshots the `ets` data to a log file on disk, and meanwhile
maintains a log of updates, which can be applied at startup. These tables
are quite performant (especially on read access), but all data is kept in
RAM, which can become a serious limitation.

A backend plugin system was proposed by Ulf Wiger in 2016, and further
developed with Klarna's support, to finally become included in OTP 19.
Klarna uses a LevelDb backend, but Aeternity, in 2017, instead chose
to implement a Rocksdb backend plugin.


### <a name="Design">Design</a> ###

As backend plugins were added on a long-since legacy-stable Mnesia,
they had to conform to the existing code structure. For this reason,
the plugin callbacks hook into the already present low-level access
API in the `mnesia_lib` module. As a consequence, backend plugins have
the same access semantics and granularity as `ets` and `dets`. This
isn't much of a disadvantage for key-value stores like LevelDb and RocksDB,
but a more serious issue is that the update part of this API is called
on _after_ the point of no return. That is, Mnesia does not expect
these updates to fail, and has no recourse if they do. As an aside,
this could also happen if a `disc_only_copies` table exceeds the 2 GB
limit (mnesia will not check it, and `dets` will not complain, but simply
drop the update.)


### <a name="Mnesia_index_plugins">Mnesia index plugins</a> ###

When adding support for backend plugins, index plugins were also added. Unfortunately, they remain undocumented.

An index plugin can be added in one of two ways:

1. When creating a schema, provide `{index_plugins, [{Name, Module, Function}]}` options.

1. Call the function `mnesia_schema:add_index_plugin(Name, Module, Function)`


`Name` must be an atom wrapped as a 1-tuple, e.g. `{words}`.

The plugin callback is called as `Module:Function(Table, Pos, Obj)`, where `Pos=={words}` in
our example. It returns a list of index terms.

<strong>Example</strong>

Given the following index plugin implementation:

```
-module(words).
-export([words_f/3]).

words_f(_,_,Obj) when is_tuple(Obj) ->
    words_(tuple_to_list(Obj)).

words_(Str) when is_binary(Str) ->
    string:lexemes(Str, [$\s, $\n, [$\r,$\n]]);
words_(L) when is_list(L) ->
    lists:flatmap(fun words_/1, L);
words_(_) ->
    [].
```

We can register the plugin and use it in table definitions:

```
Eshell V12.1.3  (abort with ^G)
1> mnesia:start().
ok
2> mnesia_schema:add_index_plugin({words}, words, words_f).
{atomic,ok}
3> mnesia:create_table(i, [{index, [{words}]}]).
{atomic,ok}
```

Note that in this case, we had neither a backend plugin, nor even a persistent schema.
Index plugins can be used with all table types. The registered indexing function (arity 3) must exist
as an exported function along the node's code path.

To see what happens when we insert an object, we can turn on call trace.

```
4> dbg:tracer().
{ok,<0.108.0>}
5> dbg:tp(words, x).
{ok,[{matched,nonode@nohost,3},{saved,x}]}
6> dbg:p(all,[c]).
{ok,[{matched,nonode@nohost,60}]}
7> mnesia:dirty_write({i,<<"one two">>, [<<"three">>, <<"four">>]}).
(<0.84.0>) call words:words_f(i,{words},{i,<<"one two">>,[<<"three">>,<<"four">>]})
(<0.84.0>) returned from words:words_f/3 -> [<<"one">>,<<"two">>,<<"three">>,
                                             <<"four">>]
(<0.84.0>) call words:words_f(i,{words},{i,<<"one two">>,[<<"three">>,<<"four">>]})
(<0.84.0>) returned from words:words_f/3 -> [<<"one">>,<<"two">>,<<"three">>,
                                             <<"four">>]
ok
8> dbg:ctp('_'), dbg:stop().
ok
9> mnesia:dirty_index_read(i, <<"one">>, {words}).
[{i,<<"one two">>,[<<"three">>,<<"four">>]}]
```

(The fact that the indexing function is called twice, seems like a performance bug.)

We can observe that the indexing callback is able to operate on the whole object.
It needs to be side-effect free and efficient, since it will be called at least once for each update
(if an old object exists in the table, the indexing function will be called on it too, before it is
replaced by the new object.)


### <a name="Rocksdb">Rocksdb</a> ###


### <a name="Usage">Usage</a> ###



## Modules ##


<table width="100%" border="0" summary="list of modules">
<tr><td><a href="https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/mnesia_rocksdb.md" class="module">mnesia_rocksdb</a></td></tr>
<tr><td><a href="https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/mnesia_rocksdb_admin.md" class="module">mnesia_rocksdb_admin</a></td></tr>
<tr><td><a href="https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/mnesia_rocksdb_app.md" class="module">mnesia_rocksdb_app</a></td></tr>
<tr><td><a href="https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/mnesia_rocksdb_lib.md" class="module">mnesia_rocksdb_lib</a></td></tr>
<tr><td><a href="https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/mnesia_rocksdb_params.md" class="module">mnesia_rocksdb_params</a></td></tr>
<tr><td><a href="https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/mnesia_rocksdb_sup.md" class="module">mnesia_rocksdb_sup</a></td></tr>
<tr><td><a href="https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/mnesia_rocksdb_tuning.md" class="module">mnesia_rocksdb_tuning</a></td></tr>
<tr><td><a href="https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/mrdb.md" class="module">mrdb</a></td></tr>
<tr><td><a href="https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/mrdb_index.md" class="module">mrdb_index</a></td></tr>
<tr><td><a href="https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/mrdb_mutex.md" class="module">mrdb_mutex</a></td></tr>
<tr><td><a href="https://github.com/aeternity/mnesia_rocksdb/blob/g3553-refactor-plugin-migration-tmp-220318/doc/mrdb_select.md" class="module">mrdb_select</a></td></tr></table>

