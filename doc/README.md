

# Mnesia Rocksdb - Rocksdb backend plugin for Mnesia #

Copyright (c) 2017-21 Aeternity Anstalt. All Rights Reserved.

__Authors:__ Ulf Wiger ([`ulf@wiger.net`](mailto:ulf@wiger.net)).

The Mnesia DBMS, part of Erlang/OTP, supports 'backend plugins', making
it possible to utilize more capable key-value stores than the `dets`
module (limited to 2 GB per table). Unfortunately, this support is
undocumented. Below, some informal documentation for the plugin system
is provided.


### <a name="Table_of_Contents">Table of Contents</a> ###

1. [Usage](#Usage)
   1. [Prerequisites](#Prerequisites)
   2. [Getting started](#Getting_started)
   3. [Special features](#Special_features)
   4. [Customization](#Customization)
   5. [Handling of errors in write operations](#Handling_of_errors_in_write_operations)
   6. [Caveats](#Caveats)
2. [Mnesia backend plugins](#Mnesia_backend_plugins)
   1. [Background](#Background)
   2. [Design](#Design)
3. [Mnesia index plugins](#Mnesia_index_plugins)
4. [Rocksdb](#Rocksdb)

## Usage

### Prerequisites

- rocksdb (included as dependency)
- sext (included as dependency)
- Erlang/OTP 21.0 or newer (https://github.com/erlang/otp)

### Getting started

Call `mnesia_rocksdb:register()` immediately after
starting mnesia.

Put `{rocksdb_copies, [node()]}` into the table definitions of
tables you want to be in RocksDB.

### Special features

RocksDB tables support efficient selects on *prefix keys*.

The backend uses the `sext` module (see
https://github.com/uwiger/sext) for mapping between Erlang terms and the
binary data stored in the tables. This provides two useful properties:

- The records are stored in the Erlang term order of their keys.
- A prefix of a composite key is ordered just before any key for which
  it is a prefix. For example, `{x,`_'}`is a prefix for keys `{x, a}`,
  `{x, b}` and so on.

This means that a prefix key identifies the start of the sequence of
entries whose keys match the prefix. The backend uses this to optimize
selects on prefix keys.

### Customization

RocksDB supports a number of customization options. These can be specified
by providing a `{Key, Value}` list named `rocksdb_opts` under `user_properties`,
for example:

```erlang
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

### Handling of errors in write operations

The RocksDB update operations return either `ok` or `{error, any()}`.
Since the actual updates are performed after the`point-of-no-return',
returning an `error` result will cause mnesia to behave unpredictably,
since the operations are expected to simply work.

#### Option 1: `on_write_error` ===

An `on_write_error` option can be provided, per-table, in the `rocksdb_opts`
user property (see [Customization](#customization) above).
Supported values indicate at which level an error indication should be reported.
Mnesia may save reported events in RAM, and may also print them,
depending on the debug level (controlled with `mnesia:set_debug_level/1`).

Mnesia debug levels are, in increasing detail, `none | verbose | debug | trace`
The supported values for `on_write_error` are:

 | Value   | Saved at debug level | Printed at debug level | Action    |
 | ------- | -------------------- | ---------------------- | --------- |
 | debug   | unless none          | verbose, debug, trace  | ignore    |
 | verbose | unless none          | verbose, debug, trace  | ignore    |
 | warning | always               | always                 | ignore    |
 | error   | always               | always                 | exception |
 | fatal   | always               | always                 | core dump |

#### Option 2: `on_write_error_store`

An `on_write_error_store` option can be provided, per-table, in the `rocksdb_opts`
user property (see [Customization](#customization) above).
When set, the backend will use the value of the option as the name for an ETS table
which is used as storage for runtime write errors. The table must be set up outside
of the backend by the clients themselves.

Entries to the table are in the form of a tuple `{{Table, Key}, Error, InsertedAt}`
where `Table` refers to the Mnesia table name, `Key` is the primary key being used by Mnesia,
`Error` is the error encountered by the backend, and `InsertedAt` refers to the time
the error was encountered as system time in milliseconds.

The backend will only insert entries and otherwise not manage the table. Thus, clients
are expected to clean up the table during runtime to prevent memory leakage.

### Caveats

Avoid placing `bag` tables in RocksDB. Although they work, each write
requires additional reads, causing substantial runtime overheads. There
are better ways to represent and process bag data (see above about
*prefix keys*).

The `mnesia:table_info(T, size)` call always returns zero for RocksDB
tables. RocksDB itself does not track the number of elements in a table, and
although it is possible to make the mnesia_rocksdb backend maintain a size
counter, it incurs a high runtime overhead for writes and deletes since it
forces them to first do a read to check the existence of the key. If you
depend on having an up to date size count at all times, you need to maintain
it yourself. If you only need the size occasionally, you may traverse the
table to count the elements.

## Mnesia backend plugins

### Background

Mnesia was initially designed to be a RAM-only DBMS, and Erlang`s
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

A backend plugin system was proposed by Ulf Wiger in YYYY, and further
developed with Klarna's support, to finally become included in OTP VV.
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


### <a name="Rocksdb">Rocksdb</a> ###


### <a name="Usage">Usage</a> ###



## Modules ##


<table width="100%" border="0" summary="list of modules">
<tr><td><a href="mnesia_rocksdb.md" class="module">mnesia_rocksdb</a></td></tr>
<tr><td><a href="mnesia_rocksdb_admin.md" class="module">mnesia_rocksdb_admin</a></td></tr>
<tr><td><a href="mnesia_rocksdb_app.md" class="module">mnesia_rocksdb_app</a></td></tr>
<tr><td><a href="mnesia_rocksdb_lib.md" class="module">mnesia_rocksdb_lib</a></td></tr>
<tr><td><a href="mnesia_rocksdb_params.md" class="module">mnesia_rocksdb_params</a></td></tr>
<tr><td><a href="mnesia_rocksdb_sup.md" class="module">mnesia_rocksdb_sup</a></td></tr>
<tr><td><a href="mnesia_rocksdb_tuning.md" class="module">mnesia_rocksdb_tuning</a></td></tr>
<tr><td><a href="mrdb.md" class="module">mrdb</a></td></tr>
<tr><td><a href="mrdb_select.md" class="module">mrdb_select</a></td></tr></table>

