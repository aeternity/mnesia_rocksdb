# mnesia_rocksdb

A RocksDB backend for Mnesia.

This permits Erlang/OTP applications to use RocksDB as a backend for
mnesia tables. It is based on Klarna's `mnesia_eleveldb`.

## Prerequisites

- rocksdb (included as dependency)
- Erlang/OTP 20.0 or newer (https://github.com/erlang/otp)

## Getting started

Call `mnesia_rocksdb:register()` immediately after
starting mnesia.

Put `{rocksdb_copies, [node()]}` into the table definitions of
tables you want to be in RocksDB.

## Special features

RocksDB tables support efficient selects on *prefix keys*.

The backend uses the `sext` module (see
https://github.com/uwiger/sext) for mapping between Erlang terms and the
binary data stored in the tables. This provides two useful properties:

- The records are stored in the Erlang term order of their keys.
- A prefix of a composite key is ordered just before any key for which
  it is a prefix. For example, `{x, '_'}` is a prefix for keys `{x, a}`,
  `{x, b}` and so on.

This means that a prefix key identifies the start of the sequence of
entries whose keys match the prefix. The backend uses this to optimize
selects on prefix keys.

## Customization

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

## Handling of errors in write operations

The RocksDB update operations return either `ok` or `{error, any()}`.
Since the actual updates are performed after the 'point-of-no-return',
returning an `error` result will cause mnesia to behave unpredictably,
since the operations are expected to simply work.

### Option 1: `on_write_error`

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

### Option 2: `on_write_error_store`

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

## Caveats

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
