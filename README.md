# mnesia_rocksdb
A RocksDb backend for Mnesia

This permits Erlang/OTP applications to use RocksDB as a backend for
mnesia tables. It is based on Klarna's `mnesia_eleveldb`.

## Prerequisites
- rocksdb
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
