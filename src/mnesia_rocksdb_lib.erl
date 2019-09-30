%%% @doc RocksDB update  wrappers, in separate module for easy tracing and mocking.
%%%
-module(mnesia_rocksdb_lib).

-export([put/4,
         write/3,
         delete/3]).


put(Ref, K, V, Opts) ->
    rocksdb:put(Ref, K, V, Opts).

write(Ref, L, Opts) ->
    rocksdb:write(Ref, L, Opts).

delete(Ref, K, Opts) ->
    rocksdb:delete(Ref, K, Opts).
