-module(mrdb_bench).

-compile(export_all).

init() ->
    mnesia:delete_schema([node()]),
    mnesia_rocksdb:create_schema([node()]),
    mnesia:start(),
    [mnesia:create_table(Name, [{Type, [node()]}, {record_name, r}])
     || {Name, Type} <- tabs()],
    ok.

tabs() ->
    [{rc, ram_copies},
     {dc, disc_copies},
     {do, disc_only_copies},
     {rocks, rocksdb_copies},
     {rdb, rocksdb_copies}].

fill(N) ->
    [{T, timer:tc(fun() -> fill_(T, N) end)} || {T,_} <- tabs()].

fill_(_, 0) ->
    ok;
fill_(T, N) when N > 0 ->
    write(T, {r, N, <<"1234567890">>}),
    fill_(T, N-1).

write(rdb, Obj) ->
    mrdb:insert(rdb, Obj);
write(T, Obj) ->
    mnesia:dirty_write(T, Obj).


fold() ->
    [{T, timer:tc(fun() -> fold_(T) end)} || {T,_} <- tabs()].

fold_(rdb) ->
    mrdb:fold(rdb, fun(_, Acc) -> Acc end, ok);
fold_(T) ->
    mnesia:activity(
      async_dirty,
      fun() ->
	      mnesia:foldl(fun(_, Acc) -> Acc end, ok, T)
      end).
				    
tx(N) ->
    [{T, timer:tc(fun() -> tx_(T, N) end)} || {T,_} <- tabs()].

%% tx_(T, N) ->
%% tx_(T, N, N, 10).

tx_(_, 0) -> ok;
tx_(T, N) when N > 0 ->
    one_tx(T, N),
    tx_(T, N-1).

one_tx(rdb, N) ->
    mrdb:activity(transaction, rocksdb_copies,
		  fun() ->
			  [{r, N, Str}] = mrdb:read(rdb, N),
			  Str2 = <<Str/binary, "x">>,
			  mrdb:insert(rdb, {r, N, Str2}),
			  [{r, N, Str2}] = mrdb:read(rdb, N)
		  end);
one_tx(T, N) ->
    mnesia:activity(transaction,
		    fun() ->
			    [{r, N, Str}] = mnesia:read(T, N),
			    Str2 = <<Str/binary, "x">>,
			    mnesia:write(T, {r, N, Str2}, write),
			    [{r, N, Str2}] = mnesia:read(T, N)
		    end).
			   
