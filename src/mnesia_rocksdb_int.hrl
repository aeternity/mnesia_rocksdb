-include_lib("hut/include/hut.hrl").

%% enable debugging messages through mnesia:set_debug_level(debug)
-ifndef(MNESIA_ROCKSDB_NO_DBG).
-define(dbg(Fmt, Args), ?log(debug, Fmt, Args)).
%% -define(dbg(Fmt, Args),
%%         %% avoid evaluating Args if the message will be dropped anyway
%%         case mnesia_monitor:get_env(debug) of
%%             none -> ok;
%%             verbose -> ok;
%%             _ -> mnesia_lib:dbg_out("~p:~p: "++(Fmt),[?MODULE,?LINE|Args])
%%         end).
-else.
-define(dbg(Fmt, Args), ok).
-endif.
