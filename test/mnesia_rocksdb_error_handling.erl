-module(mnesia_rocksdb_error_handling).

-export([run/1,
         run/5]).


run(Config) ->
    setup(Config),
    %% run only one test for 'fatal', to save time.
    [run(Type, Op, L, MaintainSz, Config) || MaintainSz <- [false, true],
                                             Type <- [set, bag],
                                             Op <- [insert, update, delete],
                                             L <- levels()]
        ++ [run(set, insert, fatal, false, Config)].

run(Type, Op, Level, MaintainSz, Config) ->
    maybe_trace(Level, fun() -> run_(Type, Op, Level, MaintainSz, Config) end).

run_(Type, Op, Level, MaintainSz, Config) ->
    setup(Config),
    {ok, Tab} = create_tab(Type, Level, MaintainSz),
    mnesia:dirty_write({Tab, a, 1}),                    % pre-existing data
    with_mock(Level, Op, Tab, fun() ->
                                      try_write(Op, Type, Tab),
                                      expect_error(Level, Tab)
                              end).

%% maybe_trace(fatal, F) ->
%%     mnesia_rocksdb_tlib:trace(F, [{g,mrdb},{l,mnesia_rocksdb},{g,mnesia_rocksdb_lib}]);
maybe_trace(_, F) ->
    F().

levels() ->
    [debug, verbose, warning, error].

setup(Config) ->
    PrivD = proplists:get_value(priv_dir, Config, "."),
    Dir = filename:join(PrivD, "mnesia_EH"),
    application:set_env(mnesia, dir, Dir),
    mnesia:stop(),
    start_mnesia().

create_tab(Type, Level, MaintainSz) ->
    TabName = tab_name(Type, Level, MaintainSz),
    %% create error store before the table
    case ets:info(?MODULE) of
        undefined ->
            ?MODULE = ets:new(?MODULE, [bag, public, named_table]),
            ok;
        _ ->
            ok
    end,
    UserProps = user_props(Level, MaintainSz),
    {atomic, ok} = mnesia:create_table(TabName, [{rdb, [node()]},
                                                 {user_properties, UserProps}]),
    {ok, TabName}.

tab_name(Type, Level, MaintainSz) ->
    binary_to_atom(iolist_to_binary(
                     ["t" | [["_", atom_to_list(A)]
                                || A <- [?MODULE, Type, Level, MaintainSz]]]), utf8).

user_props(Level, MaintainSz) ->
    [{maintain_sz, MaintainSz},
     {rocksdb_opts, [ {on_write_error, Level}
                    , {on_write_error_store, ?MODULE} ]}].

start_mnesia() ->
    mnesia_rocksdb_tlib:start_mnesia(reset),
    ok.

with_mock(Level, Op, Tab, F) ->
    mnesia:subscribe(system),
    mnesia:set_debug_level(debug),
    meck:new(mrdb, [passthrough]),
    meck:expect(mrdb, insert, 2, {error, some_put_error}),
    meck:expect(mrdb, insert, 3, {error, some_put_error}),
    meck:expect(mrdb, write, 2, {error, some_write_error}),
    meck:expect(mrdb, write, 3, {error, some_write_error}),
    meck:expect(mrdb, delete, 2, {error,some_delete_error}),
    meck:expect(mrdb, delete, 3, {error,some_delete_error}),
    try {Level, Op, Tab, F()} of
        {_, _, _, ok} ->
            ok;
        Other ->
            io:fwrite("OTHER: ~p~n", [Other]),
            ok
    catch
        exit:{{aborted,_},_} ->
            Level = error,
            ok
    after
        mnesia:set_debug_level(none),
        mnesia:unsubscribe(system),
        meck:unload(mrdb)
    end.

try_write(insert, set, Tab) ->
    mnesia:dirty_write({Tab, b, 2});
try_write(insert, bag, Tab) ->
    mnesia:dirty_write({Tab, a, 2});
try_write(update, _, Tab) ->
    mnesia:dirty_write({Tab, a, 1});
try_write(delete, _, Tab) ->
    mnesia:dirty_delete({Tab, a}).


expect_error(Level, Tab) ->
    Tag = rpt_tag(Level),
    receive
        {mnesia_system_event, {mnesia_fatal, Fmt, Args, _Core}} ->
            Tag = mnesia_fatal,
            io:fwrite("EVENT(~p, ~p):~n  ~s", [Tag, Tab, io_lib:fwrite(Fmt, Args)]),
            ok;
        {mnesia_system_event, {Tag, Fmt, Args}} ->
            io:fwrite("EVENT(~p, ~p):~n  ~s", [Tag, Tab, io_lib:fwrite(Fmt, Args)]),
            ok
    after 1000 ->
            error({expected_error, [Level, Tab]})

    end,
    %% Also verify that an error entry has been written into the error store.
    1 = ets:select_delete(?MODULE, [{{{Tab, '_'}, '_', '_'}, [], [true]}]),
    ok.

rpt_tag(fatal  ) -> mnesia_fatal;
rpt_tag(error  ) -> mnesia_error;
rpt_tag(warning) -> mnesia_warning;
rpt_tag(verbose) -> mnesia_info;
rpt_tag(debug  ) -> mnesia_info.
