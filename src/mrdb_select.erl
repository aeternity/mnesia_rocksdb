-module(mrdb_select).

-export([ select/3    %% (Ref, MatchSpec, Limit)
        , select/4    %% (Ref, MatchSpec, AccKeys, Limit)
        , select/1    %% (Cont)
        , fold/5      %% (Ref, Fun, Acc, MatchSpec, Limit)
        ]).

-import(mnesia_rocksdb_lib, [ keypos/1
                            , decode_key/2
                            , decode_val/3
                            ]).

-include("mnesia_rocksdb.hrl").

-record(sel, { alias                            % TODO: not used
             , tab
             , ref
             , keypat
             , ms                               % TODO: not used
             , compiled_ms
             , limit
             , key_only = false                 % TODO: not used
             , direction = forward              % TODO: not used
             }).

select(Ref, MS, Limit) when is_map(Ref), is_list(MS) ->
    select(Ref, MS, false, Limit).

select(Ref, MS, AccKeys, Limit)
  when is_map(Ref), is_list(MS), is_boolean(AccKeys) ->
    Sel = mk_sel(Ref, MS, Limit),
    mrdb:with_rdb_iterator(Ref, fun(I) -> i_select(I, Sel, AccKeys, []) end).

mk_sel(#{name := Tab} = Ref, MS, Limit) ->
    Keypat = keypat(MS, keypos(Tab), Ref),
    #sel{tab = Tab,
         ref = Ref,
         keypat = Keypat,
         ms = MS,
         compiled_ms = ets:match_spec_compile(MS),
         key_only = needs_key_only(MS),
         limit = Limit}.

select(Cont) ->
    case Cont of
        '$end_of_table' -> '$end_of_table';
        _               -> Cont()
    end.

fold(Ref, Fun, Acc, MS, Limit) ->
    {AccKeys, F} =
        if is_function(Fun, 3) ->
                {true, fun({K, Obj}, Acc1) ->
                               Fun(Obj, K, Acc1)
                       end};
           is_function(Fun, 2) ->
                {false, Fun};
           true ->
                mrdb:abort(invalid_fold_fun)
        end,
    fold_(select(Ref, MS, AccKeys, Limit), F, Acc).

fold_('$end_of_table', _, Acc) ->
    Acc;
fold_({L, Cont}, Fun, Acc) ->
    fold_(select(Cont), Fun, lists:foldl(Fun, Acc, L)).

i_select(I, #sel{ keypat = Pfx
                , compiled_ms = MS
                , limit = Limit
                , ref = #{vsn := Vsn, encoding := Enc} } = Sel, AccKeys, Acc) ->
    StartKey = case {Pfx, Vsn, Enc}  of
                   {<<>>, 1, {sext, _}} ->
                       <<?DATA_START>>;
                   {_, _, {term, _}} ->
                       <<>>;
                   _ ->
                       Pfx
               end,
    select_traverse(rocksdb:iterator_move(I, StartKey), Limit,
                    Pfx, MS, I, Sel, AccKeys, Acc).

needs_key_only([Pat]) ->
    needs_key_only_(Pat);
needs_key_only([_|_] = Pats) ->
    lists:all(fun needs_key_only_/1, Pats).

needs_key_only_({HP, _, Body}) ->
    BodyVars = lists:flatmap(fun extract_vars/1, Body),
    %% Note that we express the conditions for "needs more than key" and negate.
    not(wild_in_body(BodyVars) orelse
        case bound_in_headpat(HP) of
            {all,V} -> lists:member(V, BodyVars);
            Vars when is_list(Vars) -> any_in_body(lists:keydelete(2,1,Vars), BodyVars)
        end).

extract_vars([H|T]) ->
    extract_vars(H) ++ extract_vars(T);
extract_vars(T) when is_tuple(T) ->
    extract_vars(tuple_to_list(T));
extract_vars(T) when T=='$$'; T=='$_' ->
    [T];
extract_vars(T) when is_atom(T) ->
    case is_wild(T) of
        true ->
            [T];
        false ->
            []
    end;
extract_vars(_) ->
    [].

any_in_body(Vars, BodyVars) ->
    lists:any(fun({_,Vs}) ->
                      intersection(Vs, BodyVars) =/= []
              end, Vars).

intersection(A,B) when is_list(A), is_list(B) ->
    A -- (A -- B).

is_wild('_') ->
    true;
is_wild(A) when is_atom(A) ->
    case atom_to_list(A) of
        "\$" ++ S ->
            try begin
                    _ = list_to_integer(S),
                    true
                end
            catch
                error:_ ->
                    false
            end;
        _ ->
            false
    end.

wild_in_body(BodyVars) ->
    intersection(BodyVars, ['$$','$_']) =/= [].

bound_in_headpat(HP) when is_atom(HP) ->
    {all, HP};
bound_in_headpat(HP) when is_tuple(HP) ->
    [_|T] = tuple_to_list(HP),
    map_vars(T, 2).

map_vars([H|T], P) ->
    case extract_vars(H) of
        [] ->
            map_vars(T, P+1);
        Vs ->
            [{P, Vs}|map_vars(T, P+1)]
    end;
map_vars([], _) ->
    [].

select_traverse({ok, K, V}, Limit, Pfx, MS, I, #sel{ref = R} = Sel,
                AccKeys, Acc) ->
    case is_prefix(Pfx, K) of
        true ->
            DecKey = decode_key(K, R),
            Rec = decode_val(V, DecKey, R),
            case ets:match_spec_run([Rec], MS) of
                [] ->
                    select_traverse(
                      rocksdb:iterator_move(I, next), Limit, Pfx, MS,
                      I, Sel, AccKeys, Acc);
                [Match] ->
                    Acc1 = if AccKeys ->
                                  [{K, Match}|Acc];
                              true ->
                                  [Match|Acc]
                           end,
                    traverse_continue(K, decr(Limit), Pfx, MS, I, Sel, AccKeys, Acc1)
            end;
        false when Limit == infinity ->
            lists:reverse(Acc);
        false ->
            {lists:reverse(Acc), '$end_of_table'}
    end;
select_traverse({error, _}, Limit, _, _, _, _, _, Acc) ->
    select_return(Limit, {lists:reverse(Acc), '$end_of_table'}).

select_return(infinity, {L, '$end_of_table'}) ->
    L;
select_return(_, Ret) ->
    Ret.

is_prefix(A, B) when is_binary(A), is_binary(B) ->
    Sa = byte_size(A),
    case B of
        <<A:Sa/binary, _/binary>> ->
            true;
        _ ->
            false
    end.

decr(I) when is_integer(I) ->
    I-1;
decr(infinity) ->
    infinity.

traverse_continue(K, 0, Pfx, MS, _I, #sel{limit = Limit, ref = Ref} = Sel, AccKeys, Acc) ->
    {lists:reverse(Acc),
     fun() ->
             mrdb:with_rdb_iterator(
               Ref,
               fun(NewI) ->
                       select_traverse(iterator_next(NewI, K),
                                       Limit, Pfx, MS, NewI, Sel,
                                       AccKeys, [])
               end)
     end};
traverse_continue(_K, Limit, Pfx, MS, I, Sel, AccKeys, Acc) ->
    select_traverse(rocksdb:iterator_move(I, next), Limit, Pfx, MS, I, Sel, AccKeys, Acc).

iterator_next(I, K) ->
    case rocksdb:iterator_move(I, K) of
        {ok, K, _} ->
            rocksdb:iterator_move(I, next);
        Other ->
            Other
    end.

keypat([H|T], KeyPos, Ref) ->
    keypat(T, KeyPos, Ref, keypat_pfx(H, KeyPos, Ref)).

keypat(_, _, _, <<>>) -> <<>>;
keypat([H|T], KeyPos, Ref, Pfx0) ->
    Pfx = keypat_pfx(H, KeyPos, Ref),
    keypat(T, KeyPos, Ref, common_prefix(Pfx, Pfx0));
keypat([], _, _, Pfx) ->
    Pfx.

common_prefix(<<H, T/binary>>, <<H, T1/binary>>) ->
    <<H, (common_prefix(T, T1))/binary>>;
common_prefix(_, _) ->
    <<>>.

keypat_pfx({HeadPat,_Gs,_}, KeyPos, #{encoding := {sext,_}}) when is_tuple(HeadPat) ->
    KP      = element(KeyPos, HeadPat),
    sext:prefix(KP);
keypat_pfx(_, _, _) ->
    <<>>.

