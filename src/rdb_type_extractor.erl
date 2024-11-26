-module(rdb_type_extractor).
-compile({parse_transform, ct_expand}).

-export([cf_opts_allowed/0, open_opts_allowed/0, extract/2]).

cf_opts_allowed() -> ct_expand:term(extract(rocksdb, cf_options)).
open_opts_allowed() -> ct_expand:term(extract(rocksdb, db_options)).

-spec extract(Module, TypeSpecTarget) -> Result when
    Module          :: module(),
    TypeSpecTarget  :: atom(),
    Result          :: [atom()].

extract(Module, TypeSpecTarget) ->
    case beam_lib:chunks(code:which(Module), [abstract_code]) of
        {ok, {_Module, [{abstract_code, {raw_abstract_v1, Forms}}]}} ->
            TypeForms = lists:filter(fun(AstForm) -> is_target_type(TypeSpecTarget, AstForm) end, Forms),
            case TypeForms of
                [Form] ->
                    extract_keys(TypeSpecTarget, Form);
                _ ->
                    error(target_type_not_found)
            end;
        _ ->
            error(abstract_code_not_found)
    end.

-spec is_target_type(Target, AstForm) -> Result when
    Target  :: atom(),
    AstForm :: tuple(),
    Result  :: boolean().

is_target_type(Target, {attribute, _, type, {Target, _, _}}) -> true;
is_target_type(_Target, _AstForm) -> false.

-spec extract_keys(Target, AstForm) -> Result when
    Target  :: atom(),
    AstForm :: tuple(),
    Result  :: [atom()].

extract_keys(Target, {attribute, _, type, {Target, {type, _, list, [{type, _, union, Tuples }]},[]}}) ->
    lists:flatmap(fun
		({type, _, tuple, [{atom, _, Key}, _Type]}) -> [Key];
		(_) -> []
	end, Tuples).
