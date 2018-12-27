-module(connector_eqpt_id_store).

-export([
         init/1,
         insert/1,
         match_object/1,
         lookup/1
        ]).

-define(TAB_ID, ?MODULE).

init(KeyPos) ->
    ?TAB_ID = ets:new(?TAB_ID, [bag, protected, named_table, {read_concurrency, true}, {keypos, KeyPos}]).

insert(Object) ->
    ets:insert(?TAB_ID, Object).

lookup(Key) ->
    case ets:lookup(?TAB_ID, Key) of
        [] ->
            {ok, []};
        Object ->
            {ok, Object}
    end.

match_object(Pattern) ->
    case ets:match_object(?TAB_ID, Pattern) of
        [] ->
            {error, not_found};
        [Obj | _] ->
            {ok, Obj}
    end.

    
