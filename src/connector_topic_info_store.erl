-module(connector_topic_info_store).

-export([
         init/0,
         insert/1,
         lookup/1,
         delete/1,
         update/2,
         show/0
        ]).

-define(TAB_ID, ?MODULE).

init() ->
    ?TAB_ID = ets:new(?TAB_ID, [set, public, named_table, {read_concurrency, true}]).

insert(ObjectOrObjects) ->
    ets:insert(?TAB_ID, ObjectOrObjects).

lookup(Key) ->
    case ets:lookup(?TAB_ID, Key) of
        [] ->
            {error, not_found};
        [{Key, Value}] ->
            {ok, Value}
    end.

delete(Key) ->
    ets:delete(?TAB_ID, Key).

update(Key, ObjectOrObjects) ->
    delete(Key),
    insert(ObjectOrObjects).

show() ->
    case ets:tab2list(?TAB_ID) of
        Info ->
            {ok, Info};
        [] ->           
            {error, no_data}
    end.
