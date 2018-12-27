-module(iot_message_store).

%% API
-export([init/0]).

-export([insert/1,
         lookup/1,
         delete/1
]).

-define(TABLE_ID, ?MODULE).

init() ->
    ets:new(?TABLE_ID, [bag, public, named_table,{read_concurrency, true}]).

insert(Object) ->
    ets:insert(?TABLE_ID, Object),
    ok.

lookup(Key) ->
    case ets:lookup(?TABLE_ID, Key) of
        Info ->
            {ok, Info};
        [] ->
            {error, not_found}
    end.

delete(Key) ->
    ets:delete(?TABLE_ID, Key).







