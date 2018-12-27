-module(connector_gateway_id_store).

-export([init/0]).
-export([insert/1,
         lookup/1
         ]).

-define(TABLE_ID, ?MODULE).

init() ->
    ets:new(?TABLE_ID, [set, public, named_table]),
    ok.

insert(GatewayCode) ->
    ets:insert(?TABLE_ID, {GatewayCode}),
    ok.

lookup(GatewayCode) ->
    case ets:lookup(?TABLE_ID, GatewayCode) of
        [{GatewayCode}] ->
            ok;
        [] ->
            {error, not_found}    
    end.
