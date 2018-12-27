-module(connector_gateway_status_store).

-export([init/0]).
-export([insert/6,
         lookup/1,
         update/2,
         delete/1,
         show/1,
         show/0
         ]).

-define(TABLE_ID, ?MODULE).

init() ->
    ets:new(?TABLE_ID, [set, public, named_table]),
    ok.

insert(GatewayCode, GatewayType, Status, Time, Task, Creator) ->
    ets:insert(?TABLE_ID, {GatewayCode, GatewayType, Status, Time, Task, Creator}),
    ok.

lookup(GatewayCode) ->
    case ets:lookup(?TABLE_ID, GatewayCode) of
        [{GatewayCode, GatewayType, Status, Time, Task, Creator}] ->
            {ok, Task};
        [] ->
            {error, not_found}    
    end.

delete(GatewayCode) ->
    ets:delete(?TABLE_ID, GatewayCode).

update(GatewayCode, Task) ->
    ets:update_element(?TABLE_ID, GatewayCode, {5, Task}).

show() ->
    case ets:tab2list(?TABLE_ID) of
        Info ->
            {ok, Info};
        [] ->           
            {error, no_data}
    end.

show(Creator) ->
    case ets:tab2list(?TABLE_ID) of
            Info ->
                   Fun = 
                         fun({GatewayCode, GatewayType, Status, Time, Task, CreatorTmp}) ->
                               case CreatorTmp =:= Creator of
                                     true ->
                                         {true,{GatewayCode, GatewayType, Status, Time, Task}};
                                     _ ->
                                         false
                               end
                         end,
                   Data = lists:filtermap(Fun, Info),
                   {ok, Data};
            [] ->           
                   {error, no_data}
    end.
