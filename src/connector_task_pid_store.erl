-module(connector_task_pid_store).

-export([init/0]).
-export([insert/3,
         lookup/1,
         delete/1,
         show/0
         ]).

-define(TABLE_ID, ?MODULE).

init() ->
    ets:new(?TABLE_ID, [bag, public, named_table]),
    ok.

insert(TaskId, TaskType, Pid) ->
    ets:insert(?TABLE_ID, {TaskId, TaskType, Pid}),
    ok.

lookup(TaskId) ->
    case ets:lookup(?TABLE_ID, TaskId) of
        Info ->
            io:format("Info:~p~n",[Info]),
            Info;
        [] ->
            {error, not_found}    
    end.

delete(TaskId) ->
    ets:delete(?TABLE_ID, TaskId).

show() ->
    case ets:tab2list(?TABLE_ID) of
        Info ->
            {ok, Info};
        [] ->           
            {error, no_data}
    end.

    
