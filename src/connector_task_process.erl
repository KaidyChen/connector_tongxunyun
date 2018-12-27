-module(connector_task_process).

-behaviour(gen_server).

-include("cmd_obj.hrl").
-include("print.hrl").
-include("config.hrl").

-define(TIMEOUT, 60*1000).

-export([start_link/1]). 
-export([stop/1]). 
  
%% gen_server callbacks  
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,  
     terminate/2, code_change/3]).  
 
-record(state, {cjqid, metertype, meterid, cmdid, tasktype}).

start_link(Task) -> 
    gen_server:start_link(?MODULE, [Task], []).  

stop(Pid) ->
    gen_server:cast(Pid, stop).
  
init([Task]) -> 
    ?PRINT("Pid:~p Task~p~n",[self(), Task]),   
    {_, CjqIdCode, MeterType, MeterIdCode, CmdId, TaskType, _, _, _} = Task,
    State = #state{
        cjqid = CjqIdCode,
        metertype = MeterType,
        meterid = MeterIdCode,
        cmdid = CmdId,
        tasktype = TaskType}, 
    {ok, State, 0}.
  
handle_call(_Msg, _From, State) ->   
    {noreply, State, ?TIMEOUT}.   

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State)  ->   
    {noreply, State, ?TIMEOUT}.  
  
%% handle timeout message  
handle_info(timeout, State)  ->   
    ?PRINT("~p start work~n",[self()]),
    {{_, _, Data}, {Hour, Min, _}} = calendar:local_time(),
    case {Data, Hour, Min} of
        {_, 0, _} when Min =:= 10 ->
            case State#state.tasktype =:= "xs" of 
                true ->
                    hour_task_process(State);
                false ->
                    ok
            end;
        {_, 0, _} when Min =:= 20 ->
            case State#state.tasktype =:= "rd" of 
                true ->
                    data_task_process(State);
                false ->
                    ok
            end;
        {1, 0, _} when Min =:= 30 ->
            case State#state.tasktype =:= "yd" of 
                true ->
                    month_task_process(State);
                false ->
                    ok
            end;
        _ ->
            ok   
    end,
    {noreply, State, ?TIMEOUT}.  
  
terminate(Reason, State) ->   
    io:format("~p stopping~n",[?MODULE]),
    io:format("Reason:~p State:~p~n",[Reason, State]), 
    ok.  
  
code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.  

hour_task_process(State) ->
    get_cmd_content(State#state.cjqid, State#state.metertype, State#state.meterid, State#state.cmdid).    

data_task_process(State) ->
    get_cmd_content(State#state.cjqid, State#state.metertype, State#state.meterid, State#state.cmdid).

month_task_process(State) ->
    get_cmd_content(State#state.cjqid, State#state.metertype, State#state.meterid, State#state.cmdid).

get_cmd_content(CjqIdCode, MeterType, MeterId, CmdId) ->
    Cmd_obj = #cmd_obj{
                 eqpt_type = MeterType, 
                 eqpt_id_code = MeterId, 
                 cmd_type = "dsj", 
                 cmd_id = CmdId, 
                 cmd_data = "01"
              },
    case send_process:send(Cmd_obj) of
         {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} ->
            ?PRINT("Return_data:~p~n", [Body]),
            case send_process:get_result(Body) of
                {ok, TaskResult} ->
                    %%构造成json格式上报出去
                    Now_datetime = ?HELP:datetime_now_str(),
                    case Cmd_obj#cmd_obj.cmd_id of
                        "fhjlz_zdsj" ->
                            ActiveReportMsg = lists:concat(["{\"eqpttype\":\"", MeterType,"\",\"meterid\":\"", MeterId,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"0402\",\"cmd_id\":\"", Cmd_obj#cmd_obj.cmd_id, "\", \"result\":\"", TaskResult, "\"}}"]);
                        "zddjsjk" ->
                            ActiveReportMsg = lists:concat(["{\"eqpttype\":\"", MeterType,"\",\"meterid\":\"", MeterId,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"0403\",\"cmd_id\":\"", Cmd_obj#cmd_obj.cmd_id, "\", \"result\":\"", TaskResult, "\"}}"]);
                        "rdjsjk" ->
                            ActiveReportMsg = lists:concat(["{\"eqpttype\":\"", MeterType,"\",\"meterid\":\"", MeterId,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"0404\",\"cmd_id\":\"", Cmd_obj#cmd_obj.cmd_id, "\", \"result\":\"", TaskResult, "\"}}"]);
                        "zhygsjk" ->
                            ActiveReportMsg = lists:concat(["{\"eqpttype\":\"", MeterType,"\",\"meterid\":\"", MeterId,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"0405\",\"cmd_id\":\"", Cmd_obj#cmd_obj.cmd_id, "\", \"result\":\"", TaskResult, "\"}}"])
                    end,
                    [{active_report_opts,ActiveReportOpts}] = ets:lookup(?LISTEN_OPTS_TABLE,active_report_opts),
                    Flag = proplists:get_value(report, ActiveReportOpts),
                    case Flag of
                        "on" ->
                            Socket = connector_active_report:push_and_return_socket(null, ActiveReportMsg);
                        "off" ->
                            connector_active_report:emqtt_data_report({CjqIdCode, MeterId, ActiveReportMsg}),
                            ?HELP:store_report_data(MeterType, MeterId, Now_datetime, ActiveReportMsg)
                    end;
                {false, TaskResult} ->
                    ?ERROR("TaskResult:~s~n",[TaskResult]);
                false ->
                    ?ERROR("Task fail~n", []) 
            end;
         {error, Reason} ->
            ?ERROR("Task fail~p~n", [Reason])
    end.


