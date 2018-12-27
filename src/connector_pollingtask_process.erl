-module(connector_pollingtask_process).

-behaviour(gen_server).

-include("cmd_obj.hrl").
-include("print.hrl").
-include("config.hrl").

-define(TIMEOUT, 60*1000).
-define(TIMEOUT_MSG, timeout_msg).
-define(TIMEOUT_MSG_TIMER, timer_out_msg).
-export([start_link/1]). 
-export([stop/1]). 
  
%% gen_server callbacks  
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,  
     terminate/2, code_change/3]).  
  
-record(state, {cjqid, metertype, meterid, cmdid, starttime, time, taskcustom}).

start_link(Task) ->  
    gen_server:start_link(?MODULE, [Task], []).  

stop(Pid) ->
    gen_server:cast(Pid, stop).
  
init([Task]) ->     
    {_, CjqIdCode, MeterType, MeterIdCode, CmdId, TaskType, StartTime, Time, TaskCustom} = Task,
    State = #state{
        cjqid = CjqIdCode,
        metertype = MeterType,
        meterid = MeterIdCode,
        cmdid = CmdId,
        starttime = StartTime,
        time = Time,
        taskcustom = TaskCustom}, 
    {ok, State, 0}.
  
handle_call(_Msg, _From, State) ->   
    {noreply, State}.   

handle_cast(stop, State) ->
    {stop, normal, State};  
handle_cast(_Msg, State)  ->   
    {noreply, State}.  
  
%% 每隔?TIMEOUT检查一次轮询任务开始时间  
handle_info(timeout, State)  ->   
    ?PRINT("check polling task~n",[]),
    [Hour, Min] = string:tokens(State#state.starttime,":"),
    {_,{H, M, _}} = calendar:local_time(),
    {HourTmp, MinTmp} = get_time_str(H, M),
    case (State#state.taskcustom =:= null orelse State#state.taskcustom =:= []) of
        true ->
            TimeList = lists:seq(list_to_integer(Hour)*60,1440,list_to_integer(State#state.time)),
            case lists:member((H*60+M), TimeList) of
                true ->
                  ?PRINT("start task no time~~~n",[]),
                  polling_task_process(State#state.cjqid, State#state.metertype, State#state.meterid, 
                                                                        State#state.cmdid),
                  TimerRef = mytimer:start_timer(list_to_integer(State#state.time)*60*1000, 
                                                                        self(), ?TIMEOUT_MSG),
                  {noreply, State};
                _ ->
                  {noreply, State, ?TIMEOUT}
            end;
        false ->
            %% 00000300/05000800/10001200
            TimeListTmp = string:tokens(State#state.taskcustom, "/"),
            TimeList = [{list_to_integer(string:left(string:left(X, 4), 2)) * 60 + 
                                list_to_integer(string:right(string:left(X, 4), 2)), 
                                list_to_integer(string:left(string:right(X, 4), 2)) * 60 + 
                                list_to_integer(string:right(string:right(X, 4), 2))} || X <- TimeListTmp], 
            TaskTime = H*60 + M,
            TaskStartTime = list_to_integer(Hour)*60 + list_to_integer(Min),
            ?PRINT("CustomList:~p~nTimeList:~p~nTaskTime:~p~n",[TimeListTmp,TimeList, TaskTime]),
            Fun = 
                fun({A,B}) ->
                    case (TaskTime >= A andalso TaskTime =< B) of
                        true ->
                            {true, 1};
                        false ->
                            false
                    end
                end,
            FlagList = lists:filtermap(Fun, TimeList),
            case lists:member(1, FlagList) of
                true ->
                    case ((TaskTime - TaskStartTime) rem list_to_integer(State#state.time)) of
                        0 ->
                            ?PRINT("start task time 1 ~~~n",[]),
                            polling_task_process(State#state.cjqid, State#state.metertype, 
                                                                State#state.meterid, State#state.cmdid),
                            TimerRef = mytimer:start_timer(list_to_integer(State#state.time)*60*1000, 
                                                                                self(), ?TIMEOUT_MSG_TIMER),
                            {noreply, State};
                        _  ->
                            {noreply, State, ?TIMEOUT}
                    end;
                false ->
                    {noreply, State, ?TIMEOUT}
            end
    end;
    
handle_info({timeout, TimerRef, ?TIMEOUT_MSG}, State) ->
    [Hour, Min] = string:tokens(State#state.starttime,":"),
    {_,{HourTmp, MinTmp, _}} = calendar:local_time(),
    case (HourTmp >= list_to_integer(Hour)) andalso (HourTmp =< 23) of
        true ->
           ?PRINT("start task no time~~~n",[]),
           polling_task_process(State#state.cjqid, State#state.metertype, State#state.meterid, 
                                                                                State#state.cmdid),
           mytimer:start_timer(list_to_integer(State#state.time)*60*1000, self(), ?TIMEOUT_MSG),
           {noreply, State};
        false ->
           mytimer:cancel_timer(TimerRef),
           {noreply, State, ?TIMEOUT}
    end;

handle_info({timeout, TimerRef, ?TIMEOUT_MSG_TIMER}, State) ->
    [Hour, Min] = string:tokens(State#state.starttime,":"),
    {_, {HourTmp, MinTmp, _}} = calendar:local_time(),
    TimeListTmp = string:tokens(State#state.taskcustom, "/"),
    TimeList = [{list_to_integer(string:left(string:left(X, 4), 2)) * 60 +
                           list_to_integer(string:right(string:left(X, 4), 2)), 
                           list_to_integer(string:left(string:right(X, 4), 2)) * 60 + 
                           list_to_integer(string:right(string:right(X, 4), 2))} || X <- TimeListTmp], 
    TaskTime = HourTmp*60 + MinTmp,
    ?PRINT("TaskTime:~p~n",[TaskTime]),
            Fun = 
                fun({A,B}) ->
                   case (TaskTime >= A andalso TaskTime =< B) of
                       true ->
                          {true, 1};
                       false ->
                          false
                   end
            end,
    FlagList = lists:filtermap(Fun, TimeList),
    ?PRINT("FlagList:~p~n",[FlagList]),
    case lists:member(1, FlagList) of
        true ->
             ?PRINT("start task time 2 ~~~n",[]),
             polling_task_process(State#state.cjqid, State#state.metertype, State#state.meterid, 
                                                                             State#state.cmdid),
             mytimer:start_timer(list_to_integer(State#state.time)*60*1000,self(),?TIMEOUT_MSG_TIMER),
             {noreply, State};
        false ->
             ?PRINT("stop task time 2 ~~~n",[]),
             mytimer:cancel_timer(TimerRef),
             {noreply, State, ?TIMEOUT}
    end;

handle_info(Info, State) ->
    ?PRINT("~p~n", [Info]),
    {noreply, State}.
  
terminate(Reason, _State) ->   
    ?PRINT("~p ~p stopping~n",[?MODULE, Reason]),  
    ok.  
  
code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.  

polling_task_process(CjqIdCode, MeterType, MeterId, CmdId) ->
    Cmd_obj = #cmd_obj{
                 eqpt_type = MeterType, 
                 eqpt_id_code = MeterId, 
                 cmd_type = "dsj", 
                 cmd_id = CmdId, 
                 cmd_data = ""
              },
    case send_process:send(Cmd_obj) of
         {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} ->
            ?PRINT("Return_data:~p~n", [Body]),
            case send_process:get_result(Body) of
                {ok, TaskResult} ->
                    %%构造成json格式上报出去
                    Now_datetime = ?HELP:datetime_now_str(),
                    ActiveReportMsg = lists:concat(["{\"eqpttype\":\"", MeterType,"\",\"meterid\":\"", MeterId,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"0401\",\"cmd_id\":\"", Cmd_obj#cmd_obj.cmd_id, "\", \"result\":\"", TaskResult, "\"}}"]),
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

get_time_str(Hour, Min) ->
    {HourStr, MinStr} = {integer_to_list(Hour), integer_to_list(Min)},
    case {length(HourStr) =:= 1, length(MinStr) =:= 1} of
        {true, true} ->
            {"0"++HourStr, "0"++MinStr};
        {true, false} ->
            {"0"++HourStr, MinStr};
        {false, true} ->
            {HourStr, "0"++MinStr};
        {false, false} ->
            {HourStr, MinStr}
    end.


