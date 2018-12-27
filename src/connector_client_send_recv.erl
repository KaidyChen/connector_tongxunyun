-module(connector_client_send_recv).

-behaviour(gen_server).

-include("config.hrl").
-include("request.hrl").
-include("print.hrl").

%% API
-export([start_link/1]).
-export([stop/1, shutdown/1]).
-export([send_req_rd_to_client_process/2]).
-export([packet_from_gateway/2]).

-define(NIFMODULE, libnif).

-define(CMD_TYPE_MANAGE, "manage").
-define(CMD_ID_ADDRECORD, "addrecord").
-define(CMD_ID_ADDRECORDBYLORA, "addrecordbylora").
-define(CMD_ID_ADDRECORDBYWG, "addrecordbywg").
-define(CMD_ID_READRECORD, "readrecord").
-define(CMD_ID_DELETEALLRECORD, "deleteallrecord").
-define(CMD_ID_DELETERECORD, "deleterecord").
-define(CMD_ID_DZYSL, "dzysl").

-define(MAX_PRIORITY, 20).%%最大优先级
-define(MIN_PRIORITY, -20).%%最小优先级

%% Gen_server callbacks
-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% ReqTask status
-define(INIT_STATUS, 0).
-define(WAITING_STATUS, 1).

%% Timer time: wait cmd return
%%-define(TIMER_TIME, 80*1000).
%% Timer msg
-define(MY_TIMEOUT_MSG, my_timeout_msg).

-record(state, {
          gatewayType :: string(),
          gatewayId :: string(),
          parent :: pid(),
          curTask :: undefined | req_task(),%%当前任务
          reqTaskQueue :: pqueue:pqueue() %%请求任务队列
         }).

%%%============================================================================
%% API
%%%============================================================================

start_link([Gateway, GatewayId, Parent]) ->
    gen_server:start_link(?MODULE, [Gateway, GatewayId, Parent], []).

stop(Pid) ->
    gen_server:cast(Pid, {stop_reason, gateway_offline}).

shutdown(Pid) ->
    gen_server:cast(Pid, shutdown).

send_req_rd_to_client_process(Pid, {Priority, ReqRd}) ->
    gen_server:cast(Pid, {req_rd_to_client_process, Priority, ReqRd}).

packet_from_gateway(Pid, Packet) when is_pid(Pid) ->
    gen_server:cast(Pid, {packet_from_gateway, Packet});
packet_from_gateway(_, _) ->
    ok.


%%%============================================================================
%% Gen_server callbacks
%%%============================================================================

init([GatewayType, GatewayId, Parent]) ->
    ?PRINT("Parent:~p send_recv:~p~n", [Parent, self()]),
    State = #state{
               gatewayType = GatewayType,
               gatewayId = GatewayId,
               parent = Parent,
               curTask = undefined,
               reqTaskQueue = pqueue:new()
              },
    {ok, State}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(shutdown, State) ->
    {stop, normal, State};

handle_cast({stop_reason, gateway_offline}, State) ->
    ?PRINT("client send recv gateway_offline~n", []),
    {stop, normal, State};    
handle_cast({req_rd_to_client_process, PriorityTmp, ReqRd}, State) ->
    #state{
       parent = Parent,
       gatewayId = GatewayId,
       curTask = CurTask,
       reqTaskQueue = ReqTaskQueue
      } = State,

    Priority = 
        if
            PriorityTmp > ?MAX_PRIORITY ->
                ?MAX_PRIORITY;
            PriorityTmp < ?MIN_PRIORITY ->
                ?MIN_PRIORITY;
            true ->
                PriorityTmp
        end,
        
    case get_req_task(GatewayId, ReqRd) of
        {ok, NewReqTask} ->
            {NewCurTask, NewReqTaskQueue} = exec_req_task_queue(Parent, CurTask, pqueue:in(NewReqTask, Priority, ReqTaskQueue)),
            NewState = State#state{
                         curTask = NewCurTask,
                         reqTaskQueue = NewReqTaskQueue
                },
            {noreply, NewState};
        _ ->
            {noreply, State}
    end;
handle_cast({packet_from_gateway, Packet}, State) ->
    #state{
       parent = Parent,
       curTask = CurTask,
       reqTaskQueue = ReqTaskQueue
      } = State,
    FrameHex = hex_util:to_hex(Packet),
    CurTaskTmp = handler_frame_hex_from_gateway(FrameHex, Parent, CurTask),
    {NewCurTask, NewReqTaskQueue} = exec_req_task_queue(Parent, CurTaskTmp, ReqTaskQueue),
    NewState = State#state{
                 curTask = NewCurTask,
                 reqTaskQueue = NewReqTaskQueue
                },
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, TimerRef, ?MY_TIMEOUT_MSG}, State) ->
    #state{
       parent = Parent,
       curTask = CurTask,
       reqTaskQueue = ReqTaskQueue
      } = State,
    mytimer:cancel_timer(TimerRef),

    CurTaskTmp  = handler_timeout(TimerRef, Parent, CurTask),
    {NewCurTask, NewReqTaskQueue} = exec_req_task_queue(Parent, CurTaskTmp, ReqTaskQueue),
    NewState = State#state{
                 curTask = NewCurTask,
                 reqTaskQueue = NewReqTaskQueue
                },    
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    #state{
       gatewayType = GatewayType,
       gatewayId = GatewayId,
       curTask = CurTask,
       reqTaskQueue = ReqTaskQueue
      } = State,
    SelfPid = self(),
    ?PRINT("~p terminate~n", [SelfPid]),
    case connector_gateway_to_store_server:lookup_client_pid({GatewayType, GatewayId}) of
        {ok, SelfPid} ->
            %% Save gateway => Clientsendrecvpid
            connector_gateway_to_store_server:delete_client_pid({GatewayType, GatewayId});
        _ ->
            ok
    end,
    handler_gateway_offline(CurTask, ReqTaskQueue),
    {stop, Reason, State}.

code_change(_OldVsn, State, _Extra) ->
    {noreply, State}.


%%%=====================================================================
%% Internal functions
%%%=====================================================================
exec_req_task_queue(_Parent, CurTaskTmp, ReqTaskQueue) when is_record(CurTaskTmp, req_task) ->
    {CurTaskTmp, ReqTaskQueue};
exec_req_task_queue(Parent, undefined, ReqTaskQueue) ->
    ?PRINT("TaskQueue len:~p~n", [pqueue:len(ReqTaskQueue)]),
    case pqueue:out(ReqTaskQueue) of
        {empty, _} ->
            {undefined, ReqTaskQueue};
        {{value, HeadReqTask}, ReqTaskQueue1} ->
            #req_task{
               taskStatus = TaskStatus,
               inputRd = InputRd,
               outputRd = OutputRd,
               reqRd = ReqRd
              } = HeadReqTask,
            #req_rd{
               pid = Pid,
               soLibName = SoLibName,
               reqObj = ReqObj
              } = ReqRd,
            case data_pack(SoLibName, InputRd, OutputRd) of
                {ok, InputRdReturn, OutputRdReturn} ->
                    case gen_order_frame_hex(InputRdReturn, OutputRdReturn) of
                        {ok, OrderFrameHex} ->
                            ?PRINT("Gen orderFrameHex: ~p~n", [OrderFrameHex]),
                            connector_channel_protocol:recv_order_frame_from_client(Parent, OrderFrameHex),
                            %% Set a timer
                            %% TODO
                            {ok, [_, _, {_, TIMER_TIME}, _]} = file:consult(?TIME_OPTS_FILEPATH),
                            TimerRef = mytimer:start_timer(TIMER_TIME, self(), ?MY_TIMEOUT_MSG),
                            CurTask = HeadReqTask#req_task{
                                               taskStatus = ?WAITING_STATUS,
                                               timerRef = TimerRef,
                                               inputRd = InputRdReturn,
                                               outputRd = OutputRdReturn
                                              },
                            {CurTask, ReqTaskQueue1};
                        {error, Reason} ->
                            %% Gen order is error
                            %% Return result to request
                            %% TODO
                            Status = "400",
                            ResultMsg = ?HELPER:to_string(Reason),
                            Msg = "data_pack生成指令失败",
                            ReturnContent = gen_return_content(ReqObj, ResultMsg, Status, Msg),
                            request_handler:reply(Pid, ReturnContent),
                            %% loop 
                            exec_req_task_queue(Parent, undefined, ReqTaskQueue1)
                    end;
                {error, Reason} ->
                    %% Data pack is error
                    %% Return result to request
                    Status = "400",
                    ResultMsg = ?HELPER:to_string(Reason),
                    Msg = "data_pack执行失败",
                    ReturnContent = gen_return_content(ReqObj, ResultMsg, Status, Msg),
                    request_handler:reply(Pid, ReturnContent),
                    exec_req_task_queue(Parent, undefined, ReqTaskQueue1)
            end
    end.
    
%%----------------------------------------------------------------------
%% Handler packet from gateway
%%----------------------------------------------------------------------

handler_frame_hex_from_gateway(FrameHex, Parent, undefined) ->
    undefined;
handler_frame_hex_from_gateway(FrameHex, Parent, CurTask) ->
    #req_task{
       taskStatus = TaskStatus,
       timerRef = HeadTimerRef,
       inputRd = InputRd,
       outputRd = OutputRd,
       reqRd = ReqRd
      } = CurTask,
    #req_rd{
       pid = Pid,
       soLibName = SoLibName,
       reqObj = ReqObj
      } = ReqRd,
    OutputRdTmp = OutputRd#output_rd{
                    frame = FrameHex,
                    frameLen = length(FrameHex)
                   },
    case data_result(SoLibName, InputRd, OutputRdTmp) of
        {ok, InputRdReturn, OutputRdReturn} ->
            case gen_data_result_return_content(SoLibName, ReqObj, InputRd, OutputRd, InputRdReturn, OutputRdReturn) of
                {ok, ReturnContent} ->
                    ?PRINT("Gen returnContent: ~p~n", [ReturnContent]),
                    %% Cancel timer
                    mytimer:cancel_timer(HeadTimerRef),
                    
                    request_handler:reply(Pid, ReturnContent),
                    undefined;
                continue_wait ->
                    CurTask;
                {continue_send, NewOrderFrameHex, NewInputRd, NewOutputRd} ->
                    %% Continue send order to gateway
                    ?PRINT("Continue orderFrameHex: ~p~n", [NewOrderFrameHex]),
                    connector_channel_protocol:recv_order_frame_from_client(Parent, NewOrderFrameHex),
                    
                    CurTask#req_task{
                      inputRd = NewInputRd,
                      outputRd = NewOutputRd
                     };
                {error, Reason} ->
                    %% Gen returnContent is error
                    %% Return result to request
                    %% TODO
                    Status = "400",
                    ResultMsg = ?HELPER:to_string(Reason),
                    Msg = "data_result解析结果失败",
                    ReturnContent = gen_return_content(ReqObj, ResultMsg, Status, Msg),
                    request_handler:reply(Pid, ReturnContent),
                    
                    undefined
            end;
        {error, Reason} ->
            %% Data result is error
            %% Return result to request
            Status = "400",
            ResultMsg = ?HELPER:to_string(Reason),
            Msg = "data_result执行失败",
            ReturnContent = gen_return_content(ReqObj, ResultMsg, Status, Msg),
            request_handler:reply(Pid, ReturnContent),
            
            undefined
    end.

handler_timeout(TimerRef, Parent, CurTask) ->
    #req_task{
       taskStatus = TaskStatus,
       timerRef = CurTaskTimerRef,
       inputRd = _InputRd,
       outputRd = _OutputRd,
       reqRd = ReqRd
      } = CurTask,
    #req_rd{
       pid = Pid,
       reqObj = ReqObj
      } = ReqRd,
    case (TaskStatus =:= ?WAITING_STATUS) andalso (TimerRef =:= CurTaskTimerRef) of
        false ->
            CurTask;
        true ->
            %% Cancel timer
            mytimer:cancel_timer(CurTaskTimerRef),

            ResultMsg = "timeout",
            Status = "400",
            Msg = "任务执行结束",
            ReturnContent = gen_return_content(ReqObj, ResultMsg, Status, Msg),
            request_handler:reply(Pid, ReturnContent),
            undefined
    end.

get_req_task(GatewayId, ReqRd) ->
    #req_rd{
       pid = _Pid,
       reqObj = ReqObj
      } = ReqRd,
    try get_input_rd(GatewayId, ReqObj) of
        InputRd ->
            {ok, #req_task{
               taskStatus = ?INIT_STATUS,
               timerRef = undefined,
               inputRd = InputRd,
               outputRd = get_output_rd(ReqObj),
               reqRd = ReqRd
              }}
    catch
        _Class:Reason ->
            Status = "400",
            ResultMsg = ?HELPER:to_string(Reason),
            Msg = "生成指令失败",
            ReturnContent = gen_return_content(ReqObj, ResultMsg, Status, Msg),
            request_handler:reply(ReqRd#req_rd.pid, ReturnContent),
            {error, Reason}
    end.

get_input_rd(GatewayId, ReqObj) ->
    #req_obj{
       orderNumber = _OrderNumber,
       partner = _Partner,
       objs = Objs
      } = ReqObj,
    #objs{
       eqptType = EqptType,
       eqptIdCode = EqptIdCode,
       eqptPwd = EqptPwd,
       resDataType = ResDataType,
       cmdType = CmdType,
       cmdId = CmdId,
       cmdData = CmdData
      } = Objs,
    gen_input_rd(GatewayId, EqptType, EqptIdCode, EqptPwd, ResDataType, CmdType, CmdId, CmdData).

gen_input_rd(GatewayId, EqptType, EqptIdCode, EqptPwd, ResDataType, CmdType, CmdId, CmdData) ->
    MeterAddress = EqptIdCode,
    {ok, Collector2} = get_collector2(GatewayId, EqptIdCode),
    Collector1 = GatewayId,
    A3 = get_a3(),
    PFC = get_pfc(GatewayId),
    {ok, Info, InfoDataLen} = gen_info_and_info_data_len(GatewayId, A3, PFC, EqptType, EqptIdCode, CmdType, CmdId, CmdData, Collector2),
    Route1 = "",
    Route2 = "",
    IsBCD = ResDataType,
    SubSEQ = 0,
    Pwd = EqptPwd,
    #input_rd{
       meterAddress = MeterAddress,
       collector2 = Collector2,
       collector1 = Collector1,
       info = Info,
       infoDataLen = InfoDataLen,
       route1 = Route1,
       route2 = Route2,
       isBCD = IsBCD,
       subSEQ = SubSEQ,
       pwd = Pwd
      }.

%% 3761协议
%% A3: 主站地址和组地址
%% PFC: 启动帧帧序号计数器，集中器每次登录时，初始化为0，保存到数据库，每发送一次数据则+1，0-255，循环使用
%% MeasureCode: 测量点号
%% Baudrate: 波特率
%% Commport: 通讯端口号
%% ProtocolType: 协议类型，pro97(DLT645-1997协议)，pro07(DLT645-2007协议), pro188bc(北川自定义水表协议), pro188(CJT188协议)
%% MeterId: 通讯地址
%% Password: 通讯密码，默认为"000000000000" 6 byte, 12 characters
%% RateNumber: 费率个数
%% ZSW: 整数位，有功电能示值整数位个数
%% XSW：小数位，有功电能示值小数位个数
%% Collector2: 采集设备地址(二级采集设备地址)
%% DLH：大类号，用户大类号，0-15，表示16个用户大类号
%% XLH：小类号，用户小类号，0-15，表示16套1类和2类数据项
%% YBLX：仪表类型，LSB(冷水表), RSB(热水表), ZYSB(直饮水表), ZSB(中水表), JRLB(计热量表), JLLB(计冷量表), RQB(燃气表), DNB(电能表)

%% 3716 集中器操作

get_collector2(GatewayId, EqptIdCode) ->
    connector_db_store_server:get_collector2(GatewayId, EqptIdCode).

%% default set "10"
get_a3() ->
    "10".

get_pfc(GatewayId) ->
    try connector_gateway_to_store_server:get_pfc(GatewayId) of
        PFC ->
            PFC
    catch
        _:_ ->
            0
    end.

get_password() ->
    "000000000000".

to_string(null) ->
    "";
to_string(X) ->
    ?HELPER:to_string(X).

%% 集中器添加/读取/删除单个档案，需要通讯地址通过cmd_data传入
gen_info_and_info_data_len(GatewayId, A3, PFC, EqptType, EqptIdCode, ?CMD_TYPE_MANAGE = _CmdType, CmdId, CmdData, _Collector2) when 
      (CmdId =:= ?CMD_ID_ADDRECORD) orelse (CmdId =:= ?CMD_ID_ADDRECORDBYLORA) orelse (CmdId =:= ?CMD_ID_ADDRECORDBYWG) orelse (CmdId =:= ?CMD_ID_READRECORD) orelse (CmdId =:= ?CMD_ID_DELETERECORD) orelse (CmdId =:= ?CMD_ID_DZYSL) ->
    MeterId = CmdData, 
    case db_util:get_measurecode(MeterId) of
        {ok, MeasureCode} ->
            ProtocolType = get_protocol_type_by_gateway_and_meter(GatewayId, MeterId),
            %% 添加档案时，需要计量设备的上级二级采集设备
            Password = get_password(),
            {ok, NewCollector2} = get_collector2(GatewayId, MeterId),  %% 是指表的直接父级采集器
            {ok, {Baudrate, Commport, RateNumber, ZSW, XSW, DLH, XLH, YBLX}} = 
                get_addtion_info(MeterId),
            InfoList = [to_string(X) || X <- [A3, PFC, MeasureCode, Baudrate, Commport, ProtocolType, MeterId, Password, 
                                              RateNumber, ZSW, XSW, NewCollector2, DLH, XLH, YBLX]],
            NewCmdData =
                case CmdId of
                    ?CMD_ID_DZYSL ->
                        lists:flatten(io_lib:format("~4..0w", [MeasureCode]));
                    _ ->
                        ""
                end,
            Info = string:join(InfoList, ",") ++ "," ++ NewCmdData,
            InfoDataLen = length(NewCmdData), %% 添加/读取档案并不需要数据单元
            {ok, Info, InfoDataLen};
       {error, Reason} ->
            throw(Reason)
    end;
gen_info_and_info_data_len(GatewayId, A3, PFC, EqptType, EqptIdCode, CmdType, CmdId, CmdData, Collector2) ->
    MeasureCode = "0",
    MeterId = EqptIdCode,
    ProtocolType = get_protocol_type(EqptType, EqptIdCode),
    Password = get_password(),
    {ok, {Baudrate, Commport, RateNumber, ZSW, XSW, DLH, XLH, YBLX}} = 
        get_addtion_info(EqptIdCode),
    InfoList = [to_string(X) || X <- [A3, PFC, MeasureCode, Baudrate, Commport, ProtocolType, MeterId, Password, 
                                      RateNumber, ZSW, XSW, Collector2, DLH, XLH, YBLX]],
    Info = string:join(InfoList, ",") ++ "," ++ CmdData,
    InfoDataLen = length(CmdData),
    {ok, Info, InfoDataLen}.

get_protocol_type(EqptType, EqptIdCode) ->
    case connector_db_store_server:get_protocol_type(EqptType, EqptIdCode) of
        {ok, ProtocolType} ->
            ProtocolType;
        _ ->
            throw("找不到该表的协议类型")
    end.

get_protocol_type_by_gateway_and_meter(GatewayId, MeterId) ->
    case connector_db_store_server:get_protocol_type_by_gateway_and_meter(GatewayId, MeterId) of
        {ok, ProtocolType} ->
            ProtocolType;
        _ ->
            throw("找不到该表的协议类型")
    end.
    

get_addtion_info(MeterId) ->
    case db_util:get_addtion_info(MeterId) of
        {ok, []} ->
            %% 默认值
            {ok, {2400, 31, 4, 6, 2, 5, 1, ''}};
        {ok, [Row]} ->
            {ok, Row};
        {error, Reason} ->
            throw(Reason)
    end.

get_output_rd(ReqObj) ->
    Data = "",
    Frame = "",
    Error = "",
    DataLen = length(Data),
    FrameLen = length(Frame),
    ErrorLen = length(Error),
    FuncResult = -999,
    #output_rd{
       data = Data,
       frame = Frame,
       error = Error,
       dataLen = DataLen,
       frameLen = FrameLen,
       errorLen = ErrorLen,
       funcResult = FuncResult
      }.

gen_order_frame_hex(InputRd, OutputRd) ->
    #output_rd{
       data = Data,
       frame = Frame,
       error = Error,
       dataLen = DataLen,
       frameLen = FrameLen,
       errorLen = ErrorLen,
       funcResult = FuncResult
      } = OutputRd,

    ?PRINT("InputRd = ~p~n", [InputRd]),
    ?PRINT("OutputRd = ~p~n", [OutputRd]),
    case FuncResult of
        0 ->
            {ok, Frame};
        _ ->
            {error, Error}
    end.

gen_data_result_return_content(SoLibName, ReqObj, InputRd, OutputRd, InputRdReturn, OutputRdReturn) ->
    #req_obj{
       orderNumber = OrderNumber,
       sign = Sign,
       flag = Flag
      } = ReqObj,
    #output_rd{
       data = Data,
       frame = _Frame,
       error = _Error,
       dataLen = DataLen,
       frameLen = _FrameLen,
       errorLen = _ErrorLen,
       funcResult = _FuncResult
      } = OutputRd,
    #output_rd{
       data = DataReturn,
       frame = FrameReturn,
       error = ErrorReturn,
       dataLen = DataLenReturn,
       frameLen = FrameLenReturn,
       errorLen = ErrorLenReturn,
       funcResult = FuncResultReturn
      } = OutputRdReturn,

    %% {"order_no":"17010317167CBB188C3EE44BBBA93B14437221864F","result":"171554","status":"200","msg":"任务执行完成"}
    %% &sign=cecab2235d76deabbe40a06b6394ccc9
    case FuncResultReturn of
        0 ->
            Status = "200",
            ResultMsg =
                case DataLenReturn > 0 of
                    true ->
                        ?HELPER:to_string(DataReturn);
                    false ->
                        ?HELPER:to_string("Success")
                end,
            Msg = "任务执行完成",
            ReturnContent = gen_return_content(ReqObj, ResultMsg, Status, Msg),
            {ok, ReturnContent};
        1 ->
            %% Subsequment frames
            NewOrderFrameHex = FrameReturn,
            {continue_send, NewOrderFrameHex, InputRdReturn, OutputRdReturn};
        _ ->
            case {string:str(SoLibName, "ethnet") > 0, ((FuncResultReturn =:= 4) orelse (FuncResultReturn =:= 8)), 
                (FuncResultReturn =:= 6)} of
                {Result1, Result2, Result3} when ((Result1 =:= true) andalso (Result2 =:= true)) 
                                                 orelse ((Result1 =:= false) andalso (Result3 =:= true)) -> 
                    %% 非ethnet通信
                    continue_wait;
                _ ->
                    Status = "300",
                    ResultMsg = ?HELPER:to_string(ErrorReturn),
                    Msg = "任务返回失败",
                    ReturnContent = gen_return_content(ReqObj, ResultMsg, Status, Msg),
                    {ok, ReturnContent}
            end
    end.

gen_return_content(ReqObj, ResultMsg, Status, Msg) ->
    #req_obj{
       orderNumber = OrderNumber,
       objs = Objs,
       sign = Sign,
       flag = Flag
      } = ReqObj,
    #objs{
       eqptType = EqptType,
       eqptIdCode = EqptIdCode
      } = Objs,
    case string:left(ResultMsg, 2) =:= "{\"" of
        false ->
            ?PRINT("is not json!!!~n",[]),
            "{\"order_no\":\"" ++ OrderNumber ++ "\", \"eqpt_type\":\"" ++ EqptType ++ "\", \"eqpt_id_code\":\"" ++ EqptIdCode ++ "\",\"result\":\"" ++ ResultMsg ++ "\",\"status\":\"" ++ Status ++ "\",\"msg\":\"" ++ Msg ++ "\", \"sign\":\"" ++ Sign ++ "\", \"flag\":\"" ++ Flag ++ "\"}";
        _ ->
            ?PRINT("is_json!!!~n",[]),
            "{\"order_no\":\"" ++ OrderNumber ++ "\"" ++ "\," ++ "\"eqpt_type\":\"" ++ EqptType ++ "\"" ++ "\," ++ "\"eqpt_id_code\":\"" ++ EqptIdCode ++ "\"" ++ "\," ++ "\"result\":" ++ ResultMsg ++ "\," ++ "\"status\":\"" ++ Status ++ "\"" ++ "\," ++ "\"msg\":\"" ++ Msg ++ "\"" ++ "\," ++ "\"sign\":\"" ++ Sign ++ "\"" ++ "\," ++ "\"flag\":\"" ++ Flag ++ "\"" ++ "}"
    end.

exec_func(SoFileName, FuncName, InputRd, OutputRd) ->
    try ?NIFMODULE:exec_func(SoFileName, FuncName, InputRd, OutputRd) of
        {ok, {InputRdReturn, OutputRdReturn}} ->
            ?PRINT("InputRdReturn = ~p~n", [InputRdReturn]),
            ?PRINT("OutputRdReturn = ~p~n", [OutputRdReturn]),
            {ok, InputRdReturn, OutputRdReturn};
        {error, Reason} ->
            {error, Reason}
    catch
        Class:What ->
            {error, What}
    end.

data_pack(SoLibName, InputRd, OutputRd) ->
    exec_func(SoLibName, "data_pack", InputRd, OutputRd).

data_result(SoLibName, InputRd, OutputRd) ->
    exec_func(SoLibName, "data_result", InputRd, OutputRd).

handler_gateway_offline(undefined, _) ->
    ok;
handler_gateway_offline(CurTask, ReqTaskQueue) ->
    ReqTaskList = pqueue:to_list(ReqTaskQueue),
    Fun = 
        fun(ReqTask) ->
                #req_task{
                   taskStatus = TaskStatus,
                   timerRef = TimerRef,
                   inputRd = _InputRd,
                   outputRd = _OutputRd,
                   reqRd = ReqRd
                  } = ReqTask,
                #req_rd{
                   pid = Pid,
                   reqObj = ReqObj
                  } = ReqRd,
                mytimer:cancel_timer(TimerRef),

                ResultMsg = "网关已掉线",
                Status = "400",
                ReturnContent = 
                    case TaskStatus =:= ?WAITING_STATUS of
                        true ->
                            Msg = "任务执行结果未知",
                            gen_return_content(ReqObj, ResultMsg, Status, Msg);
                        false ->
                            Msg = "任务未执行",
                            gen_return_content(ReqObj, ResultMsg, Status, Msg)
                    end,
                request_handler:reply(Pid, ReturnContent)
        end,
    lists:foreach(Fun, [CurTask | ReqTaskList]),
    ok.
