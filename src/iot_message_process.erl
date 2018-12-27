-module(iot_message_process).

-behaviour(gen_server).

-include("print.hrl").
-include("log.hrl").
-include("config.hrl").

-define(TIMEOUT, 1*60*1000).
-define(TIMEOUT_MSG, timeout_msg).
-export([start_link/1]). 
-export([stop/1]). 
  
%% gen_server callbacks  
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,  
     terminate/2, code_change/3]).  
  
-record(state, {
            devSerial :: string,
            num :: integer(),
            timeout = undefined :: undefined | reference(),
            flag :: integer()
            }).

start_link([DevSerial, Message]) ->  
    gen_server:start_link(?MODULE, [DevSerial, Message], []).  

stop(Pid) ->
    gen_server:cast(Pid, stop).
  
init([DevSerial, Message]) ->
    Num = list_to_integer(string:substr(Message,43,2),16) - 51,
    ?PRINT("总包数:~p~n",[Num]),
    Seq = list_to_integer(string:substr(Message,41,2),16) - 50,
    ?PRINT("收到第~p包~n",[Seq]),
    Data = lists:nthtail(44, string:left(Message, length(Message)-4)),
    ?PRINT("Data:~p~n",[Data]),
    iot_message_store:insert({DevSerial,{Seq, Data}}), 
    TimerRef = mytimer:start_timer(?TIMEOUT, self(), ?TIMEOUT_MSG), 
    State = #state{
                devSerial = DevSerial,
                num = Num,
                timeout = TimerRef,
                flag = 0
                },   
    {ok, State, 0}.
  
handle_call(_Msg, _From, State) ->   
    {noreply, State}.   

handle_cast(stop, State) ->
    {stop, normal, State};  
handle_cast(_Msg, State)  ->   
    {noreply, State}.  
  
 %%处理iot水表数据帧   
handle_info({water, DevSerial, Message}, State)  ->
    #state{
       devSerial = DevSerial,
       num = Num,
       timeout = TimerRef,
       flag = Flag   
      } = State,
    mytimer:cancel_timer(TimerRef),
    {ok, MeterId} = iot_eqptid_store:lookup(DevSerial),
    %%MeterId = "032017100386",
    Seq = list_to_integer(string:substr(Message,41,2),16) - 50,
    ?PRINT("总包数~p~n",[Num]),
    ?PRINT("标志位~p~n",[Flag]),
    ?PRINT("收到第~p包~n",[Seq]),
    Data = lists:nthtail(44, string:left(Message, length(Message)-4)),
    ?PRINT("Data:~p~n",[Data]),
    Time = ?HELP:datetime_now_str(),
    case {Seq == Num, Flag == 0} of
        {true, true} ->
            %%接收到最后一包数据
            iot_message_store:insert({DevSerial,{Seq, Data}}),
            connector_event_server:easyiot_log(DevSerial,"start checking image datapacket---"),
            case get_iot_data(Num, DevSerial) of
                {ok, DataTmp} ->
                    Body = get_water_body(MeterId, DevSerial, get_base64:get_base64(DataTmp)),
                    Response = httpc:request(post, {?WarterUrl, [], "application/x-www-form-urlencoded", Body}, [{timeout, 30000}, {connect_timeout, 30000}], []),
                    case Response of
                        {ok, {{_Version, 200, _ReasonPhrase}, _Headers, BodyTmp}} ->                            
                            BodyReturn = ?HELP:parse(BodyTmp),
                            %%处理数据emqtt上报
                            {Code, WaterMsg, Filename, Value} = get_water_msg(BodyReturn),
                            ReportMsg = jsx:encode([{<<"devSerial">>,list_to_binary(DevSerial)},{<<"meterid">>,list_to_binary(MeterId)},{<<"time">>,list_to_binary(Time)},{<<"data">>,[{<<"result">>,[{<<"Code">>,Code},{<<"Message">>,WaterMsg},{<<"Filename">>,Filename},{<<"Value">>,Value}]},{<<"imagedata">>,list_to_binary(DataTmp)}]}]),
                            [{ipc_id, IpcId}] = ets:lookup(?COMPUTER_OPTS_TABLE, ipc_id),
                            TopicBin = list_to_binary(lists:concat(["easyiot/water", "/", IpcId, "/", DevSerial, "/", MeterId])),
                            emqttc_server ! {topic_and_data, {TopicBin, ReportMsg}},
                            ?PRINT("Result:~p~n",[ReportMsg]),
                            ?HELP:store_iot_data(MeterId, Time, Code, WaterMsg, Filename, Value),
                            iot_eqptid_store:delete(DevSerial);
                        {error, _Reason} ->
                            ?ERROR("图片识别失败!!!~n",[])
                    end,
                    %%图片收据接收完毕回复确认帧
                    MeterIdTmp = lists:concat(lists:reverse([string:substr(MeterId, X, 2) || X <- lists:seq(1,12,2)])),
                    CsData = "68" ++ MeterIdTmp ++ "68140C00334310EF33333333AB896745",
                    Cs = ?HELP:get_cs(CsData),
                    Reply = "FEFEFEFE" ++ CsData ++ Cs ++ "16",
                    Msg = "image datapacket is normal received and reply frame--> " ++ Reply,
                    connector_event_server:easyiot_log(DevSerial,Msg),
                    {ok, ServerId, AccessToken} = iot_token_store:lookup("accessToken"),
                    BodyRecall = jsx:encode([{<<"devSerial">>,list_to_binary(DevSerial)},{<<"method">>,<<"setting_1">>},{<<"params">>,[{<<"setting1">>,list_to_binary(Reply)}]}]),
                    httpc:request(post, {?UrtCommand, [{"serverID", ServerId},{"accessToken", AccessToken}], "application/json", BodyRecall}, [{timeout, 30000}, {connect_timeout, 30000}], []),
                    iot_clientpid_store:delete(DevSerial),
                    iot_message_store:delete(DevSerial),
                    {stop, normal, State};
                _ ->
                    %%图片数据缺省，开始补发获取
                    {ok, InfoList} = iot_message_store:lookup(DevSerial),
                    ?PRINT("Infolist:~p~n",[InfoList]),
                    SeqList = [SeqTmp || {_, {SeqTmp, _}} <- InfoList],
                    ?PRINT("已接收图片索引:~p Num:~p~n",[SeqList, Num]),
                    NewSeqlist = get_iot_seqlist(SeqList, Num),
                    ?PRINT("缺失图片索引:~p~n",[NewSeqlist]),
                    [get_image_data(DevSerial, MeterId, NewSeq) || NewSeq <- NewSeqlist],
                    {noreply, State#state{flag = length(NewSeqlist)-1}}
            end;
        {false, true} ->
            %%正常接收图片数据包但不是最后一包
            iot_message_store:insert({DevSerial,{Seq, Data}}),
            NewTimerRef = mytimer:start_timer(?TIMEOUT, self(), ?TIMEOUT_MSG),
            {noreply, State#state{timeout = NewTimerRef}};
        {true, false} ->
            %%补抄图片返回的数据包（只缺省最后一包）
            iot_message_store:insert({DevSerial,{Seq, Data}}),
            {ok, DataTmp} = get_iot_data(Num, DevSerial),
            Body = get_water_body(MeterId, DevSerial, get_base64:get_base64(DataTmp)),
            Response = httpc:request(post, {?WarterUrl, [], "application/x-www-form-urlencoded", Body}, [{timeout, 30000}, {connect_timeout, 30000}], []),
            case Response of
                {ok, {{_Version, 200, _ReasonPhrase}, _Headers, BodyTmp}} ->
                    BodyReturn = ?HELP:parse(BodyTmp),
                    %%处理数据emqtt上报
                    {Code, WaterMsg, Filename, Value} = get_water_msg(BodyReturn),
                    ReportMsg = jsx:encode([{<<"devSerial">>,list_to_binary(DevSerial)},{<<"meterid">>,list_to_binary(MeterId)},{<<"time">>,list_to_binary(Time)},{<<"data">>,[{<<"result">>,[{<<"Code">>,Code},{<<"Message">>,WaterMsg},{<<"Filename">>,Filename},{<<"Value">>,Value}]},{<<"imagedata">>,list_to_binary(DataTmp)}]}]),
                    [{ipc_id, IpcId}] = ets:lookup(?COMPUTER_OPTS_TABLE, ipc_id),
                    TopicBin = list_to_binary(lists:concat(["easyiot/water", "/", IpcId, "/", DevSerial, "/", MeterId])),
                    emqttc_server ! {topic_and_data, {TopicBin, ReportMsg}},
                    ?HELP:store_iot_data(MeterId, Time, Code, WaterMsg, Filename, Value),
                    iot_eqptid_store:delete(DevSerial);
                {error, _Reason} ->
                    ?ERROR("图片识别失败!!!~n",[])
            end,
            %%图片收据接收完毕回复确认帧
            MeterIdTmp = lists:concat(lists:reverse([string:substr(MeterId, X, 2) || X <- lists:seq(1,12,2)])),
            CsData = "68" ++ MeterIdTmp ++ "68140C00334310EF33333333AB896745",
            Cs = ?HELP:get_cs(CsData),
            Reply = "FEFEFEFE" ++ CsData ++ Cs ++ "16",
            Msg = "缺省图片数据接收完毕回复确认帧-->" ++ Reply,
            connector_event_server:easyiot_log(DevSerial,Msg),
            {ok, ServerId, AccessToken} = iot_token_store:lookup("accessToken"),
            Body = jsx:encode([{<<"devSerial">>,list_to_binary(DevSerial)},{<<"method">>,<<"setting_1">>},{<<"params">>,[{<<"setting1">>,list_to_binary(Reply)}]}]),
            httpc:request(post, {?UrtCommand, [{"serverID", ServerId},{"accessToken", AccessToken}], "application/json", Body}, [{timeout, 30000}, {connect_timeout, 30000}], []),
            iot_clientpid_store:delete(DevSerial),
            iot_message_store:delete(DevSerial),
            {stop, normal, State};
        {false, false} ->
            iot_message_store:insert({DevSerial,{Seq, Data}}),
            case Flag ==1 of
                true ->
                    %%补抄接收图片数据包且是缺省的数据包中的最后一包
                    {ok, DataTmp} = get_iot_data(Num, DevSerial),
                    Body = get_water_body(MeterId, DevSerial, get_base64:get_base64(DataTmp)),
                    Response = httpc:request(post, {?WarterUrl, [], "application/x-www-form-urlencoded", Body}, [{timeout, 30000}, {connect_timeout, 30000}], []),
                    case Response of
                        {ok, {{_Version, 200, _ReasonPhrase}, _Headers, BodyTmp}} ->
                            BodyReturn = ?HELP:parse(BodyTmp),
                            %%处理数据emqtt上报
                            {Code, WaterMsg, Filename, Value} = get_water_msg(BodyReturn),
                            ReportMsg = jsx:encode([{<<"devSerial">>,list_to_binary(DevSerial)},{<<"meterid">>,list_to_binary(MeterId)},{<<"time">>,list_to_binary(Time)},{<<"data">>,[{<<"result">>,[{<<"Code">>,Code},{<<"Message">>,WaterMsg},{<<"Filename">>,Filename},{<<"Value">>,Value}]},{<<"imagedata">>,list_to_binary(DataTmp)}]}]),
                            [{ipc_id, IpcId}] = ets:lookup(?COMPUTER_OPTS_TABLE, ipc_id),
                            TopicBin = list_to_binary(lists:concat(["easyiot/water", "/", IpcId, "/", DevSerial, "/", MeterId])),
                            emqttc_server ! {topic_and_data, {TopicBin, ReportMsg}},
                            ?PRINT("Result:~p~n",[ReportMsg]),
                            ?HELP:store_iot_data(MeterId, Time, Code, WaterMsg, Filename, Value),
                            iot_eqptid_store:delete(DevSerial);
                        {error, _Reason} ->
                            ?ERROR("图片识别失败!!!~n",[])
                    end,
                    %%图片收据接收完毕回复确认帧
                    MeterIdTmp = lists:concat(lists:reverse([string:substr(MeterId, X, 2) || X <- lists:seq(1,12,2)])),
                    CsData = "68" ++ MeterIdTmp ++ "68140C00334310EF33333333AB896745",
                    Cs = ?HELP:get_cs(CsData),
                    Reply = "FEFEFEFE" ++ CsData ++ Cs ++ "16",
                    Msg = "lostimage datapacket is normal received and reply frame--> " ++ Reply,
                    connector_event_server:easyiot_log(DevSerial,Msg),
                    {ok, ServerId, AccessToken} = iot_token_store:lookup("accessToken"),
                    ReplyBody = jsx:encode([{<<"devSerial">>,list_to_binary(DevSerial)},{<<"method">>,<<"setting_1">>},{<<"params">>,[{<<"setting1">>,list_to_binary(Reply)}]}]),
                    httpc:request(post, {?UrtCommand, [{"serverID", ServerId},{"accessToken", AccessToken}], "application/json", ReplyBody}, [{timeout, 30000}, {connect_timeout, 30000}], []),
                    iot_clientpid_store:delete(DevSerial),
                    iot_message_store:delete(DevSerial),
                    {stop, normal, State};
                false ->
                    %%补抄图片数据接收但不是返回图片数据的最后一包
                    NewTimerRef = mytimer:start_timer(?TIMEOUT, self(), ?TIMEOUT_MSG),
                    {noreply, State#state{timeout = NewTimerRef,flag = Flag - 1}} 
            end
    end;

handle_info({timeout, TimerRef, ?TIMEOUT_MSG}, State) ->
    #state{
       devSerial = DevSerial,
       num = Num,
       timeout = TimerRef,
       flag = Flag   
      } = State,
    mytimer:cancel_timer(TimerRef),
    iot_clientpid_store:delete(DevSerial),
    iot_message_store:delete(DevSerial),
    iot_eqptid_store:delete(DevSerial),
    Msg = "image datapacket received timeout--> ",
    connector_event_server:easyiot_log(DevSerial,Msg),
    ?PRINT("receive imagedata timeout!!!~n",[]),
    {stop,normal,State};

handle_info(Info, State) ->
    ?PRINT("~p~n", [Info]),
    {noreply, State}.
  
terminate(Reason, _State) ->   
    ?PRINT("~p ~p stopping~n",[?MODULE, Reason]),  
    ok.  
  
code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.  

get_iot_data(Num, DevSerial) ->
    case iot_message_store:lookup(DevSerial) of
        {ok, InfoList} ->
            %%{DevSerial,{Seq, Data}}
            case length(InfoList) == Num of
                true ->
                    %%对图片顺序进行排序
                    DataList = [get_iot_datalist(N, InfoList) || N <- lists:seq(1,Num)],
                    Message = lists:concat(DataList),
                    {ok, Message};
                _ ->
                    false
            end
    end.

%%过滤列表
get_iot_datalist(Elem ,List) ->
    Fun = 
         fun(Info) ->
            {_, {Seq, Data}} = Info,
            case Seq == Elem of
            true ->
                {true, Data};
             _ ->
                false
            end
         end,
    [Data] = lists:filtermap(Fun, List),
    Data.

%%获取缺省图片数据索引
get_iot_seqlist(SeqList, Num) ->
    SeqTmpList = lists:seq(0,Num) -- SeqList,
    ?PRINT("获取缺失图片索引:~p~n",[SeqTmpList]),
    SeqTmpList.
    
get_image_data(DevSerial, MeterId, NewSeq) ->
    case NewSeq of
        0 ->
            ok;
        _ ->
            MeterIdTmp = lists:concat(lists:reverse([string:substr(MeterId, X, 2) || X <- lists:seq(1,12,2)])),
            Seq = integer_to_list((NewSeq + 51),16),
            CsData = "68" ++ MeterIdTmp ++ "68110700343310EF34" ++ Seq ++ "33",
            Cs = ?HELP:get_cs(CsData),
            Reply = "FEFEFEFE" ++ CsData ++ Cs ++ "16",
            Msg = "image datas are losing and start getting losing data NO." ++ integer_to_list(NewSeq) ++ "-->" ++ Reply,
            connector_event_server:easyiot_log(DevSerial,Msg),
            {ok, ServerId, AccessToken} = iot_token_store:lookup("accessToken"),
            Body = jsx:encode([{<<"devSerial">>,list_to_binary(DevSerial)},{<<"method">>,<<"setting_1">>},{<<"params">>,[{<<"setting1">>,list_to_binary(Reply)}]}]),
            httpc:request(post, {?UrtCommand, [{"serverID", ServerId},{"accessToken", AccessToken}], "application/json", Body}, [{timeout, 30000}, {connect_timeout, 30000}], [])
    end.
    
get_water_body(MeterId, DevSerial, Data) ->
    Image = Data,
    ClientType = "PC",
    UserID = "SZBD2016",
    DeviceID = MeterId,
    MacAddress = DevSerial,
    {Date, Time} = ?HELP:datetime_now(),   
    Datetime = ?HELP:getSimpleDateTimeStr(Date, Time, "-") ++ "-1234",
    Province = "guangdong",
    City = "shenzhen",
    Body = lists:concat(["Image=",Image,"&ClientType=",ClientType,"&UserID=", UserID,"&DeviceID=",DeviceID,"&MacAddress=",MacAddress,"&Datetime=",Datetime,"&Province=",Province,"&City=",City]).

get_water_msg(Data) ->
    Code = ?HELP:data_find("Code", Data),
    Message = ?HELP:data_find("Message", Data),
    Value = ?HELP:data_find("Value", Data),
    Filename = ?HELP:data_find("filename", Data),
    {list_to_binary(Code), list_to_binary(Message), list_to_binary(Filename), list_to_binary(Value)}.
