-module(connector_active_report).

-behaviour(gen_server).

%% Test
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("print.hrl").
-include("config.hrl").

%% API
-export([start_link/1]).
-export([stop/1]).
-export([gateway_offline/1]).
-export([
         active_report_645_packet/2,
         active_report_3761_packet/2
        ]).

-export([push_and_return_socket/2]).
-export([login_report_3761/2]).
-export([emqtt_data_report/1]).
%% Gen_server callbacks
-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% Connect opts
-define(OPTS, [binary, {active, once}, {packet, raw}]).

%% RE macro
-define(GATEWAYTYPE_RE, "@gatewayType").
-define(GATEWAYID_RE, "@gatewayId").
-define(STATUS_RE, "@status").
-define(METERTYPE_RE, "@meterType").
-define(METERID_RE, "@meterI").
-define(DATAFIELD_RE, "@dataField").

%% Gateway online status
-define(ONLINE_STATUS, "1").
%% Gateway offline status
-define(OFFLINE_STATUS, "0").

%% Gateway statusmsg templet
-define(STATUSMSG_TEMPLET, "statusMsg" ++ "/" ++ ?GATEWAYTYPE_RE  ++ "#" ++ ?GATEWAYID_RE  ++ "#" ++ ?STATUS_RE).
%% Meter datamsg templet
-define(DATAMSG_TEMPLET, "dataMsg" ++ "/" ++ ?METERTYPE_RE ++ "#" ++ ?METERID_RE ++ "#" ++ ?DATAFIELD_RE).
%% Meter warnmsg templet
-define(WARNMSG_TEMPLET, "warnMsg" ++ "/" ++ ?METERTYPE_RE ++ "#" ++ ?METERID_RE ++ "#" ++ ?DATAFIELD_RE).
%% Meter controlmsg templet
-define(CONTROLMSG_TEMPLET, "controlMsg" ++ "/" ++ ?METERTYPE_RE ++ "#" ++ ?METERID_RE ++ "#" ++ ?DATAFIELD_RE).

-define(PROTOCOL_3761, protocol_3761).
-define(PROTOCOL_645, protocol_645).

-record(state, {
          gatewayType :: string(),
          gatewayId :: string(),
          parent :: pid(),
          socket = undefined :: undefined | inet:socket()
         }).

start_link([GatewayType, GatewayId, Parent]) ->
    gen_server:start_link(?MODULE, [GatewayType, GatewayId, Parent], []).

stop(Pid) ->
    gen_server:cast(Pid, {stop_reason, void}).

gateway_offline(Pid) ->
    gen_server:cast(Pid, {stop_reason, gateway_offline}).

active_report_645_packet(Pid, Packet) ->
    active_report_packet(Pid, ?PROTOCOL_645, Packet).

active_report_3761_packet(Pid, Packet) ->
    active_report_packet(Pid, ?PROTOCOL_3761, Packet).

active_report_packet(Pid, Protocol, Packet) when is_pid(Pid) ->
    gen_server:cast(Pid, {active_report_packet, Protocol, Packet});
active_report_packet(_, _, _) ->
    ok.

init([GatewayType, GatewayId, Parent]) ->
    ?PRINT("Parent:~p active pid:~p~n", [Parent, self()]),
    State = #state{
               gatewayType = GatewayType,
               gatewayId = GatewayId,
               parent = Parent,
               socket = undefined
              },
    {ok, State}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({active_report_packet, ?PROTOCOL_3761 = Protocol, 
             <<16#68, Len:16, Len:16, 16#68, Ctrl:8, _Addr:(5*8), AFN:8, Seq:8, DA:16, DT:16, 
               MeterIdBinary:7/binary-unit:8, _/binary>> = Packet}, State) when is_binary(Packet) -> 
    #state{
       gatewayType = GatewayType,
       gatewayId = GatewayId,
       socket = Socket,
       parent = Parent
      } = State,

    FrameHex = hex_util:to_hex(Packet),
    io:format("376.1 login packet!!!",[]),
    
    %% Log record: active report log
    connector_event_server:active_report_log(GatewayId, FrameHex),
    {ok, InputData} = get_input_json(FrameHex,"3761"),
    case data_report(?SOLIBNAME, InputData) of
        {ok, _, OutputJson} ->
            Parselist = ?HELP:parse(OutputJson),
            Datalist = ?HELP:data_find("data", Parselist),
            SendFrame = ?HELP:data_find("sendframe", Parselist),
            ?PRINT("Parent=====~p~n",[Parent]),
            connector_channel_protocol:recv_order_frame_from_client(Parent, SendFrame),
            ActiveReportMsg = ?HELP:json_to_str(Datalist),
            Time = ?HELP:datetime_now_str(),
            ReportMsg = "{\"eqpttype\":\"" ++ GatewayType ++ "\"" ++ "\," ++ "\"meterid\":\"" ++ GatewayId 
                    ++ "\"" ++ "\," ++ "\"time\":\"" ++ Time ++ "\"" ++ "\," ++ "\"data\":" ++ ActiveReportMsg 
                    ++ "}",
            [{active_report_opts, ActiveReportOpts}] = ets:lookup(?LISTEN_OPTS_TABLE, active_report_opts),
            Flag = proplists:get_value(report, ActiveReportOpts),
            case Flag of
                "on" ->
                    NewSocket = push_and_return_socket(Socket, ReportMsg),
                    {noreply, State#state{socket = NewSocket}};
                "off" ->
                    emqtt_data_report({GatewayId, GatewayId, ReportMsg}),
                    {noreply, State}
            end;
        {error, Reason} ->
            ?ERROR("data report fail~p~n",[Reason]),
            {noreply, State}
    end;
    
%% Gateway online
handle_cast({active_report_packet, ?PROTOCOL_645, Packet = <<16#FA, 16#07, 16#13, 16#02, 16#FA, 16#02,
                            _GatewayIdBinary:?DEVICE_ID_BYTES_SIZE/binary-unit:8, 16#FA, 16#FF>>}, State) ->
    #state{
       gatewayType = GatewayType,
       gatewayId = GatewayId,
       socket = Socket
      } = State,
    ActiveReportMsg = build_gateway_status_msg(GatewayType, GatewayId, ?ONLINE_STATUS),
    ReportMsg = connector_report_data_process:report_manage(ActiveReportMsg),
    [{active_report_opts, ActiveReportOpts}] = ets:lookup(?LISTEN_OPTS_TABLE, active_report_opts),
    Flag = proplists:get_value(report, ActiveReportOpts),
    case Flag of
        "on" ->
            %%函数处理后返回json字符串再通过socket发送 
            NewSocket = push_and_return_socket(Socket, ReportMsg),
            {noreply, State#state{socket = NewSocket}};
        "off" ->
            emqtt_data_report({GatewayId, GatewayId, ReportMsg}),
            {noreply, State}
    end;
%% Heartbeat report 
handle_cast({active_report_packet, ?PROTOCOL_645, Packet = <<16#00>>}, State) ->
    #state{
       gatewayType = GatewayType,
       gatewayId = GatewayId,
       socket = Socket
      } = State,
    ActiveReportMsg = build_gateway_status_msg(GatewayType, GatewayId, ?ONLINE_STATUS),
    ReportMsg = connector_report_data_process:report_manage(ActiveReportMsg),
    [{active_report_opts, ActiveReportOpts}] = ets:lookup(?LISTEN_OPTS_TABLE, active_report_opts),
    Flag = proplists:get_value(report, ActiveReportOpts),
    case Flag of
        "on" ->
            %%函数处理后返回json字符串再通过socket发送 
            NewSocket = push_and_return_socket(Socket, ReportMsg),
            {noreply, State#state{socket = NewSocket}};
        "off" ->
            emqtt_data_report({GatewayId, GatewayId, ReportMsg}),
            {noreply, State}
    end;
%%门锁数据上报
handle_cast({active_report_packet, ?PROTOCOL_645 = Protocol, Packet = <<16#68, _MeterId:6/binary-unit:8, 16#68, _Part:2/binary-unit:8, 16#88, 16#AA, 16#FF, 16#EF, _/binary>>}, State) ->
    #state{
           gatewayType = GatewayType,
           gatewayId = GatewayId,
           socket = Socket,
           parent = Parent
          } = State,
    MeterId = get_645_meterId(Packet),
    FrameHex = hex_util:to_hex(Packet),
    ?PRINT("门锁数据上报:~p~n", [FrameHex]),
    case get_meterType(GatewayId, MeterId) of
        {error, Reason} ->
            ?ERROR("~p:~p get_meterType(~p) is error:~p~n",[?FILE,?LINE, MeterId, Reason]),
            MeterType = "undefined";
        {ok, MeterType} ->
            MeterType
    end,
    %% Log record: active report log
    connector_event_server:active_report_log(MeterId, FrameHex),
    Time = ?HELP:datetime_now_str(),
    ActiveReportMsg = build_gateway_status_msg(MeterType, MeterId, ?ONLINE_STATUS),
    ReportMsg = connector_report_data_process:report_manage(ActiveReportMsg),
    [{active_report_opts, ActiveReportOpts}] = ets:lookup(?LISTEN_OPTS_TABLE, active_report_opts),
    Flag = proplists:get_value(report, ActiveReportOpts),
    case Flag of
        "on" ->
             NewSocket = push_and_return_socket(Socket, ReportMsg),
             {noreply, State#state{socket = NewSocket}};
        "off" ->
             emqtt_data_report({GatewayId, MeterId, ReportMsg}),
             {noreply, State}
    end;
handle_cast({active_report_packet, ?PROTOCOL_645 = Protocol, Packet}, State) when is_binary(Packet) ->
    #state{
       gatewayType = GatewayType,
       gatewayId = GatewayId,
       socket = Socket,
       parent = Parent
      } = State,

    MeterId = get_645_meterId(Packet),
    FrameHex = hex_util:to_hex(Packet),
    ?PRINT("645 report:~p~n", [FrameHex]),
    case get_meterType(GatewayId, MeterId) of
        {error, Reason} ->
            ?ERROR("~p:~p get_meterType(~p) is error:~p~n",[?FILE,?LINE, MeterId, Reason]),
            MeterType = "undefined";
        {ok, MeterType} ->
            MeterType
    end,
    %% Log record: active report log
    connector_event_server:active_report_log(MeterId, FrameHex),
    Time = ?HELP:datetime_now_str(),
    %% 充电桩主动上报数据json动态库解析，其他类型设备暂时内部解析
    case get_report_flag(Packet, MeterId, GatewayId) of
        true ->
                {ok, InputData} = get_input_json(FrameHex, "realcom"),
                {ok, _, OutputJson} = data_report(?SOLIBNAME, InputData),
                Parselist = ?HELP:parse(OutputJson),
                Datalist = ?HELP:data_find("data", Parselist),
                SendFrame = ?HELP:data_find("sendframe", Parselist),
                ?PRINT("Parent=====~p~n",[Parent]),
                connector_channel_protocol:recv_order_frame_from_client(Parent, SendFrame),
                ActiveReportMsg = ?HELP:json_to_str(Datalist),
                ReportMsg = "{\"eqpttype\":\"" ++ MeterType ++ "\"" ++ "\," ++ "\"meterid\":\"" ++ MeterId ++ "\"" ++ "\," ++ "\"time\":\"" ++ Time ++ "\"" ++ "\," ++ "\"data\":" ++ ActiveReportMsg ++ "}";
        _ ->          
                {ok, ActiveReportMsg} = build_active_report_msg(Protocol, Packet, GatewayId),
                ReportMsg = connector_report_data_process:report_manage(ActiveReportMsg)
    end,
    [{active_report_opts, ActiveReportOpts}] = ets:lookup(?LISTEN_OPTS_TABLE, active_report_opts),
    Flag = proplists:get_value(report, ActiveReportOpts),
    case Flag of
        "on" ->
                NewSocket = push_and_return_socket(Socket, ReportMsg),
                {noreply, State#state{socket = NewSocket}};
        "off" ->
                emqtt_data_report({GatewayId, MeterId, ReportMsg}),
                {noreply, State}
    end;

%% Gateway offline
handle_cast({stop_reason, gateway_offline}, State) ->
    #state{
       gatewayType = GatewayType,
       gatewayId = GatewayId,
       socket = Socket
      } = State,
    ?PRINT("active report gateway_offline~n", []),
    ActiveReportMsg = build_gateway_status_msg(GatewayType, GatewayId, ?OFFLINE_STATUS),
    ReportMsg = connector_report_data_process:report_manage(ActiveReportMsg),
    [{active_report_opts, ActiveReportOpts}] = ets:lookup(?LISTEN_OPTS_TABLE, active_report_opts),
    Flag = proplists:get_value(report, ActiveReportOpts),
    case Flag of
        "on" ->
            NewSocket = push_and_return_socket(Socket, ReportMsg),
            {stop, normal, State#state{socket = NewSocket}};
        "off" ->
            emqtt_data_report({GatewayId, GatewayId, ReportMsg}),
            {stop, normal, State}
    end;
handle_cast({stop_reason, _}, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp_closed, Socket}, State) ->
    close(Socket),
    {noreply, State#state{socket = undefined}};
handle_info({tcp_error, Socket, _Reason}, State) ->
    close(Socket),
    {noreply, State#state{socket = undefined}};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, State = #state{socket = Socket}) ->
    ?PRINT("~p terminate~n", [self()]),
    close(Socket),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

login_report_3761(GatewayType, GatewayId) ->
    Socket = undefined,
    [{active_report_opts, ActiveReportOpts}] = ets:lookup(?LISTEN_OPTS_TABLE, active_report_opts),
    ActiveReportMsg = build_gateway_status_msg(GatewayType, GatewayId, ?ONLINE_STATUS),
    ReportMsg = connector_report_data_process:report_manage(ActiveReportMsg), 
    Flag = proplists:get_value(report, ActiveReportOpts),
    case Flag of
        "on" ->
            NewSocket = push_and_return_socket(Socket, ReportMsg);
        "off" ->
            emqtt_data_report({GatewayId, GatewayId, ReportMsg})
    end.

get_meterId(MeterIdBinTmp) ->
    MeterIdBin = list_to_binary(lists:reverse(binary_to_list(MeterIdBinTmp))),
    hex_util:to_hex(MeterIdBin).    

get_3761_meterId(<<16#68, Len:16, Len:16, 16#68, Ctrl:8, _Addr:(5*8), AFN:8, Seq:8, DA:16, DT:16, NUM:8, MeterIdBinTmp:7/binary-unit:8, _/binary>> = Packet) ->
    get_meterId(MeterIdBinTmp).
    
get_645_meterId(Packet) ->
    %% Frist byte is 16#68, and 6 bytes meterId
    MeterIdBinTmp = binary:part(Packet, 1, 6),
    get_meterId(MeterIdBinTmp).

get_meterType(GatewayId, MeterId) ->
    case connector_db_store_server:get_eqpt_type(GatewayId, MeterId) of
        {ok, MeterType} -> {ok, MeterType};
        {error, Reason} -> {error, Reason}
    end.

get_645_dataField(Packet) ->
    %% Skip 16#68, meterId, 16#68, ctrlCode(1 byte)
    DataFieldLength = binary:at(Packet, 1+6+1+1),
    DataFieldBin = binary:part(Packet, 1+6+1+1+1, DataFieldLength),
    hex_util:to_hex(DataFieldBin).

get_dataFieldFlagBin(Packet) ->
    %% Skip 16#68, meterId, 16#68, ctrlCode, dataFieldLength
    %% DataFidldflag(4 bytes)
    binary:part(Packet, 1+6+1+1+1, 4).

%% Eunit
-ifdef(EUNIT).

get_XX_test() ->
    FrameHex = "682632002312136836293832ddef83593833333333b335343353354a983333c3c43a333a484433859488c7336337354853344a7416",
    Packet = hex_util:to_bin(FrameHex),
    [
     ?assertEqual("131223003226", get_645_meterId(Packet)),
     ?assertEqual("3832ddef83593833333333b335343353354a983333c3c43a333a484433859488c7336337354853344a", get_645_dataField(Packet)),
     ?assertEqual(<<16#38, 16#32, 16#dd, 16#ef>>, get_dataFieldFlagBin(Packet)),
     ?assertEqual(1, 1)
    ].

-endif.

build_active_report_msg(?PROTOCOL_3761 = Protocol, 
                        <<16#68, Len:16, Len:16, 16#68, Ctrl:8, _Addr:(5*8), AFN:8, Seq:8, DA:16, 
                          DataFieldBinary/binary>> = Packet, GatewayId) ->
    MeterId = get_3761_meterId(Packet),
    DataField = hex_util:to_hex(DataFieldBinary),
    %%io:format("GatewayId:~p MeterId:~p",[GatewayId,MeterId]),
    case get_meterType(GatewayId, MeterId) of
        {error, _} ->
            case get_meterType(GatewayId, MeterIdTmp = string:right(MeterId, 12)) of
                {ok, MeterType1} -> 
                    ?PRINT("~p active report msg: ~p~n", [Protocol, DataField]),
                    {ok, build_meter_data_msg(MeterType1, MeterIdTmp, DataField)};
                {error, Reason} ->
                    ?ERROR("~p:~p get_meterType(~p) is error: ~p~n", [?FILE, ?LINE, MeterId, Reason]),
                    {error, Reason}
            end;
        {ok, MeterType2} ->
            ?PRINT("~p active report msg: ~p~n", [Protocol, DataField]),
            {ok, build_meter_data_msg(MeterType2, MeterId, DataField)}
    end;
build_active_report_msg(?PROTOCOL_645, Packet, GatewayId) ->
    MeterId = get_645_meterId(Packet),
    case get_meterType(GatewayId, MeterId) of
        {error, Reason} ->
            ?ERROR("~p:~p get_meterType(~p) is error: ~p~n", [?FILE, ?LINE, MeterId, Reason]),
            {error, Reason};
        {ok, MeterType} ->
            DataField = get_645_dataField(Packet),
            DataFieldFlagBin = get_dataFieldFlagBin(Packet),
            IsMember1 = lists:member(DataFieldFlagBin, ?DATAMSG_DATAFIELDFLAG_LIST),
            IsMember2 = lists:member(DataFieldFlagBin, ?WARNMSG_DATAFIELDFLAG_LIST),
            IsMember3 = lists:member(DataFieldFlagBin, ?CONTROL_DATAFIELDFLAG_LIST),
            case {IsMember1, IsMember2, IsMember3} of
                {true, _, _} ->
                    {ok, build_meter_data_msg(MeterType, MeterId, DataField)};
                {_, true, _} ->
                    {ok, build_meter_warn_msg(MeterType, MeterId, DataField)};
                {_, _, true} ->
                    ?PRINT("645 control report:~p~n", [DataField]),
                    {ok, build_meter_control_msg(MeterType, MeterId, DataField)};
                _ -> 
                    ?ERROR("~p:~p DataField:~p neither dataMsg nor warnMsg, controlMsg~n", [?FILE, ?LINE, DataField]),
                    {error, not_match_data_field}
            end
    end.

%% Build meter data msg
build_meter_data_msg(MeterType, MeterId, DataField) ->
    ReplaceList = [
                   {?METERTYPE_RE, MeterType},
                   {?METERID_RE, MeterId},
                   {?DATAFIELD_RE, DataField}
                  ],
    replace_templet(?DATAMSG_TEMPLET, ReplaceList).

%% Build meter warn msg
build_meter_warn_msg(MeterType, MeterId, DataField) ->
    ReplaceList = [
                   {?METERTYPE_RE, MeterType},
                   {?METERID_RE, MeterId},
                   {?DATAFIELD_RE, DataField}
                  ],
    replace_templet(?WARNMSG_TEMPLET, ReplaceList).

%% Build meter control msg
build_meter_control_msg(MeterType, MeterId, DataField) ->
    ReplaceList = [
                   {?METERTYPE_RE, MeterType},
                   {?METERID_RE, MeterId},
                   {?DATAFIELD_RE, DataField}
                  ],
    replace_templet(?CONTROLMSG_TEMPLET, ReplaceList).
    

%% Build gateway status msg
build_gateway_status_msg(GatewayType, GatewayId, Status) ->
    ReplaceList = [
                   {?GATEWAYTYPE_RE, GatewayType},
                   {?GATEWAYID_RE, GatewayId},
                   {?STATUS_RE, Status}
                  ],
    replace_templet(?STATUSMSG_TEMPLET, ReplaceList).

%% Replace templet
replace_templet(SourceStr, ReplaceList) ->
    Fun = fun({RE, Replacement}, String) ->
                  NewString = re:replace(String, RE, Replacement, [{return, list}]),
                  NewString
          end,
    lists:foldl(Fun, SourceStr, ReplaceList).

push_and_return_socket(SocketTmp, ActiveReportMsg) ->
    Socket = get_socket(SocketTmp),
    case push(Socket, ActiveReportMsg) of
        ok -> Socket;
        {error, Reason} ->
            ?ERROR("~p:~p ActiveReportmsg:~p push is error: ~p~n", [?FILE, ?LINE, ActiveReportMsg, Reason]),
            close(Socket),
            undefined
    end.

push(Socket, ActiveReportMsg) ->
    Packet = ?HELPER:to_binary(ActiveReportMsg ++ ?RS),
    send(Socket, Packet).

send(Socket, Packet) when is_port(Socket) ->
    gen_tcp:send(Socket, Packet);
send(_, _) ->
    {error, not_socket}.

get_socket(Socket) when is_port(Socket) ->
    Socket;
get_socket(_) ->
    [{active_report_opts, ActiveReportOpts}] = ets:lookup(?LISTEN_OPTS_TABLE, active_report_opts),
    Ip = proplists:get_value(ip, ActiveReportOpts),
    Port = proplists:get_value(port, ActiveReportOpts),
    case gen_tcp:connect(Ip, Port, ?OPTS) of
        {ok, Socket} ->
            Socket;
        _ ->
            undefined
    end.

close(Socket) when is_port(Socket) ->
    gen_tcp:close(Socket);
close(_) ->
    ok.

emqtt_data_report({GatewayId, MeterId, ReportMsg}) ->
    [{ipc_id, IpcId}] = ets:lookup(?COMPUTER_OPTS_TABLE, ipc_id),
    {ok, Topic} = connector_topic_info_store:lookup({GatewayId, MeterId}),
    TopicBin = list_to_binary(lists:concat([Topic, "/", IpcId, "/", GatewayId, "/", MeterId])),
    DataBin = list_to_binary(ReportMsg),
    emqttc_server ! {topic_and_data, {TopicBin, DataBin}}.

data_report(SoLibName, InputData) ->
    exec_func(SoLibName, "data_report", InputData).

exec_func(SoFileName, FuncName, InputData) ->
    try ?NIFJSONMODULE:exec_func(SoFileName, FuncName, InputData) of
        {ok, {FuncResult, OutputData}} ->
            ?PRINT("FuncResult:~p~n OutputData:~p~n",[FuncResult, OutputData]),
            {ok, FuncResult, OutputData};
        {error, Reason} ->
            {error, Reason}
    catch
        Class:What ->
            {error, What}
    end.

get_input_json(FrameHex, ReportFrom) ->
    [{eqpt_pwd, Key}] = ets:lookup(?COMPUTER_OPTS_TABLE, eqpt_pwd),
    InputData = lists:concat(["{\"analyframe\":\"", FrameHex, "\",\"reportfrom\":\"", ReportFrom,"\",
                                      \"mainkey\":\"", Key, "\"}"]),
    {ok, InputData}.

get_report_flag(Packet, MeterId, GatewayId) ->
    case get_meterType(GatewayId, MeterId) of
        {error, Reason} ->
            ?ERROR("~p:~p get_meterType(~p) is error: ~p~n",[?FILE,?LINE,MeterId,Reason]),
            {error, Reason};
        {ok, MeterType} ->
            DataFieldFlagBin = get_dataFieldFlagBin(Packet),
            case lists:member(DataFieldFlagBin, ?LIBJSON_DATAFIELDFLAG_LIST) of
                true ->
                    true;
                _ ->
                    false
            end
    end.
