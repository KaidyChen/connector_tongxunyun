-module(iot_request_handler).

-include("print.hrl").
-include("log.hrl").
-include("config.hrl").

-export([init/2]).
-export([allowed_methods/2]).
-export([allow_miss_port/2]).
-export([content_types_provided/2]).
-export([content_types_accepted/2]).

-define(SUCCESS_STATUS, <<"200">>).
-define(FAILURE_STATUS, <<"300">>).
-define(NO_PERMISSION, "输入账户信息错误，无法进行操作").

-export([
         response_to_json/2, 
         response_to_html/2, 
         response_to_text/2
        ]).

-export([
         report_dev_callback/1,
         cmd_response_callback/1,
         hello/1
        ]).

init(Req, State) ->
    ?PRINT("REQ: ~p~n", [Req]),
    {cowboy_rest, Req, State}.

allowed_methods(Req, State) ->
    Result = [<<"GET">>, <<"POST">>, <<"HEAD">>, <<"OPTIONS">>],
    {Result, Req, State}.

allow_miss_port(Req, State) ->
    Result = false,
    {Result, Req, State}.

content_types_provided(Req, State) ->
    Result = [
              %{<<"text/html">>,  response_to_html},
              %{<<"text/plain">>, response_to_text},
              %{<<"application/x-www-form-urlencoded">>, response_to_json},
              {<<"application/json">>,  response_to_json}
             ],
    {Result, Req, State}.

content_types_accepted(Req, State) ->
    Result = [
              {<<"application/x-www-form-urlencoded">>, response_to_json},
              {<<"application/json">>,  response_to_json}
             ],
    {Result, Req, State}.

response_to_json(Req, State) ->
    Path = cowboy_req:path(Req),
    Method = cowboy_req:method(Req),
    %%QsInfo = cowboy_req:qs(Req),
    QsInfo = <<"?ipcid=180723000001">>,
    ?PRINT("QsInfo: ~p~n", [QsInfo]),
    {NewReq, Qs} = 
        case cowboy_req:has_body(Req) of
            true ->
                {ok, Body, ReqTmp} = cowboy_req:read_body(Req),
                ?PRINT("Body: ~p~n", [Body]),
                {ReqTmp, jsx:decode(Body)};
            false ->
                {Req, cowboy_req:parse_qs(Req)}
        end,

    Result = get_result(Path, Qs, QsInfo),
    ?PRINT("Path: ~p~n", [Path]),
    ?PRINT("Qs: ~p~n", [Qs]),

    case Method of
        <<"POST">> ->
            Res1 = cowboy_req:set_resp_body(Result, NewReq),
            Res2 = cowboy_req:delete_resp_header(<<"content-type">>, Res1),
            Res3 = cowboy_req:set_resp_header(<<"content-type">>, <<"application/json">>, Res2),
            {true, Res3, State};
        _ ->
            {Result, NewReq, State}
    end.

response_to_html(Req, State) ->
    Result = <<"<html>",
               "<body>",
               "<p>REST Hello World as HTML!</p>",
               "</body>",
               "</html>">>,
    {Result, Req, State}.

response_to_text(Req, State) ->
    Result = <<"Hello World!">>,
    {Result, Req, State}.

get_result(Path, Qs, QsInfo) ->
    Fun = proplists:get_value(filename:basename(Path), ?PATH_TO_FUN, hello),
    ?PRINT("Fun:~p~n", [Fun]),
    apply(?MODULE, Fun, [[Qs, QsInfo]]).

hello(Qs) ->
    Result = <<"{\"returnCode\":\"400\", \"returnMsg\": \"Hello World!\"}">>.

format_return(Reason, ReturnCode, QsInfo) ->
    ReturnMsg = ?HELPER:to_iolist(Reason),
    QsList = binary_to_list(QsInfo),
    [A | B] = string:tokens(QsList, "?"),
    case B of
        [] ->
            jsx:encode([{<<"returnCode">>, ReturnCode}, {<<"returnMsg">>, ReturnMsg}]);
        _ ->
            [C] = B,
            Flag = string:right(C, 42),
            jsx:encode([{<<"returnCode">>, ReturnCode}, {<<"flag">>, list_to_binary(Flag)}, {<<"returnMsg">>, ReturnMsg}])
    end.

data_return(Data, QsInfo) ->
    QsList = binary_to_list(QsInfo),
    [A | B] = string:tokens(QsList, "?"),
    case B of
        [] ->
            jsx:encode([{<<"returnCode">>, ?SUCCESS_STATUS}, {<<"data">>, Data}]);
        _ ->
            [C] = B,
            Flag = string:right(C, 42),
            jsx:encode([{<<"returnCode">>, ?SUCCESS_STATUS},{<<"flag">>, list_to_binary(Flag)}, {<<"data">>, Data}])
    end.

report_dev_callback([Qs, QsInfo]) ->
    DevSerial = binary_to_list(proplists:get_value(<<"devSerial">>, Qs, <<>>)),
    %%CreateTime = binary_to_list(proplists:get_value(<<"createTime">>, Qs, <<>>)),
    %%IotEventTime = binary_to_list(proplists:get_value(<<"iotEventTime">>, Qs, <<>>)),
    ServiceData = proplists:get_value(<<"serviceData">>, Qs, <<>>),
    %%LastMesssageTime = binary_to_list(proplists:get_value(<<"lastMesssageTime">>, Qs, <<>>)),
    Fun = 
            fun(PartData) ->
                case binary_to_list(proplists:get_value(<<"serviceId">>, PartData)) of
                    "message_1" ->
                        {true, {1,PartData}};%%1代表水表数据
                    "ElectricMsg" ->
                        {true, {2,PartData}};%%2代表电表数据
                    _ ->
                        false
                end
            end,
    [{Flag,Messagelist}] = lists:filtermap(Fun, ServiceData),
    ?PRINT("Flag:~p~n",[Flag]),
    case Flag of
        1 ->
            Message = binary_to_list(proplists:get_value(<<"message1">>, proplists:get_value(<<"serviceData">>,Messagelist))),
            ?PRINT("Message:~p~n",[Message]),
            %%判断是否为iot平台水表登录包，确认予以响应
            case string:left(Message,12) of
                    "FA071302FA04" ->
                        Msg = "/report_dev_callback receive iot login packet--> " ++ jsx:encode(Qs),
                        ?PRINT("收到iot水表登录包~n",[]),
                        MeterId = string:substr(Message, 13,12),
                        {Date, Time} = ?HELP:datetime_now(),
                        TimeStr = string:right(?HELP:getSimpleDateTimeStr(Date, Time, ""),12),
                        DataPart = string:left(Message,26) ++ "80" ++ TimeStr ++ "0000",
                        SendData = DataPart ++ ?HELP:get_cs(DataPart) ++ "FAFF",
                        iot_eqptid_store:insert({DevSerial, MeterId}),
                        {ok, ServerId, AccessToken} = iot_token_store:lookup("accessToken"),
                        Body = jsx:encode([{<<"devSerial">>,list_to_binary(DevSerial)},{<<"method">>,<<"setting_1">>},{<<"params">>,[{<<"setting1">>,list_to_binary(SendData)}]}]),
                        ?PRINT("Respone:~p~n",[Body]),
                        {ok, Result} = httpc:request(post, {?UrtCommand, [{"serverID", ServerId},{"accessToken", AccessToken}], "application/json", Body}, [{timeout, 30000}, {connect_timeout, 30000}], []);
                    _ ->
                        Msg = "/report_dev_callback receive iot datapacket--> " ++ jsx:encode(Qs),
                        ?PRINT("收到iot水表数据包~n",[]),
                        MeterId = string:substr(Message, 13,12),
                        case iot_clientpid_store:lookup(DevSerial) of
                            {ok, Pid} ->
                                Pid ! {water, DevSerial, Message};
                            _ ->
                                {ok, Pid} = iot_message_process:start_link([DevSerial,Message]),
                                iot_clientpid_store:insert({DevSerial, Pid})
                        end
            end;
        2 ->
            Message = binary_to_list(proplists:get_value(<<"electricity_meter">>, proplists:get_value(<<"serviceData">>,Messagelist))),
            case {string:left(Message,12) =:= "FA071304FA01", string:substr(Message, 27, 2)} of
                {true, "80"} ->
                    %%iot电表登录包
                    ?PRINT("收到iot电表登录包~n",[]),
                    Msg = "/report_dev_callback receive iot electricmeter login packet--> " ++ jsx:encode(Qs),
                    {Date, Time} = ?HELP:datetime_now(),
                    TimeStr = string:right(?HELP:getSimpleDateTimeStr(Date, Time, ""),12),
                    TimeStrRev = ?HELP:get_reverse_timestr(TimeStr),
                    DataPart = string:left(Message,26) ++ "00" ++ TimeStrRev ++ "0000",
                    SendData = DataPart ++ ?HELP:get_cs(DataPart) ++ "FAFF",
                    {ok, ServerId, AccessToken} = iot_token_store:lookup("accessToken"),
                    Body = jsx:encode([{<<"devSerial">>,list_to_binary(DevSerial)},{<<"method">>,<<"ElectricCmd">>},{<<"params">>,[{<<"electricity_meter">>,list_to_binary(SendData)}]}]),
                    httpc:request(post, {?UrtCommand, [{"serverID", ServerId},{"accessToken", AccessToken}], "application/json", Body}, [{timeout, 30000}, {connect_timeout, 30000}], []);
                {true, "92"} ->                   
                    Msg = "/report_dev_callback receive iot electricmeter datapacket--> " ++ jsx:encode(Qs),
                    ?PRINT("收到iot电表数据包~n",[]),
                    {Date, Time} = ?HELP:datetime_now(),
                    TimeStr = string:right(?HELP:getSimpleDateTimeStr(Date, Time, ""),12),
                    TimeStrRev = ?HELP:get_reverse_timestr(TimeStr),
                    DataPart = string:left(Message,26) ++ "12" ++ TimeStrRev ++ "0000",
                    SendData = DataPart ++ ?HELP:get_cs(DataPart) ++ "FAFF",
                    {ok, ServerId, AccessToken} = iot_token_store:lookup("accessToken"),
                    Body = jsx:encode([{<<"devSerial">>,list_to_binary(DevSerial)},{<<"method">>,<<"ElectricCmd">>},{<<"params">>,[{<<"electricity_meter">>,list_to_binary(SendData)}]}]),
                    httpc:request(post, {?UrtCommand, [{"serverID", ServerId},{"accessToken", AccessToken}], "application/json", Body}, [{timeout, 30000}, {connect_timeout, 30000}], []),
                    iot_message_parse:deal_with([DevSerial,Message,ServiceData]);
                _ ->
                    Msg = "/report_dev_callback receive iot unknown electricmeter datapacket--> " ++ jsx:encode(Qs),
                    ok
           end;
        _ ->
            Msg = "/report_dev_callback receive iot unknown packet--> " ++ jsx:encode(Qs),
            ok
    end,
    connector_event_server:easyiot_log(DevSerial,Msg),
    data_return(<<"OK">>, QsInfo).

cmd_response_callback([Qs, QsInfo]) ->
    %%备用拓展接口
    ?PRINT("/cmd_response_callback 收到数据包:~p~n",[Qs]),
    data_return(<<"OK">>, QsInfo).
