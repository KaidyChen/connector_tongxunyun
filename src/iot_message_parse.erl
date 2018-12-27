-module(iot_message_parse).

-include("print.hrl").
-include("config.hrl").

-export([deal_with/1]).

deal_with([DevSerial, FrameHex, ServiceData]) ->
    Imei = get_data("UserDataHead", "imei", ServiceData),
    {ok, InputData} = get_input_json(FrameHex,"nbiot"),
    {ok, _, OutputJson} = data_report(?SOLIBNAME, InputData),
    Parselist = ?HELP:parse(OutputJson),
    Datalist = ?HELP:data_find("data", Parselist),
    ActiveReportMsg = ?HELP:json_to_str(Datalist),
    Time = ?HELP:datetime_now_str(),
    [{ipc_id, IpcId}] = ets:lookup(?COMPUTER_OPTS_TABLE, ipc_id),
    ReportMsg = "{\"devserial\":\"" ++ DevSerial ++ "\"" ++ "\," ++ "\"time\":\"" ++ Time ++ "\"" ++ "\," ++ "\"data\":" ++ ActiveReportMsg ++ "}",
    Topic = lists:concat(["easyiot/electric","/", IpcId, "/", DevSerial]),
    emqttc_server ! {topic_and_data, {list_to_binary(Topic),list_to_binary(ReportMsg)}}.
    %%?HELP:store_report_data(DevSerial, Imei, Time, ReportMsg).


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

get_data(Message, TargetData, ServiceData) ->
    Fun = 
            fun(PartData) ->
                case binary_to_list(proplists:get_value(<<"serviceId">>, PartData)) of
                    Message ->
                        {true, PartData};
                    _ ->
                        false
                end
            end,
    [Messagelist] = lists:filtermap(Fun, ServiceData),
    TargetMsg = binary_to_list(proplists:get_value(list_to_binary(TargetData), proplists:get_value(<<"serviceData">>,Messagelist))).
