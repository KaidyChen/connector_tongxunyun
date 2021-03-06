-module(db_rest_handler).

-include("print.hrl").
-include("config.hrl").

-export([init/2]).
-export([allowed_methods/2]).
-export([allow_miss_port/2]).
-export([content_types_provided/2]).
-export([content_types_accepted/2]).

-define(SUCCESS_STATUS, <<"200">>).
-define(FAILURE_STATUS, <<"300">>).
-define(COLLECTOR_LEVEL, 1).
-define(REPEATERS_LEVEL, 2).
-define(METER_LEVEL, 3).

-define(DEFAULT_PAGE_SIZE, 50).
-define(DEFAULT_PAGE, 1).

-define(ILLEGAL_EQPT_TYPE, "传入非法的设备类型").
-define(ALREADY_EXISTS, "已经存在该设备,不支持重复添加").
-define(NOT_EXISTS, "不存在该设备，不支持修改/删除操作").
-define(EXISTS_METER, "该设备下挂有计量设备，不支持删除").

-export([
         response_to_json/2, 
         response_to_html/2, 
         response_to_text/2
        ]).

-export([
         add_device_type_info/1,
         delete_device_type_info/1,
         get_device_type_info/1,
         get_collector_type_info/1,
         get_repeaters_type_info/1,
         get_meter_type_info/1,

         add_collector/1,
         update_collector/1,
         delete_collector/1,
         force_delete_collector/1,

         add_meter/1,
         update_meter/1,
         delete_meter/1,
         delete_meter_by_collector/1,

         get_collector_info_total_row/1,
         get_collector_info/1,

         get_repeaters_info_total_row/1,
         get_repeaters_info/1,

         get_meter_info_total_row/1,
         get_meter_info/1,

         get_meter_network_by_collector/1,
         get_collector_info_by_eqpt_type/1,
         get_repeaters_info_by_eqpt_type/1,

         batch_add_collector/1,
         batch_add_meter/1,
         batch_get_meter/1,
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
    {NewReq, Qs} = 
        case cowboy_req:has_body(Req) of
            true ->
                {ok, Body, ReqTmp} = cowboy_req:read_body(Req),
                ?PRINT("Body: ~p~n", [Body]),
                {ReqTmp, jsx:decode(Body)};
            false ->
                {Req, cowboy_req:parse_qs(Req)}
        end,

    Result = get_result(Path, Qs),
   % ?PRINT("Result:~s~n", [binary_to_list(Result)]),

   % ?PRINT("Pid: ~p~n", [self()]),
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

get_result(Path, Qs) ->
    Fun = proplists:get_value(filename:basename(Path), ?PATH_TO_FUN, hello),
    ?PRINT("Fun: ~p~n", [Fun]),
    apply(?MODULE, Fun, [Qs]).

hello(Qs) ->
    Result = <<"{\"returnCode\":\"400\", \"returnMsg\": \"Hello World!\"}">>.

notify_update_eqpt_info() ->
    connector_db_store_server:notify_update_eqpt_info().

format_return(Reason, ReturnCode) ->
    ReturnMsg = ?HELPER:to_iolist(Reason),
    jsx:encode([{<<"returnCode">>, ReturnCode}, {<<"returnMsg">>, ReturnMsg}]).

data_return(Data) ->
    jsx:encode([{<<"returnCode">>, ?SUCCESS_STATUS}, {<<"data">>, Data}]).

total_return(Count) ->
    jsx:encode([{<<"returnCode">>, ?SUCCESS_STATUS}, {<<"totalRow">>, Count}]).

%% 字段格式化
field_format(Rows) ->
    [field_format_(Row) || Row <- Rows].

field_format_(Row) ->
    Fun = 
        fun(Item) ->
                case Item of
                    null -> <<"null">>;
                    _ -> Item
                end
        end,
    list_to_tuple(lists:map(Fun, tuple_to_list(Row))).

%%---------------------------------------------------------------------------------------
%% 获取设备的类型信息
%%---------------------------------------------------------------------------------------
%%添加设备的类型信息
add_device_type_info(Qs) ->
    EqptLevel = binary_to_list(proplists:get_value(<<"eqptLevel">>, Qs, <<>>)),
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptTypeName = binary_to_list(proplists:get_value(<<"eqptTypeName">>, Qs, <<>>)),
    ProtocolName = binary_to_list(proplists:get_value(<<"protocolName">>, Qs, <<>>)),
    ProtocolType = binary_to_list(proplists:get_value(<<"protocolType">>, Qs, <<>>)),
    case db_util:add_device_type_info(EqptLevel, EqptType, EqptTypeName, ProtocolName, ProtocolType) of
        ok ->
            db_util:commit_transaction(),
            notify_update_eqpt_info(),
            format_return("add device_type_info success", ?SUCCESS_STATUS);
        {error, Reason} ->
            db_util:rollback(),
            format_return(Reason, ?FAILURE_STATUS)
    end.
%%删除设备类型信息
delete_device_type_info(Qs) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    case db_util:delete_device_type_info(EqptType) of
        ok ->
            db_util:commit_transaction(),
            notify_update_eqpt_info(),
            format_return("delete device_type_info success", ?SUCCESS_STATUS);
        {error, Reason} ->
            db_util:rollback(),
            format_return(Reason, ?FAILURE_STATUS)
    end.

%% 获取所有设备的类型信息
get_device_type_info(_Qs) ->
    case db_util:get_device_type_info() of
        {ok, []} ->
            data_return([]);
        {ok, Rows} ->
            List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptTypeName">>, EqptTypeNameBinary}, 
                     {<<"protocolName">>, ProtocolNameBinary}, {<<"protocolType">>, ProtocolTypeBinary}] || 
                       {EqptTypeBinary, EqptTypeNameBinary, ProtocolNameBinary, ProtocolTypeBinary} <- field_format(Rows)],
            data_return(List);
        {error, Reason} ->
            format_return(Reason, ?FAILURE_STATUS)
    end.

%% 获取采集设备的类型信息
get_collector_type_info(_Qs) ->
    get_specific_type_info(?COLLECTOR_LEVEL).

%% 获取中继器设备的类型信息
get_repeaters_type_info(_Qs) ->
    get_specific_type_info(?REPEATERS_LEVEL).

%% 获取计量设备的类型信息
get_meter_type_info(_Qs) ->
    get_specific_type_info(?METER_LEVEL).

%% 获取指定级别设备的类型信息
get_specific_type_info(EqptLevel) ->
    case db_util:get_specific_type_info(EqptLevel) of
        {ok, []} ->
            data_return([]);
        {ok, Rows} ->
            List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptTypeName">>, EqptNameBinary}, 
                     {<<"protocolName">>, ProtocolNameBinary}, {<<"protocolType">>, ProtocolTypeBinary}] || 
                       {EqptTypeBinary, EqptNameBinary, ProtocolNameBinary, ProtocolTypeBinary} <- field_format(Rows)],
            data_return(List);
        {error, Reason} ->
            format_return(Reason, ?FAILURE_STATUS)
    end.


%%----------------------------------------------------------------------------------------------
%% 批量操作
%%----------------------------------------------------------------------------------------------

batch_add_collector(Qs) ->
    try get_collector_info_list(Qs) of
        CollectorInfoList ->
            db_util:begin_transaction(),
            case batch_add_collector_(CollectorInfoList) of
                ok ->
                    db_util:commit_transaction(),
                    Msg = string:join(["batch_add_collector", CollectorInfoList], "/"),
                    connector_event_server:operation_log(" ", Msg),
                    notify_update_eqpt_info(),
                    format_return("batch add collectors success", ?SUCCESS_STATUS);
                {error, Reason} ->
                    db_util:rollback(),
                    format_return(Reason, ?FAILURE_STATUS)
            end
    catch 
        _Class:What ->
            format_return(What, ?FAILURE_STATUS)
    end.

batch_add_collector_([{?COLLECTOR_LEVEL, EqptType, EqptIdCode, EqptName, Creator, ProtocolType} | T]) ->
    case db_util:add_collector(EqptType, EqptIdCode, EqptName, Creator, ProtocolType) of
        ok ->
            batch_add_collector_(T);
        {error, Reason} ->
            {error, Reason}
    end;
batch_add_collector_([{?REPEATERS_LEVEL, EqptType, EqptIdCode, EqptName, Creator, ProtocolType} | T]) ->
    case db_util:add_repeaters(EqptType, EqptIdCode, EqptName, Creator) of
        ok ->
            batch_add_collector_(T);
        {error, Reason} ->
            {error, Reason}
    end;
batch_add_collector_([]) ->
    ok.
     

get_collector_info_list(Qs) ->
    get_collector_info_list_(Qs, []).

get_collector_info_list_([Qs | T], CollectorInfoList) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    EqptName = binary_to_list(proplists:get_value(<<"eqptName">>, Qs, <<>>)),
    Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
        {ok, EqptLevel, ProtocolType} when (EqptLevel =:= ?COLLECTOR_LEVEL) 
                                           orelse (EqptLevel =:= ?REPEATERS_LEVEL) ->
            CollectorInfo = {EqptLevel, EqptType, EqptIdCode, EqptName, Creator, ProtocolType},
            ?PRINT("~p~n", [CollectorInfo]),
            get_collector_info_list_(T, [CollectorInfo | CollectorInfoList]);
        _ ->
            throw(EqptIdCode ++ " " ++ ?ILLEGAL_EQPT_TYPE)
    end;
get_collector_info_list_(_, CollectorInfoList) ->
    CollectorInfoList.


batch_add_meter(Qs) ->
    try get_meter_info_list(Qs) of
        MeterInfoList ->
            db_util:begin_transaction(),
            case batch_add_meter_(MeterInfoList) of
                ok ->
                    db_util:commit_transaction(),
                    notify_update_eqpt_info(),
                    Msg = string:join(["batch_add_meter", MeterInfoList], "/"),
                    connector_event_server:operation_log(" ", Msg),
                    format_return("batch add collectors success", ?SUCCESS_STATUS);
                {error, Reason} ->
                    db_util:rollback(),
                    format_return(Reason, ?FAILURE_STATUS)
            end
    catch 
        _Class:What ->
            format_return(What, ?FAILURE_STATUS)
    end.

batch_add_meter_([{EqptType, EqptIdCode, EqptName, EqptCjqType, EqptCjqCode, EqptZjqType, EqptZjqCode, 
                           Creator, EqptPort, EqptBaudrate, EqptRateNumber, EqptZsw, EqptXsw, EqptDlh, EqptXlh,
                           EqptYblx, EqptProtocolType, EqptCjqProtocolType} | T]) ->
    case db_util:add_meter(EqptType, EqptIdCode, EqptName, EqptCjqType, EqptCjqCode, EqptZjqType, EqptZjqCode, 
                           Creator, EqptPort, EqptBaudrate, EqptRateNumber, EqptZsw, EqptXsw, EqptDlh, EqptXlh,
                           EqptYblx, EqptProtocolType, EqptCjqProtocolType) of
        ok ->
            batch_add_meter_(T);
        {error, Reason} ->
            {error, Reason}
    end;
batch_add_meter_([]) ->
    ok.     

get_meter_info_list(Qs) ->
    get_meter_info_list_(Qs, []).

get_meter_info_list_([Qs | T], MeterInfoList) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    EqptName = binary_to_list(proplists:get_value(<<"eqptName">>, Qs, <<>>)),
    EqptCjqType = binary_to_list(proplists:get_value(<<"eqptCjqType">>, Qs, <<>>)),
    EqptCjqCode = binary_to_list(proplists:get_value(<<"eqptCjqCode">>, Qs, <<>>)),
    EqptZjqType = 
        case proplists:get_value(<<"eqptZjqType">>, Qs) of
            undefined -> null;
            EqptZjqTypeBinary -> binary_to_list(EqptZjqTypeBinary)
        end,
    EqptZjqCode = 
        case proplists:get_value(<<"eqptZjqCode">>, Qs) of
            undefined -> null;
            EqptZjqCodeBinary -> binary_to_list(EqptZjqCodeBinary)
        end,
    Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
    EqptPort = proplists:get_value(<<"eqptPort">>, Qs, 31),
    EqptBaudrate = proplists:get_value(<<"eqptBaudrate">>, Qs, 2400),
    EqptRateNumber = proplists:get_value(<<"eqptRateNumber">>, Qs, 4),
    EqptZsw = proplists:get_value(<<"eqptZsw">>, Qs, 6),
    EqptXsw = proplists:get_value(<<"eqptXsw">>, Qs, 2),
    EqptDlh = proplists:get_value(<<"eqptDlh">>, Qs, 5),
    EqptXlh = proplists:get_value(<<"eqptXlh">>, Qs, 1),
    EqptYblx = binary_to_list(proplists:get_value(<<"eqptYblx">>, Qs, <<"DNB">>)),
    case {connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType), 
          connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptCjqType)} of
        {{ok, ?METER_LEVEL, EqptProtocolType}, {ok, ?COLLECTOR_LEVEL, EqptCjqProtocolType}} ->
            MeterInfo = {EqptType, EqptIdCode, EqptName, EqptCjqType, EqptCjqCode, EqptZjqType, EqptZjqCode, 
                           Creator, EqptPort, EqptBaudrate, EqptRateNumber, EqptZsw, EqptXsw, EqptDlh, EqptXlh,
                           EqptYblx, EqptProtocolType, EqptCjqProtocolType},
            ?PRINT("~p~n", [MeterInfo]),
            get_meter_info_list_(T, [MeterInfo | MeterInfoList]);
        _ ->
            throw("EqptIdCode: " ++ EqptIdCode ++ " " ++ ?ILLEGAL_EQPT_TYPE)
    end;
get_meter_info_list_(_, MeterInfoList) ->
    MeterInfoList.


batch_get_meter(Qs) ->
    EqptCjqType = 
        case proplists:get_value(<<"eqptCjqType">>, Qs) of
            undefined -> 
                undefined;
            EqptCjqTypeTmp ->
                binary_to_list(EqptCjqTypeTmp)
        end,
    EqptCjqCode = 
        case proplists:get_value(<<"eqptCjqCode">>, Qs) of
            undefined ->
                undefined;
            EqptCjqCodeTmp ->
                binary_to_list(EqptCjqCodeTmp)
        end,
    %% eg: eqptTypes=type1,type2
    EqptTypes = get_eqpt_types(Qs),
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    
    case db_util:batch_get_meter(EqptCjqType, EqptCjqCode, EqptTypes, PageSize, Page) of
        {ok, []} ->
            data_return([]);
        {ok, Rows} ->
            List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}, 
                     {<<"eqptCjqType">>, EqptCjqTypeBinary}, {<<"eqptCjqCode">>, EqptCjqCodeBinary}, 
                     {<<"eqptZjqType">>, EqptZjqTypeBinary}, {<<"eqptZjqCode">>, EqptZjqCodeBinary},
                     {<<"createTime">>, CreateTimeBinary},
                     {<<"creator">>, CreatorBinary}, {<<"eqptPort">>, EqptPort},
                     {<<"eqptBaudrate">>, EqptBaudrate}, {<<"eqptRateNumber">>, EqptRateNumber}, 
                     {<<"eqptZsw">>, EqptZsw},
                     {<<"eqptXsw">>, EqptXsw}, {<<"eqptDlh">>, EqptDlh},
                     {<<"eqptXlh">>, EqptXlh}, {<<"eqptYblx">>, EqptYblxBinary}
                    ] || 
                       {EqptTypeBinary, EqptIdCodeBinay, EqptNameBinary, EqptCjqTypeBinary, EqptCjqCodeBinary,
                        EqptZjqTypeBinary, EqptZjqCodeBinary, CreateTimeBinary, CreatorBinary, EqptPort, EqptBaudrate,
                        EqptRateNumber, EqptZsw,
                       EqptXsw, EqptDlh, EqptXlh, EqptYblxBinary}
                           <- field_format(Rows)],
            data_return(List);
        {error, Reason} ->
            format_return(Reason, ?FAILURE_STATUS)
    end.


%%----------------------------------------------------------------------------------------------
%% 采集器/中继器相关API
%%----------------------------------------------------------------------------------------------

%% 添加采集器/中继器设备
add_collector(Qs) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    EqptName = binary_to_list(proplists:get_value(<<"eqptName">>, Qs, <<>>)),
    Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
        {ok, EqptLevel, ProtocolType} when (EqptLevel =:= ?COLLECTOR_LEVEL) ->
            case db_util:is_exists_collector(EqptType, EqptIdCode) of
                false ->
                    db_util:begin_transaction(),
                    case db_util:add_collector(EqptType, EqptIdCode, EqptName, Creator, ProtocolType) of
                        ok ->
                            db_util:commit_transaction(),
                            notify_update_eqpt_info(),
                            Msg = string:join(["add_collector", EqptType, EqptIdCode,EqptName, Creator], "/"),
                            connector_event_server:operation_log(EqptIdCode, Msg),
                            format_return("add collector success", ?SUCCESS_STATUS);
                        {error, Reason} ->
                            db_util:rollback(),
                            format_return(Reason, ?FAILURE_STATUS)
                                
                    end;
                true ->
                    format_return(?ALREADY_EXISTS, ?FAILURE_STATUS);
                {error, What} ->
                    format_return(What, ?FAILURE_STATUS)
            end;
        {ok, EqptLevel, ProtocolType} when (EqptLevel =:= ?REPEATERS_LEVEL) ->
            case db_util:is_exists_repeaters(EqptType, EqptIdCode) of
                false ->
                    db_util:begin_transaction(),
                    case db_util:add_repeaters(EqptType, EqptIdCode, EqptName, Creator) of
                        ok -> 
                            db_util:commit_transaction(),
                            format_return("add repeaters success", ?SUCCESS_STATUS);
                        {error, Reason} ->
                            db_util:rollback(),
                            format_return(Reason, ?FAILURE_STATUS)
                    end;
                true ->
                    format_return(?ALREADY_EXISTS, ?FAILURE_STATUS);
                {error, What} ->
                    format_return(What, ?FAILURE_STATUS)
            end;
        _ ->
            format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS)
    end.

%% 修改采集器/中继器设备
update_collector(Qs) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    EqptName = binary_to_list(proplists:get_value(<<"eqptName">>, Qs, <<>>)),
    Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
        {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?COLLECTOR_LEVEL) ->
            case db_util:is_exists_collector(EqptType, EqptIdCode) of
                true ->
                    case db_util:update_collector(EqptType, EqptIdCode, EqptName, Creator) of
                        ok ->
                            Msg = string:join(["update_collector", EqptType, EqptIdCode,EqptName, Creator], "/"),
                            connector_event_server:operation_log(EqptIdCode, Msg),
                            format_return("update collector success", ?SUCCESS_STATUS);
                        {error, Reason} ->
                            format_return(Reason, ?FAILURE_STATUS)
                    end;
                false ->
                    format_return(?NOT_EXISTS, ?FAILURE_STATUS);
                {error, What} ->
                    format_return(What, ?FAILURE_STATUS)
            end;
        {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?REPEATERS_LEVEL) ->
            case db_util:is_exists_repeaters(EqptType, EqptIdCode) of
                true ->
                    case db_util:update_repeaters(EqptType, EqptIdCode, EqptName, Creator) of
                        ok -> 
                            format_return("update repeaters success", ?SUCCESS_STATUS);
                        {error, Reason} ->
                            format_return(Reason, ?FAILURE_STATUS)
                    end;
                false ->
                    format_return(?NOT_EXISTS, ?FAILURE_STATUS);
                {error, What} ->
                    format_return(What, ?FAILURE_STATUS)
            end;
        _ ->
            format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS)
    end.

%% 删除采集器/中继器设备
delete_collector(Qs) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
        {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?COLLECTOR_LEVEL) ->
            case db_util:is_exists_collector(EqptType, EqptIdCode) of
                true ->
                    case db_util:is_exists_meter_of_collector(EqptType, EqptIdCode) of
                        false ->
                            case db_util:delete_collector(EqptType, EqptIdCode) of
                                ok ->
                                    notify_update_eqpt_info(),
                                    Msg = string:join(["delete_collector", EqptType, EqptIdCode], "/"),
                                    connector_event_server:operation_log(EqptIdCode, Msg),
                                    format_return("delete collector success", ?SUCCESS_STATUS);
                                {error, Reason} ->
                                    format_return(Reason, ?FAILURE_STATUS)
                            end;
                        true ->
                            format_return(?EXISTS_METER, ?FAILURE_STATUS);
                        {error, Reason1} ->
                            format_return(Reason1, ?FAILURE_STATUS)
                    end;
                false ->
                    format_return(?NOT_EXISTS, ?FAILURE_STATUS);
                {error, What} ->
                    format_return(What, ?FAILURE_STATUS)
            end;
        {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?REPEATERS_LEVEL) ->
            case db_util:is_exists_repeaters(EqptType, EqptIdCode) of
                true ->
                    case db_util:is_exists_meter_of_repeaters(EqptType, EqptIdCode) of
                        false ->
                            case db_util:delete_repeaters(EqptType, EqptIdCode) of
                                ok -> 
                                    format_return("delete repeaters success", ?SUCCESS_STATUS);
                                {error, Reason} ->
                                    format_return(Reason, ?FAILURE_STATUS)
                            end;
                        true ->
                            format_return(?EXISTS_METER, ?FAILURE_STATUS);
                        {error, Reason1} ->
                            format_return(Reason1, ?FAILURE_STATUS)
                    end;
        
                false ->
                    format_return(?NOT_EXISTS, ?FAILURE_STATUS);
                {error, What} ->
                    format_return(What, ?FAILURE_STATUS)
            end;
        _ ->
            format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS)
    end.            
    
%% 强制性删除
force_delete_collector(Qs) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
        {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?COLLECTOR_LEVEL) ->
            case db_util:is_exists_collector(EqptType, EqptIdCode) of
                true ->
                    case db_util:force_delete_collector(EqptType, EqptIdCode) of
                        ok ->
                            notify_update_eqpt_info(),
                            Msg = string:join(["force_delete_collector", EqptType, EqptIdCode], "/"),
                            connector_event_server:operation_log(EqptIdCode, Msg),
                            format_return("force delete collector success", ?SUCCESS_STATUS);
                        {error, Reason} ->
                            format_return(Reason, ?FAILURE_STATUS)
                    end;
                false ->
                    format_return(?NOT_EXISTS, ?FAILURE_STATUS);
                {error, What} ->
                    format_return(What, ?FAILURE_STATUS)
            end;
        {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?REPEATERS_LEVEL) ->
            case db_util:is_exists_repeaters(EqptType, EqptIdCode) of
                true ->
                    case db_util:force_delete_repeaters(EqptType, EqptIdCode) of
                        ok -> 
                            format_return("force delete repeaters success", ?SUCCESS_STATUS);
                        {error, Reason} ->
                            format_return(Reason, ?FAILURE_STATUS)
                    end;
                false ->
                    format_return(?NOT_EXISTS, ?FAILURE_STATUS);
                {error, What} ->
                    format_return(What, ?FAILURE_STATUS)
            end;
        _ ->
            format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS)
    end.            
    

%%---------------------------------------------------------------------------------------------
%% 计量设备相关API
%%---------------------------------------------------------------------------------------------

%% 添加计量设备
%%   EqptType: 设备类型
%%   EqptIdCode: 设备编号
%%   EqptName: 设备名称
%%   EqptCjqType: 所属采集器类型
%%   EqptCjqCode: 所属采集器编号
%%   EqptZjqType: 中继器类型（可选）
%%   EqptZjqCode: 中继器编号（可选）
%%   Creator: 创建者（可选）
%%   EqptPort: 端口号 Integer
%%   EqptBaudrate: 波特率 2400， 1200， 9600
%%   EqptRateNumber: 费率个数 4
%%   EqptZsw: 电能示值的整数位数 6
%%   EqptXsw: 电能示值的小数位数 2
%%   EqptDlh: 大类号 5
%%   EqptXlh: 小类号 1
%%   EqptYblx: 仪表类型 LSB（冷水表），RSB（热水表），DNB（电能表）等
add_meter(Qs) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    EqptName = binary_to_list(proplists:get_value(<<"eqptName">>, Qs, <<>>)),
    EqptCjqType = binary_to_list(proplists:get_value(<<"eqptCjqType">>, Qs, <<>>)),
    EqptCjqCode = binary_to_list(proplists:get_value(<<"eqptCjqCode">>, Qs, <<>>)),
    EqptZjqType = 
        case proplists:get_value(<<"eqptZjqType">>, Qs) of
            undefined -> null;
            EqptZjqTypeBinary -> binary_to_list(EqptZjqTypeBinary)
        end,
    EqptZjqCode = 
        case proplists:get_value(<<"eqptZjqCode">>, Qs) of
            undefined -> null;
            EqptZjqCodeBinary -> binary_to_list(EqptZjqCodeBinary)
        end,
    Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
    EqptPort = proplists:get_value(<<"eqptPort">>, Qs, 31),
    EqptBaudrate = proplists:get_value(<<"eqptBaudrate">>, Qs, 2400),
    EqptRateNumber = proplists:get_value(<<"eqptRateNumber">>, Qs, 4),
    EqptZsw = proplists:get_value(<<"eqptZsw">>, Qs, 6),
    EqptXsw = proplists:get_value(<<"eqptXsw">>, Qs, 2),
    EqptDlh = proplists:get_value(<<"eqptDlh">>, Qs, 5),
    EqptXlh = proplists:get_value(<<"eqptXlh">>, Qs, 1),
    EqptYblx = binary_to_list(proplists:get_value(<<"eqptYblx">>, Qs, <<"DNB">>)),
    case {connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType), 
          connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptCjqType)} of
        {{ok, ?METER_LEVEL, EqptProtocolType}, {ok, ?COLLECTOR_LEVEL, EqptCjqProtocolType}} ->
            case db_util:is_exists_meter(EqptType, EqptIdCode) of
                false ->
                    db_util:begin_transaction(),
                    case db_util:add_meter(EqptType, EqptIdCode, EqptName, EqptCjqType, EqptCjqCode, EqptZjqType, EqptZjqCode, 
                                           Creator, EqptPort, EqptBaudrate, EqptRateNumber, EqptZsw, EqptXsw, EqptDlh, EqptXlh,
                                           EqptYblx, EqptProtocolType, EqptCjqProtocolType) of
                        ok ->
                            notify_update_eqpt_info(),
                            db_util:commit_transaction(),
                            Msg = string:join(["add_meter",EqptType,EqptIdCode,EqptName,EqptCjqType,EqptCjqCode,EqptYblx,Creator], "/"),
                            connector_event_server:operation_log(EqptIdCode, Msg),
                            format_return("add meter success", ?SUCCESS_STATUS);
                        {error, Reason} ->
                            db_util:rollback(),
                            format_return(Reason, ?FAILURE_STATUS)
                    end;
                true ->
                    format_return(?ALREADY_EXISTS, ?FAILURE_STATUS);
                {error, What} ->
                    format_return(What, ?FAILURE_STATUS)
            end;
        _ ->
            format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS)
    end.

update_meter(Qs) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    EqptName = binary_to_list(proplists:get_value(<<"eqptName">>, Qs, <<>>)),
    Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
    EqptPort = proplists:get_value(<<"eqptPort">>, Qs, 31),
    EqptBaudrate = proplists:get_value(<<"eqptBaudrate">>, Qs, 2400),
    EqptRateNumber = proplists:get_value(<<"eqptRateNumber">>, Qs, 4),
    EqptZsw = proplists:get_value(<<"eqptZsw">>, Qs, 6),
    EqptXsw = proplists:get_value(<<"eqptXsw">>, Qs, 2),
    EqptDlh = proplists:get_value(<<"eqptDlh">>, Qs, 5),
    EqptXlh = proplists:get_value(<<"eqptXlh">>, Qs, 1),
    EqptYblx = binary_to_list(proplists:get_value(<<"eqptYblx">>, Qs, <<"DNB">>)),
    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
        {ok, ?METER_LEVEL, _EqptProtocolType} ->
            case db_util:is_exists_meter(EqptType, EqptIdCode) of
                true ->
                    case db_util:update_meter(EqptType, EqptIdCode, EqptName, Creator, EqptPort, EqptBaudrate, 
                                              EqptRateNumber, EqptZsw, EqptXsw, EqptDlh, EqptXlh, EqptYblx) of
                        ok ->
                            notify_update_eqpt_info(),
                            Msg = string:join(["update_meter",EqptType,EqptIdCode,EqptName,EqptYblx,Creator], "/"),
                            connector_event_server:operation_log(EqptIdCode, Msg),
                            format_return("update meter success", ?SUCCESS_STATUS);
                        {error, Reason} ->
                            format_return(Reason, ?FAILURE_STATUS)
                    end;
                false ->
                    format_return(?NOT_EXISTS, ?FAILURE_STATUS);
                {error, What} ->
                    format_return(What, ?FAILURE_STATUS)
            end;
        _ ->
            format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS)
    end.

delete_meter(Qs) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
        {ok, ?METER_LEVEL, _EqptProtocolType} ->
            case db_util:is_exists_meter(EqptType, EqptIdCode) of
                true ->
                    case db_util:delete_meter(EqptType, EqptIdCode) of
                        ok ->
                            notify_update_eqpt_info(),
                            Msg = string:join(["delete_meter", EqptType, EqptIdCode], "/"),
                            connector_event_server:operation_log(EqptIdCode, Msg),
                            format_return("delete meter success", ?SUCCESS_STATUS);
                        {error, Reason} ->
                            format_return(Reason, ?FAILURE_STATUS)
                    end;
                false ->
                    format_return(?NOT_EXISTS, ?FAILURE_STATUS);
                {error, What} ->
                    format_return(What, ?FAILURE_STATUS)
            end;
        _ ->
            format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS)
    end. 

%% 删除采集设备下的计量设备
delete_meter_by_collector(Qs) ->
    EqptCjqType = binary_to_list(proplists:get_value(<<"eqptCjqType">>, Qs, <<>>)),
    EqptCjqCode = binary_to_list(proplists:get_value(<<"eqptCjqCode">>, Qs, <<>>)),
    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptCjqType) of
        {ok, ?COLLECTOR_LEVEL, _} ->
            case db_util:delete_meter_by_collector(EqptCjqType, EqptCjqCode) of
                ok ->
                    notify_update_eqpt_info(),
                    Msg = string:join(["delete_meter_by_collector", EqptCjqType, EqptCjqCode], "/"),
                    connector_event_server:operation_log(EqptCjqCode, Msg),
                    format_return("delete meters of collector success", ?SUCCESS_STATUS);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS)
            end;
        _ ->
            format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS)
    end.
   

%%-------------------------------------------------------------------------------------------
%% 设备信息查询
%%-------------------------------------------------------------------------------------------
get_page_size(Qs) ->
    case string:to_integer(binary_to_list(proplists:get_value(<<"pageSize">>, Qs, <<>>))) of
        {error, _} -> 
            ?DEFAULT_PAGE_SIZE;
        {PageSizeTmp, _} when is_integer(PageSizeTmp) andalso (PageSizeTmp > 0) ->
            PageSizeTmp;
        _ ->
            ?DEFAULT_PAGE_SIZE
    end.

get_page(Qs) ->
    case string:to_integer(binary_to_list(proplists:get_value(<<"page">>, Qs, <<>>))) of
        {error, _} ->
            ?DEFAULT_PAGE;
        {PageTmp, _} when is_integer(PageTmp) andalso (PageTmp > 0) ->
            PageTmp;
        _ ->
            ?DEFAULT_PAGE
    end.

get_eqpt_types(Qs) ->
    case proplists:get_value(<<"eqptTypes">>, Qs) of
        undefined ->
            undefined;
        EqptTypesBinary ->
            string:join([lists:concat(["'", EqptType, "'"]) || 
                            EqptType <- string:tokens(binary_to_list(EqptTypesBinary), ",")], ",")
    end.

get_collector_info_total_row(_Qs) ->
    case db_util:get_collector_info_total_row() of
        {ok, [{Count}]} ->
            total_return(Count);
        {error, Reason} ->
            format_return(Reason, ?FAILURE_STATUS)
    end.

get_collector_info(Qs) ->
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    %% eg: eqptTypes=type1,type2
    EqptTypes = get_eqpt_types(Qs),
    case db_util:get_collector_info(EqptTypes, PageSize, Page) of
        {ok, []} ->
            data_return([]);
        {ok, Rows} ->
            List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}, 
                     {<<"eqptTypeName">>, EqptTypeNameBinary}, {<<"createTime">>, CreateTimeBinary},
                     {<<"creator">>, CreatorBinary}] || 
                       {EqptTypeBinary, EqptIdCodeBinay, EqptNameBinary, EqptTypeNameBinary, CreateTimeBinary,
                        CreatorBinary} 
                           <- field_format(Rows)],
            data_return(List);
        {error, Reason} ->
            format_return(Reason, ?FAILURE_STATUS)
    end.

get_repeaters_info_total_row(_Qs) ->
    case db_util:get_repeaters_info_total_row() of
        {ok, [{Count}]} ->
            total_return(Count);
        {error, Reason} ->
            format_return(Reason, ?FAILURE_STATUS)
    end.

get_repeaters_info(Qs) ->
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    %% eg: eqptTypes=type1,type2
    EqptTypes = get_eqpt_types(Qs),
    case db_util:get_repeaters_info(EqptTypes, PageSize, Page) of
        {ok, []} ->
            data_return([]);
        {ok, Rows} ->
            List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}, 
                     {<<"eqptTypeName">>, EqptTypeNameBinary}, {<<"createTime">>, CreateTimeBinary}, 
                     {<<"creator">>, CreatorBinary}] || 
                       {EqptTypeBinary, EqptIdCodeBinay, EqptNameBinary, EqptTypeNameBinary, CreateTimeBinary,
                        CreatorBinary} 
                           <- field_format(Rows)],
            data_return(List);
        {error, Reason} ->
            format_return(Reason, ?FAILURE_STATUS)
    end.
  
get_meter_info_total_row(_Qs) ->
    case db_util:get_meter_info_total_row() of
        {ok, [{Count}]} ->
            total_return(Count);
        {error, Reason} ->
            format_return(Reason, ?FAILURE_STATUS)
    end.

get_meter_info(Qs) ->
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    %% eg: eqptTypes=type1,type2
    EqptTypes = get_eqpt_types(Qs),
    case db_util:get_meter_info(EqptTypes, PageSize, Page) of
        {ok, []} ->
            data_return([]);
        {ok, Rows} ->
            List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}, 
                     {<<"eqptTypeName">>, EqptTypeNameBinary}, {<<"createTime">>, CreateTimeBinary}, 
                     {<<"creator">>, CreatorBinary},
                     {<<"eqptPort">>, EqptPort}, {<<"eqptBaudrate">>, EqptBaudrate}, {<<"eqptRateNumber">>, EqptRateNumber},
                     {<<"eqptZsw">>, EqptZsw}, {<<"eqptXsw">>, EqptXsw}, {<<"eqptDlh">>, EqptDlh}, {<<"eqptXlh">>, EqptXlh},
                     {<<"eqptYblx">>, EqptYblx}
                     ] || 
                       {EqptTypeBinary, EqptIdCodeBinay, EqptNameBinary, EqptTypeNameBinary, CreateTimeBinary, CreatorBinary,
                        EqptPort,
                        EqptBaudrate, EqptRateNumber, EqptZsw, EqptXsw, EqptDlh, EqptXlh, EqptYblx} 
                           <- field_format(Rows)],
            data_return(List);
        {error, Reason} ->
            format_return(Reason, ?FAILURE_STATUS)
    end.

get_meter_network_by_collector(Qs) ->  
    EqptCjqType = binary_to_list(proplists:get_value(<<"eqptCjqType">>, Qs, <<>>)),
    EqptCjqCode = binary_to_list(proplists:get_value(<<"eqptCjqCode">>, Qs, <<>>)),
    %% eg: eqptTypes=type1,type2
    EqptTypes = get_eqpt_types(Qs),
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptCjqType) of
        {ok, ?COLLECTOR_LEVEL, _} ->
            case db_util:get_meter_network_by_collector(EqptCjqType, EqptCjqCode, EqptTypes, PageSize, Page) of
                {ok, []} ->
                    data_return([]);
                {ok, Rows} ->
                    List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}, 
                             {<<"eqptTypeName">>, EqptTypeNameBinary}
                            ] || 
                               {EqptTypeBinary, EqptIdCodeBinay, EqptNameBinary, EqptTypeNameBinary}
                                   <- field_format(Rows)],
                    data_return(List);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS)
            end;
        _ ->
            format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS)
    end.


get_collector_info_by_eqpt_type(Qs) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
        {ok, ?COLLECTOR_LEVEL, _} ->
            case db_util:get_collector_info_by_eqpt_type(EqptType, PageSize, Page) of
                {ok, []} ->
                    data_return([]);
                {ok, Rows} ->
                    List = [[{<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}]
                             || {EqptIdCodeBinay, EqptNameBinary} <- field_format(Rows)],
                    data_return(List);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS)
            end;
        _ ->
            format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS)
    end.
    
get_repeaters_info_by_eqpt_type(Qs) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
        {ok, ?REPEATERS_LEVEL, _} ->
            case db_util:get_repeaters_info_by_eqpt_type(EqptType, PageSize, Page) of
                {ok, []} ->
                    data_return([]);
                {ok, Rows} ->
                    List = [[{<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}]
                             || {EqptIdCodeBinay, EqptNameBinary} <- field_format(Rows)],
                    data_return(List);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS)
            end;
        _ ->
            format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS)
    end.    

