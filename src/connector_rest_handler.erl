-module(connector_rest_handler).

-include("print.hrl").
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
         get_status/1,
         get_status_by_user/1,
         get_version/1,
         get_network/1,
         update_network/1,
         restart_network/1,
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
              {<<"application/json;charset=utf-8">>,  response_to_json},
              {<<"application/json">>,  response_to_json}
             ],
    {Result, Req, State}.

content_types_accepted(Req, State) ->
    Result = [
              {<<"application/x-www-form-urlencoded">>, response_to_json},
              {<<"application/json;charset=utf-8">>,  response_to_json},
              {<<"application/json">>,  response_to_json}
             ],
    {Result, Req, State}.

response_to_json(Req, State) ->
    Path = cowboy_req:path(Req),
    Method = cowboy_req:method(Req),
    QsInfo = cowboy_req:qs(Req),
    UserName = cowboy_req:header(<<"username">>, Req),
    UserPass = cowboy_req:header(<<"userpwd">>, Req),
    {NewReq, Qs} = 
        case cowboy_req:has_body(Req) of
            true ->
                {ok, Body, ReqTmp} = cowboy_req:read_body(Req),
                ?PRINT("Body: ~p~n", [Body]),
                {ReqTmp, jsx:decode(Body)};
            false ->
                {Req, cowboy_req:parse_qs(Req)}
        end,

    Result = get_result(Path, Qs, QsInfo, UserName, UserPass),
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

get_result(Path, Qs, QsInfo, UserName, UserPass) ->
    Fun = proplists:get_value(filename:basename(Path), ?PATH_TO_FUN, hello),
    ?PRINT("Fun: ~p~n", [Fun]),
    apply(?MODULE, Fun, [[Qs, QsInfo, UserName, UserPass]]).

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
            jsx:encode([{<<"returnCode">>, ReturnCode},{<<"flag">>, list_to_binary(Flag)},{<<"returnMsg">>, ReturnMsg}])
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

get_status([_Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User, Password) of
        true ->
            Data =
                case connector_gateway_status_store:show() of
                    {ok, Info} ->
                        io:format("Info:~p~n",[Info]),
                        [[{<<"gatewayid">>, helper_util:to_iolist(GatewayCode)}, {<<"gatewaytype">>, helper_util:to_iolist(GatewayType)}, {<<"status">>, helper_util:to_iolist(Status)}, {<<"time">>, helper_util:to_iolist(Time)}, {<<"task">>, helper_util:to_iolist(helper_util:to_binary(Task))}] || {GatewayCode, GatewayType, Status, Time, Task} <- Info];
                _ ->
                        "have no gateway"
                end,
                data_return(Data, QsInfo);
        false ->
                format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

get_status_by_user([_Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User, Password) of
        true ->
            Data =
                case connector_gateway_status_store:show(User) of
                    {ok, Info} ->
                        ?PRINT("Info:~p~n",[Info]),
                        [[{<<"gatewayid">>, helper_util:to_iolist(GatewayCode)}, {<<"gatewaytype">>, helper_util:to_iolist(GatewayType)}, {<<"status">>, helper_util:to_iolist(Status)}, {<<"time">>, helper_util:to_iolist(Time)}, {<<"task">>, helper_util:to_iolist(helper_util:to_binary(Task))}] || {GatewayCode, GatewayType, Status, Time, Task} <- Info];
                    _ ->
                        "have no gateway"
                end,
                data_return(Data, QsInfo);
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

get_version([_Qs, QsInfo, _User, _UserPass]) ->
    Vsn = 
        case application:get_env(connector, version) of
            {ok, VsnTmp} ->
                VsnTmp;
        _ ->
                "unknown version"
        end,
    DateStr =
        case application:get_env(connector, date) of
            {ok, DateTmp} ->
                DateTmp;
        _ ->
                "unknown release date"
        end,
    List = [{<<"version">>, helper_util:to_iolist(Vsn)}, {<<"date">>, helper_util:to_iolist(DateStr)}],
    data_return(List, QsInfo).

get_network([_Qs, QsInfo]) ->
    case get_network_config() of
        {ok, ConfigList} ->
            Ip = helper_util:to_iolist(proplists:get_value(?IPADDR, ConfigList, "")),
            NetMask = helper_util:to_iolist(proplists:get_value(?NETMASK, ConfigList, "")),
            Gateway = helper_util:to_iolist(proplists:get_value(?GATEWAY, ConfigList, "")),
            Dns1 = helper_util:to_iolist(proplists:get_value(?DNS1, ConfigList, "")),
            List = [{<<"ip">>, Ip}, {<<"netmask">>, NetMask}, {<<"gateway">>, Gateway}, {<<"dns1">>, Dns1}],
            data_return(List, QsInfo);
        {error, Reason} ->
            format_return(Reason, ?FAILURE_STATUS, QsInfo)
    end.

get_network_config() ->
    case file:read_file(?NETWORK_CONFIG_FILE) of
        {ok, Binary} ->
            DataList = string:tokens(binary_to_list(Binary), "\n"),
            ?PRINT("DataList:~p~n", [DataList]),
            Fun = 
                fun(DataLine, List) ->
                        case string:tokens(DataLine, "=") of
                            [Key, Value] when (Key =:= ?IPADDR) orelse (Key =:= ?NETMASK) 
                                              orelse (Key =:= ?GATEWAY) orelse (Key =:= ?DNS1) ->
                                [{Key, Value} | List];
                            _ ->
                                List
                        end
                end,
            ConfigList = lists:foldl(Fun, [], DataList),
            {ok, ConfigList};
        {error, Reason} ->
            {error, Reason}
    end.

update_network([Qs, QsInfo]) ->
    List = [{<<"ip">>, "-a"}, {<<"netmask">>, "-b"}, {<<"gateway">>, "-c"}, {<<"dns1">>, "-d"}],
    Fun = 
        fun({Param, Opt}, ParamStr) ->
           case proplists:get_value(Param, Qs) of
               undefined ->
                   ParamStr;
               ValueBin ->
                   ParamStr ++ Opt ++ " " ++  binary_to_list(ValueBin) ++ " "
           end
        end,
    ParamStr = lists:foldl(Fun, "", List),
    ref_util:update_network(ParamStr),
    ReturnMsg = "update success",
    ReturnCode = ?SUCCESS_STATUS,
    format_return(ReturnMsg, ReturnCode, QsInfo).
    
restart_network([_Qs, QsInfo]) ->
    ParamStr = "systemctl restart network.service",
    Fun = 
        fun(ParamStr) ->
                timer:sleep(1000),
                ref_util:seteuid(ParamStr)
        end,
    spawn(fun() -> Fun(ParamStr) end),
    ReturnMsg = "restart success",
    ReturnCode = ?SUCCESS_STATUS,
    format_return(ReturnMsg, ReturnCode, QsInfo).

