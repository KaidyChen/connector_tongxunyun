-module(request_handler).

-include("config.hrl").
-include("print.hrl").
-include("request.hrl").

-export([init/2]).
-export([reply/2]).

-define(TIMEOUT, 300*1000).

init(Req0, Opts) ->
    ?PRINT("Req0:~p~n", [Req0]),
    ?PRINT("Opts:~p~n", [Opts]),
    Method = cowboy_req:method(Req0),
    HasBody = cowboy_req:has_body(Req0),
    UserName = cowboy_req:header(<<"username">>, Req0, <<"0">>),
    UserPass = cowboy_req:header(<<"userpwd">>, Req0, <<"0">>),
    ?PRINT("User:~p  Pwd:~p~n",[UserName,UserPass]),
    case db_util:is_exists_user(binary_to_list(UserName), binary_to_list(UserPass)) of
        true ->
            Req = do_reply(Method, HasBody, Req0);
        false ->
            ?PRINT("unsignd account!!!~n",[]),
            Reply = jsx:encode([{<<"status">>,<<"500">>},{<<"result">>,<<"wrong account or password">>}]),
            Req = cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain;charset=utf-8">>}, Reply, Req0)
    end,
    {ok, Req, Opts}.

reply(Pid, ReturnContent) ->
    Pid ! {reply, ReturnContent}.

do_reply(<<"POST">>, true, Req0) ->
    handler_req(Req0);
do_reply(<<"POST">>, false, Req) ->
    cowboy:reply(400, [], <<"Missing body.">>, Req);
do_reply(_, _, Req) ->
    cowboy_req:reply(405, Req).

handler_req(Req0) ->
    {ok, Body, Req} = cowboy_req:read_body(Req0),
    ?PRINT("Body:~p~n", [Body]),
    case string:left(binary_to_list(Body),2) =:= "{\"" of
        false ->
            PostVals = cow_qs:parse_qs(Body),
            ?PRINT("PostVals:~p~n", [PostVals]),
            OrderNumber = binary_to_list(proplists:get_value(<<"order_no">>, PostVals, <<>>)),
            Partner = binary_to_list(proplists:get_value(<<"parent">>, PostVals, <<>>)),
            Sign = binary_to_list(proplists:get_value(<<"sign">>, PostVals, <<>>)),
            Flag = binary_to_list(proplists:get_value(<<"flag">>, PostVals, <<"FFFFFFFFFFFFFFFF">>)),
            Priority = list_to_integer(binary_to_list(proplists:get_value(<<"priority">>, PostVals, <<"0">>))),             ObjsBitString = proplists:get_value(<<"objs">>, PostVals, <<>>),
            [ObjsList] = jsx:decode(ObjsBitString),
            EqptType = binary_to_list(proplists:get_value(<<"eqpt_type">>, ObjsList, <<>>)),
            EqptIdCode = binary_to_list(proplists:get_value(<<"eqpt_id_code">>, ObjsList, <<>>)),
            %%EqptPwd = binary_to_list(proplists:get_value(<<"eqpt_pwd">>, ObjsList, <<>>)),
            [{eqpt_pwd, EqptPwd}]= ets:lookup(?COMPUTER_OPTS_TABLE, eqpt_pwd),
            ResDataType = binary_to_list(proplists:get_value(<<"resdatatype">>, ObjsList, <<>>)),
            CmdType = binary_to_list(proplists:get_value(<<"cmd_type">>, ObjsList, <<>>)),
            CmdId = binary_to_list(proplists:get_value(<<"cmd_id">>, ObjsList, <<>>)),
            CmdData = binary_to_list(proplists:get_value(<<"cmd_data">>, ObjsList, <<>>));
        true ->
            PathInfo = cow_qs:parse_qs(Body),
            ?PRINT("PathInfo:~p~n", [PathInfo]),
            case length(PathInfo) of
                2 ->
                     [BodyTmp, _] = string:tokens(binary_to_list(Body),"&"),
                     PostVals = jsx:decode(list_to_binary(BodyTmp)),
                     Flag = binary_to_list(proplists:get_value(<<"flag">>, PathInfo, <<"FFFFFFFFFFFFFFFF">>)),
                     ?PRINT("NewPostVals:~p~n", [PostVals]);
                _ ->
                     PostVals = jsx:decode(Body),
                     Flag = binary_to_list(proplists:get_value(<<"flag">>, PostVals, <<"FFFFFFFFFFFFFFFF">>)),
                     ?PRINT("PostVals:~p~n", [PostVals])
            end,
            OrderNumber = binary_to_list(proplists:get_value(<<"order_no">>, PostVals, <<>>)),
            Partner = binary_to_list(proplists:get_value(<<"parent">>, PostVals, <<>>)),
            Sign = binary_to_list(proplists:get_value(<<"sign">>, PostVals, <<>>)),
            Priority = list_to_integer(binary_to_list(proplists:get_value(<<"priority">>, PostVals, <<"0">>))),             
            ObjsList = proplists:get_value(<<"objs">>, PostVals, <<>>),
            EqptType = binary_to_list(proplists:get_value(<<"eqpt_type">>, ObjsList, <<>>)),
            EqptIdCode = binary_to_list(proplists:get_value(<<"eqpt_id_code">>, ObjsList, <<>>)),
            %%EqptPwd = binary_to_list(proplists:get_value(<<"eqpt_pwd">>, ObjsList, <<>>)),
            [{eqpt_pwd, EqptPwd}]= ets:lookup(?COMPUTER_OPTS_TABLE, eqpt_pwd),
            ResDataType = binary_to_list(proplists:get_value(<<"resdatatype">>, ObjsList, <<>>)),
            CmdType = binary_to_list(proplists:get_value(<<"cmd_type">>, ObjsList, <<>>)),
            CmdId = binary_to_list(proplists:get_value(<<"cmd_id">>, ObjsList, <<>>)),
            CmdData = binary_to_list(proplists:get_value(<<"cmd_data">>, ObjsList, <<>>))
    end,
    Objs = #objs{
              eqptType = EqptType,
              eqptIdCode = EqptIdCode,
              eqptPwd = EqptPwd,
              resDataType = ResDataType,
              cmdType = CmdType,
              cmdId = CmdId,
              cmdData = CmdData
             },
    ReqObj = #req_obj{
                orderNumber = OrderNumber,
                partner = Partner,
                objs = Objs,
                sign = Sign,
                flag = Flag
               },
    ResponseContent =
        try get_so_lib_name(Objs) of
            SoLibName ->
                ReqRd = #req_rd{
                           pid = self(),
                           soLibName = SoLibName,
                           reqObj = ReqObj
                          },
                %% Log record: request log
                RequestContent = ?HELPER:to_string(Body),
                connector_event_server:request_log(OrderNumber, RequestContent),
                {ok, [_, _, _, {_, TIMEOUT}]} = file:consult(?TIME_OPTS_FILEPATH),
                case connector_db_store_server:get_upper_eqpt_type_and_upper_eqpt_id_code(EqptType, EqptIdCode) of
                    {error, Reason} ->
                        gen_failure_return_content(OrderNumber, Sign, Flag, EqptType, EqptIdCode, Reason);
                    {ok, {GatewayType, GatewayId}} ->
                        case connector_gateway_to_store_server:lookup_client_pid({GatewayType, GatewayId}) of
                            {ok, ClientSendRecvPid} ->
                                case is_process_alive(ClientSendRecvPid) of
                                    true ->
                                        update_gateway_task(GatewayId, "1"), 
                                        connector_client_send_recv:send_req_rd_to_client_process(ClientSendRecvPid, {Priority, ReqRd}),
                                        receive
                                            {reply, Msg} ->
                                                update_gateway_task(GatewayId, "0"),
                                                Msg
                                        after
                                            TIMEOUT ->
                                                update_gateway_task(GatewayId, "0"),
                                                Reason = "超时未返回",
                                                gen_failure_return_content(OrderNumber, Sign, Flag, EqptType, EqptIdCode, Reason)
                                    end;
                                    false ->
                                        Reason = "网关不在线",
                                        gen_failure_return_content(OrderNumber, Sign, Flag, EqptType, EqptIdCode, Reason)
                                end;
                            _ ->
                                Reason = "网关未登录",
                                gen_failure_return_content(OrderNumber, Sign, Flag, EqptType, EqptIdCode, Reason)                                    
                        end
                end
        catch
            _Class:Reason ->
                gen_failure_return_content(OrderNumber, Sign, Flag, EqptType, EqptIdCode, Reason)
        end,
    Reply = ?HELPER:to_iolist(ResponseContent),
    ?PRINT("response:~s~n", [Reply]),

    %% Log record: response log
    connector_event_server:response_log(OrderNumber, Reply),

    cowboy_req:reply(200, #{
                       <<"content-type">> => <<"text/plain;charset=utf-8">>
                      }, Reply, Req).


get_library_prefix(EqptType, EqptIdCode) ->
    case connector_db_store_server:get_dll_prefix(EqptType, EqptIdCode) of
        {ok, DLLPrefix} ->
            DLLPrefix;
        _ ->
            throw("没有找到该计量表的动态库前缀")
    end.

get_so_lib_name(Objs) ->
    #objs{
       eqptType = EqptType,
       eqptIdCode = EqptIdCode,
       eqptPwd = _EqptPwd,
       resDataType = _ResDataType,
       cmdType = CmdType,
       cmdId = CmdId,
       cmdData = _CmdData
      } = Objs,
    LibraryPrefix = get_library_prefix(EqptType, EqptIdCode),
    DirName = filename:absname(?SHARE_LIB_DIR),
    SoLibName = lists:concat(["lib", LibraryPrefix, "_", CmdType, "_", CmdId, ".so"]),
    SoFileName = filename:join([DirName, SoLibName]),
    case filelib:is_file(SoFileName) of
        true ->
            SoFileName;
        false ->
            throw("没有找到该动态库名称:" ++ SoLibName)
    end.

gen_failure_return_content(OrderNumber, Sign, Flag, EqptType, EqptIdCode, Reason) ->
    Status = "300",
    ResultMsg = ?HELPER:to_string(Reason),
    Msg = "任务执行失败",
    "{\"order_no\":\"" ++ OrderNumber ++ "\", \"eqpt_type\":\"" ++ EqptType ++ "\", \"eqpt_id_code\":\"" ++ EqptIdCode ++ 
        "\",\"result\":\"" ++ ResultMsg ++ "\",\"status\":\"" ++ Status ++ "\",\"msg\":\"" ++ Msg ++ "\", \"sign\":\"" ++ Sign ++ "\", \"flag\":\"" ++ Flag ++ "\"}".

update_gateway_task(GatewayId, Flag) ->
    case connector_gateway_status_store:lookup(GatewayId) of
        {ok, Task} ->
            case Flag of
                "1" ->
                    connector_gateway_status_store:update(GatewayId, Task+1);
                _ ->
                    connector_gateway_status_store:update(GatewayId, Task-1)
            end;
        {error, not_found} ->
            ok
    end.
                    
