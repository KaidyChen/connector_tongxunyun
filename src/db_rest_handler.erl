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
-define(ALREADY_EXISTSTASK, "已经存在该任务序列,不支持重复添加").
-define(NOT_EXISTS, "不存在该设备，不支持修改/删除操作").
-define(NOT_EXISTSTASK, "不存在该任务序列，无法进行操作").
-define(EXISTS_METER, "该设备下挂有计量设备，不支持删除").
-define(QUERY_RESULT, "不存在该设备").
-define(NO_PERMISSION, "输入账户信息错误，无法进行操作").

-export([
         response_to_json/2, 
         response_to_html/2, 
         response_to_text/2
        ]).

-export([
         login/1,

         regist_user/1,
         cancel_user/1,
         
         get_history_records/1,
         get_reportdata_info_total_row/1,

         add_task/1,
         delete_task/1,
         get_task/1,

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
         update_collector_and_config/1,

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
         get_eqpt_info_by_id/1,

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
    UserName = cowboy_req:header(<<"username">>, Req,<<"0">>),
    UserPass = cowboy_req:header(<<"userpwd">>, Req,<<"0">>),
    ?PRINT("qs:~p~n",[QsInfo]),
    ?PRINT("User:~p Pass:~p~n",[UserName,UserPass]),
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

notify_update_eqpt_info() ->
    connector_db_store_server:notify_update_eqpt_info().

notify_update_task_info() ->
    connector_db_store_server:notify_update_task_info().

notify_update_eqpt_type_info() ->
    connector_db_store_server:notify_update_eqpt_type_info().

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

total_return(Count, QsInfo) ->
    QsList = binary_to_list(QsInfo),
    [A | B] = string:tokens(QsList, "?"),
    case B of
        [] ->
            jsx:encode([{<<"returnCode">>, ?SUCCESS_STATUS}, {<<"totalRow">>, Count}]);
        _ ->
            [C] = B,
            Flag = string:right(C, 42),
            jsx:encode([{<<"returnCode">>, ?SUCCESS_STATUS},{<<"flag">>, list_to_binary(Flag)}, {<<"totalRow">>, Count}])
    end.

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

%% 参数类型转换 [] -> " "
get_str(List) -> 
    case List of
        [] ->
            null;
        _ ->
            List
    end.
%%---------------------------------------------------------------------------------------
%% 对账户的相关操作
%%---------------------------------------------------------------------------------------
login([_Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User, Password) of
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo);
        true ->
            format_return("login success", ?SUCCESS_STATUS, QsInfo)
    end.

regist_user([Qs, QsInfo, _UserName, _UserPass]) ->
    User = binary_to_list(proplists:get_value(<<"userName">>, Qs, <<>>)),
    Password = binary_to_list(proplists:get_value(<<"userPwd">>, Qs, <<>>)),
    case db_util:is_exists_username(User) of
        false ->
            case db_util:add_user_info(User, Password) of
                ok ->
                    db_util:commit_transaction(),
                    format_return("账号注册成功!", ?SUCCESS_STATUS, QsInfo);
                {error, Reason} ->
                    db_util:rollback(),
                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        true ->
            format_return("账户已存在!!!", ?FAILURE_STATUS, QsInfo)
    end.
            
cancel_user([_Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User, Password) of
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo);
        true ->
            case db_util:cancel_user_info(User, Password) of
               ok ->
                  db_util:commit_transaction(),
                  format_return("账户注销成功!", ?SUCCESS_STATUS, QsInfo);
               {error, Reason} ->
                  db_util:rollback(),
                  format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end
    end.

%%---------------------------------------------------------------------------------------
%% 对任务的相关操作
%%---------------------------------------------------------------------------------------
add_task([Qs, QsInfo, UserName, UserPass]) ->
    TaskId = binary_to_list(proplists:get_value(<<"taskId">>, Qs, <<>>)),
    EqptCjqType = binary_to_list(proplists:get_value(<<"eqptCjqType">>, Qs, <<>>)),
    EqptCjqIdCode = binary_to_list(proplists:get_value(<<"eqptCjqIdCode">>, Qs, <<>>)),
    MeterType = binary_to_list(proplists:get_value(<<"meterType">>, Qs, <<>>)),
    MeterIdCode = get_str(binary_to_list(proplists:get_value(<<"meterIdCode">>, Qs, <<>>))),
    TaskType = binary_to_list(proplists:get_value(<<"taskType">>, Qs, <<>>)),
    TaskStart = get_str(binary_to_list(proplists:get_value(<<"taskStart">>, Qs, <<>>))),
    TaskTime = get_str(binary_to_list(proplists:get_value(<<"taskTime">>, Qs, <<>>))),
    TaskCustom = get_str(binary_to_list(proplists:get_value(<<"taskCustom">>, Qs, <<>>))),
    [TaskName, TaskCmdId] = 
                  case TaskType of 
                        "xs" ->
                            TaskNameTmp = "hour task",
                            TaskCmdIdTmp = "zddjsjk",
                            [TaskNameTmp, TaskCmdIdTmp];
                        "rd" ->
                            TaskNameTmp = "day task",
                            TaskCmdIdTmp = "rdjsjk",
                            [TaskNameTmp, TaskCmdIdTmp];
                        "yd" ->
                            TaskNameTmp = "month task",
                            TaskCmdIdTmp = "ydjsjk",
                            [TaskNameTmp, TaskCmdIdTmp];
                        "fh" ->
                            TaskNameTmp = "load_fh task",
                            TaskCmdIdTmp = "fhjlz_zdsj",
                            [TaskNameTmp, TaskCmdIdTmp];
                        "lx" ->
                            TaskNameTmp = "polling task",
                            TaskCmdIdTmp = binary_to_list(proplists:get_value(<<"taskCmdId">>, Qs, <<>>)),
                            [TaskNameTmp, TaskCmdIdTmp];
                        _ ->
                            format_return("add task fail", <<"illegal task type!!!">>, QsInfo)
                  end,
    case db_util:is_exists_task(TaskId) of
        false ->
          case TaskType of
                "lx" ->
                    case [TaskStart, TaskTime] of
                        [null, null]  ->
                            db_util:rollback(),
                            format_return("Error operation, missing the time parameters", ?FAILURE_STATUS, QsInfo);
                        [_, null] ->
                            db_util:rollback(),
                            format_return("Error operation, missing the time parameters", ?FAILURE_STATUS, QsInfo);
                        [null, _] ->
                            db_util:rollback(),
                            format_return("Error operation, missing the time parameters", ?FAILURE_STATUS, QsInfo);   
                        _ ->
                            case MeterIdCode of
                                null ->
                                    {ok, MeterIdCodeList} = connector_eqpt_id_store:lookup({EqptCjqType, EqptCjqIdCode, MeterType}),
                                    ?PRINT("MeterList:~p~n",[MeterIdCodeList]),
                                    [connector_task_server:run_polling_task({TaskId,EqptCjqIdCode,MeterType,NewMeterIdCode,TaskCmdId,TaskType,TaskStart,TaskTime,TaskCustom}) || {_, _, NewMeterIdCode} <- MeterIdCodeList];
                                _ ->
                                    Task = {TaskId,EqptCjqIdCode,MeterType,MeterIdCode,TaskCmdId,TaskType,TaskStart,TaskTime,TaskCustom},
                                    connector_task_server:run_polling_task(Task)
                            end, 
                            db_util:begin_transaction(),
                            case db_util:add_task_info(TaskId, EqptCjqType, EqptCjqIdCode, MeterType, MeterIdCode, TaskName, TaskCmdId, TaskType, TaskStart, TaskTime, TaskCustom) of
                                ok ->    
                                    db_util:commit_transaction(),
                                    notify_update_task_info(),
                                    format_return("add task success", ?SUCCESS_STATUS, QsInfo);
                                {error, Reason} ->
                                    db_util:rollback(),
                                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
                            end
                    end;
                  _ ->
                    case MeterIdCode of
                      null ->
                          {ok, MeterIdCodeList} = connector_eqpt_id_store:lookup({EqptCjqType, EqptCjqIdCode, MeterType}),
                          [connector_task_server:run_normal_task({TaskId,EqptCjqIdCode,MeterType,NewMeterIdCode,TaskCmdId,TaskType,TaskStart,TaskTime, TaskCustom}) || {_,_,NewMeterIdCode} <- MeterIdCodeList];
                      _ ->
                          Task = {TaskId,EqptCjqIdCode,MeterType,MeterIdCode,TaskCmdId,TaskType,TaskStart,TaskTime,TaskCustom},
                              connector_task_server:run_normal_task(Task)
                    end,
                    db_util:begin_transaction(),
                    case db_util:add_task_info(TaskId, EqptCjqType, EqptCjqIdCode, MeterType, MeterIdCode, TaskName, TaskCmdId, TaskType, TaskStart, TaskTime, TaskCustom) of
                          ok ->
                              db_util:commit_transaction(),
                              notify_update_task_info(),
                              format_return("add task success", ?SUCCESS_STATUS, QsInfo);
                          {error, Reason} ->
                              db_util:rollback(),
                              format_return(Reason, ?FAILURE_STATUS, QsInfo)
                    end
          end;
        true ->
            format_return(?ALREADY_EXISTSTASK, ?FAILURE_STATUS, QsInfo)
    end.  
    
delete_task([Qs,QsInfo, UserName, UserPass]) ->
    TaskId = binary_to_list(proplists:get_value(<<"taskId">>, Qs, <<>>)),
    case TaskId of
        [] ->
            format_return("missing paraments!!!", ?FAILURE_STATUS, QsInfo);
        _  ->
            case db_util:is_exists_task(TaskId) of
                true ->
                    case db_util:delete_task_info(TaskId) of
                        ok ->
                            TaskIdList = connector_task_pid_store:lookup(TaskId),
                            ?PRINT("taskidlist:~p~n",[TaskIdList]),
                            case connector_task_pid_store:lookup(TaskId) of
                                TaskIdList ->
                                        Fun = 
                                            fun({_, TaskType, Pid}) ->
                                                    case TaskType of
                                                        "lx" ->
                                                            connector_pollingtask_process:stop(Pid);
                                                        _ ->
                                                            connector_task_process:stop(Pid)
                                                    end
                                            end,
                                        lists:foreach(Fun, TaskIdList),
                                        connector_task_pid_store:delete(TaskId);
                                _ ->
                                    ?PRINT("error, task pid isn't existed~n",[])
                            end,
                            db_util:commit_transaction(),
                            notify_update_task_info(),
                            format_return("delete task success", ?SUCCESS_STATUS, QsInfo);
                        {error, Reason} ->
                            db_util:rollback(),
                            format_return(Reason, ?FAILURE_STATUS, QsInfo)
                    end;
                false ->
                    format_return(?NOT_EXISTSTASK, ?FAILURE_STATUS, QsInfo)
            end
    end.

get_task([_Qs, QsInfo, UserName, UserPass]) ->
    case db_util:get_task_info() of
            {ok, []} ->
                data_return([], QsInfo);
            {ok, Rows} ->
                List = [[{<<"taskId">>, TaskId}, {<<"eqptCjqType">>, EqptCjqType}, 
                         {<<"eqptCjqIdCode">>, EqptCjqIdCode}, {<<"meterType">>, MeterType}, {<<"meterIdCode">>, MeterIdCode}, {<<"taskName">>, TaskName}, {<<"taskCmdId">>, TaskCmdId}, {<<"taskType">>, TaskType}, {<<"taskStart">>, TaskStart}, {<<"taskTime">>, TaskTime}, {<<"taskCustom">>, TaskCustom}] || {TaskId, EqptCjqType, EqptCjqIdCode, MeterType, MeterIdCode, TaskName, TaskCmdId, TaskType, TaskStart, TaskTime, TaskCustom} <- field_format(Rows)],
                data_return(List, QsInfo);
            {error, Reason} ->
                format_return(Reason, ?FAILURE_STATUS, QsInfo)
    end.
%%---------------------------------------------------------------------------------------
%% 获取设备的类型信息
%%---------------------------------------------------------------------------------------
%%添加设备的类型信息
add_device_type_info([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    EqptLevel = binary_to_list(proplists:get_value(<<"eqptLevel">>, Qs, <<>>)),
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptTypeName = binary_to_list(proplists:get_value(<<"eqptTypeName">>, Qs, <<>>)),
    ProtocolName = binary_to_list(proplists:get_value(<<"protocolName">>, Qs, <<>>)),
    ProtocolType = binary_to_list(proplists:get_value(<<"protocolType">>, Qs, <<>>)),
    case db_util:is_exists_user(User, Password) of
        true ->
            case db_util:add_device_type_info(EqptLevel, EqptType, EqptTypeName, ProtocolName, ProtocolType) of
                ok ->
                    db_util:commit_transaction(),
                    notify_update_eqpt_info(),
                    notify_update_eqpt_type_info(),
                    format_return("add device_type_info success", ?SUCCESS_STATUS, QsInfo);
                {error, Reason} ->
                    db_util:rollback(),
                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
   end.

%%删除设备类型信息
delete_device_type_info([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    case db_util:is_exists_user(User, Password) of
        true ->
            case db_util:delete_device_type_info(EqptType) of
                ok ->
                    db_util:commit_transaction(),
                    notify_update_eqpt_info(),
                    notify_update_eqpt_type_info(),
                    format_return("delete device_type_info success", ?SUCCESS_STATUS, QsInfo);
                {error, Reason} ->
                    db_util:rollback(),
                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

%% 获取所有设备的类型信息
get_device_type_info([_Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User, Password) of
        true ->
            case db_util:get_device_type_info() of
                {ok, []} ->
                    data_return([], QsInfo);
                {ok, Rows} ->
                    List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptTypeName">>, EqptTypeNameBinary}, 
                            {<<"protocolName">>, ProtocolNameBinary}, {<<"protocolType">>, ProtocolTypeBinary}] || 
                            {EqptTypeBinary, EqptTypeNameBinary, ProtocolNameBinary, ProtocolTypeBinary} <- field_format(Rows)],
                    data_return(List, QsInfo);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

%% 获取采集设备的类型信息
get_collector_type_info([_Qs, QsInfo, UserName, UserPass]) ->
    get_specific_type_info(?COLLECTOR_LEVEL, QsInfo, UserName, UserPass).

%% 获取中继器设备的类型信息
get_repeaters_type_info([_Qs, QsInfo, UserName, UserPass]) ->
    get_specific_type_info(?REPEATERS_LEVEL, QsInfo, UserName, UserPass).

%% 获取计量设备的类型信息
get_meter_type_info([_Qs, QsInfo, UserName, UserPass]) ->
    get_specific_type_info(?METER_LEVEL, QsInfo, UserName, UserPass).

%% 获取指定级别设备的类型信息
get_specific_type_info(EqptLevel, QsInfo, UserName, UserPass) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User, Password) of
        true ->
            case db_util:get_specific_type_info(EqptLevel) of
                {ok, []} ->
                    data_return([], QsInfo);
                {ok, Rows} ->
                    List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptTypeName">>, EqptNameBinary}, 
                            {<<"protocolName">>, ProtocolNameBinary}, {<<"protocolType">>, ProtocolTypeBinary}] || 
                            {EqptTypeBinary, EqptNameBinary, ProtocolNameBinary, ProtocolTypeBinary} <- field_format(Rows)],
                    data_return(List, QsInfo);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION,?FAILURE_STATUS, QsInfo)
    end.

%%----------------------------------------------------------------------------------------------
%% 批量操作
%%----------------------------------------------------------------------------------------------
batch_add_collector([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User, Password) of
        true ->
            try get_collector_info_list(Qs, User) of
                CollectorInfoList ->
                    db_util:begin_transaction(),
                    case batch_add_collector_(CollectorInfoList) of
                        ok ->
                            db_util:commit_transaction(),
                            Msg = string:join(["batch_add_collector", CollectorInfoList], "/"),
                            connector_event_server:operation_log(" ", Msg),
                            notify_update_eqpt_info(),
                            format_return("batch add collectors success", ?SUCCESS_STATUS, QsInfo);
                        {error, Reason} ->
                            db_util:rollback(),
                            format_return(Reason, ?FAILURE_STATUS, QsInfo)
                    end
            catch 
                _Class:What ->
                    format_return(What, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION,?FAILURE_STATUS, QsInfo)
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
     

get_collector_info_list(Qs,User) ->
    get_collector_info_list_(Qs, [], User).

get_collector_info_list_([Qs | T], CollectorInfoList, User) ->
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    EqptName = binary_to_list(proplists:get_value(<<"eqptName">>, Qs, <<>>)),
    %%Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
    Creator = User,
    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
        {ok, EqptLevel, ProtocolType} when (EqptLevel =:= ?COLLECTOR_LEVEL) 
                                           orelse (EqptLevel =:= ?REPEATERS_LEVEL) ->
            CollectorInfo = {EqptLevel, EqptType, EqptIdCode, EqptName, Creator, ProtocolType},
            ?PRINT("~p~n", [CollectorInfo]),
            get_collector_info_list_(T, [CollectorInfo | CollectorInfoList], User);
        _ ->
            throw(EqptIdCode ++ " " ++ ?ILLEGAL_EQPT_TYPE)
    end;
get_collector_info_list_(_, CollectorInfoList, User) ->
    CollectorInfoList.

batch_add_meter([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User, Password) of
        true ->
            try get_meter_info_list(Qs, User) of
                MeterInfoList ->
                    db_util:begin_transaction(),
                    case batch_add_meter_(MeterInfoList) of
                        ok ->
                            db_util:commit_transaction(),
                            notify_update_eqpt_info(),
                            Msg = string:join(["batch_add_meter", MeterInfoList], "/"),
                            connector_event_server:operation_log(" ", Msg),
                            format_return("batch add collectors success", ?SUCCESS_STATUS, QsInfo);
                        {error, Reason} ->
                            db_util:rollback(),
                            format_return(Reason, ?FAILURE_STATUS, QsInfo)
                    end
            catch 
                _Class:What ->
                    format_return(What, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION,?FAILURE_STATUS, QsInfo)
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

get_meter_info_list(Qs, Creator) ->
    get_meter_info_list_(Qs, [], Creator).

get_meter_info_list_([Qs | T], MeterInfoList, Creator) ->
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
    %%Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
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
            get_meter_info_list_(T, [MeterInfo | MeterInfoList], Creator);
        _ ->
            throw("EqptIdCode: " ++ EqptIdCode ++ " " ++ ?ILLEGAL_EQPT_TYPE)
    end;
get_meter_info_list_(_, MeterInfoList, Creator) ->
    MeterInfoList.


batch_get_meter([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User, Password) of
        true ->
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
            Creator = User,
            EqptTypes = get_eqpt_types(Qs),
            PageSize = get_page_size(Qs),
            Page = get_page(Qs),
    
            case db_util:batch_get_meter(EqptCjqType, EqptCjqCode, EqptTypes, Creator, PageSize, Page) of
                {ok, []} ->
                    data_return([], QsInfo);
                {ok, Rows} ->
                    List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}, 
                            {<<"eqptCjqType">>, EqptCjqTypeBinary}, {<<"eqptCjqCode">>, EqptCjqCodeBinary}, 
                            {<<"eqptZjqType">>, EqptZjqTypeBinary}, {<<"eqptZjqCode">>, EqptZjqCodeBinary},
                            {<<"createTime">>, CreateTimeBinary},{<<"eqptMeasureCode">>, EqptMeasureCode},
                            {<<"creator">>, CreatorBinary}, {<<"eqptPort">>, EqptPort},
                            {<<"eqptBaudrate">>, EqptBaudrate}, {<<"eqptRateNumber">>, EqptRateNumber}, 
                            {<<"eqptZsw">>, EqptZsw},
                            {<<"eqptXsw">>, EqptXsw}, {<<"eqptDlh">>, EqptDlh},
                            {<<"eqptXlh">>, EqptXlh}, {<<"eqptYblx">>, EqptYblxBinary}
                            ] || {EqptTypeBinary, EqptIdCodeBinay, EqptNameBinary, EqptCjqTypeBinary, EqptCjqCodeBinary,
                            EqptZjqTypeBinary, EqptZjqCodeBinary, CreateTimeBinary, EqptMeasureCode, CreatorBinary, EqptPort, EqptBaudrate,
                            EqptRateNumber, EqptZsw,EqptXsw, EqptDlh, EqptXlh, EqptYblxBinary}<- field_format(Rows)],
                    data_return(List, QsInfo);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION,?FAILURE_STATUS, QsInfo)
    end.
    
%%----------------------------------------------------------------------------------------------
%% 采集器/中继器相关API
%%----------------------------------------------------------------------------------------------

%% 添加采集器/中继器设备
add_collector([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    EqptName = binary_to_list(proplists:get_value(<<"eqptName">>, Qs, <<>>)),
    %%Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
    Creator = User,
    case db_util:is_exists_user(User, Password) of
        true ->
            case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
                {ok, EqptLevel, ProtocolType} when (EqptLevel =:= ?COLLECTOR_LEVEL) ->
                    case db_util:is_exists_collector(EqptType, EqptIdCode) of
                        false ->
                            db_util:begin_transaction(),
                            case db_util:add_collector(EqptType, EqptIdCode, EqptName, Creator, ProtocolType) of
                                ok ->
                                    db_util:commit_transaction(),
                                    notify_update_eqpt_info(),
                                    connector_topic_info_store:insert({{EqptIdCode,EqptIdCode},Creator}),
                                    Msg = string:join(["add_collector", EqptType, EqptIdCode,EqptName, Creator], "/"),
                                    connector_event_server:operation_log(EqptIdCode, Msg),
                                    format_return("add collector success", ?SUCCESS_STATUS, QsInfo);
                                {error, Reason} ->
                                    db_util:rollback(),
                                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
                            end;
                        true ->
                            format_return(?ALREADY_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                {ok, EqptLevel, ProtocolType} when (EqptLevel =:= ?REPEATERS_LEVEL) ->
                    case db_util:is_exists_repeaters(EqptType, EqptIdCode) of
                        false ->
                            db_util:begin_transaction(),
                            case db_util:add_repeaters(EqptType, EqptIdCode, EqptName, Creator) of
                                ok -> 
                                    db_util:commit_transaction(),
                                    format_return("add repeaters success", ?SUCCESS_STATUS, QsInfo);
                                {error, Reason} ->
                                    db_util:rollback(),
                                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
                            end;
                        true ->
                            format_return(?ALREADY_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                _ ->
                    format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

%% 修改采集器/中继器设备
update_collector([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    EqptName = binary_to_list(proplists:get_value(<<"eqptName">>, Qs, <<>>)),
    %%Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
    Creator = User,
    case db_util:is_exists_user(User, Password) of
        true ->
            case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
                {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?COLLECTOR_LEVEL) ->
                    case db_util:is_exists_collector(EqptType, EqptIdCode) of
                        true ->
                            case db_util:update_collector(EqptType, EqptIdCode, EqptName, Creator) of
                                ok ->
                                    connector_topic_info_store:update({EqptIdCode,EqptIdCode},{{EqptIdCode,EqptIdCode},Creator}),
                                    Msg = string:join(["update_collector", EqptType, EqptIdCode, EqptName, Creator], "/"),
                                    connector_event_server:operation_log(EqptIdCode, Msg),
                                    format_return("update collector success", ?SUCCESS_STATUS, QsInfo);
                                {error, Reason} ->
                                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
                            end;
                        false ->
                            format_return(?NOT_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?REPEATERS_LEVEL) ->
                    case db_util:is_exists_repeaters(EqptType, EqptIdCode) of
                        true ->
                            case db_util:update_repeaters(EqptType, EqptIdCode, EqptName, Creator) of
                                ok -> 
                                    format_return("update repeaters success", ?SUCCESS_STATUS, QsInfo);
                                {error, Reason} ->
                                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
                            end;
                        false ->
                            format_return(?NOT_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                _ ->
                    format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

%% 删除采集器/中继器设备
delete_collector([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    Creator = User,
    case db_util:is_exists_user(User,Password) of
        true ->
            case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
                {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?COLLECTOR_LEVEL) ->
                    case db_util:is_exists_collector(EqptType, EqptIdCode) of
                        true ->
                            case db_util:is_exists_meter_of_collector(EqptType, EqptIdCode) of
                                false ->
                                    case db_util:delete_collector(EqptType, EqptIdCode, Creator) of
                                        ok ->
                                            notify_update_eqpt_info(),
                                            connector_topic_info_store:delete({EqptIdCode,EqptIdCode}),
                                            Msg = string:join(["delete_collector", EqptType, EqptIdCode], "/"),
                                            connector_event_server:operation_log(EqptIdCode, Msg),
                                            format_return("delete collector success", ?SUCCESS_STATUS, QsInfo);
                                        {error, Reason} ->
                                            format_return(Reason, ?FAILURE_STATUS, QsInfo)
                                    end;
                                true ->
                                    format_return(?EXISTS_METER, ?FAILURE_STATUS, QsInfo);
                                {error, Reason1} ->
                                    format_return(Reason1, ?FAILURE_STATUS, QsInfo)
                            end;
                        false ->
                            format_return(?NOT_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?REPEATERS_LEVEL) ->
                    case db_util:is_exists_repeaters(EqptType, EqptIdCode) of
                        true ->
                            case db_util:is_exists_meter_of_repeaters(EqptType, EqptIdCode) of
                                false ->
                                    case db_util:delete_repeaters(EqptType, EqptIdCode) of
                                        ok -> 
                                            format_return("delete repeaters success", ?SUCCESS_STATUS, QsInfo);
                                        {error, Reason} ->
                                            format_return(Reason, ?FAILURE_STATUS, QsInfo)
                                    end;
                                true ->
                                    format_return(?EXISTS_METER, ?FAILURE_STATUS, QsInfo);
                                {error, Reason1} ->
                                    format_return(Reason1, ?FAILURE_STATUS, QsInfo)
                            end;  
                        false ->
                            format_return(?NOT_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                _ ->
                    format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.
    
%% 强制性删除
force_delete_collector([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    Creator = User,
    case db_util:is_exists_user(User, Password) of
        true ->
            case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
                {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?COLLECTOR_LEVEL) ->
                    case db_util:is_exists_collector(EqptType, EqptIdCode) of
                        true ->
                            case db_util:force_delete_collector(EqptType, EqptIdCode, Creator) of
                                ok ->
                                    notify_update_eqpt_info(),
                                    connector_topic_info_store:delete({EqptIdCode,EqptIdCode}),
                                    Msg = string:join(["force_delete_collector", EqptType, EqptIdCode], "/"),
                                    connector_event_server:operation_log(EqptIdCode, Msg),
                                    format_return("force delete collector success", ?SUCCESS_STATUS, QsInfo);
                                {error, Reason} ->
                                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
                            end;
                        false ->
                            format_return(?NOT_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?REPEATERS_LEVEL) ->
                    case db_util:is_exists_repeaters(EqptType, EqptIdCode) of
                        true ->
                            case db_util:force_delete_repeaters(EqptType, EqptIdCode, Creator) of
                                ok -> 
                                    format_return("force delete repeaters success", ?SUCCESS_STATUS, QsInfo);
                                {error, Reason} ->
                                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
                            end;
                        false ->
                            format_return(?NOT_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                _ ->
                    format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

%更新采集器及其组网关系
update_collector_and_config([Qs, QsInfo, UserName, UserPass]) ->
    EqptTypeOld = binary_to_list(proplists:get_value(<<"oldeqptType">>, Qs, <<>>)),
    EqptIdCodeOld = binary_to_list(proplists:get_value(<<"oldeqptIdCode">>, Qs, <<>>)),
    EqptTypeNew = binary_to_list(proplists:get_value(<<"neweqptType">>, Qs, <<>>)),
    EqptIdCodeNew = binary_to_list(proplists:get_value(<<"neweqptIdCode">>, Qs, <<>>)),
    EqptName = binary_to_list(proplists:get_value(<<"eqptName">>, Qs, <<>>)),
    %%Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    Creator = User,
    case db_util:is_exists_user(User, Password) of
        true ->
            case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptTypeOld) of
                {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?COLLECTOR_LEVEL) ->
                    case db_util:is_exists_collector(EqptTypeOld, EqptIdCodeOld) of
                        true ->
                            case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptTypeNew) of
                                {ok, _, ProtocolType} ->
                                    case db_util:update_collector_and_config(EqptTypeOld, EqptIdCodeOld, EqptTypeNew, EqptIdCodeNew, EqptName, Creator, ProtocolType) of
                                        ok ->
                                            notify_update_eqpt_info(),
                                            connector_topic_info_store:update({EqptIdCodeOld,EqptIdCodeOld},{{EqptIdCodeNew,EqptIdCodeNew},Creator}),
                                            Msg = string:join(["update_collector_and_config", EqptTypeOld, EqptIdCodeOld,EqptTypeNew,EqptIdCodeNew,EqptName,Creator,ProtocolType], "/"),
                                            connector_event_server:operation_log(EqptIdCodeNew, Msg),
                                            format_return("update collector_and_config success", ?SUCCESS_STATUS, QsInfo);
                                        {error, Reason} ->
                                            format_return(Reason, ?FAILURE_STATUS, QsInfo)
                                    end;
                               _ ->
                                    format_return("采集器协议类型不存在", ?FAILURE_STATUS, QsInfo)
                            end;
                        false ->
                            format_return(?NOT_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                {ok, EqptLevel, _ProtocolType} when (EqptLevel =:= ?REPEATERS_LEVEL) ->
                    case db_util:is_exists_repeaters(EqptTypeOld, EqptIdCodeOld) of
                        true ->
                            case db_util:update_repeater_and_config(EqptTypeOld, EqptIdCodeOld, EqptTypeNew, EqptIdCodeNew, EqptName, Creator) of
                                ok -> 
                                    notify_update_eqpt_info(),
                                    Msg = string:join(["update_collector_and_config", EqptTypeOld, EqptIdCodeOld,EqptTypeNew,EqptIdCodeNew,EqptName,Creator], "/"),
                                    connector_event_server:operation_log(EqptIdCodeNew, Msg),
                                    format_return("update repeater_and_config success", ?SUCCESS_STATUS, QsInfo);
                                {error, Reason} ->
                                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
                            end;
                        false ->
                            format_return(?NOT_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                _ ->
                    format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
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
add_meter([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    EqptName = binary_to_list(proplists:get_value(<<"eqptName">>, Qs, <<>>)),
    EqptCjqType = binary_to_list(proplists:get_value(<<"eqptCjqType">>, Qs, <<>>)),
    EqptCjqCode = binary_to_list(proplists:get_value(<<"eqptCjqCode">>, Qs, <<>>)),
    EqptZjqType = 
        case proplists:get_value(<<"eqptZjqType">>, Qs) of
            undefined -> null;
            <<>> -> null;
            EqptZjqTypeBinary -> binary_to_list(EqptZjqTypeBinary)
        end,
    EqptZjqCode = 
        case proplists:get_value(<<"eqptZjqCode">>, Qs) of
            undefined -> null;
            <<>> -> null;
            EqptZjqCodeBinary -> binary_to_list(EqptZjqCodeBinary)
        end,
    %%Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
    Creator = User,
    EqptMeasureCode = proplists:get_value(<<"eqptMeasureCode">>, Qs, 4),
    EqptPort = proplists:get_value(<<"eqptPort">>, Qs, 31),
    EqptBaudrate = proplists:get_value(<<"eqptBaudrate">>, Qs, 2400),
    EqptRateNumber = proplists:get_value(<<"eqptRateNumber">>, Qs, 4),
    EqptZsw = proplists:get_value(<<"eqptZsw">>, Qs, 6),
    EqptXsw = proplists:get_value(<<"eqptXsw">>, Qs, 2),
    EqptDlh = proplists:get_value(<<"eqptDlh">>, Qs, 5),
    EqptXlh = proplists:get_value(<<"eqptXlh">>, Qs, 1),
    EqptYblx = binary_to_list(proplists:get_value(<<"eqptYblx">>, Qs, <<"DNB">>)),
    case db_util:is_exists_user(User,Password) of
        true ->
            case {connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType), 
                connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptCjqType)} of
                {{ok, ?METER_LEVEL, EqptProtocolType}, {ok, ?COLLECTOR_LEVEL, EqptCjqProtocolType}} ->
                    case db_util:is_exists_meter(EqptType, EqptIdCode) of
                        false ->
                            db_util:begin_transaction(),
                            case db_util:add_meter(EqptType, EqptIdCode, EqptName, EqptCjqType, EqptCjqCode, EqptZjqType, EqptZjqCode, 
                                                EqptMeasureCode, Creator, EqptPort, EqptBaudrate, EqptRateNumber, EqptZsw, EqptXsw, EqptDlh, 
                                                EqptXlh, EqptYblx, EqptProtocolType, EqptCjqProtocolType) of
                                ok ->
                                    notify_update_eqpt_info(),
                                    db_util:commit_transaction(),
                                    connector_topic_info_store:insert({{EqptCjqCode,EqptIdCode},Creator}),
                                    Msg=string:join(["add_meter",EqptType,EqptIdCode,EqptName,EqptCjqType,EqptCjqCode,EqptYblx,Creator],"/"),
                                    connector_event_server:operation_log(EqptIdCode, Msg),
                                    format_return("add meter success", ?SUCCESS_STATUS, QsInfo);
                                {error, Reason} ->
                                    db_util:rollback(),
                                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
                            end;
                        true ->
                            format_return(?ALREADY_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                _ ->
                    format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

update_meter([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    EqptName = binary_to_list(proplists:get_value(<<"eqptName">>, Qs, <<>>)),
    %%Creator = binary_to_list(proplists:get_value(<<"creator">>, Qs, <<"admin">>)),
    Creator = User,
    EqptMeasureCode = proplists:get_value(<<"eqptMeasureCode">>, Qs, 4),
    EqptBaudrate = proplists:get_value(<<"eqptBaudrate">>, Qs, 2400),
    EqptRateNumber = proplists:get_value(<<"eqptRateNumber">>, Qs, 4),
    EqptZsw = proplists:get_value(<<"eqptZsw">>, Qs, 6),
    EqptXsw = proplists:get_value(<<"eqptXsw">>, Qs, 2),
    EqptDlh = proplists:get_value(<<"eqptDlh">>, Qs, 5),
    EqptXlh = proplists:get_value(<<"eqptXlh">>, Qs, 1),
    EqptYblx = binary_to_list(proplists:get_value(<<"eqptYblx">>, Qs, <<"DNB">>)),
    EqptPort = proplists:get_value(<<"eqptPort">>, Qs, 31),
    case db_util:is_exists_user(User, Password) of
          true ->
            case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
                {ok, ?METER_LEVEL, _EqptProtocolType} ->
                    case db_util:is_exists_meter(EqptType, EqptIdCode) of
                        true ->
                            case db_util:update_meter(EqptType, EqptIdCode, EqptName, Creator, EqptMeasureCode, EqptPort, EqptBaudrate, EqptRateNumber, EqptZsw, EqptXsw, EqptDlh, EqptXlh, EqptYblx) of
                                ok ->
                                    notify_update_eqpt_info(),
                                    {ok, {_, EqptCjqCode}} = connector_db_store_server:get_upper_eqpt_type_and_upper_eqpt_id_code(EqptType,EqptIdCode),
                                    connector_topic_info_store:update({EqptCjqCode,EqptIdCode},{{EqptCjqCode,EqptIdCode},Creator}),
                                    Msg = string:join(["update_meter",EqptType,EqptIdCode,EqptName,EqptYblx,Creator], "/"),
                                    connector_event_server:operation_log(EqptIdCode, Msg),
                                    format_return("update meter success", ?SUCCESS_STATUS, QsInfo);
                                {error, Reason} ->
                                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
                            end;
                        false ->
                            format_return(?NOT_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                _ ->
                    format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
            end;
          false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

delete_meter([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    Creator = User,
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    case db_util:is_exists_user(User, Password) of
        true ->
            case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
                {ok, ?METER_LEVEL, _EqptProtocolType} ->
                    case db_util:is_exists_meter(EqptType, EqptIdCode) of
                        true ->
                            case db_util:delete_meter(EqptType, EqptIdCode, Creator) of
                                ok ->
                                    notify_update_eqpt_info(),
                                    {ok, {_, EqptCjqCode}} = connector_db_store_server:get_upper_eqpt_type_and_upper_eqpt_id_code(EqptType,EqptIdCode),
                                    connector_topic_info_store:delete({EqptCjqCode,EqptIdCode}),
                                    Msg = string:join(["delete_meter", EqptType, EqptIdCode], "/"),
                                    connector_event_server:operation_log(EqptIdCode, Msg),
                                    format_return("delete meter success", ?SUCCESS_STATUS, QsInfo);
                                {error, Reason} ->
                                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
                            end;
                        false ->
                            format_return(?NOT_EXISTS, ?FAILURE_STATUS, QsInfo);
                        {error, What} ->
                            format_return(What, ?FAILURE_STATUS, QsInfo)
                    end;
                _ ->
                    format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
            end;
       false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
   end.

%% 删除采集设备下的计量设备
delete_meter_by_collector([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    Creator = User,
    EqptCjqType = binary_to_list(proplists:get_value(<<"eqptCjqType">>, Qs, <<>>)),
    EqptCjqCode = binary_to_list(proplists:get_value(<<"eqptCjqCode">>, Qs, <<>>)),
    case db_util:is_exists_user(User,Password) of
        true ->
            case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptCjqType) of
                {ok, ?COLLECTOR_LEVEL, _} ->
                    case db_util:delete_meter_by_collector(EqptCjqType, EqptCjqCode, Creator) of
                        ok ->
                            notify_update_eqpt_info(),
                            Msg = string:join(["delete_meter_by_collector", EqptCjqType, EqptCjqCode], "/"),
                            connector_event_server:operation_log(EqptCjqCode, Msg),
                            format_return("delete meters of collector success", ?SUCCESS_STATUS, QsInfo);
                        {error, Reason} ->
                            format_return(Reason, ?FAILURE_STATUS, QsInfo)
                    end;
                _ ->
                    format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
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

get_collector_info_total_row([_Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User,Password) of
        true ->
            case db_util:get_collector_info_total_row(User) of
                {ok, [{Count}]} ->
                    total_return(Count, QsInfo);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

get_collector_info([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    %% eg: eqptTypes=type1,type2
    EqptTypes = get_eqpt_types(Qs),
    case db_util:is_exists_user(User, Password) of
        true ->
            case db_util:get_collector_info(EqptTypes, User, PageSize, Page) of
                {ok, []} ->
                    data_return([], QsInfo);
                {ok, Rows} ->
                    List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}, 
                            {<<"eqptTypeName">>, EqptTypeNameBinary}, {<<"createTime">>, CreateTimeBinary},
                            {<<"creator">>, CreatorBinary}] || {EqptTypeBinary, EqptIdCodeBinay, EqptNameBinary, EqptTypeNameBinary,
                            CreateTimeBinary,CreatorBinary} <- field_format(Rows)],
                    data_return(List, QsInfo);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
   end.

get_repeaters_info_total_row([_Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User, Password) of
        true ->
            case db_util:get_repeaters_info_total_row(User) of
                {ok, [{Count}]} ->
                    total_return(Count, QsInfo);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

get_repeaters_info([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    %% eg: eqptTypes=type1,type2
    EqptTypes = get_eqpt_types(Qs),
    case db_util:is_exists_user(User, Password) of
        true ->
            case db_util:get_repeaters_info(EqptTypes, User, PageSize, Page) of
                {ok, []} ->
                    data_return([],QsInfo);
                {ok, Rows} ->
                    List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}, 
                            {<<"eqptTypeName">>, EqptTypeNameBinary}, {<<"createTime">>, CreateTimeBinary}, 
                            {<<"creator">>, CreatorBinary}] || {EqptTypeBinary, EqptIdCodeBinay, EqptNameBinary, EqptTypeNameBinary, 
                            CreateTimeBinary, CreatorBinary} <- field_format(Rows)],
                    data_return(List, QsInfo);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.
  
get_meter_info_total_row([_Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User, Password) of
        true ->
            case db_util:get_meter_info_total_row(User) of
                {ok, [{Count}]} ->
                    total_return(Count, QsInfo);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

get_meter_info([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    %% eg: eqptTypes=type1,type2
    EqptTypes = get_eqpt_types(Qs),
    case db_util:is_exists_user(User, Password) of
        true ->
            case db_util:get_meter_info(EqptTypes, User, PageSize, Page) of
                {ok, []} ->
                    data_return([], QsInfo);
                {ok, Rows} ->
                    List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}, 
                            {<<"eqptTypeName">>, EqptTypeNameBinary}, {<<"createTime">>, CreateTimeBinary}, 
                            {<<"creator">>, CreatorBinary},
                            {<<"eqptPort">>, EqptPort}, {<<"eqptBaudrate">>, EqptBaudrate}, {<<"eqptRateNumber">>, EqptRateNumber},
                            {<<"eqptZsw">>, EqptZsw}, {<<"eqptXsw">>, EqptXsw}, {<<"eqptDlh">>, EqptDlh}, {<<"eqptXlh">>, EqptXlh},
                            {<<"eqptYblx">>, EqptYblx}
                            ] || {EqptTypeBinary, EqptIdCodeBinay, EqptNameBinary, EqptTypeNameBinary, CreateTimeBinary, CreatorBinary,
                             EqptPort, EqptBaudrate, EqptRateNumber, EqptZsw, EqptXsw, EqptDlh, EqptXlh, EqptYblx} <- field_format(Rows)],
                    data_return(List, QsInfo);
                {error, Reason} ->
                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

get_meter_network_by_collector([Qs, QsInfo, UserName, UserPass]) ->  
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    EqptCjqType = binary_to_list(proplists:get_value(<<"eqptCjqType">>, Qs, <<>>)),
    EqptCjqCode = binary_to_list(proplists:get_value(<<"eqptCjqCode">>, Qs, <<>>)),
    %% eg: eqptTypes=type1,type2
    EqptTypes = get_eqpt_types(Qs),
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    case db_util:is_exists_user(User,Password) of
        true ->
            case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptCjqType) of
                {ok, ?COLLECTOR_LEVEL, _} ->
                    case db_util:get_meter_network_by_collector(EqptCjqType, EqptCjqCode, EqptTypes, User, PageSize, Page) of
                        {ok, []} ->
                            data_return([], QsInfo);
                        {ok, Rows} ->
                            List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary},
                                    {<<"eqptTypeName">>, EqptTypeNameBinary}] || {EqptTypeBinary, EqptIdCodeBinay, EqptNameBinary,
                                    EqptTypeNameBinary} <- field_format(Rows)],
                            data_return(List, QsInfo);
                        {error, Reason} ->
                            format_return(Reason, ?FAILURE_STATUS, QsInfo)
                    end;
                _ ->
                    format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
   end.


get_collector_info_by_eqpt_type([Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    case db_util:is_exists_user(User, Password) of
        true ->
            case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
                {ok, ?COLLECTOR_LEVEL, _} ->
                    case db_util:get_collector_info_by_eqpt_type(EqptType, User, PageSize, Page) of
                        {ok, []} ->
                            data_return([], QsInfo);
                        {ok, Rows} ->
                            List = [[{<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}]
                                    || {EqptIdCodeBinay, EqptNameBinary} <- field_format(Rows)],
                            data_return(List, QsInfo);
                        {error, Reason} ->
                            format_return(Reason, ?FAILURE_STATUS, QsInfo)
                    end;
                _ ->
                    format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.
    
get_repeaters_info_by_eqpt_type([Qs,QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    case db_util:is_exists_user(User,Password) of
        true ->
            case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
                {ok, ?REPEATERS_LEVEL, _} ->
                    case db_util:get_repeaters_info_by_eqpt_type(EqptType, User, PageSize, Page) of
                        {ok, []} ->
                            data_return([], QsInfo);
                        {ok, Rows} ->
                            List = [[{<<"eqptIdCode">>, EqptIdCodeBinay}, {<<"eqptName">>, EqptNameBinary}]
                                    || {EqptIdCodeBinay, EqptNameBinary} <- field_format(Rows)],
                            data_return(List, QsInfo);
                        {error, Reason} ->
                            format_return(Reason, ?FAILURE_STATUS, QsInfo)
                    end;
                _ ->
                    format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

get_eqpt_info_by_id([Qs,QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    EqptLevel = binary_to_list(proplists:get_value(<<"eqptLevel">>, Qs, <<>>)),
    EqptType = binary_to_list(proplists:get_value(<<"eqptType">>, Qs, <<>>)),
    EqptIdCode = binary_to_list(proplists:get_value(<<"eqptIdCode">>, Qs, <<>>)),
    case db_util:is_exists_user(User,Password) of
        true ->
            case EqptLevel of
                "1" ->
                    case db_util:get_collector_info_by_type_and_id(EqptType, EqptIdCode, User) of
                        {ok, []} ->
                            format_return(?QUERY_RESULT, ?FAILURE_STATUS, QsInfo);
                        {ok, Rows} ->
                            List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptIdCode">>, EqptIdCodeBinay}, 
                                    {<<"eqptName">>, EqptNameBinary},{<<"createTime">>, CreateTimeBinary},
                                    {<<"creator">>, CreatorBinary}] || {EqptTypeBinary, EqptIdCodeBinay, 
                                    EqptNameBinary,CreateTimeBinary,CreatorBinary} <- field_format(Rows)],
                            data_return(List, QsInfo);
                        {error, Reason} ->
                            format_return(Reason, ?FAILURE_STATUS, QsInfo)
                    end;
                "2" ->
                    case connector_db_store_server:get_eqpt_level_and_protocol_type_by_eqpt_type(EqptType) of
                        {ok, ?REPEATERS_LEVEL, _} ->
                            case db_util:get_repeaters_info_by_type_and_id(EqptType, EqptIdCode, User) of
                                {ok, []} ->
                                    format_return(?QUERY_RESULT, ?FAILURE_STATUS, QsInfo);
                                {ok, Rows} ->
                                    List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptZjqCode">>,EqptZjqCodeBinay}, 
                                            {<<"eqptZjqName">>, EqptZjqNameBinary},{<<"createTime">>,CreateTimeBinary},
                                            {<<"creator">>, CreatorBinary}] || {EqptTypeBinary, EqptZjqCodeBinay, 
                                            EqptZjqNameBinary, CreateTimeBinary,CreatorBinary} <- field_format(Rows)],
                                    data_return(List, QsInfo);
                                {error, Reason} ->
                                    format_return(Reason, ?FAILURE_STATUS, QsInfo)
                            end;
                        _ ->
                            format_return(?ILLEGAL_EQPT_TYPE, ?FAILURE_STATUS, QsInfo)
                    end;
                "3" ->
                    case db_util:get_meter_info_by_type_and_id(EqptType, EqptIdCode, User) of
                        {ok, []} ->
                            format_return(?QUERY_RESULT, ?FAILURE_STATUS, QsInfo);
                        {ok, Rows} ->
                            List = [[{<<"eqptType">>, EqptTypeBinary}, {<<"eqptIdCode">>, EqptIdCodeBinay}, 
                                    {<<"eqptName">>, EqptNameBinary}, {<<"createTime">>, CreateTimeBinary}, 
                                    {<<"creator">>, CreatorBinary},{<<"eqptPort">>, EqptPort},
                                    {<<"eqptBaudrate">>, EqptBaudrate}, {<<"eqptRateNumber">>, EqptRateNumber},
                                    {<<"eqptZsw">>, EqptZsw}, 
                                    {<<"eqptXsw">>, EqptXsw}, {<<"eqptDlh">>, EqptDlh}, {<<"eqptXlh">>, EqptXlh},
                                    {<<"eqptYblx">>, EqptYblx}] || {EqptTypeBinary, EqptIdCodeBinay, EqptNameBinary, 
                                    CreateTimeBinary, CreatorBinary,EqptPort,EqptBaudrate,EqptRateNumber, 
                                    EqptZsw, EqptXsw, EqptDlh,EqptXlh, EqptYblx} <- field_format(Rows)],
                            data_return(List, QsInfo);
                        {error, Reason} ->
                            format_return(Reason, ?FAILURE_STATUS, QsInfo)
                    end
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

get_history_records([Qs,QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    PageSize = get_page_size(Qs),
    Page = get_page(Qs),
    case db_util:is_exists_user(User,Password) of
        true ->
            case mysql_handler:get_report_data_info(User, PageSize, Page) of
                {ok, []} ->
                      data_return([], QsInfo);
                {ok, Rows} ->
                      List = [[{<<"eqptType">>, EqptTypeBinary},{<<"meterid">>,MeterIdBinary},{<<"time">>, TimeBinary}, 
                                {<<"datatype">>, DataTypeBinary}, {<<"data">>, DataBinary},{<<"chargeno">>, ChargenBinary}] || 
                                {EqptTypeBinary, MeterIdBinary, TimeBinary, DataTypeBinary,DataBinary,ChargenBinary} <- field_format(Rows)],
                      data_return(List, QsInfo);
                {error, Reason} ->
                      format_return(Reason, ?FAILURE_STATUS, QsInfo)
            end;
        false ->
            format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.

get_reportdata_info_total_row([_Qs, QsInfo, UserName, UserPass]) ->
    User = binary_to_list(UserName),
    Password = binary_to_list(UserPass),
    case db_util:is_exists_user(User,Password) of
        true ->
           case mysql_handler:get_reportdata_info_total_row() of
               {ok, [{Count}]} ->
                   total_return(Count, QsInfo);
               {error, Reason} ->
                   format_return(Reason, ?FAILURE_STATUS, QsInfo)
           end;
        false ->
           format_return(?NO_PERMISSION, ?FAILURE_STATUS, QsInfo)
    end.
