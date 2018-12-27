-module(db_util).

-include("print.hrl").
-include("db.hrl").
-include("config.hrl").

-compile([export_all]).
-define(EMPTY_LIST, "rows_is_empty_list").

-define(BEGIN_TRANSACTION, "BEGIN TRANSACTION;").
-define(COMMIT_TRANSACTION, "COMMIT TRANSACTION;").
-define(ROLLBACK, "ROLLBACK;").

exec_select(SQL, Params) ->
    case exec_sql(SQL, Params) of
        [{columns, Columns}, {rows, Rows}] when (length(Rows) >= 1) ->
            ?PRINT("Columns:~p~n", [Columns]),
            ?PRINT("Rows:~p~n", [Rows]),
            {ok, Rows};
        [{columns, _}, {rows, []}] ->
            {ok, []};
        {error, _Code, Reason} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

exec_insert(SQL, Params) ->
    case exec_sql(SQL, Params) of
        {rowid, Rowid} ->
            {ok, Rowid};
        {error, _Code, Reason} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

exec_update(SQL, Params) ->
    case exec_sql(SQL, Params) of
        ok ->
            ok;
        {error, _Code, Reason} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

exec_delete(SQL, Params) ->
    case exec_sql(SQL, Params) of
        ok ->
            ok;
        {error, _Code, Reason} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

exec_sql(SQL, Params) ->
    ?PRINT("SQL:~p~n", [SQL]),
    ?PRINT("Params:~p~n", [Params]),
    sqlite3:sql_exec(?DBNAME, SQL, Params).

begin_transaction() ->
    exec_sql(?BEGIN_TRANSACTION, []).

commit_transaction() ->    
    exec_sql(?COMMIT_TRANSACTION, []).

rollback() ->
    exec_sql(?ROLLBACK, []).

%%%=============================================================================================================
%% Db server for Connector functions
%%%=============================================================================================================
load_eqpt_info() ->
    SQL = <<"SELECT eqptIdCode, eqptType, eqptCjqType, eqptCjqCode, eqptZjqType, eqptZjqCode, eqptProtocolType, ", 
            "eqptCjqProtocolType FROM network_config;">>,
    Params = [],
    {ok, Ref} = sqlite3:prepare(?DBNAME, SQL),
    {ok, Ref}.

load_eqpt_task() ->
    SQL = <<"SELECT taskId, eqptCjqType, eqptCjqIdCode, meterType, meterIdCode, taskName, taskCmdId, taskType, taskStart, taskTime, taskCustom FROM eqpt_task;">>,
    Params = [],
    {ok, Ref} = sqlite3:prepare(?DBNAME, SQL),
    {ok, Ref}.

load_eqpt_id() ->
    SQL = <<"SELECT eqptCjqType, eqptCjqCode, eqptType, eqptIdCode FROM network_config;">>,
    Params = [],
    {ok, Ref} = sqlite3:prepare(?DBNAME, SQL),
    {ok, Ref}.


read_row(Ref) ->
    %% if not next return done
    sqlite3:next(?DBNAME, Ref).

finalize(Ref) ->
    ok = sqlite3:finalize(?DBNAME, Ref).

select_eqpt_type_and_protocol_type() ->
    SQL = <<"SELECT eqptType, eqptLevel, protocolType FROM eqpt_type;">>,
    Params = [],
    exec_select(SQL, Params).

select_eqpt_topic_info() ->
    SQL = <<"SELECT eqptCjqCode, eqptIdCode, creator FROM network_config;">>,
    Params = [],
    exec_select(SQL, Params).

add_device_type_info(EqptLevel, EqptType, EqptTypeName, ProtocolName, ProtocolType) ->
    SQL = <<"INSERT OR REPLACE INTO eqpt_type (eqptLevel, eqptType, eqptTypeName, protocolName, protocolType) VALUES ",
            "(:eqptLevel, :eqptType, :eqptTypeName, :protocolName, :protocolType);">>,
    Params = [{":eqptLevel", EqptLevel}, {":eqptType", EqptType}, 
               {":eqptTypeName", EqptTypeName}, {":protocolName", ProtocolName},
               {":protocolType", ProtocolType}],              
    case exec_insert(SQL, Params) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

delete_device_type_info(EqptType) ->
    SQL = <<"DELETE FROM eqpt_type WHERE eqptType = :eqptType;">>,
    Params = [{":eqptType", EqptType}],
    case exec_delete(SQL, Params) of
        ok ->
            ok;
        {error, _Code, Reason} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

get_device_type_info() ->
    SQL = <<"SELECT eqptType, eqptTypeName, protocolName, protocolType FROM eqpt_type;">>,
    Params = [],
    exec_select(SQL, Params).

%%注册用户
add_user_info(UserName, UserPass) ->
    SQL = <<"INSERT OR REPLACE INTO system_UserInfo (UserName, Password, Rightlevel) VALUES (:UserName, :Password, :Rightlevel);">>,
    Params = [{":UserName", UserName}, {":Password", UserPass}, {":Rightlevel", 2}],
    case exec_insert(SQL, Params) of
         {ok, _} ->
              ok;
         {error, Reason} ->
              {error, Reason}
    end.

%%注销用户
cancel_user_info(UserName, UserPass) ->
    SQL = <<"DELETE FROM system_UserInfo WHERE UserName = :UserName AND Password = :Password AND Rightlevel = :Rightlevel;">>,
    Params = [{":UserName", UserName}, {":Password", UserPass}, {":Rightlevel", 2}],
    exec_delete(SQL, Params).
%% 查询用户信息
get_user_info(UserName, UserPass) ->
    SQL = <<"SELECT UserName, Password FROM system_UserInfo WHERE UserName = :UserName AND Password = :UserPass;">>,
    Params = [{":UserName", UserName}, {":UserPass", UserPass}],
    exec_select(SQL, Params).

%%获取任务信息
get_task_info() ->
    SQL = <<"SELECT taskId, eqptCjqType, eqptCjqIdCode, meterType, meterIdCode, taskName, taskCmdId, taskType, taskStart, taskTime, taskCustom FROM eqpt_task;">>,
    Params = [],
    exec_select(SQL, Params).

add_task_info(TaskId, EqptCjqType, EqptCjqIdCode, MeterType, MeterIdCode, TaskName, TaskCmdId, TaskType, TaskStart, TaskTime, TaskCustom) ->
    SQL = <<"INSERT OR REPLACE INTO eqpt_task (taskId, eqptCjqType, eqptCjqIdCode, meterType, meterIdCode, taskName, taskCmdId, taskType, taskStart, taskTime, taskCustom) VALUES ",
            "(:taskId, :eqptCjqType, :eqptCjqIdCode, :meterType, :meterIdCode, :taskName, :taskCmdId, :taskType, :taskStart, :taskTime, :taskCustom);">>,
    Params = [{":taskId", TaskId}, {":eqptCjqType", EqptCjqType}, 
               {":eqptCjqIdCode", EqptCjqIdCode}, {":meterType", MeterType},
               {":meterIdCode", MeterIdCode}, {":taskName", TaskName}, {":taskCmdId", TaskCmdId},
                {":taskType", TaskType}, {":taskStart", TaskStart}, {":taskTime", TaskTime}, {":taskCustom", TaskCustom}],
    case exec_insert(SQL, Params) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

delete_task_info(TaskId) ->
    SQL = <<"DELETE FROM eqpt_task WHERE taskId = :taskId;">>,
    Params = [{":taskId", TaskId}],
    case exec_delete(SQL, Params) of
        ok ->
            ok;
        {error, _Code, Reason} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

get_specific_type_info(EqptLevel) ->
    SQL = <<"SELECT eqptType, eqptTypeName, protocolName, protocolType FROM eqpt_type WHERE eqptLevel = :eqptLevel;">>,
    Params = [{":eqptLevel", EqptLevel}],
    exec_select(SQL, Params).
        

%% 获取测量点号
get_measurecode(MeterId) ->
    SQL = <<"SELECT eqptMeasureCode FROM network_config WHERE eqptIdCode = :eqptIdCode;">>,
    Params = [{":eqptIdCode", MeterId}],
    case exec_select(SQL, Params) of
        {ok, []} ->
            {error, "没有找到该表的测量点号"};
        {ok, [{MeasureCode}]} when is_integer(MeasureCode)->
            {ok, MeasureCode};
        {error, Reason} ->
            {error, Reason};
        Other ->
            {error, Other} 
    end.        

%% 获取计量设备的额外信息
get_addtion_info(MeterId) ->
    SQL = <<"SELECT eqptBaudrate, eqptPort, eqptRateNumber, eqptZsw, eqptXsw, eqptDlh, eqptXlh, ",
            "eqptYblx FROM eqpt_info WHERE eqptIdCode = :eqptIdCode LIMIT 1;">>,
    Params = [{":eqptIdCode", MeterId}],
    exec_select(SQL, Params).


%%%=============================================================================================================
%% DB rest handler functions
%%%=============================================================================================================

%%--------------------------------------------------------------------------------------------------------------
%% 查看某种设备/任务是否存在
%%--------------------------------------------------------------------------------------------------------------
is_admin_user(UserName, UserPass) ->
    SQL = <<"SELECT UserName, Password Rightlevel FROM system_UserInfo WHERE UserName = :UserName AND Password = :UserPass AND Rightlevel = :Rightlevel;">>,
    Params = [{":UserName", UserName}, {":UserPass", UserPass}, {":Rightlevel", 1}],
    case exec_select(SQL, Params) of
        {ok, []} ->
            false;
        {ok, [_]} ->
            true;
        {error, Reason} ->
            {error, Reason}
    end.

is_exists_username(UserName) ->
    SQL = <<"SELECT UserName FROM system_UserInfo WHERE UserName = :UserName;">>,
    Params = [{":UserName", UserName}],
    case exec_select(SQL, Params) of
         {ok, []} ->
                 false;
         {ok, [_]} ->
                 true;
         {error, Reason} ->
                 {error, Reason}
    end.

is_exists_user(UserName, UserPass) ->
    SQL = <<"SELECT UserName, Password FROM system_UserInfo WHERE UserName = :UserName AND Password = :UserPass;">>,
    Params = [{":UserName", UserName}, {":UserPass", UserPass}],
    case exec_select(SQL, Params) of
        {ok, []} ->
              false;
        {ok, [_]} ->
              true;
        {error, Reason} ->
              {error, Reason}
    end.

is_exists_task(TaskId) ->
    SQL = <<"SELECT taskId FROM eqpt_task WHERE taskId = :taskId;">>,
    Params = [{":taskId", TaskId}],
    case exec_select(SQL, Params) of
        {ok, []} ->
            false;
        {ok, [_]} ->
            true;
        {error, Reason} ->
            {error, Reason}
    end.

is_exists_collector(EqptType, EqptCjqCode) ->
    SQL = <<"SELECT count(seq) FROM eqpt_cjq_info WHERE eqptType = :eqptType AND eqptCjqCode = :eqptCjqCode;">>,
    Params = [{":eqptType", EqptType}, {":eqptCjqCode", EqptCjqCode}],
    case exec_select(SQL, Params) of
        {ok, [{0}]} ->
            false;
        {ok, [{Count}]} when Count > 0 ->
            true;
        {error, Reason} ->
            {error, Reason}
    end.

is_exists_repeaters(EqptType, EqptZjqCode) ->
    SQL = <<"SELECT count(seq) FROM eqpt_zjq_info WHERE eqptType = :eqptType AND eqptZjqCode = :eqptZjqCode;">>,
    Params = [{":eqptType", EqptType}, {":eqptZjqCode", EqptZjqCode}],
    case exec_select(SQL, Params) of
        {ok, [{0}]} ->
            false;
        {ok, [{Count}]} when Count > 0 ->
            true;
        {error, Reason} ->
            {error, Reason}
    end.

is_exists_meter(EqptType, EqptIdCode) ->
    SQL = <<"SELECT count(seq) FROM eqpt_info WHERE eqptType = :eqptType AND eqptIdCode = :eqptIdCode;">>,
    Params = [{":eqptType", EqptType}, {":eqptIdCode", EqptIdCode}],
    case exec_select(SQL, Params) of
        {ok, [{0}]} ->
            false;
        {ok, [{Count}]} when Count > 0 ->
            true;
        {error, Reason} ->
            {error, Reason}
    end.

is_exists_meter_of_collector(EqptCjqType, EqptCjqCode) ->
    SQL = <<"SELECT count(seq) FROM network_config WHERE eqptCjqType = :eqptCjqType AND eqptCjqCode = ",
            ":eqptCjqCode AND eqptIdCode <> eqptCjqCode">>,
    Params = [{":eqptCjqType", EqptCjqType}, {":eqptCjqCode", EqptCjqCode}],
    case exec_select(SQL, Params) of
        {ok, [{0}]} ->
            false;
        {ok, [{Count}]} when Count > 0 ->
            true;
        {error, Reason} ->
            {error, Reason}
    end.

is_exists_meter_of_repeaters(EqptZjqType, EqptZjqCode) ->
    SQL = <<"SELECT count(seq) FROM network_config WHERE eqptZjqType = :eqptZjqType AND eqptZjqCode = ",
            ":eqptZjqCode AND eqptIdCode <> eqptZjqCode">>,
    Params = [{":eqptZjqType", EqptZjqType}, {":eqptZjqCode", EqptZjqCode}],
    case exec_select(SQL, Params) of
        {ok, [{0}]} ->
            false;
        {ok, [{Count}]} when Count > 0 ->
            true;
        {error, Reason} ->
            {error, Reason}
    end.
       
%% 添加采集器设备
%% param:
%%   EqptType: 采集器类型
%%   EqptCjqCode: 采集器编号
%%   EqptCjqName: 采集器名称
%%   Creator: 创建者
%%   EqptCjqProtocolType: 采集器协议类型
add_collector(EqptType, EqptCjqCode, EqptCjqName, Creator, EqptCjqProtocolType) ->
    SQL1 = <<"INSERT OR REPLACE INTO eqpt_cjq_info (eqptType, eqptCjqCode, eqptCjqName, creator) VALUES ",
            "(:eqptType, :eqptCjqCode, :eqptCjqName, :creator);">>,
    Params1 = [{":eqptType", EqptType}, {":eqptCjqCode", EqptCjqCode}, 
               {":eqptCjqName", EqptCjqName}, {":creator", Creator}],              
    case exec_insert(SQL1, Params1) of
        {ok, RowId} ->
            SQL2 = <<"INSERT OR REPLACE INTO network_config (eqptType, eqptIdCode, eqptName, eqptSeq, eqptCjqType, eqptCjqCode, ",
                     "eqptMeasureCode, creator, eqptCjqProtocolType) VALUES (:eqptType, :eqptIdCode, :eqptName, :eqptSeq, ",
                     ":eqptCjqType, :eqptCjqCode, :eqptMeasureCode, :creator, :eqptCjqProtocolType);">>,
            %% 测量点号，初始化为3，当添加计量设备时，+1，刚好从4开始
            Params2 = [{":eqptType", EqptType}, {":eqptIdCode", EqptCjqCode}, {":eqptName", EqptCjqName},
                       {":eqptSeq", RowId}, {":eqptCjqType", EqptType}, {":eqptCjqCode", EqptCjqCode},
                       {":eqptMeasureCode", 3},{":creator", Creator},{":eqptCjqProtocolType", EqptCjqProtocolType}], 
            case exec_insert(SQL2, Params2) of
                {ok, _RowId} ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, What} ->
            {error, What}
    end.

%% 更新采集器名称等信息
update_collector(EqptType, EqptCjqCode, EqptCjqName, Creator) ->
    SQL = <<"UPDATE eqpt_cjq_info SET eqptCjqName = :eqptCjqName, creator = :creator WHERE eqptType = :eqptType ",
            "AND eqptCjqCode = :eqptCjqCode">>,
    Params = [{":eqptCjqName", EqptCjqName}, {":creator", Creator}, 
              {":eqptType", EqptType}, {":eqptCjqCode", EqptCjqCode}],
    exec_update(SQL, Params).

%% 更新采集器及其组网关系
update_collector_and_config(EqptTypeOld,EqptCjqCodeOld,EqptTypeNew,EqptCjqCodeNew,EqptCjqName,Creator,EqptProtocolType) ->
    SQL1 = <<"UPDATE eqpt_cjq_info SET eqptType = :eqptType, eqptCjqCode = :eqptCjqCode, eqptCjqName = :eqptCjqName, 
                        creator = :creator WHERE eqptType = :eqptTypeOld ","AND eqptCjqCode = :eqptCjqCodeOld">>,
    Params1 = [{":eqptType", EqptTypeNew},{":eqptCjqCode", EqptCjqCodeNew},{":eqptCjqName",EqptCjqName},{":creator", Creator},
               {":eqptTypeOld", EqptTypeOld},{":eqptCjqCodeOld", EqptCjqCodeOld}],
    SQL2 = <<"UPDATE network_config SET eqptType = :eqptType, eqptIdCode = :eqptIdCode, eqptCjqType = :eqptCjqType, 
                eqptCjqCode = :eqptCjqCode, eqptName = :eqptName,creator = :creator, eqptCjqProtocolType = :eqptCjqProtocolType 
                WHERE eqptType = :eqptTypeOld AND eqptCjqCode = :eqptCjqCodeOld">>,
    Params2 = [{":eqptType", EqptTypeNew},{":eqptIdCode", EqptCjqCodeNew},{":eqptCjqType", EqptTypeNew},{":eqptCjqCode", EqptCjqCodeNew},
               {":eqptName",EqptCjqName},{":creator", Creator},{":eqptCjqProtocolType",EqptProtocolType},{":eqptTypeOld", EqptTypeOld},
               {":eqptCjqCodeOld", EqptCjqCodeOld}],
    SQL3 = <<"UPDATE network_config SET eqptCjqType = :eqptCjqType, eqptCjqCode = :eqptCjqCode, eqptName = :eqptName, 
                creator = :creator, eqptCjqProtocolType = :eqptCjqProtocolType WHERE eqptCjqType = :eqptTypeOld AND 
                eqptCjqCode = :eqptCjqCodeOld">>,
    Params3 = [{":eqptCjqType", EqptTypeNew},{":eqptCjqCode", EqptCjqCodeNew},{":eqptName",EqptCjqName},{":creator", Creator},
               {":eqptCjqProtocolType",EqptProtocolType},{":eqptTypeOld", EqptTypeOld},{":eqptCjqCodeOld", EqptCjqCodeOld}], 
    begin_transaction(),
    case exec_update_list_sql([{SQL1, Params1}, {SQL2, Params2},{SQL3, Params3}]) of
        ok ->
             commit_transaction(),
             ok;
        {error, Reason} ->
             rollback(),
             {error, Reason}
    end.

%% 更新中继器及其组网关系
update_repeater_and_config(EqptZjqTypeOld,EqptZjqCodeOld,EqptZjqTypeNew,EqptZjqCodeNew,EqptZjqName,Creator) ->
    SQL1 = <<"UPDATE eqpt_zjq_info SET eqptType = :eqptType, eqptZjqCode = :eqptZjqCode, eqptZjqName = :eqptZjqName, creator = :creator WHERE eqptType = :eqptZjqTypeOld ","AND eqptZjqCode = :eqptZjqCodeOld">>,
    Params1 = [{":eqptType", EqptZjqTypeNew},{":eqptZjqCode", EqptZjqCodeNew},{":eqptZjqName",EqptZjqName},{":creator", Creator},
               {":eqptZjqTypeOld", EqptZjqTypeOld},{":eqptZjqCodeOld", EqptZjqCodeOld}],
    SQL2 = <<"UPDATE network_config SET eqptZjqType = :eqptZjqType, eqptZjqCode = :eqptZjqCode WHERE eqptZjqType = :eqptZjqTypeOld AND eqptZjqCode = :eqptZjqCodeOld">>,
    Params2 = [{":eqptZjqType", EqptZjqTypeNew},{":eqptZjqCode", EqptZjqCodeNew},{":eqptZjqTypeOld", EqptZjqTypeOld},{":eqptZjqCodeOld", EqptZjqCodeOld}],
    begin_transaction(),
    case exec_update_list_sql([{SQL1, Params1}, {SQL2, Params2}]) of
        ok ->
             commit_transaction(),
             ok;
        {error, Reason} ->
             rollback(),
             {error, Reason}
    end.

%% 删除采集器
delete_collector(EqptType, EqptCjqCode, Creator) ->
    SQL1 = <<"DELETE FROM eqpt_cjq_info WHERE eqptType = :eqptType AND eqptCjqCode = :eqptCjqCode AND creator = :creator;">>,
    Params1 = [{":eqptType", EqptType}, {":eqptCjqCode", EqptCjqCode}, {":creator", Creator}],    
    SQL2 = <<"DELETE FROM network_config WHERE eqptType = :eqptType AND eqptIdCode = :eqptIdCode AND creator = :creator;">>,
    Params2 = [{":eqptType", EqptType}, {":eqptIdCode", EqptCjqCode}, {":creator", Creator}],
    begin_transaction(),
    case exec_delete_list_sql([{SQL1, Params1}, {SQL2, Params2}]) of
        ok ->
            commit_transaction(),
            ok;
        {error, Reason} ->
            rollback(),
            {error, Reason}
    end.

force_delete_collector(EqptType, EqptCjqCode, Creator) ->
    SQL1 = <<"DELETE FROM eqpt_cjq_info WHERE eqptType = :eqptType AND eqptCjqCode = :eqptCjqCode AND creator = :creator;">>,
    Params1 = [{":eqptType", EqptType}, {":eqptCjqCode", EqptCjqCode}, {":creator", Creator}],    
    SQL2 = <<"DELETE FROM eqpt_info WHERE Seq IN (SELECT eqptSeq FROM network_config WHERE ",
             "eqptCjqType = :eqptCjqType AND eqptCjqCode = :eqptCjqCode AND creator = :creator AND eqptIdCode <> eqptCjqCode);">>,
    Params2 = [{":eqptCjqType", EqptType}, {":eqptCjqCode", EqptCjqCode}, {":creator", Creator}],
    SQL3 = <<"DELETE FROM network_config WHERE eqptCjqType = :eqptCjqType AND eqptCjqCode = :eqptCjqCode AND creator = :creator;">>,
    Params3 = [{":eqptCjqType", EqptType}, {":eqptCjqCode", EqptCjqCode}, {":creator", Creator}],
    begin_transaction(),
    case exec_delete_list_sql([{SQL1, Params1}, {SQL2, Params2}, {SQL3, Params3}]) of
        ok ->
            commit_transaction(),
            ok;
        {error, Reason} ->
            rollback(),
            {error, Reason}
    end.
    
exec_delete_list_sql([{SQL, Params} | T]) ->
    case exec_delete(SQL, Params) of
        ok ->
            exec_delete_list_sql(T);
        {error, Reason} ->
            {error, Reason}
    end;
exec_delete_list_sql([]) ->
    ok.

exec_update_list_sql([{SQL, Params} | T]) ->
    case exec_update(SQL, Params) of
        ok ->
            exec_update_list_sql(T);
        {error, Reason} ->
            {error, Reason}
    end;
exec_update_list_sql([]) ->
    ok.
%% 添加中继器
add_repeaters(EqptType, EqptZjqCode, EqptZjqName, Creator) ->
    SQL = <<"INSERT OR REPLACE INTO eqpt_zjq_info (eqptType, eqptZjqCode, eqptZjqName, creator) VALUES ",
            "(:eqptType, :eqptZjqCode, :eqptZjqName, :creator);">>,
    Params = [{":eqptType", EqptType}, {":eqptZjqCode", EqptZjqCode}, 
               {":eqptZjqName", EqptZjqName}, {":creator", Creator}],
    case exec_insert(SQL, Params) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% 修改中继器
update_repeaters(EqptType, EqptZjqCode, EqptZjqName, Creator) ->
    SQL = <<"UPDATE eqpt_zjq_info SET eqptZjqName = :eqptZjqName, creator = :creator WHERE eqptType = :eqptType ",
            "AND eqptZjqCode = :eqptZjqCode">>,
    Params = [{":eqptZjqName", EqptZjqName}, {":creator", Creator}, 
              {":eqptType", EqptType}, {":eqptZjqCode", EqptZjqCode}],
    exec_update(SQL, Params).

%% 删除中继器
delete_repeaters(EqptType, EqptZjqCode, Creator) ->
    SQL = <<"DELETE FROM eqpt_zjq_info WHERE eqptType = :eqptType AND eqptZjqCode = :eqptZjqCode AND creator = :creator;">>,
    Params = [{":eqptType", EqptType}, {":eqptZjqCode", EqptZjqCode}, {":creator", Creator}],    
    exec_delete(SQL, Params).

%% 强制删除中继器
force_delete_repeaters(EqptType, EqptZjqCode, Creator) ->
    exec_sql(?BEGIN_TRANSACTION, []),
    SQL1 = <<"DELETE FROM eqpt_zjq_info WHERE eqptType = :eqptType AND eqptZjqCode = :eqptZjqCode AND creator = :creator;">>,
    Params1 = [{":eqptType", EqptType}, {":eqptZjqCode", EqptZjqCode}, {":creator", Creator}],    
    SQL2 = <<"DELETE FROM eqpt_info WHERE seq IN (SELECT eqptSeq FROM network_config WHERE ",
             "eqptZjqType = :eqptZjqType AND eqptZjqCode = :eqptZjqCode AND creator = :creator);">>,
    Params2 = [{":eqptZjqType", EqptType}, {":eqptZjqCode", EqptZjqCode}, {":creator", Creator}],
    SQL3 = <<"DELETE FROM network_config WHERE eqptZjqType = :eqptZjqType AND eqptZjqCode = :eqptZjqCode AND creator = :creator;">>,
    Params3 = [{":eqptZjqType", EqptType}, {":eqptZjqCode", EqptZjqCode}, {":creator", Creator}],

    begin_transaction(),
    case exec_delete_list_sql([{SQL1, Params1}, {SQL2, Params2}, {SQL3, Params3}]) of
        ok ->
            commit_transaction(),
            ok;
        {error, Reason} ->
            rollback(),
            {error, Reason}
    end.

%% 添加计量设备
add_meter(EqptType, EqptIdCode, EqptName, EqptCjqType, EqptCjqCode, EqptZjqType, EqptZjqCode,EqptMeasureCode, 
          Creator, EqptPort, EqptBaudrate, EqptRateNumber, EqptZsw, EqptXsw, EqptDlh, EqptXlh,
          EqptYblx, EqptProtocolType, EqptCjqProtocolType) ->
    
    SQL1 = <<"INSERT OR REPLACE INTO eqpt_info (eqptType, eqptIdCode, eqptName, creator, eqptPort, eqptBaudrate, eqptRateNumber, ",
             "eqptZsw, eqptXsw, eqptDlh, eqptXlh, eqptYblx) VALUES (:eqptType, :eqptIdCode, :eqptName, :creator, :eqptPort, ",
             ":eqptBaudrate, :eqptRateNumber, :eqptZsw, :eqptXsw, :eqptDlh, :eqptXlh, :eqptYblx);">>,
    Params1 = [{":eqptType", EqptType}, {":eqptIdCode", EqptIdCode}, {":eqptName", EqptName}, {":creator", Creator},
               {":eqptPort", EqptPort}, {":eqptBaudrate", EqptBaudrate}, {":eqptRateNumber", EqptRateNumber},
               {":eqptZsw", EqptZsw}, {":eqptXsw", EqptXsw}, {":eqptDlh", EqptDlh}, {":eqptXlh", EqptXlh},
               {":eqptYblx", EqptYblx}],
    case exec_insert(SQL1, Params1) of
        {ok, RowId} ->
            SQL2 = <<"INSERT OR REPLACE INTO network_config (eqptType, eqptIdCode, eqptName, eqptSeq, eqptCjqType, eqptCjqCode, ",
                     "eqptZjqType, eqptZjqCode, eqptMeasureCode, creator, eqptProtocolType, eqptCjqProtocolType) VALUES ",
                     "(:eqptType, :eqptIdCode, :eqptName, :eqptSeq, :eqptCjqType, :eqptCjqCode, :eqptZjqType, ",
                     ":eqptZjqCode, :eqptMeasureCode, :creator, :eqptProtocolType, :eqptCjqProtocolType);">>,
            Params2 = [{":eqptType", EqptType}, {":eqptIdCode", EqptIdCode}, {":eqptName", EqptName}, {":eqptSeq", RowId}, 
                       {":eqptCjqType", EqptCjqType}, {":eqptCjqCode", EqptCjqCode},{":eqptZjqType", EqptZjqType}, 
                       {":eqptZjqCode", EqptZjqCode},{":eqptMeasureCode", EqptMeasureCode},{":creator",Creator},
                       {":eqptProtocolType", EqptProtocolType}, {":eqptCjqProtocolType", EqptCjqProtocolType}],
            case  exec_insert(SQL2, Params2) of
                {ok, _RowId} ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, What} ->
            {error, What}
    end.    

%% 更新计量设备属性信息
update_meter(EqptType, EqptIdCode, EqptName, Creator, EqptMeasureCode, EqptPort, EqptBaudrate, EqptRateNumber, EqptZsw, EqptXsw, EqptDlh, EqptXlh, EqptYblx) ->
    SQL1 = <<"UPDATE eqpt_info SET eqptName = :eqptName, creator = :creator, eqptPort = :eqptPort, eqptBaudrate = ",":eqptBaudrate, eqptRateNumber = :eqptRateNumber, eqptZsw = :eqptZsw, eqptXsw = :eqptXsw, eqptDlh = ",":eqptDlh, eqptXlh = :eqptXlh, eqptYblx = :eqptYblx WHERE eqptType = :eqptType AND eqptIdCode = ",":eqptIdCode;">>,
    Params1 = [{":eqptName", EqptName}, {":creator", Creator}, {":eqptPort", EqptPort}, 
               {":eqptBaudrate", EqptBaudrate},
               {":eqptRateNumber", EqptRateNumber}, {":eqptZsw", EqptZsw}, {":eqptXsw", EqptXsw},
               {":eqptDlh", EqptDlh}, {":eqptXlh", EqptXlh}, {":eqptYblx", EqptYblx}, {":eqptType", EqptType},
               {":eqptIdCode", EqptIdCode}],
    SQL2 = <<"UPDATE network_config SET eqptName = :eqptName, eqptMeasureCode = :eqptMeasureCode, creator = :creator WHERE eqptType = ", 
                ":eqptType AND eqptIdCode = :eqptIdCode;">>,
    Params2 = [{":eqptName", EqptName}, {":eqptMeasureCode", EqptMeasureCode},
               {":creator", Creator},{":eqptType", EqptType},
               {":eqptIdCode", EqptIdCode}],
    ?PRINT("Params2:~p~n",[Params2]),
    begin_transaction(),
    case exec_update_list_sql([{SQL1, Params1},{SQL2, Params2}]) of
        ok ->
           commit_transaction(),
           ok;
        {error, Reason} ->
           rollback(),
           {error, Reason}
    end.

delete_meter(EqptType, EqptIdCode, Creator) ->
    exec_sql(?BEGIN_TRANSACTION, []),
    SQL1 = <<"DELETE FROM eqpt_info WHERE eqptType = :eqptType AND eqptIdCode = :eqptIdCode AND creator = :creator;">>,
    Params1 = [{":eqptType", EqptType}, {":eqptIdCode", EqptIdCode}, {":creator", Creator}],
    SQL2 = <<"DELETE FROM network_config WHERE eqptType = :eqptType AND eqptIdCode = :eqptIdCode AND creator = :creator;">>,
    Params2 = [{":eqptType", EqptType}, {":eqptIdCode", EqptIdCode},{":creator", Creator}],
    begin_transaction(),
    case exec_delete_list_sql([{SQL1, Params1}, {SQL2, Params2}]) of
        ok ->
            commit_transaction(),
            ok;
        {error, Reason} ->
            rollback(),
            {error, Reason}
    end.

delete_meter_by_collector(EqptCjqType, EqptCjqCode, Creator) ->
    SQL1 = <<"DELETE FROM eqpt_info WHERE Seq IN (SELECT eqptSeq FROM network_config WHERE ",
             "eqptCjqType = :eqptCjqType AND eqptCjqCode = :eqptCjqCode AND creator = :creator AND eqptIdCode <> eqptCjqCode);">>,
    Params1 = [{":eqptCjqType", EqptCjqType}, {":eqptCjqCode", EqptCjqCode},{":creator", Creator}],
    SQL2 = <<"DELETE FROM network_config WHERE eqptCjqType = :eqptCjqType AND eqptCjqCode = :eqptCjqCode AND creator = :creator",
             "AND eqptIdCode <> eqptCjqCode;">>,
    Params2 = [{":eqptCjqType", EqptCjqType}, {":eqptCjqCode", EqptCjqCode},{":creator", Creator}],
    begin_transaction(),
    case exec_delete_list_sql([{SQL1, Params1}, {SQL2, Params2}]) of
        ok ->
            commit_transaction(),
            ok;
        {error, Reason} ->
            rollback(),
            {error, Reason}
    end.

get_collector_infolist() ->
    SQL = <<"SELECT eqptType, eqptCjqCode, creator FROM eqpt_cjq_info;">>,
    Params = [],
    exec_select(SQL, Params).

get_collector_info_total_row(Creator) ->
    SQL = <<"SELECT count(seq) FROM eqpt_cjq_info WHERE creator = :creator;">>,
    Params = [{":creator", Creator}],
    exec_select(SQL, Params).

get_collector_info(undefined, Creator, PageSize, Page) ->
    SQL = <<"SELECT a.eqptType, eqptCjqCode, eqptCjqName, eqptTypeName, createTime, creator FROM eqpt_cjq_info a LEFT JOIN ",
            "eqpt_type b ON a.eqptType = b.eqptType WHERE a.creator = :creator LIMIT :limit OFFSET :offset;">>,
    Params = [{":limit", PageSize}, {":offset", (Page-1)*PageSize},{":creator", Creator}],
    exec_select(SQL, Params);
get_collector_info(EqptTypes, Creator, PageSize, Page) ->
    SQL = binary:list_to_bin(["SELECT a.eqptType, eqptCjqCode, eqptCjqName, eqptTypeName, createTime, creator FROM eqpt_cjq_info ",
                              "a LEFT JOIN eqpt_type b ON a.eqptType = b.eqptType WHERE a.eqptType IN (", EqptTypes, 
                              ") WHERE a.creator = :creator LIMIT :limit OFFSET :offset;"]),
    Params = [{":limit", PageSize}, {":offset", (Page-1)*PageSize}, {":creator", Creator}],
    exec_select(SQL, Params).


get_repeaters_info_total_row(Creator) ->
    SQL = <<"SELECT count(seq) FROM eqpt_zjq_info WHERE creator = :creator;">>,
    Params = [{":creator", Creator}],
    exec_select(SQL, Params).

get_repeaters_info(undefined, Creator, PageSize, Page) ->
    SQL = <<"SELECT a.eqptType, eqptZjqCode, eqptZjqName, eqptTypeName, createTime, creator FROM eqpt_zjq_info a LEFT JOIN ",
            "eqpt_type b ON a.eqptType = b.eqptType WHERE a.creator = :creator LIMIT :limit OFFSET :offset;">>,
    Params = [{":creator", Creator}, {":limit", PageSize}, {":offset", (Page-1)*PageSize}],
    exec_select(SQL, Params);
get_repeaters_info(EqptTypes, Creator, PageSize, Page) ->
    SQL = binary:list_to_bin(["SELECT a.eqptType, eqptZjqCode, eqptZjqName, eqptTypeName, createTime, creator FROM eqpt_zjq_info ",
                              "a LEFT JOIN eqpt_type b ON a.eqptType = b.eqptType WHERE a.eqptType IN (", EqptTypes,
                              ") AND a.creator = :creator LIMIT :limit OFFSET :offset;"]),
    Params = [{":creator", Creator},{":limit", PageSize}, {":offset", (Page-1)*PageSize}],
    exec_select(SQL, Params).


get_meter_info_total_row(Creator) ->
    SQL = <<"SELECT count(seq) FROM eqpt_info WHERE creator = :creator;">>,
    Params = [{":creator", Creator}],
    exec_select(SQL, Params).
    
get_meter_info(undefined, Creator, PageSize, Page) ->
    SQL = <<"SELECT a.eqptType, eqptIdCode, eqptName, eqptTypeName, createTime, creator, eqptPort, eqptBaudrate, eqptRateNumber, eqptZsw, eqptXsw, eqptDlh, eqptXlh, eqptYblx FROM eqpt_info a LEFT JOIN eqpt_type b ON a.eqptType = b.eqptType WHERE a.creator = :creator LIMIT :limit OFFSET :offset;">>,
    Params = [{":creator", Creator},{":limit", PageSize},{":offset", (Page-1)*PageSize}],
    exec_select(SQL, Params);
get_meter_info(EqptTypes, Creator,  PageSize, Page) ->
    SQL = binary:list_to_bin(["SELECT a.eqptType, eqptIdCode, eqptName, eqptTypeName, createTime, creator, eqptPort, eqptBaudrate, ",
                              "eqptRateNumber, eqptZsw, eqptXsw, eqptDlh, eqptXlh, eqptYblx FROM eqpt_info a LEFT JOIN ",
                              "eqpt_type b ON a.eqptType = b.eqptType WHERE a.eqptType IN (", EqptTypes, 
                              ") AND a.creator = :creator LIMIT :limit OFFSET :offset;"]),
    Params = [{":creator", Creator}, {":limit", PageSize}, {":offset", (Page-1)*PageSize}],
    exec_select(SQL, Params).

get_meter_network_by_collector(EqptCjqType, EqptCjqCode, EqptTypes, Creator, PageSize, Page) when is_list(EqptTypes) ->
    SQL = binary:list_to_bin(["SELECT a.eqptType, eqptIdCode, eqptName, eqptTypeName FROM network_config a LEFT JOIN ",
                              "eqpt_type b ON a.eqptType = b.eqptType WHERE eqptCjqType = :eqptCjqType AND ", 
                              "eqptCjqCode = :eqptCjqCode AND eqptIdCode <> eqptCjqCode AND a.eqptType IN (", 
                              EqptTypes, ") AND creator = :creator LIMIT :limit OFFSET :offset;"]),
    Params = [{":eqptCjqType", EqptCjqType}, {":eqptCjqCode", EqptCjqCode}, {":creator", Creator},
              {":limit", PageSize}, {":offset", (Page-1)*PageSize}],
    exec_select(SQL, Params);
get_meter_network_by_collector(EqptCjqType, EqptCjqCode, _MeterTypes, Creator, PageSize, Page) ->
    SQL = <<"SELECT a.eqptType, eqptIdCode, eqptName, eqptTypeName FROM network_config a LEFT JOIN ",
            "eqpt_type b ON a.eqptType = b.eqptType ", 
            "WHERE eqptCjqType = :eqptCjqType AND eqptCjqCode = :eqptCjqCode AND eqptIdCode <> eqptCjqCode AND creator = :creator",
            "LIMIT :limit OFFSET :offset;">>,
    Params = [{":eqptCjqType", EqptCjqType}, {":eqptCjqCode", EqptCjqCode}, {":creator", Creator},
              {":limit", PageSize}, {":offset", (Page-1)*PageSize}],
    exec_select(SQL, Params).

get_collector_info_by_eqpt_type(EqptType, Creator, PageSize, Page) ->
    SQL = <<"SELECT eqptCjqCode, eqptCjqName FROM eqpt_cjq_info WHERE eqptType = :eqptType AND creator = :creator LIMIT :limit OFFSET :offset;">>,
    Params = [{":eqptType", EqptType}, {":limit", PageSize}, {":offset", (Page-1)*PageSize},{":creator", Creator}],
    exec_select(SQL, Params).
    
get_repeaters_info_by_eqpt_type(EqptType, Creator, PageSize, Page) ->
    SQL = <<"SELECT eqptZjqCode, eqptZjqName FROM eqpt_zjq_info WHERE eqptType = :eqptType AND creator = :creator LIMIT :limit OFFSET :offset;">>,
    Params = [{":eqptType", EqptType}, {":limit", PageSize}, {":offset", (Page-1)*PageSize},{":creator", Creator}],
    exec_select(SQL, Params).
    
batch_get_meter(EqptCjqType, EqptCjqCode, EqptTypes, Creator, PageSize, Page) ->
    Condition1 = 
        case EqptCjqType of
            undefined ->
                "1 = 1";
            _ ->
                "eqptCjqType = \"" ++ EqptCjqType ++ "\""
        end,
    Condition2 = 
        case EqptCjqCode of
            undefined ->
                "1 = 1";
            _ ->
                "eqptCjqCode = \"" ++ EqptCjqCode ++ "\""
        end,
    Condition3 = 
        case EqptTypes of
            undefined ->
                "1 = 1";
            _ ->
                "a.eqptType IN (" ++ EqptTypes ++ ")"
        end,
    SQL = binary:list_to_bin(["SELECT a.eqptType, a.eqptIdCode, c.eqptName, eqptCjqType, eqptCjqCode, eqptZjqType, eqptZjqCode, ", 
                              "a.createTime, a.eqptMeasureCode, a.creator, c.eqptPort, c.eqptBaudrate, c.eqptRateNumber, c.eqptZsw, c.eqptXsw, c.eqptDlh, c.eqptXlh, ","c.eqptYblx FROM network_config a LEFT JOIN eqpt_type b ON a.eqptType = b.eqptType ", "LEFT JOIN eqpt_info c ON a.eqptSeq = c.seq  WHERE a.creator = :creator AND a.eqptIdCode <> a.eqptCjqCode AND ", 
                              Condition1, " AND ", Condition2, " AND ", Condition3, 
                              " LIMIT :limit OFFSET :offset;"]),
    Params = [{":creator", Creator}, {":limit", PageSize}, {":offset", (Page-1)*PageSize}],
    exec_select(SQL, Params).
    
get_collector_info_by_type_and_id(EqptType, EqptIdCode, Creator) ->
    SQL = <<"SELECT eqptType, eqptCjqCode, eqptCjqName, createTime, creator FROM eqpt_cjq_info WHERE eqptType = :eqptType AND eqptCjqCode = :eqptCjqCode AND creator = :creator;">>,
    Params = [{":eqptType", EqptType}, {":eqptCjqCode", EqptIdCode},{":creator", Creator}],
    exec_select(SQL, Params).

get_repeaters_info_by_type_and_id(EqptType, EqptIdCode, Creator) ->
    SQL = <<"SELECT eqptType, eqptZjqCode, eqptZjqName, createTime, creator FROM eqpt_zjq_info WHERE eqptType = :eqptType AND eqptZjqCode = :eqptZjqCode AND creator =:creator;">>,
    Params = [{":eqptType", EqptType}, {":eqptZjqCode", EqptIdCode},{":creator", Creator}],
    exec_select(SQL, Params).

get_meter_info_by_type_and_id(EqptType, EqptIdCode, Creator) ->
    SQL = <<"SELECT eqptType, eqptIdCode, eqptName, createTime, creator, eqptPort, eqptBaudrate, eqptRateNumber, eqptZsw, eqptXsw, eqptDlh, eqptXlh, eqptYblx FROM eqpt_info WHERE eqptType = :eqptType AND eqptIdCode = :eqptIdCode AND creator =:creator;">>,
    Params = [{":eqptType", EqptType}, {":eqptIdCode", EqptIdCode},{":creator", Creator}],
    exec_select(SQL, Params).
