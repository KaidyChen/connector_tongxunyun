-module(mysql_handler).

-include("print.hrl").
-include("config.hrl").

-compile([export_all]).
-define(EMPTY_LIST, "rows_is_empty_list").

-define(BEGIN_TRANSACTION, "BEGIN TRANSACTION;").
-define(COMMIT_TRANSACTION, "COMMIT TRANSACTION;").
-define(ROLLBACK, "ROLLBACK;").

exec_select(SqlFd, Params) ->
    Result = emysql:execute(hello_pool, SqlFd, Params),
    case emysql:result_type(Result) of
        ok ->
            ok;
        error ->
            ?ERROR("emysql query fail!!!~n",[]);
        result ->
            emysql:as_json(Result)
    end.

exec_insert(SqlFd, Params) ->
    ?PRINT("Params:~p~n",[Params]),
    Result = emysql:execute(hello_pool, SqlFd, Params),
    case emysql:result_type(Result) of
        ok ->
            ok;
        error ->
            ?ERROR("emysql insert fail!!!~n",[]);
        result ->
            emysql:as_json(Result)
    end.   

exec_delete(SqlFd, Params) ->
    Result = emysql:execute(hello_pool, SqlFd, Params),
    case emysql:result_type(Result) of
        ok ->
            ok;
        error ->
            ?ERROR("emysql delete fail!!!~n",[]);
        result ->
            emysql:as_json(Result)
    end.

%% 不带条件执行SQL语句
exec_query(SQL) ->
    ?PRINT("SQL:~p~n", [SQL]),
    Result = emysql:execute(hello_pool, SQL),
    case emysql:result_type(Result) of
        ok ->
            ok;
        error ->
            ?ERROR("emysql query fail!!!~n",[]);
        result ->
            emysql:as_json(Result)
    end.

%%%=============================================================================================================
%% Db server for Connector functions
%%%=============================================================================================================
%% 按条件查询上报数据
select_report_data_info(ParamsType, Values) ->
    %% ParamsType:["eqpttype","meterid","time","datatype","data","chargeno"] Values:["0a0003ahup", "000663007098", "2018-06-13 15:58:17", "0001", "{"dataType":"00","status":"1"}", "0012asc21"] 
    SqlPart = string:join(ParamsType, "= ? AND ") ++ "= ?",
    SQL = list_to_binary("SELECT eqpttype, meterid, time, datatype, data, chargeno FROM report_data WHERE " ++ SqlPart),
    emysql:prepare(my_stmt, SQL),
    exec_select(my_stmt, Values).
%%获取全部信息
get_report_data_info() ->
    SQL = <<"SELECT eqpttype, meterid, time, datatype, data, chargeno FROM report_data;">>,
    exec_query(SQL).

%%主动上报数据插入数据库
add_report_data_info(DataList) ->
    SQL = <<"INSERT INTO report_data (eqpttype, meterid, time, datatype, data, chargeno) VALUES (?, ?, ?, ?, ?, ?)">>,
    emysql:prepare(my_ins, SQL),
    exec_insert(my_ins, DataList).

%%成为EMQTT上报数据插入数据库
add_emqtt_data_info(DataList) ->
    SQL = <<"INSERT INTO emqtt_data (eqptid, time, data) VALUES (?, ?, ?)">>,
    emysql:prepare(my_ins, SQL),
    exec_insert(my_ins, DataList).

delete_report_data_info(ParamsType, Values) ->
    SqlPart = string:join(ParamsType, "= ? AND ") ++ "= ?",
    SQL = list_to_binary("DELETE FROM report_data WHERE " ++ SqlPart),
    emysql:prepare(my_del, SQL),
    exec_select(my_del, Values).


get_sql(List) ->
     get_sql(List, "SELECT ").

get_sql([], Str) -> Str;
get_sql([H|T], Str) -> get_sql(T, (Str ++ H ++ ",")).
    
%% test sql
test_sql1() ->
    emysql:execute(hello_pool,<<"INSERT INTO hello_table SET hello_text = 'Hello World!'">>),
    {_,_,_,Result,_} = emysql:execute(hello_pool,<<"select hello_text from hello_table">>),
    io:format("~n~p~n",[Result]),
    ok.
    
%% test unit
%% --------------------------------------------------------------------------------------------------------------------
test_sql() ->
    SQL = <<"INSERT INTO report_data (eqpttype, meterid, time, datatype, data, chargeno) VALUES (?, ?, ?, ?, ?, ?)">>,
    emysql:prepare(my_ins, SQL),
    exec_insert(my_ins, ["abc","001","12:00","0101", "happy","1-2-3-4-56789"]),
    { _, _, _, Result, _ } = emysql:execute(hello_pool,<<"select * from report_data">>),
    io:format("~n~p~n", [Result]),
    ok.

