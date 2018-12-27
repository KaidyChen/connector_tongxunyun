-module(help).
-compile(export_all).

%%=========================================================================================
%% 时间日期相关的API
%%=========================================================================================

%% 计算日期比如由今天的日期Date，计算昨天的日期：addDay(Date, -1), 明天的日期addDay(Date, 1)
addDay(Date, Count) ->
    Days = calendar:date_to_gregorian_days(Date),
    NewDays = Days + Count,
    calendar:gregorian_days_to_date(NewDays).

addHour(Datetime, Hour_num) ->
    addSecond(Datetime, Hour_num*60*60).

addSecond(Datetime, Second_num) ->
    Old_second = calendar:datetime_to_gregorian_seconds(Datetime),
    calendar:gregorian_seconds_to_datetime(Old_second+Second_num).

getDateTimeStr(_Datetime = {Date, Time}) ->
    getDateTimeStr(Date, Time).

getDateTimeStr(Date, Time) ->
    getDateTimeStr(Date, Time, " ").

%% 日期 时间 连接符 {2016, 5, 12} {9, 8, 30} " " => "2016-05-12 09:08:30"
getDateTimeStr(Date, Time, JoinChar) ->
    string:join([dateToStr(Date, "-"), timeToStr(Time, ":")], JoinChar).

%% 日期 时间 简约描述 {2016, 5, 12} {9, 8, 30} " " => "20160512090830"
getSimpleDateTimeStr(Date, Time, JoinChar) ->
    string:join([dateToStr(Date, ""), timeToStr(Time, "")], JoinChar).

%% 日期转成字符串格式 参数： 2016, 5, 12 "-" => "2016-05-12"
%% 日期转成字符串格式 {2016, 5, 12} "-" => "2016-05-12"
dateToStr(Date) ->
    dateToStr(Date, "-").

dateToStr(Year, Month, Day, JoinChar) ->
    dateToStr({Year, Month, Day}, JoinChar).
    
dateToStr({Year, Month, Day}, JoinChar) ->
    Month_Str = intToStr(Month, 10),
    Day_Str = intToStr(Day, 10),
    Year_Str = integer_to_list(Year),
    string:join([Year_Str, Month_Str, Day_Str], JoinChar).



%% 时间转成字符串格式 参数： 9, 5, 12 "：" => "09:05:12"
timeToStr(Time) ->
    timeToStr(Time, ":").

timeToStr(Hour, Minute, Second, JoinChar) ->
    timeToStr({Hour, Minute, Second}, JoinChar).

timeToStr({Hour, Minute, Second}, JoinChar) ->
    Hour_Str = intToStr(Hour, 10),
    Minute_Str = intToStr(Minute, 10),
    Second_Str = intToStr(Second, 10),
    string:join([Hour_Str, Minute_Str, Second_Str], JoinChar).

%% "YYYY-MM-DD" -> {Year, Month, Day}
strToDate(Date_Str) ->
    strToDate(Date_Str, "-").

strToDate(Date_Str, Separator) ->
    [Year, Month, Day] = [list_to_integer(X) || X <- string:tokens(Date_Str, Separator)],
    {Year, Month, Day}.

%% "hh:mm:ss" -> {Hour, Minute, Second}
strToTime(Time_Str) ->
    strToTime(Time_Str, ":").

strToTime(Time_Str, Separator) ->
    [Hour, Minute, Second] = [list_to_integer(X) || X <- string:tokens(Time_Str, Separator)],
    {Hour, Minute, Second}.

%% 获取两个时间的差值(秒数)，DateTime_1 < DateTime_2
getTimeDiff(DateTime_1, DateTime_2) when (DateTime_1 =< DateTime_2) ->
    calendar:datetime_to_gregorian_seconds(DateTime_2) - calendar:datetime_to_gregorian_seconds(DateTime_1).


%% @spec(datetime_now() -> {date(), time()}).
datetime_now() ->
    calendar:now_to_local_time(erlang:timestamp()).

datetime_now_str() ->
    getDateTimeStr(datetime_now()).

date() ->
    erlang:date().

%% @doc Get broker datetime
-spec(datetime() -> string()).
datetime() ->
    {{Y, M, D}, {H, MM, S}} = calendar:local_time(),
    lists:flatten(
        io_lib:format(
            "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w", [Y, M, D, H, MM, S])).

datetime_str() ->
    datetime_str(calendar:local_time()).

datetime_str({{Y, M, D}, {H, MM, S}}) ->
    lists:flatten(
        io_lib:format(
            "~4..0w~2..0w~2..0w~2..0w~2..0w~2..0w", [Y, M, D, H, MM, S])).

last_day_of_the_month(Year, Month) ->
    calendar:last_day_of_the_month(Year, Month).

year_and_month_str(Year, Month) ->
    lists:flatten(io_lib:format("~4..0w-~2..0w", [Year, Month])).

%%=========================================================================================
%% end
%%=========================================================================================

%% 整数转换成字符串小于指定进制的数前置0 如参数 9, 10 => "09"   10, 16 => "0A"
intToStr(N, Scale) when is_integer(N) andalso is_integer(Scale) ->
    if
        N < Scale ->
            lists:concat(["0", integer_to_list(N, Scale)]);
        true ->
            integer_to_list(N, Scale)
    end.

%% Fun根据参数Argv过滤列表
filtermap(Func, Argv, List1) ->
    lists:foldr(fun(Elem, Acc) ->
                       case Func(Argv, Elem) of
                           false -> Acc;
                           true -> [Elem|Acc];
                           {true,Value} -> [Value|Acc]
                       end
                end, [], List1).


%% 对浮点数进行截取指定位数的小数 23.4555 2 => 23.45 
floatDecimal(Float_Num, Decimal) ->
    list_to_float(float_to_list(Float_Num, [{decimals, Decimal}, compact])).

%% 对浮点数进行截取指定位数的小数字符串 23.4555 2 => "23.45" 
float_to_decimal_str(Float_Num, Decimal) ->
    float_to_list(Float_Num, [{decimals, Decimal}, compact]).
 
sub51(X) when is_integer(X) ->
    X_tmp = if
            X < 51 ->
                X + 256 - 51;
            true ->
                X - 51
    end,
    intToStr(X_tmp, 16).

invertedOrder(String) ->
    Tmps = [lists:sublist(String, X, 2) || X <- lists:seq(1, length(String)), X rem 2 =:= 1],
    string:join(lists:reverse(Tmps), "").

invertedAndSub51(String) ->
    Tmps = [lists:sublist(String, X, 2) || X <- lists:seq(1, length(String)), X rem 2 =:= 1],
    string:join(lists:reverse([sub51(list_to_integer(X, 16)) || X <- Tmps]), "").

is_number(Float) when is_float(Float) ->
    true;
is_number(Integer) when is_integer(Integer) ->
    true;
is_number(String) when is_list(String) ->
    string_is_number(String).

string_is_float(String) ->
    case catch erlang:list_to_float(String) of
        {'EXIT', {badarg, _}} ->
            false;
        _ ->
            true
    end.                

string_is_integer(String) ->
    case catch erlang:list_to_integer(String) of
        {'EXIT', {badarg, _}} ->
            false;
        _ ->
            true
    end.                

string_is_number(String) ->
    string_is_float(String) orelse string_is_integer(String).

%% 去除两边的空格和换行
strip(Str) ->
    Str_1 = string:strip(Str),
    Str_2 = string:strip(Str_1, both, $\n),
    string:strip(Str_2, both, $\r).

get_env(AppName, Key, Default) ->
    case application:get_env(AppName, Key) of
        undefined -> {ok, Default};
        Found     -> Found
    end.

%% 两个时间的小时差
how_hourly_interval(DateTime_1, DateTime_2) ->
    erlang:round(getTimeDiff(DateTime_1, DateTime_2) / 3600).

%%==================================================================================================

strip([Charactor | CharList], DataStr) ->
    strip(CharList, string:strip(DataStr, right, Charactor));
strip([], DataStr) ->
    DataStr.

%%==================================================================================================
%% 数据格式之间的转换
%%==================================================================================================

%%--------------------------------------------------------------------------------------------------
%% @doc
%% 字符串报文转二级制报文：
%%     由十六进制表示的字符串构造成二进制报文
%% @spec string_to_binary(String) -> Binary
%% @param string String: 字符串报文
%% @param binary Binary: 二进制报文
%% @end
%%--------------------------------------------------------------------------------------------------
string_to_binary(String) ->
    Length = length(String),
    %% 截取每两个字符并转换成16进制的integer类型列表
    Number_list = [list_to_integer(lists:sublist(String, X, 2), 16) || X <- lists:seq(1, Length, 2)],
    %% integer列表转换成binary
    list_to_binary(Number_list).

%% "hello" -> <<"hello">> 
to_binary(String) when is_list(String) -> 
    list_to_binary(String).

%% "99.9" or <<"99.9">> -> 99.9 
to_float(String) when is_list(String) ->
    list_to_float(String);
to_float(Bitstring) when is_binary(Bitstring) ->
    to_float(binary_to_list(Bitstring)).

%%---------------------------------------------------------------------------------------------------
%% @doc
%% Json字符串解析和获取
%% @end
%%---------------------------------------------------------------------------------------------------
parse_list(Data) ->
    case rfc4627:decode(Data) of
        {ok, Obj_Json, _} ->
            {_, Parselist} = Obj_Json,
            {ok, Parselist};
        _ ->
            {error, Data}
    end.

parse(Data) ->
    case rfc4627:decode(Data) of
        {ok, Obj_Json, _} ->
            {_, Parselist} = Obj_Json,
            Parselist;
        _ ->
            Data
    end.

data_find(Datalist, Parselist) ->
    case lists:keyfind(Datalist,1,Parselist) of
        {_, List} ->
            case is_binary(List) of
                true ->
                    binary_to_list(List);
                false ->
                    List
            end;
        _ ->
            ""
    end.

json_to_str(JsonData) ->
    rfc4627:encode_noauto(JsonData).

%---------------------------------------------------------------------------------------------------
% @doc
% 对上报数据进行存储
% @end
%---------------------------------------------------------------------------------------------------
store_report_data(EqptType, MeterId, Time, ActiveReportMsg) ->
    Parselist = parse(ActiveReportMsg),
    {_, Datalist} = data_find("data", Parselist),
    DataType = data_find("datatype", Datalist),
    ChargeNo = data_find("chargeno", Datalist),
    mysql_handler:add_report_data_info([EqptType, MeterId, Time, DataType, ActiveReportMsg, ChargeNo]).

store_emqtt_data(EqptId, Time, Data) ->
    mysql_handler:add_emqtt_data_info([EqptId, Time, Data]).
  
store_iot_data(EqptId, Time, Code, WaterMsg, Filename, Value) ->
    Data = binary_to_list(jsx:encode([{<<"code">>,Code},{<<"filename">>,Filename},{<<"value">>,Value}])),
    io:format("Data:~p~n",[Data]),
    mysql_handler:add_iot_data_info([EqptId, Time, Data]).

%--------------------------------------------------------------------------------------------------
% @doc
% 16进制字符串计算校验和
% @end
%--------------------------------------------------------------------------------------------------
get_cs(Data) ->
    List = lists:seq(1, length(Data), 2),
    Datalist = [list_to_integer(string:substr(Data, X, 2), 16) || X <- List],
    Cs = hex_util:to_hex(lists:sum(Datalist) rem 256),
    string:to_upper(Cs).
%--------------------------------------------------------------------------------------------------
% @doc
% 时间字符串逆序
% @end
%--------------------------------------------------------------------------------------------------
get_reverse_timestr(TimeStr) ->
    Length = length(TimeStr),
    List = [string:substr(TimeStr,X,2) || X <- lists:seq(1, Length,2)],
    lists:concat(lists:reverse(List)).

%--------------------------------------------------------------------------------------------------
% @doc
% 补位函数
% @end
%-----------------------------------------------------------------------------------------------
% "11" 3 -> "011"   "9" 2 -> "09"  "9" 3 -> "009"
get_complete_data(Data, Flag, Len) ->
    case length(Data) < Len of
         true ->
             lists:concat([complete(Len - length(Data), Flag), Data]);
         _ ->
             Data
    end.

complete(Count, Str) ->
    complete(Count, Str, []).

complete(Count, Str, Acc) when (Count > 0) ->
    complete(Count-1, Str, [Str | Acc]);
complete(0, Str, Acc) ->
    lists:concat(Acc).
