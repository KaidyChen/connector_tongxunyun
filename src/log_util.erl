-module(log_util).

-compile([export_all]).

-include("config.hrl").
-include("log.hrl").
-include("print.hrl").

write_log(File, Content) ->
    filelib:ensure_dir(File),
    case file:open(File, [append, binary]) of
        {ok, IoDevice} ->
            io:fwrite(IoDevice, "~s~n", [Content]),
            file:close(IoDevice),
            ok;
        {error, Reason} ->
            ?ERROR("~p:~p ~p write_log is error: ~p~n", [?FILE, ?LINE, File, Reason]),
            ok
    end.

get_gateway_log_file(Datetime) ->
    get_log_file(Datetime, ?GATEWAY_LOG_DIR).

get_active_report_log_file(Datetime) ->
    get_log_file(Datetime, ?ACTIVE_REPORT_LOG_DIR).

get_gateway_send_recv_log_file(Datetime) ->
    get_log_file(Datetime, ?ORDER_SEND_RECV_LOG_DIR).

get_request_response_log_file(Datetime) ->
    get_log_file(Datetime, ?REQUEST_RESPONSE_LOG_DIR).

get_operation_log_file(Datetime) ->
    get_log_file(Datetime, ?OPERATION_LOG_DIR).

get_easyiot_log_file(Datetime) ->
    get_log_file(Datetime, ?EASYIOT_LOG_DIR).

get_log_file(Datetime, Dir1) ->
    {Date , Time} = Datetime,
    YMFiledir = ?HELPER:get_YM_filedir(Date),
    YMDFilename = ?HELPER:get_YMD_filename(Date) ++ ?LOG_FILE_EXTENSION,
    filename:join([?LOG_DIR, Dir1, YMFiledir, YMDFilename]).

gen_content(Datetime, MsgType, IdCode, Msg) ->
    DatetimeString = ?HELPER:datetime_string(Datetime),
    Content = string:join([DatetimeString, MsgType, IdCode, Msg], " "),
    Content.
