-module(emqttc_server).

-behaivor(gen_server).

-define(SERVER, ?MODULE).

-include("config.hrl").
-include("print.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([start/3, stop/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {mqttc}).

start(Host, Port, ClientId) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Host, Port, ClientId],[]).

stop() ->
    gen_server:call(?SERVER, stop).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Host, Port, ClientId]) ->
    {ok, C} = emqttc:start_link([{host, Host}, 
                                 {port, Port},
                                 {client_id, ClientId},
                                 {logger, {error_logger, info}}]),
    {ok, #state{mqttc = C}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%%client connect emqttd servcer
handle_info({mqttc, C, connected}, State = #state{mqttc = C}) ->
    ?PRINT("client:~p is connecting emqttd server...~n",[C]),
    emqttc:subscribe(C, {<<"yc/report/gateway/#">>, 1}),
    {noreply, State};
%%client disconnect emqttd server
handle_info({mqttc, C, disconnect}, State = #state{mqttc = C}) ->
    ?PRINT("Client ~p is disconnected~n", [C]),
    emqttc:disconnect(C),
    {noreply, State};
%%Subscribe topic
handle_info({subscribe_topic, {Topic_Bin}}, State = #state{mqttc = C}) ->
    emqttc:subscribe(C, {Topic_Bin, 1}),
    {noreply, State};
%%Publish message
handle_info({topic_and_data, {Topic_Bin, Data_Bin}}, State = #state{mqttc = C}) ->
    emqttc:publish(C, Topic_Bin, Data_Bin, [{qos, 1}]),
    {noreply, State};
%%Receive message
handle_info({publish, Topic, Payload}, State = #state{mqttc = C}) ->
    ?PRINT("Payload:~p Topic:~p~n", [Payload, Topic]),
    [H | T] = string:tokens(binary_to_list(Topic),"/"),
    case H of
        "cmd" ->
            [Method, UserInfo, Data]= string:tokens(binary_to_list(Payload),"#"),
            [IpcId] = T,   
            case Method of
                "post" ->
                     case data_process_post(list_to_binary(Data), UserInfo) of
                         {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} ->
                            Body;
                         {error, _Reason} ->
                            Body = [{400, [], <<"Bad request">>}],
                            Body
                     end;
                "put" ->
                    case data_process_put(list_to_binary(Data), UserInfo) of
                         {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} ->
                            Body;
                         {error, _Reason} ->
                            Body = [{400, [], <<"Bad request">>}],
                            Body
                    end;
                "get" ->
                    case data_process_get(list_to_binary(Data), UserInfo) of
                         {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} ->
                            Body;
                         {error, _Reason} ->
                            Body = [{400, [], <<"Bad request">>}],
                            Body
                         end;
                "head" ->
                    case data_process_head(list_to_binary(Data), UserInfo) of
                         {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} ->
                            Body;
                         {error, _Reason} ->
                            Body = [{400, [], <<"Bad request">>}],
                            Body
                    end
            end,
            ResponseBin = list_to_binary(Body),
            TopicBin = list_to_binary(lists:concat(["reply/",IpcId])), 
            emqttc:publish(C, TopicBin, ResponseBin, [{qos,1}]);
        _ ->
            data_process(Topic, Payload)
        end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

data_process_post(DataBin, UserInfo) ->
    [User, Password]= string:tokens(UserInfo, "$"),
    Url = get_url() ++ "/",
    application:start(inets),
    ?PRINT("Url:~p~n",[Url]),
    httpc:request(post, {Url, [{"username",User},{"userpwd",Password}], "application/x-www-form-urlencoded", DataBin}, [{timeout, 300000}, {connect_timeout, 300000}], []).

data_process_put(DataBin, UserInfo) ->
    [User, Password]= string:tokens(UserInfo, "$"),
    [Path, Flag, Body] = string:tokens(binary_to_list(DataBin),"&"),
    ?PRINT("RawPath:~p~n Flag:~p~n Body:~p~n",[Path,Flag,Body]),
    Url = get_url() ++ Path,
    application:start(inets),
    ?PRINT("Url:~p~n",[Url]),
    httpc:request(post, {Url, [{"username",User},{"userpwd",Password}], "application/json", list_to_binary(Body ++ "&" ++ Flag)}, [{timeout, 300000}, {connect_timeout, 300000}], []).

data_process_get(DataBin, UserInfo) ->
    [User, Password]= string:tokens(UserInfo, "$"),
    ?PRINT("DataBin:~p~n",[DataBin]),
    Url = get_url() ++ binary_to_list(DataBin),
    application:start(inets),
    ?PRINT("Url:~p~n",[Url]),
    httpc:request(get, {Url,[{"username",User},{"userpwd",Password}]},[],[]).

data_process_head(DataBin, UserInfo) ->
    [User, Password]= string:tokens(UserInfo, "$"),
    [Path, RequestContent] = string:tokens(binary_to_list(DataBin), "@"),
    Url = get_url() ++ Path,
    RequestContentBin = list_to_binary(RequestContent),
    application:start(inets),
    ?PRINT("Url:~p~n",[Url]),
    httpc:request(post, {Url, [{"username",User},{"userpwd",Password}], "application/x-www-form-urlencoded", RequestContentBin}, [{timeout, 300000}, {connect_timeout, 300000}], []).

data_process(Topic_Bin, Payload_Bin) ->
    Topic = binary_to_list(Topic_Bin),
    Data = binary_to_list(Payload_Bin),
    TopicList = string:tokens(Topic, "/"),
    EqptId = lists:nth(4, TopicList) ++ "/" ++ lists:nth(5, TopicList),
    ?PRINT("EqptId:~p Payload:~p~n",[EqptId, Data]),
    DataList = [hex_util:to_hex(X) || X <- Data],
    AnalyFrame = lists:concat(DataList),
    ?PRINT("analyframe:~p~n",[AnalyFrame]),
    From = "mqtt",
    FunName = "data_report",
    Input = lists:concat(["{\"analyframe\":\"", AnalyFrame, "\", \"reportfrom\":\"", From, "\"}"]),
    SoLibName = ?SOLIBNAME,
    case data_report(SoLibName, FunName, Input) of
        {ok, OutputData} ->
             Parse_list = ?HELP:parse(OutputData),
             Data_list = ?HELP:data_find("data", Parse_list),
             Data = ?HELP:json_to_str(Data_list),
             Time = ?HELP:datetime_now_str(),
             ActiveReportMsg = "{\"eqpttype\":\"" ++ EqptId ++ "\"" ++ "\," ++ "\"time\":\"" ++ Time ++ "\"" 
                                  ++ "\," ++ "\"data\":" ++ Data ++ "}",                                       
             self() ! {topic_and_data, {<<"report">>, list_to_binary(ActiveReportMsg)}};
             %%case ?HELP:store_emqtt_data(EqptId, Time, Data) of
             %%    ok ->
             %%        ?PRINT("insert data seccess~~~n",[]);
             %%    {error, Reason} ->
             %%        ?PRINT("insert fail Reason:~p~n",[Reason])
             %%end;
        {error, Reason} ->
            ?PRINT("解析失败:~p~n",[Reason])
    end.

data_process_test(Topic_Bin, Payload_Bin) ->
    AnalyFrame = "68010100002d154304123030303030303131313131313131313131311901001f1122010000804f430011220200000000000011220300000000000011220400000000004011220500000000004011220600000000004011220700000000000011220800000000000011220900000000000011220a00000080bf0011220b00000000000011220c00000000000011220d00000000000011220e00000000000011220f000000000000112210000000000000112211000000000000112212000000000000112213000000000000112214000000000000112215000000000000112216000000000000112217000000c84200112414000000c84200112219000000c8420011221a000000c8420011221b00f62848420011221c00000000000011221d00000000000011221e00000000004011221f0000000000409b16",
    From = "mqtt",
    Input = lists:concat(["{\"analyframe\":\"", AnalyFrame, "\", \"reportfrom\":\"", From, "\"}"]),
    SoLibName = ?SOLIBNAME,
    FunName = "data_report",
    case data_report(SoLibName, FunName, Input) of
        {ok, OutputData} ->
             ?PRINT("Outputdata:~p~n",[OutputData]),
             Parse_list = ?HELP:parse(OutputData),
             Data_list = ?HELP:data_find("data", Parse_list),
             Data = ?HELP:json_to_str(Data_list),
             Time = ?HELP:datetime_now_str(),
             EqptId = "001",
             ActiveReportMsg = "{\"eqpttype\":\"" ++ EqptId ++ "\"" ++ "\," ++ "\"time\":\"" ++ Time ++ "\"" 
                                  ++ "\," ++ "\"data\":" ++ Data ++ "}",                                       
             self() ! {topic_and_data, {<<"report">>, list_to_binary(ActiveReportMsg)}},
             case ?HELP:store_emqtt_data(EqptId, Time, Data) of
                 ok ->
                     ?PRINT("insert data seccess~~~n",[]);
                 {error, Reason} ->
                     ?PRINT("insert fail Reason:~p~n",[Reason])
             end;
        {error, Reason} ->
            ?PRINT("解析失败:~p~n",[Reason])
    end.

get_url() ->
    [{client_request_opts, ClientRequestOpts}] = ets:lookup(?LISTEN_OPTS_TABLE, client_request_opts),
    [{_, Port}, _] = proplists:get_value(transOpts, ClientRequestOpts, []),
    Url = "http://127.0.0.1" ++ ":" ++ integer_to_list(Port),
    Url.

data_report(SoLibName, FunName, Indata) ->
    exec_func(SoLibName, FunName, Indata).

exec_func(SoFileName, FuncName, Inputdata) ->
    try ?NIFJSONMODULE:exec_func(SoFileName, FuncName, Inputdata) of
        {ok, {FuncResult, OutputData}} ->
            ?PRINT("Output:~s~n",[OutputData]),
            {ok, OutputData};
        {error, Reason} ->
            {error, Reason}
    catch 
        Class:What ->
            {error, What}
    end.
