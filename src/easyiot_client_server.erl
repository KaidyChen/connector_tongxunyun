-module(easyiot_client_server).

-behaviour(gen_server).

-include("config.hrl").
-include("log.hrl").
-include("print.hrl").

%% API
-export([
         start/0,
         stop/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TIMEOUT_LOGIN, 18*60*1000). %% 18min登陆一次easyIot平台，确保token为最新状态

-record(state, {}).

start() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [],[]).

stop() ->
    gen_server:cast(?SERVER, stop).

init([]) ->
    iot_token_store:init(),
    iot_clientpid_store:init(),
    iot_eqptid_store:init(),
    iot_message_store:init(),
    {ok, #state{}, 0}.

handle_call(_Msg, _From, State) ->   
    {noreply, State}.   
   
handle_cast(stop, State) ->
    {stop, normal, State};  
handle_cast(_Msg, State)  ->   
    {noreply, State}.

handle_info(timeout, State) ->
    ?PRINT("start login easyiot~~~n",[]),
    login(),
    {noreply, State, ?TIMEOUT_LOGIN};
handle_info(Info, State) ->
    ?PRINT("~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

login() ->
    application:start(inets),
    [{easyiot_opts, EasyIotOpts}] = ets:lookup(?LISTEN_OPTS_TABLE, easyiot_opts),
    ServerId = proplists:get_value(serverid, EasyIotOpts),
    Password = proplists:get_value(password, EasyIotOpts),
    SendBody = lists:concat(["{\"serverId\":\"", ServerId, "\",\"password\":\"", Password,"\"}"]),
    Response = httpc:request(post, {?LoginUrl, [], "application/json", list_to_binary(SendBody)}, [{timeout, 300000}, {connect_timeout, 300000}], []),
    case Response of
        {ok, {{_, 200, _}, _, Body}} ->
            ?PRINT("Body = ~p~n",[Body]),
            Bodylist = ?HELP:parse(Body),
            AccessToken = ?HELP:data_find("accessToken", Bodylist),
            OptResult = ?HELP:data_find("optResult", Bodylist),
            ?PRINT("OptResult = ~p~n",[OptResult]),
            case OptResult of
                "0" ->
                    Msg = "easyIot login success--> " ++ AccessToken,
                    connector_event_server:easyiot_log(ServerId,Msg),
                    iot_token_store:insert({"accessToken", ServerId, AccessToken});
                _ ->
                    Msg = "easyIot login fail",
                    connector_event_server:easyiot_log(ServerId,Msg),
                    ?ERROR("EasyIot平台登录失败!!!~n",[])
            end;
        {error, Reason}->
            Msg = "easyIot login fail",
            connector_event_server:easyiot_log(ServerId,Msg),
            ?ERROR("EasyIot平台登录失败!!!~p~n",[Reason]);
        _ ->
            ""
    end.
