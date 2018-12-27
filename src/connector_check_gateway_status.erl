-module(connector_check_gateway_status).

-behaviour(gen_server).

-include("config.hrl").
-include("print.hrl").

%% Public API
-export([start/0]).  

%% gen_server callback API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 5*60*1000).

-record(state, {}).

start() ->
    gen_server:start_link({local,?SERVER}, ?MODULE,[],[]).

stop() ->
    gen_server:cast(?SERVER, stop).

init([]) ->
    {ok, #state{}, ?TIMEOUT}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->  
    {noreply, State}. 

handle_call(_Request, _From, State) -> 
    {noreply, State}.  

terminate(_Reason, State) ->
    ok. 

code_change(_OldVersion, State, _Extra) -> 
    {ok, State}. 

%%程序启动后10分钟对比一下数据库和缓存中的采集器状态数据，筛选出不在线的采集器并发布消息  
handle_info(timeout, State) ->
    case db_util:get_collector_infolist() of
                {ok, Rows} ->
                    List = [{binary_to_list(EqptTypeBinary),binary_to_list(EqptIdCodeBinay),binary_to_list(CreatorBinary)} || {EqptTypeBinary, EqptIdCodeBinay, CreatorBinary} <- field_format(Rows)];
                _ ->
                    List = []
    end,
    [check_gateway_status(EqptType, EqptIdCode, Creator) || {EqptType, EqptIdCode, Creator} <- List],
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

check_gateway_status(GatewayType,GatewayCode,Creator) ->
    case connector_gateway_status_store:lookup(GatewayCode) of
            {ok, _} ->
                ok;
            _ ->
                gateway_status_publish(GatewayType, GatewayCode, Creator)
    end.

gateway_status_publish(GatewayType,GatewayCode,Creator) ->
    [{ipc_id, IpcId}] = ets:lookup(?COMPUTER_OPTS_TABLE, ipc_id),
    Time = ?HELP:datetime_now_str(),
    ReportMsg = lists:concat(["{\"eqpttype\":\"", GatewayType,"\",\"meterid\":\"", GatewayCode,"\",\"time\":\"", Time,"\",\"data\":{\"datatype\":\"0002\",\"status\":\"", "00", "\"}}"]),
    TopicBin = list_to_binary(lists:concat([Creator, "/", IpcId, "/", GatewayCode])),
    DataBin = list_to_binary(ReportMsg),
    emqttc_server ! {topic_and_data, {TopicBin, DataBin}}.

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
