-module(connector_report_data_process).

-include("config.hrl").
-include("print.hrl").

-export([report_manage/1]).

%%==================================================================================
%% External API
%%==================================================================================
report_manage(MsgList) ->
    %%io:format("MsgList:~p~n",[MsgList]),
    Now_datetime = ?HELP:datetime_now_str(),
    case string:tokens(MsgList, "/") of
        [Msg_type, Data_field_str] when (Msg_type =:= ?DATAMSG) orelse (Msg_type =:= ?STATUSMSG) orelse (Msg_type =:= ?WARNMSG) orelse (Msg_type =:= ?CONTROLMSG) ->
                %%io:format("datetime_now:~p~n",[Now_datetime]),
                report_data(Msg_type, Data_field_str, Now_datetime);
        _ -> 
            ?ERROR("Report_data:~p is not match~n", [MsgList])       
    end.

%%==================================================================================

report_data(Msg_type = ?DATAMSG, Data_field_str, Now_datetime) ->
    process_datamsg(Msg_type, Data_field_str, Now_datetime);
report_data(Msg_type = ?CONTROLMSG, Data_field_str, Now_datetime) ->
    process_controlmsg(Msg_type, Data_field_str, Now_datetime);
report_data(Msg_type = ?STATUSMSG, Data_field_str, Now_datetime) ->
    process_statusmsg(Msg_type, Data_field_str, Now_datetime);
report_data(Msg_type = ?WARNMSG, Data_field_str, Now_datetime) ->
    process_warnmsg(Msg_type, Data_field_str, Now_datetime).

%%==================================================================================
%% Internal API
%%==================================================================================

%% 处理数据块上报信息
process_datamsg(Msg_type, Data_field_str, Now_datetime) ->
    case parse_data_field(Data_field_str) of
        [Meter_type, Meter, Datagram] ->
            case parse_datagram(Datagram) of
                %% 上报报文中各个字段信息
                %% Content: "电量 电压 电流 。。。"
                {air_detector, [PM1dot0, PM2dot5, PM10, Temperature, Humidity, HCHO, TVOC, CO2, CO, O2, Time]}  ->
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"", Meter,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"01\",\"pm1.0\":\"", PM1dot0, "\", \"pm2.5\":\"", PM2dot5, "\", \"pm10\":\"", PM10, "\", \"temperature\":\"", Temperature, "\", \"humidity\":\"", Humidity, "\", \"hcho\":\"", HCHO, "\", \"tvoc\":\"", TVOC, "\", \"co2\":\"", CO2, "\", \"co\":\"", CO, "\", \"o2\":\"", O2, "\", \"time\":\"", Time, "\"}}"]);

                {ac, [Electric_power, Voltage, Electric_current, Active_power, Temperature, Power_system_frequency, Power_factor, Relay_status, Status_word, Time]}  -> 
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"",Meter,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"02\",\"electric_power\":\"", Electric_power, "\", \"voltage\":\"", Voltage, "\", \"electric_current\":\"", Electric_current, "\", \"active_power\":\"", Active_power, "\", \"temperature\":\"", Temperature, "\", \"power_system_frequency\":\"", Power_system_frequency, "\", \"power_factor\":\"", Power_factor, "\", \"relay_status\":\"", Relay_status, "\", \"status_word\":\"", Status_word, "\", \"time\":\"", Time, "\"}}"]); 

                {socket0, [Electric_power, Voltage, Electric_current, Active_power, Power_system_frequency, Power_factor, Relay_status]}  ->
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"", Meter,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"03\",\"electric_power\":\"", Electric_power, "\", \"voltage\":\"", Voltage, "\", \"electric_current\":\"", Electric_current, "\", \"active_power\":\"", Active_power, "\", \"power_system_frequency\":\"", Power_system_frequency, "\", \"power_factor\":\"", Power_factor, "\", \"relay_status\":\"", Relay_status, "\"}}"]);

                {socket1, [Electric_power, Voltage, Electric_current, Active_power, Power_system_frequency, Power_factor, Relay_status, Status_word, Time]}  ->
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"", Meter,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"04\",\"electric_power\":\"", Electric_power, "\", \"voltage\":\"", Voltage, "\", \"electric_current\":\"", Electric_current, "\", \"active_power\":\"", Active_power, "\", \"power_system_frequency\":\"", Power_system_frequency, "\", \"power_factor\":\"", Power_factor, "\", \"relay_status\":\"", Relay_status, "\", \"status_word\":\"", Status_word, "\", \"time\":\"", Time, "\"}}"]);

                {central_ac, [Electric_power, Active_power, Temperature, Relay_status, Temp_and_mode, Wind_speed_gears, Low_speed_used_time, Medium_speed_used_time, High_speed_used_time, Amount, Status_word, Time]}  ->
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"", Meter,"\".\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"05\",\"electric_power\":\"", Electric_power, "\", \"active_power\":\"", Active_power, "\", \"temperature\":\"", Temperature, "\", \"relay_status\":\"", Relay_status, "\", \"temp_and_mode\":\"", Temp_and_mode, "\", \"wind_speed_gears\":\"", Wind_speed_gears, "\", \"low_speed_used_time\":\"", Low_speed_used_time, "\", \"amount\":\"", Amount, "\", \"status_word\":\"", Status_word, "\", \"time\":\"", Time, "\"}}"]);

                {water_vapour, [MeterId, Datetime, AccumulativeVolume]}  -> 
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"", MeterId,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"06\",\"accumulativevolume\":\"", AccumulativeVolume, "\",  \"time\":\"", Datetime, "\"}}"]);

                {heat, [MeterId, Datetime, TotalCalories]}  ->  
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"", MeterId,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"07\",\"totalCalories\":\"", TotalCalories, "\", \"time\":\"", Datetime, "\"}}"]);

                {electric_meter_F25, [MeterId, Datetime,TotalActivePower,AActivePower,BActivePower,CActivePower,TotalReactivePower,AReactivePower,BReactivePower,CReactivePower,TotalPowerFactor,APowerFactor,BPowerFactor,CPowerFactor,AVoltage,BVoltage,CVoltage,ACurrent,BCurrent,CCurrent,ZeroCurrent,TotalApparentPower,AApparentPower,BApparentPower,CApparentPower]}  ->
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"", MeterId,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"08\",\"totalActivePower\":\"", TotalActivePower, "\", \"AactivePower\":\"", AActivePower, "\", \"BactivePower\":\"", BActivePower, "\", \"CactivePower\":\"", CActivePower, "\", \"totalreactivepower\":\"", TotalReactivePower, "\", \"Areactivepower\":\"", AReactivePower, "\", \"Breactivepower\":\"", BReactivePower, "\", \"CreactivePower\":\"",CReactivePower, "\", \"totalPowerFactor\":\"", TotalPowerFactor, "\", \"ApowerFactor\":\"", APowerFactor, "\",\"BpowerFactor\":\"", BPowerFactor, "\", \"CpowerFactor\":\"", CPowerFactor, "\",\"Avoltage\":\"", AVoltage, "\",\"Bvoltage\":\"", BVoltage, "\",\"Cvoltage\":\"", CVoltage, "\", \"Acurrent\":\"", ACurrent, "\",\"Bcurrent\":\"", BCurrent, "\", \"Ccurrent\":\"", CCurrent, "\",\"ZeroCurrent\":\"", ZeroCurrent, "\", \"totalApparentPower\":\"", TotalApparentPower, "\",\"AapparentPower\":\"", AApparentPower, "\", \"BapparentPower\":\"", BApparentPower, "\",\"CapparentPower\":\"", CApparentPower, "\", \"time\":\"", Datetime, "\"}}"]);

                {electric_meter_F129,[MeterId, Datetime, Energy]} ->
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"", MeterId,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"09\",\"electric_consumption\":\"", Energy, "\", \"time\":\"", Datetime, "\"}}"]);

                {electric_meter_F129_part,[MeterId, Datetime, PositiveActiveEle, Rate1PositiveActiveEle, Rate2PositiveActiveEle, Rate3PositiveActiveEle, Rate4PositiveActiveEle, InverseActiveEle, Rate1InverseActiveEle, Rate2InverseActiveEle, Rate3InverseActiveEle, Rate4InverseActiveEle]} ->
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"", MeterId,"\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"10\",\"Positiveactiveele\":\"", PositiveActiveEle, "\", \"Rate1PositiveActiveEle\":\"",Rate1PositiveActiveEle, "\",  \"Rate2PositiveActiveEle\":\"", Rate2PositiveActiveEle, "\", \"Rate3PositiveActiveEle\":\"", Rate3PositiveActiveEle, "\",  \"Rate4PositiveActiveEle\":\"", Rate4PositiveActiveEle, "\", \"InverseActiveEle\":\"", InverseActiveEle, "\",  \"Rate1InverseActiveEle\":\"", Rate1InverseActiveEle, "\", \"Rate2InverseActiveEle\":\"", Rate2InverseActiveEle, "\", \"Rate3InverseActiveEle\":\"",Rate3InverseActiveEle, "\", \"Rate4InverseActiveEle\":\"", Rate4InverseActiveEle, "\", \"time\":\"", Datetime, "\"}}"]);

                false -> ?ERROR("parse_datagram is error:~p", [Data_field_str])
            end;
        _ -> 
            ?ERROR("Msg_type:~p data field:~p not match.~n", [Msg_type, Data_field_str])
    end.

process_controlmsg(Msg_type, Data_field_str, Now_datetime) ->
    ?PRINT("~p/~p~n", [Msg_type, Data_field_str]),
    case parse_data_field(Data_field_str) of
        [Meter_type, Meter, Datagram] ->
            Datagram_bin = ?HELP:string_to_binary(Datagram),
            case Datagram_bin of
                %% 触摸开关策略控制上报
                <<Id:8, 16#3B, 16#0F, 16#EF, 16#25, TaskIdBinary:16/binary-unit:8, _/binary>> when 
                      (Id =:= 16#34); (Id =:= 16#35); (Id =:= 16#36); (Id =:= 16#37) ->
                    TaskId = ?CALHELP:get_touch_task_id(TaskIdBinary),
                    ?PRINT("Touch taskId:~p~n", [TaskId]),
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"", Meter, "\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"11\",\"taskid\":\"", TaskId, "\"}}"]);
                %% 灯的控制聚合上报
                <<16#39, 16#3A, 16#0F, 16#EF, _Len1Tmp:8, NumberTmp:8, Rest/binary>> ->
                    %_Number = NumberTmp - 51,
                    ShortNumberAndCtrlStatus = get_short_number_and_ctrl_status(Rest),
                    ?PRINT("ShortNumberAndCtrlStatus:~p~n", [ShortNumberAndCtrlStatus]),
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"", Meter, "\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"12\",\"shortNumberAndCtrlStatus\":\"", ShortNumberAndCtrlStatus, "\"}}"]);
                %% 灯的控制上报
                <<16#35, 16#36, 16#0F, 16#EF, RelayStatusAndPwmSelect:8, Pwm:8/binary-unit:8, ActivePowerTmp:(3*8),
                  CtrlStatusTmp:8>> ->
                    <<_Retain:3, ReplyStatusInt:1, _PwmSelete:4>> = <<(RelayStatusAndPwmSelect-51):8>>,                   
                    ActivePowerFloat = ?CALHELP:get_active_power_float(ActivePowerTmp),
                    OnOffStatus = 
                        case {ReplyStatusInt, ActivePowerFloat >= 5.0} of
                            {1, true} ->  %% 继电器状态1为合闸
                                1;
                            _ ->
                                0
                        end,
                        %% 0 表示灯灭，1 表示灯亮, 灯的不是表示继电器状态                                        
                    CtrlStatus = CtrlStatusTmp - 51,
                    ?PRINT("~p/~p ReplyStatusInt:~p ActicvePower:~p CtrlStatus:~p OnOffStatus:~p~n",[Meter_type, Meter, ReplyStatusInt, ActivePowerFloat, CtrlStatus, OnOffStatus]),
                    lists:concat(["{\"eqpttype\":\"", Meter_type,"\",\"meterid\":\"", Meter, "\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"13\",\"replystatusint\":\"", ReplyStatusInt, "\", \"ActivePower\":\"", ActivePowerFloat,"\", \"CtrlStatus\":\"", CtrlStatus,"\", \"OnOffStatus\":\"", OnOffStatus,"\"}}"]);

                _ ->
                    ok
            end,
            ok;
        _ ->
            ?ERROR("Msg_type:~p data field:~p not match.~n", [Msg_type, Data_field_str])
    end.

get_short_number_and_ctrl_status(Rest) ->
    get_short_number_and_ctrl_status_(Rest, []).

get_short_number_and_ctrl_status_(<<ShortNumberTmp, CtrlStatusTmp, Rest/binary>>, List) ->
    get_short_number_and_ctrl_status_(Rest, [{ShortNumberTmp-51, CtrlStatusTmp-51} | List]);
get_short_number_and_ctrl_status_(_, List) ->
    List.

process_statusmsg(Msg_type, Data_field_str, Now_datetime) ->
    case parse_data_field(Data_field_str) of
        [Gateway_type, Gateway, Status] ->
            ?PRINT("Gateway_type:~p Gateway:~p Status:~p~n", [Gateway_type, Gateway, Status]),
        lists:concat(["{\"eqpttype\":\"", Gateway_type,"\",\"meterid\":\"", Gateway, "\",\"time\":\"", Now_datetime,"\",\"data\":{\"datatype\":\"0002\",\"status\":\"", Status, "\"}}"]);
        _ -> 
            ?ERROR("Msg_type:~p data field:~p not match.~n", [Msg_type, Data_field_str])
    end.

process_warnmsg(Msg_type, Data_field_str, Now_datetime) ->
    ?ERROR("~p: ~p", [Msg_type, Data_field_str]),
    ok.
    
parse_data_field(Data_field_str) ->
    string:tokens(Data_field_str, "#").

%% 解析数据报
parse_datagram(Datagram) ->
    Datagram_bin = ?HELP:string_to_binary(Datagram),
    case Datagram_bin of
        %%空气检测仪
        <<16#32, 16#33, 16#21, 16#ef, PM1dot0:(2*8), PM2dot5:(2*8), PM10:(2*8), Temperature:(2*8), Humidity:(1*8), HCHO:(2*8),
          TVOC:(2*8), CO2:(2*8), CO:(2*8), O2:(2*8), _Reserved:(2*8), DateTime:(5*8)>> ->
            {air_detector, get_air_detector_content_and_info({PM1dot0, PM2dot5, PM10, Temperature, Humidity, HCHO, TVOC, CO2, CO, O2, DateTime})};
        %% 分体空调
        <<16#38, 16#32, 16#ff, 16#ef, Electric_power:(4*8), Voltage:(2*8), Electric_current:(3*8), Active_power:(3*8), Temp:(2*8), _:8, Power_system_frequency:(2*8), Power_factor:(2*8), Relay_status:8, Status_word:(3*8), DateTime:(5*8)>> ->
            {ac, get_ac_content_and_info({Electric_power, Voltage, Electric_current, Active_power, Temp, Power_system_frequency, Power_factor, Relay_status, Status_word, DateTime})};

        %% 插座或四路开关面板不含状态字和时标
        <<16#34, 16#32, 16#ff, 16#ef, Electric_power:(4*8), Voltage:(2*8), Electric_current:(3*8), Active_power:(3*8), _:(3*8), Power_system_frequency:(2*8), Power_factor:(2*8), Relay_status:8>> ->
            {socket0, get_socket_content_and_info_1({Electric_power, Voltage, Electric_current, Active_power, Power_system_frequency, Power_factor, Relay_status})};

        %% 插座或四路开关面板含有状态字和时标    
        <<16#34, 16#32, 16#ff, 16#ef, Electric_power:(4*8), Voltage:(2*8), Electric_current:(3*8), Active_power:(3*8), _:(3*8), Power_system_frequency:(2*8), Power_factor:(2*8), Relay_status:8, Status_word:(3*8), DateTime:(5*8)>> ->
            {socket1, get_socket_content_and_info({Electric_power, Voltage, Electric_current, Active_power, Power_system_frequency, Power_factor, Relay_status, Status_word, DateTime})};
        %%中央空调
        <<16#38, 16#32, 16#dd, 16#ef, Electric_power:(4*8), Active_power:(3*8), Temp:(2*8), Relay_status:8, Temp_and_mode:(2*8), Wind_speed_gears:8, Low_speed_used_time:(4*8), Medium_speed_used_time:(4*8), High_speed_used_time:(4*8), Amount:(4*8), Status_word:(3*8), DateTime:(5*8)>> ->
            {central_ac, get_central_ac_content_and_info({Electric_power, Active_power, Temp, Relay_status, Temp_and_mode, Wind_speed_gears, Low_speed_used_time, Medium_speed_used_time, High_speed_used_time, Amount, Status_word, DateTime})};

        %% 水气表腾讯版本
%%        <<16#08, 16#17, MeterIdBinary:7/binary-unit:8, DatetimeBinary:5/binary-unit:8, Type:8, 
%%          AccumulativeVolumeBinary:5/binary-unit:8, _/binary>> when ((16#10 =< Type) andalso (Type =< 19)) 
%%                                                              orelse ((16#30 =< Type) andalso (Type =< 49)) ->
%%            try ?CALHELP:get_3761_accumulative_volume_float(AccumulativeVolumeBinary) of
%%                AccumulativeVolume ->
%%                    MeterId = ?CALHELP:get_3761_meter_id(MeterIdBinary),
%%                    Datetime = ?CALHELP:get_3761_date_time(DatetimeBinary),
%%                    AccumulativeVolumeStr = help:float_to_decimal_str(AccumulativeVolume, 2),
%%                    {water_vapour, [MeterId, Datetime, AccumulativeVolumeStr]}
%%            catch
%%               _:_ ->
%%                    ok
%%            end;
        %% F188水表
        <<16#08, 16#17, ReportNumBinary:8, MeterIdBinary:7/binary-unit:8, UnitBinary:8,            AccumulativeVolumeBinary:4/binary-unit:8, DatetimeBinary:7/binary-unit:8, _/binary>>  ->
                try ?CALHELP:get_3761_accumulative_volume_float(AccumulativeVolumeBinary) of
                  AccumulativeVolume ->
                        MeterId = ?CALHELP:get_3761_meter_id(MeterIdBinary),
                        DatetimeTmp = ?CALHELP:get_3761_date_time(DatetimeBinary),
                        Datetime = ?HELP:getDateTimeStr(DatetimeTmp),
                        AccumulativeVolumeStr = help:float_to_decimal_str(AccumulativeVolume, 2),
                        {water_vapour, [MeterId, Datetime, AccumulativeVolumeStr]}
                catch
                     _:_ ->
                            ok
                end;

        %% 热表
        <<16#08, 16#17, MeterIdBinary:7/binary-unit:8, DatetimeBinary:5/binary-unit:8, Type:8, 
          _DayCaloriesBinary:5/binary-unit:8, TotalCaloriesBinary:5/binary-unit:8, _/binary>> when 
              ((16#20 =< Type) andalso (Type =< 29)) ->
            MeterId = ?CALHELP:get_3761_meter_id(MeterIdBinary),
            Datetime = ?CALHELP:get_3761_date_time(DatetimeBinary),
            try ?CALHELP:get_3761_calories_float(TotalCaloriesBinary) of
                TotalCalories ->
                    TotalCaloriesStr = help:float_to_decimal_str(TotalCalories, 2),
                    {heat, [MeterId, Datetime, TotalCaloriesStr]}
            catch
                _:_ ->
                    ok
            end;
        %%F129电表正向数据特征字
        <<16#01, 16#10, _/binary>> ->
            cal_dnb_data(Datagram_bin);
        %%F129电表反向数据特征字
        <<16#04, 16#10, _/binary>> ->
            cal_dnb_data(Datagram_bin);
        <<16#01, 16#03, _/binary>> ->
            cal_dnb_data(Datagram_bin);
        Other ->
            false 
    end.

cal_dnb_data(Packet) ->
    ?PRINT("Packet:~p~n", [hex_util:to_hex(Packet)]),
    case Packet of
        %% 电表数据F25，各项功率数据
        <<16#01, 16#03, MeterIdBinary:7/binary-unit:8, DatetimeBinary:5/binary-unit:8, TotalActivePower:3/binary-unit:8, 
          AActivePower:3/binary-unit:8, BActivePower:3/binary-unit:8, CActivePower:3/binary-unit:8, 
          TotalReactivePower:3/binary-unit:8, AReactivePower:3/binary-unit:8, BReactivePower:3/binary-unit:8, 
          CReactivePower:3/binary-unit:8, TotalPowerFactor:2/binary-unit:8, APowerFactor:2/binary-unit:8, 
          BPowerFactor:2/binary-unit:8, CPowerFactor:2/binary-unit:8, AVoltage:2/binary-unit:8, BVoltage:2/binary-unit:8, 
          CVoltage:2/binary-unit:8, ACurrent:3/binary-unit:8, BCurrent:3/binary-unit:8, CCurrent:3/binary-unit:8, 
          ZeroCurrent:3/binary-unit:8, TotalApparentPower:3/binary-unit:8, AApparentPower:3/binary-unit:8, 
          BApparentPower:3/binary-unit:8, CApparentPower:3/binary-unit:8, _:16, Rest/binary>> ->
            MeterId = ?CALHELP:get_3761_meter_id(MeterIdBinary),
            Datetime = ?CALHELP:get_3761_date_time(DatetimeBinary),
            TotalActivePowerStr = ?CALHELP:get_3761_active_power(TotalActivePower),
            AActivePowerStr = ?CALHELP:get_3761_active_power(AActivePower),
            BActivePowerStr = ?CALHELP:get_3761_active_power(BActivePower),
            CActivePowerStr = ?CALHELP:get_3761_active_power(CActivePower),            
            TotalReactivePowerStr = ?CALHELP:get_3761_active_power(TotalReactivePower),
            AReactivePowerStr = ?CALHELP:get_3761_active_power(AReactivePower),
            BReactivePowerStr = ?CALHELP:get_3761_active_power(BReactivePower),
            CReactivePowerStr = ?CALHELP:get_3761_active_power(CReactivePower),
            TotalPowerFactorStr = ?CALHELP:get_3761_power_factor(TotalPowerFactor),
            APowerFactorStr = ?CALHELP:get_3761_power_factor(APowerFactor),
            BPowerFactorStr = ?CALHELP:get_3761_power_factor(BPowerFactor),
            CPowerFactorStr = ?CALHELP:get_3761_power_factor(CPowerFactor),
            AVoltageStr = ?CALHELP:get_3761_voltage(AVoltage),
            BVoltageStr = ?CALHELP:get_3761_voltage(BVoltage),
            CVoltageStr = ?CALHELP:get_3761_voltage(CVoltage),
            ACurrentStr = ?CALHELP:get_3761_current(ACurrent),
            BCurrentStr = ?CALHELP:get_3761_current(BCurrent),
            CCurrentStr = ?CALHELP:get_3761_current(CCurrent),
            ZeroCurrentStr = ?CALHELP:get_3761_current(ZeroCurrent),
            TotalApparentPowerStr = ?CALHELP:get_3761_apparent_power(TotalApparentPower),
            AApparentPowerStr = ?CALHELP:get_3761_apparent_power(AApparentPower),
            BApparentPowerStr = ?CALHELP:get_3761_apparent_power(BApparentPower),
            CApparentPowerStr = ?CALHELP:get_3761_apparent_power(CApparentPower),
            cal_dnb_data(Rest),
            {electric_meter_F25, [MeterId, Datetime,TotalActivePowerStr,AActivePowerStr,BActivePowerStr,CActivePowerStr,TotalReactivePowerStr,AReactivePowerStr,BReactivePowerStr,CReactivePowerStr,TotalPowerFactorStr,APowerFactorStr,BPowerFactorStr,CPowerFactorStr,AVoltageStr,BVoltageStr,CVoltageStr,ACurrentStr,BCurrentStr,CCurrentStr,ZeroCurrentStr,TotalApparentPowerStr,AApparentPowerStr,BApparentPowerStr,CApparentPowerStr]};        
        
        <<16#01, 16#10, MeterIdBinary:7/binary-unit:8, DatetimeBinary:5/binary-unit:8, RateNumber:8, 
          PositiveActiveEle:5/binary-unit:8, Rate1PositiveActiveEle:5/binary-unit:8, Rate2PositiveActiveEle:5/binary-unit:8,
          Rate3PositiveActiveEle:5/binary-unit:8, Rate4PositiveActiveEle:5/binary-unit:8, _:2/binary-unit:8, 
          16#04, 16#10, MeterIdBinary:7/binary-unit:8, _DatetimeBinary:5/binary-unit:8, _RateNumber:8, 
          InverseActiveEle:5/binary-unit:8, Rate1InverseActiveEle:5/binary-unit:8, Rate2InverseActiveEle:5/binary-unit:8,
          Rate3InverseActiveEle:5/binary-unit:8, Rate4InverseActiveEle:5/binary-unit:8, _/binary>> ->
            MeterId = ?CALHELP:get_3761_meter_id(MeterIdBinary),
            Datetime = ?CALHELP:get_3761_date_time(DatetimeBinary),
            PositiveActiveEleStr = ?CALHELP:get_3761_active_electricity_str(PositiveActiveEle),
            Rate1PositiveActiveEleStr = ?CALHELP:get_3761_active_electricity_str(Rate1PositiveActiveEle),
            Rate2PositiveActiveEleStr = ?CALHELP:get_3761_active_electricity_str(Rate2PositiveActiveEle),
            Rate3PositiveActiveEleStr = ?CALHELP:get_3761_active_electricity_str(Rate3PositiveActiveEle),
            Rate4PositiveActiveEleStr = ?CALHELP:get_3761_active_electricity_str(Rate4PositiveActiveEle),
            InverseActiveEleStr = ?CALHELP:get_3761_active_electricity_str(InverseActiveEle),
            Rate1InverseActiveEleStr = ?CALHELP:get_3761_active_electricity_str(Rate1InverseActiveEle),
            Rate2InverseActiveEleStr = ?CALHELP:get_3761_active_electricity_str(Rate2InverseActiveEle),
            Rate3InverseActiveEleStr = ?CALHELP:get_3761_active_electricity_str(Rate3InverseActiveEle),
            Rate4InverseActiveEleStr = ?CALHELP:get_3761_active_electricity_str(Rate4InverseActiveEle),
            PositiveActiveEleFloat = list_to_float(PositiveActiveEleStr),
            InverseActiveEleFloat = list_to_float(InverseActiveEleStr),
            %ElectricityFloat = PositiveActiveEleFloat+InverseActiveEleFloat,
            %?HELP:float_to_decimal_str(ElectricityFloat, 2),      
            {electric_meter_F129_part, [MeterId, Datetime, PositiveActiveEleStr, Rate1PositiveActiveEleStr, Rate2PositiveActiveEleStr, Rate3PositiveActiveEleStr, Rate4PositiveActiveEleStr, InverseActiveEleStr, Rate1InverseActiveEleStr, Rate2InverseActiveEleStr, Rate3InverseActiveEleStr, Rate4InverseActiveEleStr]};
            
        %% 电表数据F129，各项总电能
        <<16#01, 16#10, MeterIdBinary:7/binary-unit:8, DatetimeBinary:5/binary-unit:8, RateNumber:8, TotalActiveElectricityBinary:5/binary-unit:8, _/binary>> ->
            MeterId = ?CALHELP:get_3761_meter_id(MeterIdBinary),
            Datetime = ?CALHELP:get_3761_date_time(DatetimeBinary),
            TotalActiveElectricity = ?CALHELP:get_3761_active_electricity_float(TotalActiveElectricityBinary),
            TotalElectricity = ?HELP:float_to_decimal_str(TotalActiveElectricity, 2),
            {electric_meter_F129, [MeterId, Datetime, TotalElectricity]};
        _ ->
            ok
    end.

get_air_detector_content_and_info({PM1dot0, PM2dot5, PM10, Temperature, Humidity, HCHO, TVOC, CO2, CO, O2, DateTime}) ->
    PM1dot0_str = ?CALHELP:get_pm1dot0_str(PM1dot0), 
    PM2dot5_str = ?CALHELP:get_pm2dot5_str(PM2dot5), 
    PM10_str = ?CALHELP:get_pm10_str(PM10), 
    Temperature_str = ?CALHELP:get_temperature_str(Temperature), 
    Humidity_str = ?CALHELP:get_humidity_str(Humidity), 
    HCHO_str = ?CALHELP:get_hcho_str(HCHO), 
    TVOC_str = ?CALHELP:get_tvoc_str(TVOC), 
    CO2_str = ?CALHELP:get_co2_str(CO2), 
    CO_str = ?CALHELP:get_co_str(CO), 
    O2_str = ?CALHELP:get_o2_str(O2),
    DateTime_tuple = ?CALHELP:get_datetime(DateTime),
    [PM1dot0_str, PM2dot5_str, PM10_str, Temperature_str, Humidity_str, HCHO_str, TVOC_str, CO2_str,CO_str, O2_str, ?HELP:getDateTimeStr(DateTime_tuple)].
        
get_ac_content_and_info({Electric_power, Voltage, Electric_current, Active_power, Temp, Power_system_frequency, Power_factor, Relay_status, Status_word, DateTime}) ->
    {Electric_power_str, Voltage_str, Electric_current_str, Active_power_str, Power_system_frequency_str, Power_factor_str, Relay_status_str, Status_word_str, DateTime_tuple} = get_normal_info({Electric_power, Voltage, Electric_current, Active_power, Power_system_frequency, Power_factor, Relay_status, Status_word, DateTime}),
    Temp_str = ?CALHELP:get_temp(Temp),
    [Electric_power_str, Voltage_str, Electric_current_str, Active_power_str, Temp_str, Power_system_frequency_str, Power_factor_str, Relay_status_str, Status_word_str, ?HELP:getDateTimeStr(DateTime_tuple)].

get_socket_content_and_info({Electric_power, Voltage, Electric_current, Active_power, Power_system_frequency, Power_factor, Relay_status, Status_word, DateTime}) ->
    {Electric_power_str, Voltage_str, Electric_current_str, Active_power_str, Power_system_frequency_str, Power_factor_str, Relay_status_str, Status_word_str, DateTime_tuple} = get_normal_info({Electric_power, Voltage, Electric_current, Active_power, Power_system_frequency, Power_factor, Relay_status, Status_word, DateTime}),
    [Electric_power_str, Voltage_str, Electric_current_str, Active_power_str, Power_system_frequency_str, Power_factor_str, Relay_status_str, Status_word_str, ?HELP:getDateTimeStr(DateTime_tuple)].

get_central_ac_content_and_info({Electric_power, Active_power, Temp, Relay_status, Temp_and_mode, Wind_speed_gears, Low_speed_used_time, Medium_speed_used_time, High_speed_used_time, Amount, Status_word, DateTime}) ->
    Electric_power_str = ?CALHELP:get_electric_power(Electric_power),
    Active_power_str = ?CALHELP:get_active_power(Active_power),
    Temp_str = ?CALHELP:get_temp(Temp),
    Relay_status_str = ?CALHELP:get_relay_status(Relay_status),
    Temp_and_mode_str = ?CALHELP:get_temp_and_mode(Temp_and_mode),
    Wind_speed_gears_str = ?CALHELP:get_wind_speed_gears(Wind_speed_gears),
    Low_speed_used_time_str = ?CALHELP:get_xxx_speed_used_time(Low_speed_used_time),
    Medium_speed_used_time_str = ?CALHELP:get_xxx_speed_used_time(Medium_speed_used_time),
    High_speed_used_time_str = ?CALHELP:get_xxx_speed_used_time(High_speed_used_time),
    Amount_str = ?CALHELP:get_amount(Amount),
    Status_word_str = ?CALHELP:get_status_word(Status_word),
    DateTime_tuple = ?CALHELP:get_datetime(DateTime),
    [Electric_power_str, Active_power_str, Temp_str, Relay_status_str, Temp_and_mode_str, Wind_speed_gears_str, Low_speed_used_time_str, Medium_speed_used_time_str, High_speed_used_time_str, Amount_str, Status_word_str, ?HELP:getDateTimeStr(DateTime_tuple)].

%% 兼容上报没有状态字和时标的插座
get_socket_content_and_info_1({Electric_power, Voltage, Electric_current, Active_power, Power_system_frequency, Power_factor, Relay_status}) ->
    Electric_power_str = ?CALHELP:get_electric_power(Electric_power),
    Voltage_str = ?CALHELP:get_votage(Voltage),
    Electric_current_str = ?CALHELP:get_electric_current(Electric_current),
    Active_power_str = ?CALHELP:get_active_power(Active_power),
    Power_system_frequency_str = ?CALHELP:get_power_system_frequency(Power_system_frequency),
    Power_factor_str = ?CALHELP:get_power_factor(Power_factor),
    Relay_status_str = ?CALHELP:get_relay_status(Relay_status),
    [Electric_power_str, Voltage_str, Electric_current_str, Active_power_str, Power_system_frequency_str, Power_factor_str, Relay_status_str].


%% 获取共有的属性信息
get_normal_info({Electric_power, Voltage, Electric_current, Active_power, Power_system_frequency, Power_factor, Relay_status, Status_word, DateTime}) ->
    Electric_power_str = ?CALHELP:get_electric_power(Electric_power),
    Voltage_str = ?CALHELP:get_votage(Voltage),
    Electric_current_str = ?CALHELP:get_electric_current(Electric_current),
    Active_power_str = ?CALHELP:get_active_power(Active_power),
    Power_system_frequency_str = ?CALHELP:get_power_system_frequency(Power_system_frequency),
    Power_factor_str = ?CALHELP:get_power_factor(Power_factor),
    Relay_status_str = ?CALHELP:get_relay_status(Relay_status),
    Status_word_str = ?CALHELP:get_status_word(Status_word),
    DateTime_tuple = ?CALHELP:get_datetime(DateTime),
    {Electric_power_str, Voltage_str, Electric_current_str, Active_power_str, Power_system_frequency_str, Power_factor_str, Relay_status_str, Status_word_str, DateTime_tuple}.
    
to_binary(String) when is_list(String) ->
    ?HELP:to_binary(String).

