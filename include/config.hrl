%% Config info

%% helper util macro
-define(HELPER, helper_util).

-define(HELP, help).

-define(CALHELP, calHelp).

%% Gateway/Meter id bytes_size macro
-define(DEVICE_ID_BYTES_SIZE, 6).

%% Record separator
-define(RS, "\r\n").

%% Active report datafieldflag list

%% 动态库解析特征数据域
-define(LIBJSON_DATAFIELDFLAG_LIST, 
        [
         %% 分体空调
         <<16#38, 16#32, 16#FF, 16#EF>>,
         %% 四路灯控面板 插座表 灯控计量模块
         <<16#34, 16#32, 16#FF, 16#EF>>,
         %% 环境检测仪
         <<16#32, 16#33, 16#21, 16#EF>>,
         %% 中央空调
         <<16#38, 16#32, 16#DD, 16#EF>>,
         %% 充电桩
         <<16#34, 16#34, 16#93, 16#37>>,
         %%充电桩继电器异常
         <<16#34, 16#35, 16#93, 16#37>>,
         %% 光照传感器
         <<16#34, 16#33, 16#FF, 16#EF>>,
         %% 计量灯控控制上报
         <<16#35, 16#36, 16#0F, 16#EF>>,
         %% 单纯群控，复合场景1，2开关的四个按键控制上报
         <<16#34, 16#35, 16#0F, 16#EF>>,
         <<16#35, 16#35, 16#0F, 16#EF>>,
         <<16#36, 16#35, 16#0F, 16#EF>>,
         <<16#37, 16#35, 16#0F, 16#EF>>
        ]).

%% Data msg datafidldflag list
%% 定时上报
-define(DATAMSG_DATAFIELDFLAG_LIST,
        [
         %% 分体空调
         <<16#38, 16#32, 16#FF, 16#EF>>,
         %% 四路灯控面板 插座表 灯控计量模块
         <<16#34, 16#32, 16#FF, 16#EF>>,
         %% 环境检测仪
         <<16#32, 16#33, 16#21, 16#EF>>,
         %% 中央空调
         <<16#38, 16#32, 16#DD, 16#EF>>,
         %% 光照传感器
         <<16#34, 16#33, 16#FF, 16#EF>>,
         %%充电桩继电器异常
         <<16#34, 16#35, 16#93, 16#37>>,
         %% 充电桩
         <<16#34, 16#34, 16#93, 16#37>>
        ]).

%% 控制上报
-define(CONTROL_DATAFIELDFLAG_LIST,
        [
         %% 计量灯控控制上报
         <<16#35, 16#36, 16#0F, 16#EF>>,
         %% 单纯群控，复合场景1，2开关的四个按键控制上报
         <<16#34, 16#35, 16#0F, 16#EF>>,
         <<16#35, 16#35, 16#0F, 16#EF>>,
         <<16#36, 16#35, 16#0F, 16#EF>>,
         <<16#37, 16#35, 16#0F, 16#EF>>,
         %% 策略控制上报
         <<16#34, 16#3B, 16#0F, 16#EF>>,
         <<16#35, 16#3B, 16#0F, 16#EF>>,
         <<16#36, 16#3B, 16#0F, 16#EF>>,
         <<16#37, 16#3B, 16#0F, 16#EF>>
        ]).

%% WARN msg datafieldflag list
-define(WARNMSG_DATAFIELDFLAG_LIST,
        [
         %% 功率超限报警
         <<16#35, 16#32, 16#FF, 16#EF>>,
         %%门锁上下线告警
         <<16#88, 16#AA, 16#FF, 16#EF>>
        ]).

%% Long length datafieldflag list
-define(LONG_LENGTH_DATAFIELDFLAG_LIST,
        [
         %% 设置/查询网关内存储单灯安装信息命令
         <<16#34, 16#3A, 16#0F, 16#EF>>,
         %% PWM数据查询
         <<16#63, 16#33, 16#21, 16#EF>>,
         %% 电网参数数据块查询
         <<16#64, 16#33, 16#21, 16#EF>>,
         %% 网关聚合结果上报
         <<16#39, 16#3A, 16#0F, 16#EF>>
        ]).

%%NIF dir
-define(NIFJSONMODULE, libnifjson).

%% Share_lib dir
-define(SHARE_LIB_DIR, "./share_lib").
-define(SOLIBNAME, "./share_lib/libdata_report.so").

%% Listen options config filepath
-define(LISTEN_OPTS_TABLE, connector_listen_opts).
-define(LISTEN_OPTS_FILEPATH, "rel/listen.config").

%% Env options config filepath
-define(ENV_OPTS_FILEPATH, "rel/env.config").

%% Timer options config filepath
-define(TIME_OPTS_FILEPATH, "rel/time.config").

%% Computer options config filepath
-define(COMPUTER_OPTS_TABLE, connector_computer_opts).
-define(COMPUTER_OPTS_FILEPATH, "rel/computer.config").

%% Network config file
-define(NETWORK_CONFIG_FILE, "/etc/sysconfig/network-scripts/ifcfg-eth0").
-define(IPADDR, "IPADDR").
-define(NETMASK, "NETMASK").
-define(GATEWAY, "GATEWAY").
-define(DNS1, "DNS1").

%% 计量表数据块上报标识
-define(DATAMSG, "dataMsg").
%% 采集器上线标识
-define(STATUSMSG, "statusMsg").
%% 计量表功率超限标识
-define(WARNMSG, "warnMsg").
%% 控制上报
-define(CONTROLMSG, "controlMsg").

%%EasyIot平台登录接口连接
-define(LoginUrl, "https://www.easy-iot.cn/idev/3rdcap/server/login").
%%EasyIot平台设备数据上报回调接口
-define(UrtCommand, "https://www.easy-iot.cn/idev/3rdcap/dev-control/urt-command").
%%云识图片上传接口
-define(WarterUrl, "http://120.76.220.251:5678/water/hardware/getDigit").

%% Db server client request path to fun 
-define(PATH_TO_FUN,
        [
         {<<"login">>, login},
         {<<"registUser">>, regist_user},
         {<<"cancelUser">>, cancel_user},

         {<<"getHistoryRecords">>, get_history_records},
         {<<"getReportDataInfoTotalRow">>, get_reportdata_info_total_row},

         {<<"addTask">>, add_task},
         {<<"deleteTask">>, delete_task},
         {<<"updateTask">>, update_task},
         {<<"getTask">>, get_task},

         {<<"addDeviceTypeInfo">>, add_device_type_info},
         {<<"deleteDeviceTypeInfo">>, delete_device_type_info},
         {<<"getDeviceTypeInfo">>, get_device_type_info},
         {<<"getCollectorTypeInfo">>, get_collector_type_info},
         {<<"getRepeatersTypeInfo">>, get_repeaters_type_info},
         {<<"getMeterTypeInfo">>, get_meter_type_info},

         {<<"addCollector">>, add_collector},
         {<<"updateCollector">>, update_collector},
         {<<"deleteCollector">>, delete_collector},
         {<<"forceDeleteCollector">>, force_delete_collector},
         {<<"updateCollectorAndConfig">>, update_collector_and_config},

         {<<"addMeter">>, add_meter},
         {<<"updateMeter">>, update_meter},
         {<<"deleteMeter">>, delete_meter},
         {<<"deleteMeterByCollector">>, delete_meter_by_collector},

         {<<"getCollectorInfoTotalRow">>, get_collector_info_total_row},
         {<<"getCollectorInfo">>, get_collector_info},

         {<<"getRepeatersInfoTotalRow">>, get_repeaters_info_total_row},
         {<<"getRepeatersInfo">>, get_repeaters_info},
         
         {<<"getMeterInfoTotalRow">>, get_meter_info_total_row},
         {<<"getMeterInfo">>, get_meter_info},

         {<<"getMeterNetworkByCollector">>, get_meter_network_by_collector},
         {<<"getCollectorInfoByEqptType">>, get_collector_info_by_eqpt_type},
         {<<"getRepeatersInfoByEqptType">>, get_repeaters_info_by_eqpt_type},
         {<<"getEqptInfoByID">>, get_eqpt_info_by_id},
        
         {<<"batchAddCollector">>, batch_add_collector},
         {<<"batchAddMeter">>, batch_add_meter},
         {<<"batchGetMeter">>, batch_get_meter},

         {<<"getVersion">>, get_version},
         {<<"getStatus">>, get_status},
         {<<"getStatusByUser">>, get_status_by_user},

         {<<"getNetwork">>, get_network},
         {<<"updateNetwork">>, update_network},
         {<<"restartNetwork">>, restart_network},

         {<<"report-dev-callback">>, report_dev_callback},
         {<<"cmd-response-callback">>, cmd_response_callback},
         {<<"hello">>, hello}
        ]). 


