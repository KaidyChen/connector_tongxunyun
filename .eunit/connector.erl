%%%======================================================
%% @doc Connector API to start and stop application
%% @author GIHO Lee
%% @datetime 2016-12-27 16:30:00
%%%======================================================

-module(connector).

%% eunit
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start/0,
         stop/0
]).

-define(APP, ?MODULE).
-define(APPLIST, [lager, ?APP]).

%% @doc Start application
start() ->
    [app_util:start_app(App) || App <- ?APPLIST],
    ok.

%% @doc Stop application
stop() ->
    [app_util:stop_app(App) || App <- ?APPLIST],
    ok.

-ifdef(EUNIT).

start_test_() ->
    start(),
    ?assert(1 =:= 1),
    [
     ?assertEqual(ok, application:ensure_started(lager)),
     ?assertEqual(ok, application:ensure_started(?APP))
    ].

-endif.
