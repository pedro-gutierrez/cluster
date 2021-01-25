-module(cluster_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    ok = pg2:create(cluster_events),
    ok = pg2:create(cluster_store_events),
    ok = cluster_metrics:register_metrics(),
    cluster_sup:start_link().

stop(_) ->
    ok.
