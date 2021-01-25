-module(cluster_sup).

-behaviour(supervisor).

-export([init/1, start_link/0]).

start_link() ->
    ChildsSpec =
        [worker(cluster_monitor, [cluster:neighbours()]),
         worker(cluster_http, [cluster:http_port()]),
         worker(cluster_leader),
         worker(cluster_store),
         worker(cluster_metrics)],
    supervisor:start_link({local, ?MODULE}, ?MODULE, {{one_for_one, 10, 60}, ChildsSpec}).

init(ChildSpecs) ->
    {ok, ChildSpecs}.

worker(Mod) ->
    worker(Mod, []).

worker(Mod, Args) ->
    {Mod, {Mod, start_link, Args}, permanent, 5000, worker, [Mod]}.
