-module(cluster_metrics).

-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([register_metrics/0, inc/1, set/2]).

register_metrics() ->
    prometheus_gauge:new([{name, cluster_expected_size},
                          {help, "expected size of the cluster"}]),
    prometheus_gauge:new([{name, cluster_size}, {help, "current size of the cluster"}]),
    prometheus_boolean:new([{name, cluster_green}, {help, "is the cluster is healthy?"}]),
    prometheus_boolean:new([{name, cluster_leader}, {help, "is the node is the leader?"}]),
    prometheus_boolean:new([{name, cluster_store_ready},
                            {help, "is the cluster store ready?"}]),
    prometheus_gauge:new([{name, cluster_store_subscriptions},
                          {help, "the number of subcriptions to the store"}]),
    prometheus_gauge:new([{name, cluster_store_size}, {help, "size of the cluster store"}]),
    prometheus_counter:new([{name, cluster_store_partitions},
                            {help, "total number of the cluster store partitions"}]),
    prometheus_counter:new([{name, cluster_store_connections},
                            {help, "total number of connections between cluster stores"}]),
    prometheus_counter:new([{name, cluster_store_disconnections},
                            {help, "total number of disconnections between cluster stores"}]),
    prometheus_counter:new([{name, cluster_leader_elections},
                            {help, "total number of leader elections"}]),

    ok.

inc(Metric) ->
    prometheus_counter:inc(Metric).

set(Metric, Value) when is_integer(Value) ->
    prometheus_gauge:set(Metric, Value);
set(Metric, Value) when is_boolean(Value) ->
    prometheus_boolean:set(Metric, Value).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    ok = pg2:join(cluster_events, self()),
    ClusterMembers = [node() | nodes()],
    set(cluster_expected_size, length(cluster:members())),
    set(cluster_store_ready, cluster_store:is_ready()),
    set(cluster_size, length(ClusterMembers)),
    set(cluster_green, cluster:state() =:= green),
    set(cluster_leader, cluster:is_leader()),
    {ok, []}.

handle_info({cluster, nodes_changed}, State) ->
    ClusterMembers = [node() | nodes()],
    set(cluster_size, length(ClusterMembers)),
    set(cluster_green, cluster:state() =:= green),
    {noreply, State};
handle_info({cluster, leader_changed}, State) ->
    set(cluster_leader, cluster:is_leader()),
    inc(cluster_leader_elections),
    {noreply, State};
handle_info(_, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_call(_, _, State) ->
    {reply, ok, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
