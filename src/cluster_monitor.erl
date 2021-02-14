-module(cluster_monitor).

-behaviour(gen_server).

-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([disconnect/0, set_recovery/1, recovery/0, info/0]).

-define(DISCONNECT_STRATEGY, {1000, 10}).

start_link(Neighbours) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Neighbours, []).

set_recovery(Recovery) ->
    gen_server:call(?MODULE, {set_recovery, Recovery}).

recovery() ->
    gen_server:call(?MODULE, recovery).

disconnect() ->
    gen_server:call(?MODULE, disconnect).

info() ->
    gen_server:call(?MODULE, info).

init([]) ->
    lager:notice("CLUSTER is disabled"),
    {ok,
     #{recovery => manual,
       disconnecting => false,
       disconnect_timer => undefined,
       state => disabled,
       neighbours => []}};
init(Neighbours) ->
    ok = pg2:create(cluster_events),
    global_group:monitor_nodes(true),
    ClusterState = cluster:state(Neighbours),
    State =
        #{recovery => auto,
          disconnect_retries => 0,
          disconnecting => false,
          disconnect_timer => undefined,
          state => ClusterState,
          neighbours => Neighbours},
    reconnect_nodes(State),
    lager:notice("CLUSTER is ~p (neighbours: ~p)~n", [ClusterState, Neighbours]),
    {ok, State}.

handle_info({nodeup, N},
            #{disconnecting := Disconnecting, neighbours := Neighbours} = State) ->
    ClusterState = cluster:state(Neighbours),
    lager:notice("CLUSTER is ~p (~p is UP)~n", [ClusterState, N]),
    case Disconnecting of
        false ->
            notify_nodes_changed();
        true ->
            lager:notice("CLUSTER we are leaving the cluster. Won't notify about ~p being up",
                         [nodes(), N])
    end,
    {noreply, State#{state => ClusterState}};
handle_info({nodedown, N}, #{neighbours := Neighbours} = State) ->
    ClusterState = cluster:state(Neighbours),
    lager:notice("CLUSTER is ~p (~p is DOWN)~n", [ClusterState, N]),
    notify_nodes_changed(),
    reconnect_nodes(State),
    {noreply, State#{state => ClusterState}};
handle_info(reconnect_nodes, #{recovery := auto, neighbours := Neighbours} = State) ->
    case cluster:state(Neighbours) of
        red ->
            lager:notice("CLUSTER still red and recovery is auto"),
            reconnect_nodes(State);
        green ->
            ok
    end,
    {noreply, State};
handle_info(reconnect_nodes, #{neighbours := Neighbours} = State) ->
    case cluster:state(Neighbours) of
        red ->
            lager:notice("CLUSTER still red, but recovery is manual. Recovery won't be "
                         "attempted"),
            ok;
        _ ->
            ok
    end,
    {noreply, State};
handle_info(disconnect,
            #{disconnecting := true, disconnect_retries := RetriesLeft} = State) ->
    case nodes() of
        [] ->
            lager:notice("CLUSTER now disconnected from cluster"),
            {noreply, State};
        Nodes ->
            {Timeout, MaxRetries} = ?DISCONNECT_STRATEGY,
            case RetriesLeft of
                0 ->
                    lager:warning("CLUSTER still connected to ~p ~pms after leaving the cluster",
                                  [Nodes, Timeout * MaxRetries]),
                    State2 = State#{disconnecting => false},
                    {noreply, State2};
                _ ->
                    lager:notice("CLUSTER still disconnecting from ~p", [Nodes]),
                    [erlang:disconnect_node(N) || N <- Nodes],
                    {Timeout, _} = ?DISCONNECT_STRATEGY,
                    Timer = erlang:send_after(Timeout, self(), disconnect),
                    State2 = State#{disconnect_timer => Timer},
                    {noreply, State2}
            end
    end;
handle_info(_, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_call(disconnect, _, #{state := disabled} = State) ->
    {reply, {ok, nodes()}, State};
handle_call(disconnect, _, State) ->
    lager:notice("CLUSTER disconnecting from cluster..."),
    [erlang:disconnect_node(N) || N <- nodes()],
    {Timeout, Retries} = ?DISCONNECT_STRATEGY,
    State2 = State#{disconnecting => true, disconnect_retries := Retries - 1},
    Timer = erlang:send_after(Timeout, self(), disconnect),
    State3 = State2#{disconnect_timer => Timer},
    {reply, {ok, nodes()}, State3};
handle_call({set_recovery, Recovery}, _, State) ->
    State2 = State#{recovery => Recovery},
    cluster_store:set_reconnect_method(Recovery),
    lager:notice("CLUSTER recovery is set to ~p", [Recovery]),
    reconnect_nodes(State2),
    State3 = cancel_disconnect_timer(Recovery, State2),
    {reply, ok, State3};
handle_call(recovery, _, #{recovery := Recovery} = State) ->
    {reply, Recovery, State};
handle_call(info, _, State) ->
    {reply, State, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(Reason, State) ->
    lager:warning("CLUSTER terminating with reason: ~p, and state: ~p~n", [Reason, State]),
    ok.

notify_nodes_changed() ->
    cluster:notify_observers({cluster, nodes_changed}).

reconnect_nodes(#{neighbours := Neighbours, recovery := auto}) ->
    % Under certain network conditions, pinging other networks
    % might be slow or timeout. If so, we want to do in a separate
    % context without blocking this cluster monitor
    lager:notice("CLUSTER auto recovery triggered"),
    timer:send_after(5000, self(), reconnect_nodes),
    spawn_link(fun() -> cluster:join(Neighbours) end);
reconnect_nodes(_) ->
    ok.

cancel_disconnect_timer(manual, State) ->
    State;
cancel_disconnect_timer(auto, #{disconnect_timer := undefined} = State) ->
    State;
cancel_disconnect_timer(auto, #{disconnect_timer := Timer} = State) ->
    erlang:cancel_timer(Timer),
    State#{disconnecting => false, disconnect_timer => undefined}.
