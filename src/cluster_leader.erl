-module(cluster_leader).

-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, handle_continue/2, terminate/2,
         code_change/3]).
-export([attempt_leader/0, uptime/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    ok = pg2:join(cluster_events, self()),
    {ok, #{}, {continue, attempt_leader}}.

handle_continue(attempt_leader, State) ->
    attempt_leader(),
    {noreply, State}.

handle_info({cluster, nodes_changed}, State) ->
    attempt_leader(),
    {noreply, State};
handle_info(_, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_call(_, _, State) ->
    {reply, ok, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(Reason, _State) ->
    lager:notice("CLUSTER leader process terminated with reason ~p~n", [Reason]),
    ok.

attempt_leader() ->
    attempt_leader(cluster:is_leader()).

attempt_leader(false) ->
    case global:register_name(cluster_leader, self(), fun resolve/3) of
        yes ->
            lager:notice("CLUSTER new leader is ~p", [node()]),
            cluster:notify_observers({cluster, leader_changed});
        no ->
            Pid = global:whereis_name(cluster_leader),
            lager:notice("CLUSTER existing leader remains ~p", [node(Pid)])
    end,
    ok;
attempt_leader(true) ->
    ok.

uptime() ->
    erlang:monotonic_time() - erlang:system_info(start_time).

resolve(_, Pid1, Pid2) ->
    Node1 = node(Pid1),
    Node2 = node(Pid2),
    Up1 = rpc:call(Node1, ?MODULE, uptime, []),
    Up2 = rpc:call(Node2, ?MODULE, uptime, []),
    Winner = resolve(Pid1, Up1, Pid2, Up2),
    lager:notice("CLUSTER name conflict winner ~p", [node(Winner)]),
    Winner.

resolve(Pid1, Up1, Pid2, Up2) when is_integer(Up1) andalso is_integer(Up2) ->
    case Up1 > Up2 of 
        true ->
            Pid1;
        false ->
            Pid2
    end;

resolve(Pid1, _, _, _) -> Pid1.

