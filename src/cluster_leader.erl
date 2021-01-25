-module(cluster_leader).

-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([attempt_leader/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    ok = pg2:join(cluster_events, self()),
    attempt_leader(),
    {ok, []}.

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
    case global:register_name(cluster_leader, self()) of
        yes ->
            lager:notice("CLUSTER new leader is ~p", [node()]),
            cluster:notify_observers({cluster, leader_changed});
        no ->
            Pid = global:whereis_name(cluster_leader),
            lager:notice("CLUSTER existing leader remains ~p", [node(Pid)])
    end;
attempt_leader(true) ->
    ok.
