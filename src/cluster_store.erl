-module(cluster_store).

-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([maybe_init_store/0, is_ready/0, write/2, read/1, delete/1, size/0, info/0,
         purge/0, dump/0, foreach/1, observers/0, subscribe/1, unsubscribe/1, set_reconnect_method/1]).

-define(DEFAULT_RECONNECT_SECONDS, 60).
-define(TAB_NAME, cluster_items).

-record(cluster_items, {key, data}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    ok = pg2:join(cluster_events, self()),
    ok = maybe_init_store(),
    cluster_metrics:set(cluster_store_ready, is_ready()),
    {ok, _} = mnesia:subscribe(system),
    {ok, []}.

handle_info({cluster, leader_changed}, State) ->
    ok = maybe_init_store(),
    {noreply, State};
handle_info({cluster, nodes_changed}, State) ->
    ok = maybe_init_store(),
    {noreply, State};
handle_info({mnesia_system_event, {inconsistent_database, Context, Node}}, State) ->
    cluster_metrics:inc(cluster_store_partitions),
    lager:notice("CLUSTER store netsplit detected by Mnesia: ~p, ~p", [Context, Node]),
    {noreply, State};
handle_info({mnesia_system_event, {mnesia_down, Node}}, State) ->
    cluster_metrics:inc(cluster_store_disconnections),
    notify_local_observers(disconnected),
    lager:notice("CLUSTER store disconnected from ~p", [Node]),
    {noreply, State};
handle_info({mnesia_system_event, {mnesia_up, Node}}, State) ->
    cluster_metrics:inc(cluster_store_connections),
    notify_local_observers(connected),
    lager:notice("CLUSTER store connected to ~p", [Node]),
    {noreply, State};
handle_info({mnesia_table_event, {write, {cluster_items, K, V}, _}}, State) ->
    Size = table_info(cluster_items, size),
    cluster_metrics:set(cluster_store_size, Size),
    notify_local_observers(written, K, V),
    {noreply, State};
handle_info({mnesia_table_event, {delete, {cluster_items, K}, _}}, State) ->
    Size = table_info(cluster_items, size),
    cluster_metrics:set(cluster_store_size, Size),
    notify_local_observers(deleted, K, undefined),
    {noreply, State};
handle_info(Other, State) ->
    lager:notice("CLUSTER store ignoring ~p", [Other]),
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

maybe_init_store() ->
    init_store(cluster:is_leader(), cluster:state()).

init_store(true, _) ->
    case is_ready() of
        false ->
            lager:notice("CLUSTER STORE initializing store..."),
            AllNodes = cluster:members(),
            mnesia:change_config(extra_db_nodes, AllNodes),
            lager:notice("CLUSTER STORE creating table ~p...", [?TAB_NAME]),

            mnesia:create_table(cluster_items,
                                [{type, set},
                                 {attributes, record_info(fields, cluster_items)},
                                 {ram_copies, AllNodes}]),

            lager:notice("CLUSTER STORE waiting for ~p to be ready...", [?TAB_NAME]),
            ok = mnesia:wait_for_tables([cluster_items], 5000),

            ConflictResolutionStrategy = conflict_resolution_strategy(),
            lager:notice("CLUSTER store using conflict resolution: ~p", [ConflictResolutionStrategy]),
            mnesia:write_table_property(kvs, {reunion_compare, ConflictResolutionStrategy}),
            {ok, _} = subscribe_tab(cluster_items, 5),
            lager:notice("CLUSTER STORE created and subscribed to table ~p", [?TAB_NAME]);
        true ->
            lager:notice("CLUSTER STORE table ~p already exists", [?TAB_NAME])
    end;
init_store(false, green) ->
    lager:notice("CLUSTER store adding table copy"),
    case mnesia:add_table_copy(cluster_items, node(), ram_copies) of 
        {aborted,{already_exists, _, _ }} ->
            ok;
        ok ->
            ok
    end,
    notify_local_observers(joined);
init_store(_, _) ->
    ok.

subscribe_tab(_, 0) ->
    {error, error_subscribing};
subscribe_tab(Tab, RetriesLeft) ->
    case mnesia:subscribe({table, Tab, simple}) of
        {error, {not_active_local, Tab}} ->
            %% Set up RAM replica on this node, and then try one more time
            mnesia:add_table_copy(Tab, node(), ram_copies),
            subscribe_tab(Tab, RetriesLeft - 1);
        {error, _} = Other ->
            Other;
        {ok, _} = Ok ->
            Ok
    end.

write(Key, Value) ->
    write(#cluster_items{key = Key, data = Value}).

write(#cluster_items{} = Item) ->
    mnesia:activity(transaction, fun() -> mnesia:write(Item) end).

read(Key) ->
    mnesia:activity(transaction,
                    fun() ->
                       case mnesia:read({cluster_items, Key}) of
                           [] -> {error, not_found};
                           [{_, _, Value}] -> {ok, Value}
                       end
                    end).

delete(Key) ->
    mnesia:activity(transaction,
                    fun() ->
                       case mnesia:read({cluster_items, Key}) of
                           [] -> {error, not_found};
                           [_ | _] -> mnesia:delete(cluster_items, Key, write)
                       end
                    end).

size() ->
    table_info(cluster_items, size).

info() ->
    ActiveReplicas = table_info(cluster_items, active_replicas),
    AllReplicas = table_info(cluster_items, all_nodes),
    RamCopies = table_info(cluster_items, ram_copies),

    #{size => size(),
      ram_copies => cluster:hosts(RamCopies),
      replicas =>
          #{all => cluster:hosts(AllReplicas), active => cluster:hosts(ActiveReplicas)}}.

table_info(Tab, Kind) ->
    try
        mnesia:table_info(Tab, Kind)
    catch
        _:_ ->
            []
    end.

purge() ->
    {atomic, ok} = mnesia:clear_table(cluster_items),
    ok.

foreach(UserFun) ->
    foreach(UserFun, {0, 0}, mnesia:dirty_first(cluster_items)).

foreach(_UserFun, Acc, '$end_of_table') ->
    Acc;
foreach(UserFun, {Total, Failed}, Key) ->
    [#cluster_items{key = Key, data = Value}] = mnesia:dirty_read(cluster_items, Key),
    case UserFun(Key, Value) of
        ok ->
            foreach(UserFun, {Total + 1, Failed}, mnesia:dirty_next(cluster_items, Key));
        {ok, _} ->
            foreach(UserFun, {Total + 1, Failed}, mnesia:dirty_next(cluster_items, Key));
        _ ->
            foreach(UserFun, {Total, Failed + 1}, mnesia:dirty_next(cluster_items, Key))
    end.

subscribe(Pid) ->
    case lists:member(Pid, observers()) of
        false ->
            ok = pg2:join(cluster_store_events, Pid),
            cluster_metrics:set(cluster_store_subscriptions, length(observers()));
        true ->
            ok
    end.

unsubscribe(Pid) ->
    pg2:leave(cluster_store_events, Pid),
    cluster_metrics:set(cluster_store_subscriptions, length(observers())).

observers() ->
    pg2:get_members(cluster_store_events).

notify_local_observers(Event) ->
    LocalMembers = pg2:get_local_members(cluster_store_events),
    lager:notice("CLUSTER store notifying ~p observers: ~p", [length(LocalMembers), Event]),
    [Pid ! {cluster_store, Event} || Pid <- LocalMembers].

notify_local_observers(Event, K, V) ->
    LocalMembers = pg2:get_local_members(cluster_store_events),
    lager:notice("CLUSTER store notifying ~p observers: ~p", [length(LocalMembers), Event]),
    [Pid ! {cluster_store, Event, K, V} || Pid <- LocalMembers].

is_ready() ->
    case table_info(cluster_items, active_replicas) of
        [] ->
            false;
        _ ->
            true
    end.

dump() ->
    foreach(fun(K, V) ->
                    lager:notice("CLUSTER store dumping ~p => ~p", [K, V]),
                    ok
            end).

set_reconnect_method(manual) ->
    lager:notice("CLUSTER store disabled automatic reconnects"),
    application:set_env(reunion, reconnect, never);

set_reconnect_method(auto) ->
    lager:notice("CLUSTER store enabled automatic reconnects"),
    application:set_env(reunion, reconnect, ?DEFAULT_RECONNECT_SECONDS).

conflict_resolution_strategy() ->
    {reunion_lib, last_modified, []}.
