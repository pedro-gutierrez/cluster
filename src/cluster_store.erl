-module(cluster_store).

-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([maybe_init_store/0, is_ready/0, write/2, read/1, info/0, purge/0, observers/0,
         subscribe/1, unsubscribe/1]).

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
handle_info({mnesia_table_event, {write, {cluster_items, K, V}, _}}, State) ->
    Size = table_info(cluster_items, size),
    cluster_metrics:set(cluster_store_size, Size),
    notify_local_observers(K, V),
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

            lager:notice("CLUSTER STORE waiting for ~p to be ready..." , [?TAB_NAME]),
            ok = mnesia:wait_for_tables([cluster_items], 5000),
            mnesia:write_table_property(kvs, {reunion_compare, {reunion_lib, last_modified, []}}),
            {ok, _} = subscribe_tab(cluster_items, 5),
            lager:notice("CLUSTER STORE created and subscribed to table ~p", [?TAB_NAME]);
        true ->
            lager:notice("CLUSTER STORE table ~p already exists", [?TAB_NAME])
    end;
init_store(false, green) ->
    mnesia:add_table_copy(cluster_items, node(), ram_copies);
init_store(_, _) ->
    ok.


subscribe_tab(_, 0) ->
    {error, error_subscribing};

subscribe_tab(Tab, RetriesLeft) ->
    case mnesia:subscribe({table, Tab, simple}) of
        {error, {not_active_local, Tab}} ->
            %% Set up RAM replica on this node, and then try one more time
            mnesia:add_table_copy(Tab, node(), ram_copies),
            subscribe_tab(Tab, RetriesLeft-1);
        {error, _} = Other -> 
            Other;

        {ok, _} = Ok -> Ok
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

info() ->
    Size = table_info(cluster_items, size),
    ActiveReplicas = table_info(cluster_items, active_replicas),
    AllReplicas = table_info(cluster_items, all_nodes),
    RamCopies = table_info(cluster_items, ram_copies),

    #{size => Size,
      ram_copies => cluster_http:hosts(RamCopies),
      replicas =>
          #{all => cluster_http:hosts(AllReplicas), active => cluster_http:hosts(ActiveReplicas)}}.

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

notify_local_observers(K, V) ->
    LocalMembers = pg2:get_local_members(cluster_store_events),
    lager:notice("CLUSTER store notifying ~p observers", [length(LocalMembers)]),
    [Pid ! {cluster_store, written, K, V} || Pid <- LocalMembers].

is_ready() ->
    case table_info(cluster_items, active_replicas) of
        [] ->
            false;
        _ ->
            true
    end.
