-module(cluster).

-export([members/0, start/0, join/1, state/0, state/1, neighbours/0, leader/0,
         is_leader/0, http_port/0, size/0, service/0, namespace/0, join/0, leave/1, env/1,
         set_recovery/1, recovery/0, notify_observers/1, notify_local_observers/1, subscribe/1,
         observers/0, host/0, host/1, hosts/1, node/1]).


state() ->
    state(neighbours()).

state([]) ->
    green;
state(Neighbours) ->
    state(length(nodes()), length(Neighbours)).

state(0, _) ->
    red;
state(N, N) ->
    green;
state(_, _) ->
    yellow.

start() ->
    Neighbours = neighbours(),
    lager:notice("CLUSTER config: neighbours=~p, node=~p~n", [Neighbours, node()]),
    ok.

neighbours() ->
    members() -- [node()].

size() ->
    erlang:list_to_integer(env("CLUSTER_SIZE")).

namespace() ->
    erlang:list_to_binary(env("CLUSTER_NAMESPACE")).

service() ->
    erlang:list_to_binary(env("CLUSTER_SERVICE")).

env(Name) ->
    case os:getenv(Name) of
        false ->
            throw("missing env variable: " ++ Name);
        Value ->
            Value
    end.

members() ->
    Ns = namespace(),
    Service = service(),
    Size = size(),
    nodes(Service, Ns, Size).

nodes(_, _, 0) ->
    [];
nodes(Service, Ns, Size) ->
    lists:map(fun(Id) -> node(Service, Ns, erlang:integer_to_binary(Id - 1)) end,
              lists:seq(1, Size)).

node(Service, Ns, Id) ->
    Host =
        <<Service/binary,
          "-",
          Id/binary,
          ".",
          Service/binary,
          ".",
          Ns/binary,
          ".svc.cluster.local">>,
    list_to_atom(binary_to_list(<<Service/binary, "@", Host/binary>>)).

leader() ->
    case global:whereis_name(cluster_leader) of
        undefined ->
            none;
        Pid ->
            erlang:node(Pid)
    end.

is_leader() ->
    leader() == node().

http_port() ->
    application:get_env(cluster, http_port, 8080).

join() ->
    Neighbours = neighbours(),
    join(Neighbours).

join(Nodes) ->
    [net_adm:ping(N) || N <- Nodes].

leave(normal) ->
    rpc:eval_everywhere(erlang, disconnect_node, [node()]),
    ok;
leave(halt) ->
    erlang:halt().

set_recovery(Recovery) ->
    rpc:eval_everywhere(cluster_monitor, set_recovery, [Recovery]),
    ok.

recovery() ->
    cluster_monitor:recovery().

subscribe(Pid) ->
    pg2:join(cluster_events, Pid).

notify_local_observers(Event) ->
    LocalMembers = pg2:get_local_members(cluster_events),
    lager:notice("CLUSTER notifying ~p observers: ~p", [length(LocalMembers), Event]),
    [Pid ! {cluster, Event} || Pid <- LocalMembers].

notify_observers(Event) ->
    Members = observers(),
    lager:notice("CLUSTER notifying ~p observers: ~p", [length(Members), Event]),
    [Pid ! Event || Pid <- Members].

observers() ->
    pg2:get_members(cluster_events).

host() ->
    host(node()).

host(Node0) ->
    Node = erlang:atom_to_binary(Node0, latin1),
    [_, Fqdn] = binary:split(Node, <<"@">>),
    [Host, _] = binary:split(Fqdn, <<".">>),
    Host.

hosts(Nodes) ->
    lists:map(fun host/1, Nodes).

node(Host) ->
    Service = cluster:service(),
    Ns = cluster:namespace(),
    Node =
        <<Service/binary,
          "@",
          Host/binary,
          ".",
          Service/binary,
          ".",
          Ns/binary,
          ".svc.cluster.local">>,
    try
        erlang:list_to_existing_atom(
            erlang:binary_to_list(Node))
    catch
        _:_ ->
            unknown
    end.
