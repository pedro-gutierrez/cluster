-module(cluster_http).

-behaviour(gen_server).

-export([host/0, host/1, hosts/1, node/1, created/1, not_found/2, ok/1, ok/2,
         unavailable/1, reply/3]).
-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link(Port) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Port, []).

init(Port) ->
    {ok, Pid} = start_server(Port),
    {ok, #{server => Pid, port => Port}}.

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

start_server(Port) ->
    Dispatch =
        cowboy_router:compile([{'_',
                                [{"/", cluster_http_info, []},
                                 {"/metrics", cluster_http_metrics, []},
                                 {"/hosts/:host", cluster_http_host, []},
                                 {"/keys", cluster_http_store, []},
                                 {"/keys/:key", cluster_http_store_key, []},
                                 {"/readiness", cluster_http_readiness, []}]}]),
    cowboy:start_http(http, 1, [{port, Port}], [{env, [{dispatch, Dispatch}]}]).

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

created(Req) ->
    reply(201, #{}, Req).

not_found(Data, Req) ->
    reply(404, Data, Req).

ok(Req) ->
    ok(#{}, Req).

ok(Data, Req) ->
    reply(200, Data, Req).

unavailable(Req) ->
    unavailable(#{}, Req).

unavailable(Data, Req) ->
    reply(503, Data, Req).

reply(Status, {text, Body}, Req0) ->
    Host = host(node()),
    Req = cowboy_req:reply(Status,
                           [{<<"host">>, Host}, {<<"content-type">>, <<"text/plain">>}],
                           Body,
                           Req0),
    {ok, Req, []};
reply(Status, Body, Req0) ->
    Host = host(node()),
    RawBody = jiffy:encode(Body),
    Req = cowboy_req:reply(Status,
                           [{<<"host">>, Host}, {<<"content-type">>, <<"application/json">>}],
                           RawBody,
                           Req0),
    {ok, Req, []}.
