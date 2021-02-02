-module(cluster_http_host).

-export([init/2]).

init(Req, _) ->
    Method = cowboy_req:method(Req),
    Host = cowboy_req:binding(host, Req),
    do(Method, Host, Req).

do(<<"POST">>, Host, Req) ->
    host(join, cluster:node(Host), Host, Req);
do(<<"DELETE">>, Host, Req) ->
    Mode = leave_mode(Req),
    host(Mode, cluster:node(Host), Host, Req).

host(_, unknown, Host, Req) ->
    cluster_http:not_found(#{host => Host}, Req);
host(join, Node, Host, Req) ->
    rpc:call(Node, cluster, join, []),
    cluster_http:ok(#{join => Host}, Req);
host(leave, Node, Host, Req) ->
    rpc:eval_everywhere(cluster, leave, [normal]),
    lager:notice("CLUSTER command for ~p to leave the cluster", [Node]),
    lager:notice("CLUSTER nodes after leaving cluster: ~p", [nodes()]),
    cluster_http:ok(#{disconnect => Host}, Req);
host(halt, Node, Host, Req) ->
    rpc:async_call(Node, cluster, leave, [halt]),
    cluster_http:ok(#{halt => Host}, Req).

leave_mode(Req) ->
    case cowboy_req:match_qs([mode], Req) of
        #{mode := <<"halt">>} ->
            halt;
        _ ->
            leave
    end.
