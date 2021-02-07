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
host(join, Node, _, Req) ->
    rpc:call(Node, cluster, join, []),
    Info = cluster_http_info:info(),
    cluster_http:ok(Info, Req);
host(leave, _, _, Req) ->
    cluster:leave(normal),
    lager:notice("CLUSTER nodes after leaving cluster: ~p", [nodes()]),
    Info = cluster_http_info:info(),
    cluster_http:ok(Info, Req);
host(halt, Node, _, Req) ->
    rpc:async_call(Node, cluster, leave, [halt]),
    Info = cluster_http_info:info(),
    cluster_http:ok(Info, Req).

leave_mode(Req) ->
    case cowboy_req:match_qs([{mode, [], <<"halt">>}], Req) of
        #{mode := <<"halt">>} ->
            halt;
        _ ->
            leave
    end.
