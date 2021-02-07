-module(cluster_http_info).

-export([init/2, info/0]).

init(Req, _) ->
    Method = cowboy_req:method(Req),
    do(Method, Req).

do(<<"GET">>, Req) ->
    Info = info(),
    cluster_http:ok(Info, Req);
do(<<"PUT">>, Req) ->
    Recovery = recovery(Req),
    rpc:eval_everywhere(cluster, set_recovery, [Recovery]),
    do(<<"GET">>, Req).

recovery(Req) ->
    case cowboy_req:match_qs([{recovery, [], <<"auto">>}], Req) of
        #{recovery := <<"manual">>} ->
            manual;
        _ ->
            auto
    end.

info() ->
    Leader =
        cluster:host(
            cluster:leader()),
    Hosts = cluster:hosts([node() | nodes()]),
    #{recovery => cluster:recovery(),
                        state => cluster:state(),
                        size => cluster:size(),
                        leader => Leader,
                        nodes => Hosts,
                        store => cluster_store:info()}.
