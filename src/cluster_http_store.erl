-module(cluster_http_store).

-export([init/2]).

init(Req, _) ->
    Method = cowboy_req:method(Req),
    do(Method, Req).

do(<<"DELETE">>, Req) ->
    ok = cluster_store:purge(),
    cluster_http:ok(Req).
