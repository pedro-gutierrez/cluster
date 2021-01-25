-module(cluster_http_readiness).

-export([init/2]).

init(Req, _) ->
    case cluster:is_leader() of
        true ->
            cluster_http:ok(Req);
        _ ->
            cluster_http:unavailable(Req)
    end.
