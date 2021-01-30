-module(cluster_http_store_key).

-export([init/2]).

init(Req, _) ->
    Method = cowboy_req:method(Req),
    Key = cowboy_req:binding(key, Req),
    do(Method, Key, Req).

do(<<"GET">>, Key, Req) ->
    case cluster_store:read(Key) of
        {ok, Value} ->
            cluster_http:ok(#{key => Key, value => Value}, Req);
        {error, not_found} ->
            cluster_http:not_found(#{key => Key}, Req)
    end;
do(<<"DELETE">>, Key, Req) ->
    case cluster_store:delete(Key) of
        ok ->
            cluster_http:ok(#{key => Key}, Req);
        {error, not_found} ->
            cluster_http:not_found(#{key => Key}, Req)
    end;
do(<<"PUT">>, Key, Req) ->
    {ok, Data, Req2} = cowboy_req:body(Req),
    ok = cluster_store:write(Key, Data),
    cluster_http:ok(#{key => Key}, Req2).
