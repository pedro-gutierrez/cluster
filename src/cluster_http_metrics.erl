-module(cluster_http_metrics).

-export([init/2]).

init(Req, _) ->
    Method = cowboy_req:method(Req),
    do(Method, Req).

do(<<"GET">>, Req) ->
    Text = prometheus_text_format:format(),
    cluster_http:ok({text, Text}, Req).
