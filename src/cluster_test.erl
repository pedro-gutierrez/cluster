-module(cluster_test).

-include_lib("eunit/include/eunit.hrl").

-define(RETRY_ATTEMPTS, 120).
-define(RETRY_SLEEP, 1000).
-define(TEST_TIMEOUT, 300).
-define(DEFAULT_ENDPOINT, "https://cluster-pedro-gutierrez.cloud.okteto.net").
-define(KEYS_TO_WRITE, 50).

endpoint() ->
    case os:getenv("TEST_ENDPOINT") of
        false ->
            ?DEFAULT_ENDPOINT;
        Endpoint ->
            Endpoint
    end.

cluster_test_() ->
    {timeout,
     ?TEST_TIMEOUT,
     fun() ->
        test_halt_host(),
        test_disconnect_host(),
        test_netsplit_manual_recovery(),
        test_netsplit_automatic_recovery()
     end}.

test_halt_host() ->
    print("~n== TEST test_halt_host()"),
    setup(),
    assert_cluster_state(<<"green">>),
    Hosts = cluster_hosts(),
    assert_active_replicas(length(Hosts)),
    set_cluster_recovery("manual"),
    Leader = cluster_leader(),
    halt_host(Leader),
    assert_cluster_state(<<"red">>),
    refute_cluster_leader(Leader),
    join_host(Leader),
    assert_cluster_state(<<"green">>).

test_disconnect_host() ->
    print("~n== TEST test_disconnect_host()"),
    setup(),
    assert_cluster_state(<<"green">>),
    Hosts = cluster_hosts(),
    assert_active_replicas(length(Hosts)),
    set_cluster_recovery("manual"),
    Hosts = cluster_hosts(),
    disconnect_hosts_and_wait_for_cluster_state(Hosts, <<"red">>),
    join_hosts_and_wait_for_cluster_state(Hosts, <<"green">>).

test_netsplit_manual_recovery() ->
    print("~n== TEST test_netsplit_manual_recovery()"),
    setup(),
    assert_cluster_state(<<"green">>),
    Hosts = cluster_hosts(),
    assert_active_replicas(length(Hosts)),
    set_cluster_recovery("manual"),
    delete_all_keys(),
    assert_store_size(0),
    write_keys("a", ?KEYS_TO_WRITE),
    assert_store_size(?KEYS_TO_WRITE),
    disconnect_hosts_and_wait_for_cluster_state(Hosts, <<"red">>),
    assert_store_size(?KEYS_TO_WRITE),
    write_keys("b", ?KEYS_TO_WRITE),
    join_hosts_and_wait_for_cluster_state(Hosts, <<"green">>),
    assert_store_size(2 * ?KEYS_TO_WRITE).

test_netsplit_automatic_recovery() ->
    print("~n== TEST test_netsplit_automatic_recovery()"),
    setup(),
    assert_cluster_state(<<"green">>),
    Hosts = cluster_hosts(),
    assert_active_replicas(length(Hosts)),
    set_cluster_recovery("manual"),
    delete_all_keys(),
    assert_store_size(0),
    write_keys("a", ?KEYS_TO_WRITE),
    assert_store_size(?KEYS_TO_WRITE),
    disconnect_hosts_and_wait_for_cluster_state(Hosts, <<"red">>),
    assert_store_size(?KEYS_TO_WRITE),
    write_keys("b", ?KEYS_TO_WRITE),
    set_cluster_recovery("auto"),
    assert_store_size(2 * ?KEYS_TO_WRITE).

assert_cluster_state(State) ->
    print("asserting cluster state ~p", [State]),
    retry(fun() -> do_assert_cluster_state(State) end,
          <<"cluster not in state: ", State/binary>>),
    ok.

do_assert_cluster_state(State) ->
    Url = endpoint(),
    {ok, #{status := 200, body := #{<<"state">> := State}}} = http(Url).

assert_active_replicas(Count) ->
    print("asserting ~p active replicas", [Count]),
    retry(fun() ->
             Url = endpoint(),
             {ok,
              #{status := 200,
                body := #{<<"store">> := #{<<"replicas">> := #{<<"active">> := Replicas}}}}} =
                 http(Url),
             Count = length(Replicas),
             ok
          end,
          <<"expected active replicas to be ", (erlang:integer_to_binary(Count))/binary>>).

refute_cluster_leader(Host) ->
    print("refuting cluster leader ~p", [Host]),
    retry(fun() ->
             Url = endpoint(),
             {ok, #{status := 200, body := #{<<"leader">> := Leader}}} = http(Url),
             ?assert(Host =/= Leader)
          end,
          <<"expected leader not to be ", Host/binary>>).

cluster_leader() ->
    print("retrieving cluster leader"),
    {ok, Leader} =
        retry(fun() ->
                 Url = endpoint(),
                 {ok, #{status := 200, body := #{<<"leader">> := Leader}}} = http(Url),
                 {ok, Leader}
              end,
              <<"cluster does not have a leader">>),
    Leader.

assert_store_size(Size) ->
    print("asserting store size ~p", [Size]),
    retry(fun() ->
             % do a few iterations until we are sure it is settled
             [Size = store_size() || _ <- lists:seq(1, 50)],
             ok
          end,
          <<"Expected store size should be ", (erlang:integer_to_binary(Size))/binary>>).

store_size() ->
    Url = endpoint(),
    {ok, #{status := 200, body := #{<<"store">> := #{<<"size">> := Size}}}} = http(Url),
    Size.

halt_host(Host) ->
    print("halting host ~p", [Host]),
    retry(fun() ->
             Path = erlang:binary_to_list(<<"/hosts/", Host/binary, "?mode=halt">>),
             Url = url(Path),
             {ok, #{status := 200}} = http(delete, Url)
          end,
          <<"could not halt host ", Host/binary>>).

disconnect_hosts_and_wait_for_cluster_state(Hosts, State) ->
    print("disconnecting hosts ~p and waiting for cluster state ~p", [Hosts, State]),
    retry(fun() ->
             disconnect_hosts(Hosts),
             do_assert_cluster_state(State)
          end,
          <<"disconnecting hosts did not result in a ", State/binary, " cluster state">>).

disconnect_hosts(Hosts) ->
    lists:foreach(fun(Host) ->
                     Path = erlang:binary_to_list(<<"/hosts/", Host/binary, "?mode=disconnect">>),
                     Url = url(Path),
                     {ok, #{status := 200}} = http(delete, Url)
                  end,
                  Hosts).

join_host(Host) ->
    retry(fun() -> do_join_host(Host) end, <<"could not join host ", Host/binary>>).

do_join_hosts(Hosts) ->
    lists:foreach(fun(Host) ->
                     Path = erlang:binary_to_list(<<"/hosts/", Host/binary>>),
                     Url = url(Path),
                     Res = http(post, Url),
                     {ok, #{status := 200}} = Res
                  end,
                  Hosts).

do_join_host(Host) ->
    print("joining host ~p", [Host]),
    retry(fun() ->
             Path = erlang:binary_to_list(<<"/hosts/", Host/binary>>),
             Url = url(Path),
             Res = http(post, Url),
             {ok, #{status := 200}} = Res
          end,
          <<"could not join host ", Host/binary>>).

join_hosts_and_wait_for_cluster_state(Hosts, State) ->
    print("joining hosts ~p and waiting for cluster state ~p", [Hosts, State]),
    retry(fun() ->
             do_join_hosts(Hosts),
             do_assert_cluster_state(State)
          end,
          <<"joining hosts did not result in a ", State/binary, " cluster state">>).

cluster_hosts() ->
    print("getting cluster hosts"),
    {ok, Hosts} =
        retry(fun() ->
                 Url = endpoint(),
                 {ok, #{status := 200, body := #{<<"nodes">> := Hosts}}} = http(Url),
                 {ok, Hosts}
              end,
              <<"could not get cluster hosts">>),
    Hosts.

delete_all_keys() ->
    print("deleting all keys"),
    retry(fun() ->
             Url = url("/keys"),
             {ok, #{status := 200}} = http(delete, Url)
          end,
          <<"could not delete keys">>).

write_keys(Prefix, N) ->
    BatchSize = 10,
    NumberOfBatches = floor(N / BatchSize),
    print("writing ~p keys with prefix ~p in ~p batches of ~p",
          [N, Prefix, NumberOfBatches, BatchSize]),
    retry(fun() -> write_key_batches(Prefix, BatchSize, NumberOfBatches) end,
          <<"could not write keys">>).

write_key_batches(_, _, 0) ->
    ok;
write_key_batches(Prefix, BatchSize, BatchNumber) ->
    progress(),
    write_key_batch(Prefix, BatchSize, BatchNumber),
    timer:sleep(1000),
    write_key_batches(Prefix, BatchSize, BatchNumber - 1).

write_key_batch(Prefix, BatchSize, BatchNumber) ->
    To = BatchNumber * BatchSize,
    From = (BatchNumber - 1) * BatchSize + 1,
    write_keys(Prefix, From, To).

write_keys(Prefix, From, To) ->
    print("writing batch ~p -> ~p", [From, To]),
    Keys = lists:seq(From, To),
    lists:foreach(fun(I) ->
                     K = Prefix ++ erlang:integer_to_list(I),
                     {ok, #{status := 200}} = do_write_key(K, "value")
                  end,
                  Keys).

do_write_key(K, V) ->
    Url = url("/keys/key" ++ K),
    Resp = http(put, Url, [], V),
    Resp.

set_cluster_recovery(Recovery) ->
    print("setting cluster recovery to ~p", [Recovery]),
    retry(fun() ->
             Url = url("/?recovery=" ++ Recovery),
             ExpectedRecovery = erlang:list_to_binary(Recovery),
             {ok, #{status := 200, body := #{<<"recovery">> := ExpectedRecovery}}} = http(put, Url)
          end,
          <<"could not set cluster recovery">>).

setup() ->
    inets:start(),
    ssl:start().

url(Path) ->
    url(endpoint(), Path).

url(Base, Path) ->
    Base ++ Path.

retry(Fun, Msg) ->
    retry(?RETRY_ATTEMPTS, ?RETRY_SLEEP, Fun, Msg, undefined).

retry(0, _, _, Msg, LastError) ->
    print("Last error: ~p", [LastError]),
    throw(Msg);
retry(Attempts, Sleep, Fun, Msg, _) ->
    progress(),
    case safe(Fun) of
        ok ->
            ok;
        {ok, _} = Result ->
            Result;
        {error, _} = _Err ->
            timer:sleep(Sleep),
            retry(Attempts - 1, Sleep, Fun, Msg, undefined)
    end.

safe(Fun) ->
    try
        Fun()
    catch
        Type:Error ->
            {error, {Type, Error}}
    end.

http(Url) ->
    http(get, Url).

http(post, Url) ->
    http(post, Url, [], "");
http(Method, Url) ->
    http(Method, Url, []).

http(Method, Url, Headers) ->
    Request = {Url, Headers},
    request(Method, Request).

http(Method, Url, Headers, Body) ->
    Req = {Url, Headers, "", Body},
    Resp = request(Method, Req),
    maybe_print(#{request => Req, response => Resp}),
    Resp.

request(Method, Request) ->
    case httpc:request(Method, Request, [], []) of
        {ok, {{_, Status, _}, Headers, Body}} ->
            case decode_json(Body) of
                {ok, DecodedBody} ->
                    DecodedHeaders =
                        lists:foldl(fun({K, V}, Map) ->
                                       maps:put(
                                           erlang:list_to_binary(K), erlang:list_to_binary(V), Map)
                                    end,
                                    #{},
                                    Headers),
                    {ok,
                     #{status => Status,
                       headers => DecodedHeaders,
                       body => DecodedBody}};
                {error, _} ->
                    {error, {invalid_json, Body}}
            end;
        {error, _} = Err ->
            Err
    end.

decode_json(<<>>) ->
    {ok, <<>>};
decode_json([]) ->
    {ok, <<>>};
decode_json(Raw) ->
    try
        {ok, jiffy:decode(Raw, [return_maps])}
    catch
        _:_ ->
            {error, raw}
    end.

print(Msg) ->
    print(Msg, []).

print(Msg, Args) ->
    io:format(user, "~n" ++ Msg ++ " ", Args).

maybe_print(Term) ->
    case os:getenv("TEST_DEBUG") of
        "true" ->
            print("~p", [Term]);
        _ ->
            ok
    end.

progress() ->
    io:format(user, ".", []).
