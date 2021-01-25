# Cluster: Simple clustering with Erlang and Kubernetes

An Erlang/OTP application that makes it easy to turn your Erlang backend into a distributed 
system running inside Kubernetes using **statefulsets**.

## Usage

Add this dependency to your `rebar3.config`:

```
{cluster, {git, "https://github.com/pedro-gutierrez/cluster", {branch, "main"}
```

Then make sure the `cluster` app is part of your release.


## Writing to the Key-Value store

To write a key:

```erlang
1> ok = cluster_store:write(<<"foo">>, <<"bar">>).
```

To read a key:

```erlang
2> {ok, <<"bar">>} = cluster_store:read(<<"foo">>).
```

## Subscribing to store updates

It is possible to get notified when a new key has been written to the store:

```erlang
1> ok = cluster_store:subscribe(self()).
2> cluster_store:write(<<"foo">>, <<"bar">>).
3> flush().
Shell got {cluster_store,written,<<"foo">>,<<"bar">>}
```

If you are no longer interested in receving updates, then you call `cluster_store:unsubscribe/1`
to remove your process subscription.

This feature is built with `pg2` so if your process dies, then the subscription
will be automatically removed.

It is only possible to subscribe once from the same Pid.


## Live Demo

Available [Here](http://cluster-pedro-gutierrez.cloud.okteto.net).

## Configuration

`cluster` is designed to work inside Kubernetes using **statefulsets**. If you are
not using statefulset, this library won't work. 

| Env variable | Description |
| --- | --- |
| `CLUSTER_SIZE` | the size of your cluster, eg: `2` |
| `CLUSTER_NAMESPACE` | your Kubernetes namespace |
| `CLUSTER_SERVICE` | the name of the Kubernetes headless service linked to your Statefulset |
| `ERLANG_COOKIE` | Erlang cookie for distribution |

Note: for now this library assumes a cluster of a fixed size. In the future, a dynamic 
cluster size might be supported.

## Monitoring

The following Prometheus metrics are exposed at path `/metrics`:

| Name | Kind | Help |
| --- | --- | --- | 
| `cluster_expected_size` | Gauge | expected size of the erlang cluster |
| `cluster_size` | Gauge | current size of the erlang cluster |
| `cluster_green` | Boolean | is the cluster is healthy? |
| `cluster_leader` | Boolean | is the node the leader? | 
| `cluster_leader_elections` | Counter | total number of leader elections |
| `cluster_store_ready` | Boolean | is the cluster store ready? |
| `cluster_store_size` | Gauge | size of the cluster store | 
| `cluster_store_subscriptions` | Counter | the number of subcriptions to the store |
| `cluster_store_partitions` | Counter | total number of network partitions | 

## Features


- [x] Easy to embed as an extra application in your OTP release
- [x] Very simple configuration via environment variables
- [x] Uses Erlang distribution
- [x] Implements leader election for Active/Passive architectures 
- [x] Embedded and replicated in memory Key-Value store based on Mnesia
- [x] Detects network partitions
- [ ] Custom conflict resolution 
- [x] Both `automatic` and `manual` netplit recovery modes
- [ ] Exports Prometheus metrics
- [x] Cluster management via REST
- [x] Key-Value store subscriptions

# Dependencies

This library has been writen in standard Erlang/OTP with minimal 
library dependencies: 

* Cowboy `2.0.0-pre.1` (web server)
* Prometheus `4.2.0` (monitoring)
* Jiffy `0.14.11"` (JSON library)
* Lagger `3.2.2"` (Logging)
* Reunion `master` (Mnesia partition healing)


