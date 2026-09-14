# st06-final-test-twin — Kafka Pull Service (Rust)

Vendored from the `kafka-pull-service` golden template in [cosmonic-labs/awesome-cosmonic](https://github.com/cosmonic-labs/awesome-cosmonic/blob/main/components/kafka/README.md)
(`components/kafka` @ f5326286); its README says when to choose this
pattern over the other three.

## Run it on Cosmonic Desktop

This is a Cosmonic Desktop project: `.wash/config.yaml` carries the build
command and, under `workload.hostInterfaces`, the same `cosmonic:kafka`
binding as `workload.yaml`. The Builder's agent, `cosmonic dev`, or the
`cosmonic_dev_start` MCP tool builds it (`cargo build --target wasm32-wasip2
--release`), runs it, and hot-restarts it on every save. `cosmonic publish` /
`cosmonic_project_publish` then pushes the component to the built-in registry
and prints a durable Workload draft that carries the binding.

> **Host support.** The binding needs the `cosmonic:kafka` plugin in the
> host: Cosmonic Control ships it, and Cosmonic Desktop compiles in the same
> plugin. On Desktop a binding's broker is pinned before the component runs:
> this manifest's `bootstrap.servers`, which overrides any default in the
> host's `kafka.yaml` (drop it to inherit that default instead); a binding
> with a broker from neither is refused. The Kubernetes manifest in `deploy/`
> runs on Control as it is.

The broker, the topic grant and the handler keys live in the workload's own
`cosmonic:kafka` entry (the manifests here show `127.0.0.1:9092`); the
component never sees a broker address in its code. Credentials go through
`secretFrom`, never inline: on Desktop a secret ref's key is an env-var name,
so register it as `SASL_PASSWORD` and the binding receives `sasl.password`.
`wit/deps/` ships with the scaffold, so a build
needs no registry access; to re-fetch it after bumping `wkg.lock`:
`WKG_CONFIG_FILE=./wkg-registries.toml wash wit fetch`.

A local Kafka for this template, once (any broker on `127.0.0.1:9092`; the
topics the manifest names):

```bash
kafka-topics --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic demo.events
kafka-topics --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic demo.enriched
kafka-topics --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic demo.events.dlq
```

## Build by hand

Prereqs: Rust 1.85+ with the `wasm32-wasip2` target (the one Cosmonic
Desktop's Preflight doctor provisions).

```sh
cargo build --target wasm32-wasip2 --release
# component: target/wasm32-wasip2/release/st06_final_test_twin.wasm
```

`.wash/config.yaml` names that command and where its output lands, which is
what lets Desktop's dev loop and `wash build` find the artifact without being
told. The `cosmonic:kafka@0.5.0` WIT under `wit/deps/` is pinned by
`wkg.lock`; `wkg-registries.toml` maps the `cosmonic` namespace to the
registry that serves it, should you need to fetch it again.

## Deploy

- **Cosmonic Desktop**: submit `workload.yaml` through its workload API or MCP
  integration.
- **Kubernetes** (wasmCloud runtime-operator / Cosmonic Control): run
  `kubectl apply -f deploy/workload-deployment.yaml`.

Both point at the component published from this template. Set the broker,
topic, group, and environment placeholders before applying a manifest. Once
you change the source, build it, push it to your own registry, and replace the
image reference.

The broker address and topic names in the manifests are placeholders. The
workload's `cosmonic:kafka` entry under `hostInterfaces` is where the
connection lives — broker, credentials, the topic grant, and
`consumer.group.id`. The component calls `consumer.open()` without arguments
and cannot supply or override these values. Use `secretFrom` for the credential
rather than inlining it.

See [the pattern guide](https://github.com/cosmonic-labs/awesome-cosmonic/blob/main/components/kafka/README.md#where-the-broker-and-credentials-are-configured)
for binding and topic-grant rules.

## When to use this pattern

Prefer the handler template for ordinary elastic processing. Use this Service
when guest code needs direct session operations such as assign, pause, seek,
rebalance events, pull pacing, or arbitrary commits.

This Service owns a long-lived consumer session and does not scale to zero.
Adding replicas adds Kafka group members and may move partitions.

`BATCH_SIZE` defaults to 1 for bounded latency. A value above 1 waits until
that many output records arrive or the record stream ends; this template has
no batch timer. Values are capped at 100. Increase it only for a steady stream
where throughput matters more than partial-batch latency.

The service commits stored input positions only after every output delivery
and every per-partition commit result succeeds. A failure exits the Service so
its supervisor restarts it from committed offsets. Transform failures are sent
to `DLQ_TOPIC`; pending output is flushed before that record is committed.
