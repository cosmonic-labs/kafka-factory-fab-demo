# st04-inspection — Kafka Handler Consumer (Rust)

Vendored from the `kafka-handler-consumer` golden template in [cosmonic-labs/awesome-cosmonic](https://github.com/cosmonic-labs/awesome-cosmonic/blob/main/components/kafka/README.md)
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
kafka-topics --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic demo.events.dlq
```

This is the recommended starting point for serverless Kafka processing. The
host preserves consumer-group membership while component instances scale with
partition work and can return to zero when idle.

## Build by hand

Prereqs: Rust 1.85+ with the `wasm32-wasip2` target (the one Cosmonic
Desktop's Preflight doctor provisions).

```sh
cargo build --target wasm32-wasip2 --release
# component: target/wasm32-wasip2/release/st04_inspection.wasm
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
subscription, group, grant, and dead-letter placeholders before applying a
manifest. Create those topics first unless your broker allows automatic topic
creation. Once you change the source, build it, push it to your own registry,
and replace the image reference.

The broker address and topic names in the manifests are placeholders. The
workload's `cosmonic:kafka` entry under `hostInterfaces` is where the
connection lives — broker, credentials, the topic grant, and
`handler.group.id`. The component cannot supply or override these values. Use
`secretFrom` for the credential rather than inlining it. See
[the pattern guide](https://github.com/cosmonic-labs/awesome-cosmonic/blob/main/components/kafka/README.md#where-the-broker-and-credentials-are-configured).

## Scaling

Useful concurrency per workload replica is bounded by assigned partitions, the
plugin's 64-call ceiling, and `poolSize × maxConcurrency`. Increasing the pool
does not create Kafka group members or trigger a rebalance. Instances are
created on demand; `reclaimWindowSeconds` and `reclaimMinInstances: 0` let the
pool return to zero after the burst. Keep `maxConcurrency: 1` unless handler
code is safe for overlapping calls on one instance.

Scale-to-zero applies to guest component instances. The host keeps its native
Kafka consumer, connection, and group membership alive so new records do not
wait for a rebalance or a pod start.

Increasing `spec.replicas` is different: it creates another Kafka group member
and can move partitions. Use replicas for host-level availability or when more
group members are needed, and use the component pool for elastic compute on
the partitions already assigned to a replica.

## Delivery

Each call receives a non-empty, offset-ordered batch from one partition.
Return `Ok(None)` after handling the full batch. If a later record fails after
earlier records succeeded, return `Ok(Some(offset))` with the last successful
record's offset; the host preserves that progress and redelivers the suffix.
Return `Transient` when the head record may succeed later and `Permanent` when
it should go directly to `dead-letter.topic`.

Delivery is at least once. A crash can repeat a side effect before its offset
is committed, so handlers must be idempotent. Do not rely on instance-local
state: calls can land on different instances and idle instances are reclaimed.

The sample treats tombstones as handled and sends invalid UTF-8 values to the
dead-letter topic. Replace the marked block in `src/lib.rs` with application
logic while preserving its partial-progress behavior.

## Publish from a handler

A handler can also import `cosmonic:kafka/producer@0.5.0` for
consume-transform-produce workloads. Add the producer import to `wit/world.wit`,
add `producer` to the manifest's Kafka interfaces, and include every output
topic in `topics`. The host-owned producer remains available across elastic
handler instances; this does not require the pull-service template.

Use the transactional Service when outputs and consumed offsets must commit
atomically. Ordinary handler-produced output is at least once and may be
duplicated after a crash.
