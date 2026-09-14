# st01-probe-intake — Kafka HTTP Producer (Rust)

Vendored from the `http-kafka-producer` golden template in [cosmonic-labs/awesome-cosmonic](https://github.com/cosmonic-labs/awesome-cosmonic/blob/main/components/kafka/README.md)
(`components/kafka` @ f5326286); its README says when to choose this
pattern over the other three.

## Run it on Cosmonic Desktop

This is a Cosmonic Desktop project: `.wash/config.yaml` carries the build
command and, under `workload.hostInterfaces`, the same `cosmonic:kafka`
binding as `deploy/workload.yaml`. The Builder's agent, `cosmonic dev`, or the
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
```

## Build by hand

Prereqs: Rust 1.85+ with the `wasm32-wasip2` target (the one Cosmonic
Desktop's Preflight doctor provisions).

```sh
cargo build --target wasm32-wasip2 --release
# component: target/wasm32-wasip2/release/st01_probe_intake.wasm
```

`.wash/config.yaml` names that command and where its output lands, which is
what lets Desktop's dev loop and `wash build` find the artifact without being
told. The `cosmonic:kafka@0.5.0` WIT under `wit/deps/` is pinned by
`wkg.lock`; `wkg-registries.toml` maps the `cosmonic` namespace to the
registry that serves it, should you need to fetch it again.

## Deploy

- **Cosmonic Desktop**: `deploy/workload.yaml` is the Workload this station
  runs as — `scripts/run.sh` digest-pins its image and applies it (the
  Kubernetes `WorkloadDeployment` form the template shipped was removed; the
  Workload spec is the same schema on Control).

Both point at the component published from this template. Set the broker and
topic placeholders before applying a manifest. Once you change the source,
build it, push it to your own registry, and replace the image reference.

The broker address and topic names in the manifests are placeholders. The
workload's `cosmonic:kafka` entry under `hostInterfaces` is where the
connection lives — broker, credentials, client policy, and the topic grant.
The component cannot supply or override these values. Use `secretFrom` for the
credential rather than inlining it.

See [the pattern guide](https://github.com/cosmonic-labs/awesome-cosmonic/blob/main/components/kafka/README.md#where-the-broker-and-credentials-are-configured)
for binding and topic-grant rules.

## Design notes

Producer functions use the binding's host-owned native client directly. The
component creates no Kafka client per HTTP request.

## API

- `POST /produce?topic=T&key=K&value=V` sends one record and returns its
  `partition:offset` acknowledgement.
- `POST /produce-batch?topic=T&count=N&size=S` sends 1–10,000 records with
  values up to 1 MiB and a total value payload up to 16 MiB. It returns one
  delivery result per line. The response can contain both successes and
  failures because batch outcomes are positional.

The requested topic must be present in the binding's `topics` grant.
