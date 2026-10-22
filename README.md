# dbos-postgres-benchmark

Postgres and DBOS benchmarks.

## Launch the cluster

```bash
cd terraform
terraform init
terraform apply -var="key_name=YOUR_SSH_KEY"
```

SSH into the EC2 using the public IP from `terraform output`.

## Run benchmarks

On the EC2:

```bash
uv sync
```

### Postgres insert throughput

```bash
uv run python benchmarks/postgres_insert.py --rps 1000 --duration 900
```

### DBOS start_workflow throughput

```bash
uv run python benchmarks/dbos_start_workflow.py --rps 1000 --duration 900
```

### DBOS queue throughput

```bash
uv run python benchmarks/dbos_queue.py --rps 1000 --duration 900 --workers 8 --enqueuers 64
```

### DBOS partitioned queue throughput

```bash
uv run python benchmarks/dbos_partition_queue.py --rps 1000 --duration 900 --partitions 1000
```

### Stream throughput

```bash
uv run benchmarks/dbos_stream.py --processes 64 --streams-per-worker 16 --pool-size 20 --reader-processes 64 --rps 1000 --duration 900
```

## Sharded scaling benchmark

Measures how DBOS throughput scales across a sharded system database. Each shard is an
RDS Postgres instance; PgDog runs on every client host and routes by shard key. One DBOS
queue spans all shards, and every client both enqueues and runs no-op workflows.

Sharding needs a DBOS license key; set it as `DBOS_TRANSACT_ENTERPRISE_KEY`.

Provision one client host per two shards:

```bash
cd terraform/sharded
terraform init
terraform apply -parallelism=100 -var key_name=YOUR_SSH_KEY -var shard_count=50 -var client_count=25
```

Run from your machine:

```bash
export DBOS_TRANSACT_ENTERPRISE_KEY=YOUR_LICENSE_KEY
uv run python benchmarks/sharded/orchestrate.py --ssh-key ~/YOUR_SSH_KEY.pem \
  --terraform-dir terraform/sharded --shards 50 --rps-per-shard 20000 --processes 2400 \
  --duration 3600
```

Results go to `results/sharded/<timestamp>/`. To watch a run, use `orchestrate.py status`
with the same `--ssh-key` and `--terraform-dir`, plus `--watch`.
