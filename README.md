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