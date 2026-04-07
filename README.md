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
uv run python benchmarks/postgres_insert.py --rps 5000 --duration 30
```

### DBOS start_workflow throughput

```bash
uv run python benchmarks/dbos_start_workflow.py --rps 5000 --duration 30
```

### DBOS queue throughput

```bash
uv run python benchmarks/dbos_queue.py --rps 5000 --duration 30
```
