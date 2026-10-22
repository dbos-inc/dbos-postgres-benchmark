# The sharded benchmark's fleet: shard_count RDS Postgres shards and client_count client
# hosts, each client running DBOS processes and a PgDog sidecar that routes to every shard.
#
# Provision once for the largest shard count you will sweep; orchestrate.py uses the
# first N shards and clients for each point. Separate state from ../main.tf, so the
# single-database stack is unaffected.
#
#   terraform init
#   terraform apply -parallelism=100 -var key_name=KEY -var shard_count=8 -var client_count=8
#
# RDS instances take ~15 minutes to create; Terraform's default parallelism of 10 would
# create a 50-shard fleet five batches at a time.
#
# Quotas to raise before a large fleet: RDS "DB instances" defaults to 40 per region, and
# the EC2 "Running On-Demand Standard instances" vCPU quota (or "All Standard Spot
# Instance Requests" with client_spot=true) must cover client_count * 96 for c8g.24xlarge.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# --- Variables ---

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "key_name" {
  description = "EC2 SSH key pair name"
  type        = string
}

variable "ssh_cidr" {
  description = "CIDR allowed to SSH to the clients"
  type        = string
  default     = "0.0.0.0/0"
}

variable "shard_count" {
  description = "RDS shards to provision: the largest shard count the sweep will use"
  type        = number
  default     = 2
}

variable "client_count" {
  description = "Client hosts to provision: the largest client count the sweep will use"
  type        = number
  default     = 2
}

# The single-database stack's instance class, so one shard here is the unit it measures.
variable "shard_instance_class" {
  description = "RDS instance class of every shard"
  type        = string
  default     = "db.m7i.4xlarge"
}

variable "shard_storage_gb" {
  description = "gp3 storage per shard; 400 GB is where RDS gp3 baseline reaches 12,000 IOPS"
  type        = number
  default     = 400
}

# Graviton4: 96 physical cores, where c7i.24xlarge's 96 vCPUs are 48 hyperthreaded
# cores, for DBOS processes that are each CPU-bound on one core.
variable "client_instance_type" {
  description = "EC2 instance type of every client host (arm64 or x86_64)"
  type        = string
  default     = "c8g.24xlarge"
}

# Off by default: spot c8g.24xlarge capacity in us-east-1 is scarce (AWS rates its
# interruption frequency >20%), and a reclaimed client fails the run in progress.
variable "client_spot" {
  description = "Run the clients as one-time spot instances; an interrupted client is terminated and the next apply recreates it"
  type        = bool
  default     = false
}

variable "monitoring_interval" {
  description = "RDS Enhanced Monitoring interval in seconds (0 disables it)"
  type        = number
  default     = 1
}

resource "random_password" "db" {
  length  = 16
  special = false
}

locals {
  db_username = "postgres"
  db_password = random_password.db.result
  az          = "${var.aws_region}a"
  shard_ids   = [for i in range(var.shard_count) : "dbos-bench-shard-${i}"]

  # Ubuntu 24.04 LTS us-east-1, both from the same release, picked by the client
  # instance type's architecture.
  client_ami = {
    arm64  = "ami-0bc0f64eea5d47edf"
    x86_64 = "ami-04eaa218f1349d88b"
  }[contains(data.aws_ec2_instance_type.client.supported_architectures, "arm64") ? "arm64" : "x86_64"]
}

data "aws_ec2_instance_type" "client" {
  instance_type = var.client_instance_type
}

# --- Security Groups ---

resource "aws_security_group" "client" {
  name_prefix = "dbos-bench-shard-client-"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "dbos-bench-shard-client-sg" }
}

resource "aws_security_group" "shard" {
  name_prefix = "dbos-bench-shard-rds-"

  ingress {
    description     = "Postgres from the clients and their PgDog sidecars"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.client.id]
  }

  tags = { Name = "dbos-bench-shard-rds-sg" }
}

# --- CloudWatch log groups for the RDS log exports ---
#
# Declared so they expire and are deleted on destroy, as in ../main.tf: RDS would
# otherwise create never-expiring groups outside Terraform's state.

resource "aws_cloudwatch_log_group" "shard_postgresql" {
  count             = var.shard_count
  name              = "/aws/rds/instance/${local.shard_ids[count.index]}/postgresql"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "shard_upgrade" {
  count             = var.shard_count
  name              = "/aws/rds/instance/${local.shard_ids[count.index]}/upgrade"
  retention_in_days = 7
}

# --- Enhanced Monitoring IAM role ---

data "aws_iam_policy_document" "rds_monitoring_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["monitoring.rds.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "rds_monitoring" {
  name_prefix        = "dbos-bench-shard-mon-"
  assume_role_policy = data.aws_iam_policy_document.rds_monitoring_assume.json
}

resource "aws_iam_role_policy_attachment" "rds_monitoring" {
  role       = aws_iam_role.rds_monitoring.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

# --- Parameter group ---
#
# The single-database stack's settings (see ../main.tf for why each is set), so a shard
# here and the database there differ only in what they are sent.

resource "aws_db_parameter_group" "shard" {
  name_prefix = "dbos-bench-shard-pg18-"
  family      = "postgres18"

  parameter {
    name         = "shared_preload_libraries"
    value        = "pg_stat_statements,pg_tle"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "pg_stat_statements.max"
    value        = "10000"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "track_activity_query_size"
    value        = "16384"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "track_io_timing"
    value        = "1"
    apply_method = "pending-reboot"
  }
  parameter {
    name  = "track_wal_io_timing"
    value = "1"
  }
  parameter {
    name  = "track_functions"
    value = "all"
  }
  parameter {
    name  = "compute_query_id"
    value = "on"
  }
  parameter {
    name  = "pg_stat_statements.track"
    value = "ALL"
  }
  parameter {
    name  = "pg_stat_statements.track_planning"
    value = "0"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# --- RDS shards ---

resource "aws_db_instance" "shard" {
  count          = var.shard_count
  identifier     = local.shard_ids[count.index]
  engine         = "postgres"
  engine_version = "18"

  depends_on = [
    aws_cloudwatch_log_group.shard_postgresql,
    aws_cloudwatch_log_group.shard_upgrade,
  ]

  instance_class          = var.shard_instance_class
  allocated_storage       = var.shard_storage_gb
  storage_type            = "gp3"
  storage_encrypted       = true
  db_name                 = "postgres"
  username                = local.db_username
  password                = local.db_password
  port                    = 5432
  availability_zone       = local.az
  publicly_accessible     = false
  skip_final_snapshot     = true
  backup_retention_period = 0
  apply_immediately       = true
  vpc_security_group_ids  = [aws_security_group.shard.id]
  parameter_group_name    = aws_db_parameter_group.shard.name

  performance_insights_enabled          = true
  performance_insights_retention_period = 7

  monitoring_interval = var.monitoring_interval
  monitoring_role_arn = var.monitoring_interval > 0 ? aws_iam_role.rds_monitoring.arn : null

  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  tags = { Name = local.shard_ids[count.index] }
}

# --- Client hosts ---
#
# Each runs DBOS processes and a PgDog container. Software comes from orchestrate.py,
# which installs wheels built from the local dbos-transact-py and dbos-enterprise-py
# checkouts, so the hosts need only uv, Docker, and psql. No placement group: clients
# talk only to the shards, never to each other.

resource "aws_instance" "client" {
  count                       = var.client_count
  ami                         = local.client_ami
  instance_type               = var.client_instance_type
  vpc_security_group_ids      = [aws_security_group.client.id]
  key_name                    = var.key_name
  availability_zone           = local.az
  associate_public_ip_address = true

  # Spot's price cap defaults to the on-demand price.
  dynamic "instance_market_options" {
    for_each = var.client_spot ? [1] : []
    content {
      market_type = "spot"
      spot_options {
        spot_instance_type             = "one-time"
        instance_interruption_behavior = "terminate"
      }
    }
  }

  root_block_device {
    volume_size = 100
    volume_type = "gp3"
  }

  user_data = <<-EOF
    #!/bin/bash
    set -euxo pipefail

    apt-get update
    apt-get install -y postgresql-client docker.io rsync
    usermod -aG docker ubuntu

    curl -LsSf https://astral.sh/uv/install.sh | sh
    cp /root/.local/bin/uv /usr/local/bin/
    cp /root/.local/bin/uvx /usr/local/bin/

    # Hundreds of DBOS processes, each with a pool to PgDog, and PgDog with a pool to
    # every shard: raise the descriptor and ephemeral-port limits.
    cat > /etc/security/limits.d/90-bench.conf <<LIMITS
    ubuntu soft nofile 1048576
    ubuntu hard nofile 1048576
    LIMITS
    cat > /etc/sysctl.d/90-bench.conf <<SYSCTL
    net.ipv4.ip_local_port_range = 1024 65000
    net.core.somaxconn = 65535
    net.ipv4.tcp_max_syn_backlog = 65535
    fs.file-max = 4194304
    SYSCTL
    sysctl --system

    install -d -o ubuntu -g ubuntu /home/ubuntu/bench
  EOF

  tags = { Name = "dbos-bench-shard-client-${count.index}" }
}

# --- Outputs ---

output "client_public_ips" {
  description = "Public IPs of the client hosts"
  value       = aws_instance.client[*].public_ip
}

output "shard_endpoints" {
  description = "RDS endpoints of the shards, in shard order"
  value       = aws_db_instance.shard[*].address
}

# What orchestrate.py --terraform-dir reads.
output "inventory" {
  description = "Hosts and shards for orchestrate.py"
  sensitive   = true
  value = {
    clients           = [for c in aws_instance.client : { ssh = "ubuntu@${c.public_ip}" }]
    user              = local.db_username
    password          = local.db_password
    router_port       = 6432
    pgdog_docker_args = "--network host"
    python            = "venv/bin/python"
    workdir           = "bench"
    shards = [
      for db in aws_db_instance.shard : {
        host       = db.address
        port       = 5432
        admin_host = db.address
        admin_port = 5432
        identifier = db.identifier
      }
    ]
  }
}
