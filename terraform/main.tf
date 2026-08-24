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

resource "random_password" "db" {
  length  = 16
  special = false
}

locals {
  db_username = "postgres"
  db_password = random_password.db.result
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

# --- Security Groups ---

resource "aws_security_group" "ec2" {
  name_prefix = "dbos-bench-ec2-"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "dbos-bench-ec2-sg" }
}

resource "aws_security_group" "rds" {
  name_prefix = "dbos-bench-rds-"

  ingress {
    description     = "Postgres from EC2"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.ec2.id]
  }

  tags = { Name = "dbos-bench-rds-sg" }
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
  name_prefix        = "dbos-bench-rds-monitoring-"
  assume_role_policy = data.aws_iam_policy_document.rds_monitoring_assume.json
}

resource "aws_iam_role_policy_attachment" "rds_monitoring" {
  role       = aws_iam_role.rds_monitoring.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

# --- Parameter group: pin the observability counters on ---
#
# Most of these already match the postgres18 defaults; they are pinned
# explicitly so a future default change cannot silently turn them off.
# Deliberately NOT set: log_min_duration_statement and auto_explain. Logging
# every statement at benchmark rates distorts the very throughput being
# measured. Turn them on ad hoc when debugging a specific query, not for a run.

resource "aws_db_parameter_group" "postgres" {
  name_prefix = "dbos-bench-pg18-"
  family      = "postgres18"

  # --- Static: require a reboot, applied at create time ---
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
  # Full statement text in Performance Insights rather than a 4KB truncation.
  parameter {
    name         = "track_activity_query_size"
    value        = "16384"
    apply_method = "pending-reboot"
  }

  # --- Dynamic ---
  # Block read/write timings, so PI attributes waits to actual IO.
  parameter {
    name  = "track_io_timing"
    value = "1"
  }
  parameter {
    name  = "track_wal_io_timing"
    value = "1"
  }
  parameter {
    name  = "track_functions"
    value = "all"
  }
  # Always compute query IDs so pg_stat_statements and PI agree on identity.
  parameter {
    name  = "compute_query_id"
    value = "on"
  }
  # ALL also counts statements nested inside functions and procedures.
  parameter {
    name  = "pg_stat_statements.track"
    value = "ALL"
  }
  # The one observability knob deliberately left OFF. Upstream Postgres warns
  # that track_planning causes a noticeable penalty when many concurrent
  # connections execute statements with identical structure, because they
  # contend on the same pg_stat_statements entry. That is precisely this
  # benchmark's workload, so enabling it would distort the throughput being
  # measured. Set to 1 only when investigating planning time specifically.
  parameter {
    name  = "pg_stat_statements.track_planning"
    value = "0"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# --- RDS (db.m7i.24xlarge) --- (original, high-end config)
#
# resource "aws_db_instance" "postgres" {
#   identifier     = "dbos-bench-postgres"
#   engine         = "postgres"
#   engine_version = "18"
#
#   instance_class          = "db.m7i.24xlarge"
#   allocated_storage       = 300
#   storage_type            = "io2"
#   iops                    = 120000
#   db_name                 = "postgres"
#   username                = local.db_username
#   password                = local.db_password
#   port                    = 5432
#   availability_zone       = "${var.aws_region}a"
#   publicly_accessible     = false
#   skip_final_snapshot     = true
#   backup_retention_period = 0
#   apply_immediately       = true
#   vpc_security_group_ids  = [aws_security_group.rds.id]
#
#   tags = { Name = "dbos-bench-postgres" }
# }

# --- RDS (db.m7i.4xlarge) ---

resource "aws_db_instance" "postgres" {
  identifier     = "dbos-bench-postgres"
  engine         = "postgres"
  engine_version = "18"

  instance_class          = "db.m7i.4xlarge"
  allocated_storage       = 400
  storage_type            = "gp3"
  db_name                 = "postgres"
  username                = local.db_username
  password                = local.db_password
  port                    = 5432
  availability_zone       = "${var.aws_region}a"
  publicly_accessible     = false
  skip_final_snapshot     = true
  backup_retention_period = 0
  apply_immediately       = true
  vpc_security_group_ids  = [aws_security_group.rds.id]
  parameter_group_name    = aws_db_parameter_group.postgres.name

  # Performance Insights: 7-day retention is the free tier. Longer retention is
  # billed per vCPU, and a benchmark never needs more than the current run.
  performance_insights_enabled          = true
  performance_insights_retention_period = 7

  # Enhanced Monitoring at 1s. OS-level CPU/IO/memory sampled per second, which
  # is the granularity a benchmark run actually needs; metrics are delivered to
  # CloudWatch Logs and billed at ingestion rates, so this scales with run time.
  monitoring_interval = 1
  monitoring_role_arn = aws_iam_role.rds_monitoring.arn

  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  tags = { Name = "dbos-bench-postgres" }
}

# --- EC2 (c7i.48xlarge) ---

resource "aws_instance" "bench" {
  count                       = 1
  ami                         = "ami-04eaa218f1349d88b" # Ubuntu 24.04 LTS amd64 us-east-1
  instance_type               = "c7i.48xlarge"
  vpc_security_group_ids      = [aws_security_group.ec2.id]
  key_name                    = var.key_name
  availability_zone           = "${var.aws_region}a"
  associate_public_ip_address = true

  root_block_device {
    volume_size = 100
    volume_type = "gp3"
  }

  user_data = <<-EOF
    #!/bin/bash
    set -euxo pipefail

    # Install psql
    apt-get update
    apt-get install -y postgresql-client

    # Install uv
    curl -LsSf https://astral.sh/uv/install.sh | sh
    cp /root/.local/bin/uv /usr/local/bin/
    cp /root/.local/bin/uvx /usr/local/bin/

    # Set environment variables for ubuntu user
    cat >> /home/ubuntu/.bashrc <<ENVEOF
    export POSTGRES_DATABASE_URL="postgresql://${local.db_username}:${local.db_password}@${aws_db_instance.postgres.address}:5432/postgres"
    export BENCHMARK_DATABASE_URL="postgresql://${local.db_username}:${local.db_password}@${aws_db_instance.postgres.address}:5432/benchmark"
    ENVEOF
  EOF

  tags = { Name = "dbos-bench-ec2-${count.index}" }
}

# --- Outputs ---

output "ec2_public_ips" {
  description = "Public IPs of the benchmark EC2 instances"
  value       = aws_instance.bench[*].public_ip
}

output "rds_endpoint" {
  description = "RDS endpoint"
  value       = aws_db_instance.postgres.address
}

output "postgres_database_url" {
  description = "Connection URL for the postgres database"
  value       = "postgresql://${local.db_username}:${local.db_password}@${aws_db_instance.postgres.address}:5432/postgres"
  sensitive   = true
}

output "benchmark_database_url" {
  description = "Connection URL for the benchmark database"
  value       = "postgresql://${local.db_username}:${local.db_password}@${aws_db_instance.postgres.address}:5432/benchmark"
  sensitive   = true
}
