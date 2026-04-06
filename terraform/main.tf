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

# --- RDS (db.m7i.2xlarge) ---

resource "aws_db_instance" "postgres" {
  identifier     = "dbos-bench-postgres"
  engine         = "postgres"
  engine_version = "16"

  instance_class          = "db.m7i.2xlarge"
  allocated_storage       = 100
  storage_type            = "io2"
  iops                    = 40000
  db_name                 = "postgres"
  username                = local.db_username
  password                = local.db_password
  port                    = 5432
  availability_zone       = "${var.aws_region}a"
  publicly_accessible     = false
  skip_final_snapshot     = true
  backup_retention_period = 0
  vpc_security_group_ids  = [aws_security_group.rds.id]

  tags = { Name = "dbos-bench-postgres" }
}

# --- EC2 (m7i.2xlarge) ---

resource "aws_instance" "bench" {
  ami                         = "ami-04eaa218f1349d88b" # Ubuntu 24.04 LTS amd64 us-east-1
  instance_type               = "m7i.2xlarge"
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

  tags = { Name = "dbos-bench-ec2" }
}
