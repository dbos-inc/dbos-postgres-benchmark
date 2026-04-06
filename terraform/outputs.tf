output "ec2_public_ip" {
  description = "Public IP of the benchmark EC2 instance"
  value       = aws_instance.bench.public_ip
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
