variable "project_name" { type = string }
variable "environment" { type = string }

resource "random_password" "db" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "app" {
  name = "${var.project_name}/${var.environment}/app-secrets"
}

resource "aws_secretsmanager_secret_version" "app" {
  secret_id = aws_secretsmanager_secret.app.id

  secret_string = jsonencode({
    DB_PASSWORD    = random_password.db.result
    JWT_SECRET     = random_password.db.result
    REDIS_PASSWORD = ""
  })
}

output "secrets_arn" { value = aws_secretsmanager_secret.app.arn }
output "db_password" {
  value     = random_password.db.result
  sensitive = true
}