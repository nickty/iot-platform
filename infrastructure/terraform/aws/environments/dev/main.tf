terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # backend "s3" {
    # Configure after creating the state bucket
    # bucket = "iot-platform-terraform-state"
    # key    = "dev/terraform.tfstate"
    # region = "us-east-1"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "iot-platform"
      Environment = "dev"
      ManagedBy   = "terraform"
    }
  }
}

variable "aws_region" {
  default = "us-east-1"
}

variable "project_name" {
  default = "iot-platform"
}

# ── Networking ─────────────────────────────────────────────────────────
module "networking" {
  source       = "../../modules/networking"
  project_name = var.project_name
  environment  = "dev"
}

# ── ECR Repositories ───────────────────────────────────────────────────
module "ecr" {
  source       = "../../modules/ecr"
  project_name = var.project_name
  services = [
    "ingest-http",
    "processor-realtime",
    "processor-batch",
    "api-rest",
    "api-websocket",
    "archive-consumer",
    "silver-transform",
  ]
}

# ── Secrets Manager ────────────────────────────────────────────────────
module "secrets" {
  source       = "../../modules/secrets"
  project_name = var.project_name
  environment  = "dev"
}

# ── RDS PostgreSQL ─────────────────────────────────────────────────────
module "rds" {
  source            = "../../modules/rds"
  project_name      = var.project_name
  environment       = "dev"
  vpc_id            = module.networking.vpc_id
  private_subnet_ids = module.networking.private_subnet_ids
  db_password       = module.secrets.db_password
}

# ── ECS Cluster ────────────────────────────────────────────────────────
module "ecs" {
  source             = "../../modules/ecs"
  project_name       = var.project_name
  environment        = "dev"
  vpc_id             = module.networking.vpc_id
  public_subnet_ids  = module.networking.public_subnet_ids
  private_subnet_ids = module.networking.private_subnet_ids
  ecr_repositories   = module.ecr.repository_urls
  secrets_arn        = module.secrets.secrets_arn
  rds_endpoint       = module.rds.endpoint
}

# ── Outputs ────────────────────────────────────────────────────────────
output "alb_dns_name" {
  value = module.ecs.alb_dns_name
}

output "ecr_urls" {
  value = module.ecr.repository_urls
}

output "rds_endpoint" {
  value = module.rds.endpoint
}