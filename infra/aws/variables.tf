variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix used for AWS resource names and tags."
  type        = string
  default     = "nss-ingestor"
}

variable "vpc_id" {
  description = "Target VPC ID."
  type        = string
}

variable "subnet_id" {
  description = "Target private subnet ID for the instance."
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type."
  type        = string
  default     = "m7i.large"
}

variable "root_volume_size_gb" {
  description = "Root EBS volume size in GiB."
  type        = number
  default     = 100
}

variable "nss_port" {
  description = "TCP port exposed for Zscaler NSS feed."
  type        = number
  default     = 514

  validation {
    condition     = var.nss_port >= 1 && var.nss_port <= 65535
    error_message = "nss_port must be between 1 and 65535."
  }
}

variable "nss_source_cidrs" {
  description = "CIDR list allowed to send NSS logs to the instance."
  type        = list(string)

  validation {
    condition     = length(var.nss_source_cidrs) > 0
    error_message = "nss_source_cidrs must contain at least one CIDR."
  }
}

variable "allow_ssh" {
  description = "Allow SSH ingress from admin_cidrs."
  type        = bool
  default     = false
}

variable "admin_cidrs" {
  description = "CIDR list allowed for optional SSH and optional remote metrics."
  type        = list(string)
  default     = []
}

variable "allow_metrics_from_admin" {
  description = "Allow metrics port ingress from admin_cidrs."
  type        = bool
  default     = false
}

variable "metrics_port" {
  description = "Metrics server port."
  type        = number
  default     = 9090
}

variable "metrics_bind_addr" {
  description = "Metrics bind address passed to install.sh."
  type        = string
  default     = "127.0.0.1:9090"
}

variable "repo_url" {
  description = "Git repository URL to clone."
  type        = string
  default     = "https://github.com/EggertsIT/nss-to-parquet.git"
}

variable "repo_ref" {
  description = "Git branch, tag, or commit to checkout."
  type        = string
  default     = "main"
}
