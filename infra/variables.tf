variable "cloud_provider" {
  description = "Cloud provider target: aws or azure"
  type        = string
  default     = "aws"

  validation {
    condition     = contains(["aws", "azure"], lower(var.cloud_provider))
    error_message = "cloud_provider deve ser 'aws' ou 'azure'."
  }
}

variable "project_name" {
  description = "Prefixo para nomear os recursos"
  type        = string
  default     = "single-node-dw"
}

variable "aws_region" {
  description = "Região AWS"
  type        = string
  default     = "us-east-1"
}

variable "azure_location" {
  description = "Região Azure"
  type        = string
  default     = "eastus"
}

variable "aws_instance_type" {
  description = "Tipo da instância EC2"
  type        = string
  default     = "t3.medium"
}

variable "azure_vm_size" {
  description = "Tipo da VM Azure"
  type        = string
  default     = "Standard_B2s"
}

variable "aws_ssh_username" {
  description = "Usuário SSH para AWS"
  type        = string
  default     = "ec2-user"
}

variable "azure_ssh_username" {
  description = "Usuário SSH para Azure"
  type        = string
  default     = "azureuser"
}

locals {
  selected_cloud = lower(var.cloud_provider)

  bootstrap_script = <<-EOT
    #!/bin/bash
    set -euxo pipefail

    if command -v apt-get >/dev/null 2>&1; then
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -y
      apt-get install -y ca-certificates curl gnupg lsb-release docker.io docker-compose-plugin
      systemctl enable docker
      systemctl start docker
      usermod -aG docker ${local.ssh_user}
      if ! command -v docker-compose >/dev/null 2>&1; then
        curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        chmod +x /usr/local/bin/docker-compose
      fi
    elif command -v dnf >/dev/null 2>&1; then
      dnf update -y
      dnf install -y docker
      systemctl enable docker
      systemctl start docker
      usermod -aG docker ${local.ssh_user}
      if ! command -v docker-compose >/dev/null 2>&1; then
        curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        chmod +x /usr/local/bin/docker-compose
      fi
    elif command -v yum >/dev/null 2>&1; then
      yum update -y
      yum install -y docker
      systemctl enable docker
      systemctl start docker
      usermod -aG docker ${local.ssh_user}
      if ! command -v docker-compose >/dev/null 2>&1; then
        curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        chmod +x /usr/local/bin/docker-compose
      fi
    fi
  EOT

  ssh_user = local.selected_cloud == "aws" ? var.aws_ssh_username : var.azure_ssh_username
}
