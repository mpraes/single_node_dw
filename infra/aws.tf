resource "tls_private_key" "vm_ssh" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

data "aws_ami" "amazon_linux_2023" {
  count       = local.selected_cloud == "aws" ? 1 : 0
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_vpc" "main" {
  count                = local.selected_cloud == "aws" ? 1 : 0
  cidr_block           = "10.10.0.0/16"
  enable_dns_hostnames = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

resource "aws_subnet" "main" {
  count                   = local.selected_cloud == "aws" ? 1 : 0
  vpc_id                  = aws_vpc.main[0].id
  cidr_block              = "10.10.1.0/24"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-subnet"
  }
}

resource "aws_internet_gateway" "main" {
  count  = local.selected_cloud == "aws" ? 1 : 0
  vpc_id = aws_vpc.main[0].id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

resource "aws_route_table" "main" {
  count  = local.selected_cloud == "aws" ? 1 : 0
  vpc_id = aws_vpc.main[0].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main[0].id
  }

  tags = {
    Name = "${var.project_name}-rt"
  }
}

resource "aws_route_table_association" "main" {
  count          = local.selected_cloud == "aws" ? 1 : 0
  subnet_id      = aws_subnet.main[0].id
  route_table_id = aws_route_table.main[0].id
}

resource "aws_security_group" "vm" {
  count       = local.selected_cloud == "aws" ? 1 : 0
  name        = "${var.project_name}-sg"
  description = "Allow SSH, Postgres and Mage"
  vpc_id      = aws_vpc.main[0].id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Postgres"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Mage.ai"
    from_port   = 6789
    to_port     = 6789
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-sg"
  }
}

resource "aws_key_pair" "vm" {
  count      = local.selected_cloud == "aws" ? 1 : 0
  key_name   = "${var.project_name}-key"
  public_key = tls_private_key.vm_ssh.public_key_openssh
}

resource "aws_instance" "vm" {
  count                  = local.selected_cloud == "aws" ? 1 : 0
  ami                    = data.aws_ami.amazon_linux_2023[0].id
  instance_type          = var.aws_instance_type
  subnet_id              = aws_subnet.main[0].id
  vpc_security_group_ids = [aws_security_group.vm[0].id]
  key_name               = aws_key_pair.vm[0].key_name
  user_data              = local.bootstrap_script

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  tags = {
    Name = "${var.project_name}-vm"
  }
}
