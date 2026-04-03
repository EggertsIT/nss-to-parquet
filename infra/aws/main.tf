data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["137112412989"] # Amazon

  filter {
    name   = "name"
    values = ["al2023-ami-2023*-x86_64"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

data "aws_subnet" "selected" {
  id = var.subnet_id
}

resource "aws_security_group" "nss_ingestor" {
  name        = "${var.name_prefix}-sg"
  description = "Security group for NSS ingestor sample"
  vpc_id      = var.vpc_id

  ingress {
    description = "NSS TCP ingest"
    from_port   = var.nss_port
    to_port     = var.nss_port
    protocol    = "tcp"
    cidr_blocks = var.nss_source_cidrs
  }

  dynamic "ingress" {
    for_each = var.allow_ssh ? [1] : []
    content {
      description = "SSH admin access"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = var.admin_cidrs
    }
  }

  dynamic "ingress" {
    for_each = var.allow_metrics_from_admin ? [1] : []
    content {
      description = "Metrics and dashboard admin access"
      from_port   = var.metrics_port
      to_port     = var.metrics_port
      protocol    = "tcp"
      cidr_blocks = var.admin_cidrs
    }
  }

  egress {
    description = "Allow outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.name_prefix}-sg"
  }
}

resource "aws_iam_role" "ec2_ssm_role" {
  name = "${var.name_prefix}-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.ec2_ssm_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "${var.name_prefix}-ec2-profile"
  role = aws_iam_role.ec2_ssm_role.name
}

resource "aws_instance" "nss_ingestor" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.nss_ingestor.id]
  iam_instance_profile        = aws_iam_instance_profile.ec2_profile.name
  associate_public_ip_address = false

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_size_gb
    encrypted             = true
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    repo_url          = var.repo_url
    repo_ref          = var.repo_ref
    nss_port          = var.nss_port
    metrics_bind_addr = var.metrics_bind_addr
  })

  tags = {
    Name = "${var.name_prefix}-ec2"
  }

  lifecycle {
    precondition {
      condition     = data.aws_subnet.selected.vpc_id == var.vpc_id
      error_message = "subnet_id must belong to vpc_id."
    }

    precondition {
      condition     = !data.aws_subnet.selected.map_public_ip_on_launch
      error_message = "subnet_id must be a private subnet (map_public_ip_on_launch = false)."
    }

    precondition {
      condition     = (!var.allow_ssh && !var.allow_metrics_from_admin) || length(var.admin_cidrs) > 0
      error_message = "admin_cidrs must be set when allow_ssh or allow_metrics_from_admin is enabled."
    }
  }
}
