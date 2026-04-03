# AWS Terraform Sample

This sample deploys a single EC2 host for `nss-ingestor` on AWS.

It creates:
- Security group with NSS ingress allowlist
- EC2 instance profile with `AmazonSSMManagedInstanceCore`
- One Amazon Linux 2023 EC2 instance in a private subnet
- Bootstrapping via `user_data` to install and start `nss-ingestor`

## Notes

- This is a sample baseline, not HA architecture.
- TLS/mTLS on ingest path is intentionally not included (per current project scope).
- Keep `nss_source_cidrs` strict and minimal.
- Public ingest IPs are blocked by design in this Terraform sample.

## Prerequisites

- Terraform CLI installed
- AWS credentials configured (`aws configure`, SSO, or environment variables)
- Existing VPC and subnet
- Subnet must be private (`map_public_ip_on_launch = false`)

## Deploy

```bash
cd infra/aws
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:
- Set `vpc_id` and `subnet_id`
- Set precise `nss_source_cidrs`
- Set `repo_url` to your repo if needed
- Optionally set `allow_metrics_from_admin = true` and `admin_cidrs`

Apply:

```bash
terraform init
terraform plan
terraform apply
```

After apply, use output `nss_endpoint` (for example `10.0.10.25:514`) as your Zscaler NSS TCP destination over private connectivity (VPN / Direct Connect / private interconnect).

## Operations

Check bootstrap logs through SSM Session Manager:

```bash
sudo tail -n 200 /var/log/nss-ingestor-bootstrap.log
sudo systemctl status nss-ingestor --no-pager
curl -s http://127.0.0.1:9090/metrics | head
```

Destroy:

```bash
terraform destroy
```
