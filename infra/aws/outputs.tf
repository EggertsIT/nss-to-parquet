output "instance_id" {
  description = "EC2 instance ID."
  value       = aws_instance.nss_ingestor.id
}

output "private_ip" {
  description = "Private IP for NSS feed destination."
  value       = aws_instance.nss_ingestor.private_ip
}

output "security_group_id" {
  description = "Security group attached to the instance."
  value       = aws_security_group.nss_ingestor.id
}

output "nss_endpoint" {
  description = "Convenience endpoint string for NSS destination configuration."
  value       = "${aws_instance.nss_ingestor.private_ip}:${var.nss_port}"
}
