# terraform apply 후 터미널에 출력되는 값들

output "s3_bucket_name" {
  description = "S3 버킷 이름"
  value       = aws_s3_bucket.data.bucket
}

output "security_group_id" {
  description = "EC2에 적용할 Security Group ID"
  value       = aws_security_group.ec2_sg.id
}

output "ec2_instance_profile_name" {
  description = "EC2에 적용할 Instance Profile 이름 (IAM Role 연결용)"
  value       = aws_iam_instance_profile.ec2_profile.name
}

output "ec2_public_ip" {
  description = "EC2 퍼블릭 IP"
  value       = aws_instance.server.public_ip
}

output "ec2_instance_id" {
  description = "EC2 인스턴스 ID"
  value       = aws_instance.server.id
}
