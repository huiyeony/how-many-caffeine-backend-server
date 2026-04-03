variable "aws_region" {
  type        = string
  default     = "ap-northeast-2"
  description = "AWS 리전 (서울)"
}

variable "s3_bucket_name" {
  type        = string
  description = "S3 버킷 이름"
}

variable "my_ip" {
  type        = string
  description = "SSH 접속을 허용할 내 공인 IP (예: 123.456.789.0)"
}

variable "ec2_instance_type" {
  type        = string
  default     = "t2.micro"
  description = "EC2 인스턴스 타입"
}

variable "project_name" {
  type        = string
  default     = "howmanycaffeine"
  description = "프로젝트 이름 (리소스 이름 prefix로 사용)"
}

variable "ami_id" {
  type        = string
  description = "EC2 AMI ID"
}

variable "key_pair_name" {
  type        = string
  description = "EC2 SSH 키페어 이름"
}

variable "vpc_id" {
  type        = string
  description = "VPC ID"
}

variable "subnet_id" {
  type        = string
  description = "Subnet ID"
}
