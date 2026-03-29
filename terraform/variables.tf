variable "aws_region" {
  default = "ap-northeast-2"
}

variable "lambda_image_uri" {
  description = "ECR Lambda 컨테이너 이미지 URI"
}

variable "openai_api_key" {
  sensitive = true
}

variable "postgres_host" {}

variable "postgres_port" {
  default = "5432"
}

variable "postgres_user" {
  sensitive = true
}

variable "postgres_password" {
  sensitive = true
}

variable "postgres_db" {}

variable "s3_bucket_name" {}
