provider "aws" {
  region = var.aws_region
}

# Lambda 실행 IAM Role
resource "aws_iam_role" "lambda_exec" {
  name = "crawl-pipeline-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# CloudWatch 로그 권한
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# S3 접근 권한
resource "aws_iam_role_policy_attachment" "lambda_s3" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# Lambda 함수
resource "aws_lambda_function" "crawl_pipeline" {
  function_name = "howmanycaffeine-crawl-pipeline"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = var.lambda_image_uri
  timeout       = 600  # 10분
  memory_size   = 512

  environment {
    variables = {
      OPENAI_API_KEY      = var.openai_api_key
      POSTGRES_HOST       = var.postgres_host
      POSTGRES_PORT       = var.postgres_port
      POSTGRES_USER       = var.postgres_user
      POSTGRES_PASSWORD   = var.postgres_password
      POSTGRES_DB         = var.postgres_db
      AWS_S3_BUCKET_NAME  = var.s3_bucket_name
    }
  }
}
