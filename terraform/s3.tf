resource "aws_s3_bucket" "data" {
  bucket = var.s3_bucket_name

  tags = {
    project = var.project_name
  }
}

# 외부에서 S3에 직접 접근 못하게 차단
resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
