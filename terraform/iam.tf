# EC2가 사용할 Role (신분증)
resource "aws_iam_role" "ec2_role" {
  name = "${var.project_name}-ec2-role"

  # "EC2 서비스가 이 Role을 사용할 수 있다"는 신뢰 정책
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })

  tags = {
    project = var.project_name
  }
}

# Role에 S3 접근 권한 부여
resource "aws_iam_role_policy" "s3_access" {
  name = "${var.project_name}-s3-access"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",    # 파일 읽기
        "s3:PutObject",    # 파일 업로드
        "s3:DeleteObject", # 파일 삭제
        "s3:ListBucket"    # 버킷 목록 조회
      ]
      Resource = [
        "arn:aws:s3:::${var.s3_bucket_name}",       # 버킷 자체
        "arn:aws:s3:::${var.s3_bucket_name}/*"       # 버킷 안의 모든 파일
      ]
    }]
  })
}

# Role을 EC2에 붙이기 위한 Instance Profile
# (EC2는 Role을 직접 받지 못하고 Instance Profile을 통해 받음)
resource "aws_iam_instance_profile" "ec2_profile" {
  name = "${var.project_name}-ec2-profile"
  role = aws_iam_role.ec2_role.name
}
