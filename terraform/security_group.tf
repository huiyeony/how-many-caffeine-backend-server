resource "aws_security_group" "ec2_sg" {
  name        = "${var.project_name}-sg"
  description = "Security group for ${var.project_name} EC2"

  # HTTP — 전체 허용 (nginx가 받음)
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # HTTPS — 전체 허용 (nginx가 받음)
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # SSH — 내 IP만 허용
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["${var.my_ip}/32"]  # /32 = 딱 이 IP 하나만
  }

  # 8000 (FastAPI) — ingress 없음 = 외부 접근 불가
  # nginx 내부에서만 접근하므로 열 필요 없음

  # 나가는 트래픽 전체 허용 (API 호출, S3 접근 등)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"          # 모든 프로토콜
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    project = var.project_name
  }
}
