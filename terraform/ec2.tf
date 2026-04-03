resource "aws_instance" "server" {
  ami                    = var.ami_id            # Ubuntu 24.04
  instance_type          = var.ec2_instance_type # t3.small
  key_name               = var.key_pair_name     # howmanycaffeine-key
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]

  # IAM Role 연결 (S3 접근을 키 없이 가능하게)
  iam_instance_profile = aws_iam_instance_profile.ec2_profile.name

  # 루트 볼륨 설정
  root_block_device {
    volume_size = 20    # GB
    volume_type = "gp3"
  }

  tags = {
    Name    = "${var.project_name}-server"
    project = var.project_name
  }
}
