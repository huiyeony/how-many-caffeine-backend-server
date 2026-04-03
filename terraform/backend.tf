terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "howmanycaffeine-terraform-state"
    key    = "terraform.tfstate"
    region = "ap-northeast-2"
  }
}
