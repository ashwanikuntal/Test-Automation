resource "aws_s3_bucket" "b" {
  bucket = "akuntal-tf-test-bucket"
  acl    = "private"

  tags = {
    Name        = "My tf bucket"
    Environment = "Dev"
  }
}