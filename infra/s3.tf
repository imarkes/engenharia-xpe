# Define  a criaçaõ do bucket
resource "aws_s3_bucket" "bronze" {
  bucket = var.bucket-bronze

  tags = {
    Name        = "imos"
    Environment = "Dev"
  }
}

# Define o status ACL
resource "aws_s3_bucket_acl" "s3imos_acl" {
  bucket = aws_s3_bucket.bronze.id
  acl    = "private"
}

#Criptografia default
resource "aws_s3_bucket_server_side_encryption_configuration" "cripts3" {
  bucket = aws_s3_bucket_acl.s3imos_acl.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}