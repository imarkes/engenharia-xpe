# Envia arquivos
resource "aws_s3_object" "job_donwload_rais" {
  bucket = aws_s3_bucket.bronze.id
  key    = "jobs/python/job_download_rais.py"
  source = "../job_download_rais.py"
  etag   = filemd5("../job_download_rais.py")
}