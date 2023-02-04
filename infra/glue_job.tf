resource "aws_s3_object" "upload_script_glue" {
  bucket = var.bucket-bronze
  key    = "scripts/glue/job_download_rais.py"
}

resource "aws_glue_job" "glue-job" {
  name        = var.job-name
  role_arn    = var.glue-arn
  timeout     = 2800
  max_retries = "1"
  command {
    script_location = "s3://${var.bucket-bronze}/scripts/glue/job_download_rais.py"
    python_version  = "3"
  }
  execution_property {
    max_concurrent_runs = 1
  }
  glue_version = "3.0"
}