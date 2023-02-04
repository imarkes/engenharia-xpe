# ------------------ Bucket var
variable "bucket-bronze" {
  type    = string
  default = "imos-datalake-bronze"
}

variable "region-name" {
  type    = string
  default = "us-east-1"
}

variable "job-language" {
  type    = string
  default = "python"
}

# ------------------ Glue var
variable "bucket-landing" {
  type    = string
  default = "imos-datalake-landing"
}

variable "glue-arn" {
  type    = string
  default = "arn:aws:iam::xxxxx"
}

variable "job-name" {
  type    = string
  default = "download_rais_job"
}

