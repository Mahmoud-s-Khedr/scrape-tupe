variable "project_id" { type = string }
variable "region" {
  type    = string
  default = "us-central1"
}
variable "billing_account" { type = string }
variable "master_image" { type = string }
variable "normalizer_image" { type = string }
variable "downloader_image" { type = string }
variable "operator_members" { type = set(string) }
variable "alert_email" { type = string }
variable "operator_api_key" {
  type      = string
  sensitive = true
}
variable "normalizer_parallelism" {
  type    = number
  default = 25
}
