variable "minio_endpoint" {
  default = "http://localhost:9000"
}

variable "minio_root_user" {
  default = "minioadmin"
}

variable "minio_root_password" {
  default   = "minioadmin"
  sensitive = true
}
