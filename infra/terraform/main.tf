resource "aws_s3_bucket" "bronze" {
  bucket        = "bronze"
  force_destroy = true
}

resource "aws_s3_bucket" "silver" {
  bucket        = "silver"
  force_destroy = true
}

resource "aws_s3_bucket" "gold" {
  bucket        = "gold"
  force_destroy = true
}
