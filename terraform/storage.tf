data "aws_caller_identity" "current" {

}
resource "aws_s3_bucket" "checkpoints" {
  bucket        = "${var.project}-${data.aws_caller_identity.current.account_id}-${var.region}-evidence"
  force_destroy = false
}
resource "aws_s3_bucket_public_access_block" "checkpoints" {
  bucket                  = aws_s3_bucket.checkpoints.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_server_side_encryption_configuration" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }

}
resource "aws_s3_directory_bucket" "wal" {
  bucket = "${var.project}-${data.aws_caller_identity.current.account_id}--${var.wal_az_id}--x-s3"
  location {
    name = var.wal_az_id
  }
  force_destroy = false
}
resource "aws_ecr_repository" "runtime" {
  name                 = "${var.project}-runtime"
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }

}
resource "aws_ecr_lifecycle_policy" "runtime" {
  repository = aws_ecr_repository.runtime.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1, description = "Retain ten newest images", selection = {
        tagStatus = "any", countType = "imageCountMoreThan", countNumber = 10
        }, action = {
        type = "expire"
      }
    }]
  })
}
