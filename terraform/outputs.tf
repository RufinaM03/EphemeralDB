output "cluster_name" {
  value = aws_eks_cluster.lab.name
}
output "runtime_role_arn" {
  value = aws_iam_role.runtime.arn
}
output "ecr_repository" {
  value = aws_ecr_repository.runtime.repository_url
}
output "s3_express_bucket" {
  value = aws_s3_directory_bucket.wal.bucket
}
output "s3_checkpoint_bucket" {
  value = aws_s3_bucket.checkpoints.bucket
}
output "aurora_endpoint" {
  value = try(aws_rds_cluster.aurora[0].endpoint, null)
}
output "aurora_secret_arn" {
  value = try(aws_rds_cluster.aurora[0].master_user_secret[0].secret_arn, null)
}
output "wal_az_name" {
  value = data.aws_availability_zone.wal.name
}
