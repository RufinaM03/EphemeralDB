resource "aws_security_group" "aurora" {
  count       = var.enable_aurora ? 1 : 0
  name_prefix = "${var.project}-aurora-"
  vpc_id      = aws_vpc.lab.id
  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_eks_cluster.lab.vpc_config[0].cluster_security_group_id]
  }

}
resource "aws_db_subnet_group" "aurora" {
  count      = var.enable_aurora ? 1 : 0
  name       = "${var.project}-aurora"
  subnet_ids = aws_subnet.database[*].id
}
resource "aws_rds_cluster" "aurora" {
  count                       = var.enable_aurora ? 1 : 0
  cluster_identifier          = "${var.project}-baseline"
  engine                      = "aurora-postgresql"
  engine_mode                 = "provisioned"
  engine_version              = var.aurora_engine_version
  database_name               = "benchmark"
  master_username             = "benchmark_admin"
  manage_master_user_password = true
  storage_encrypted           = true
  db_subnet_group_name        = aws_db_subnet_group.aurora[0].name
  vpc_security_group_ids      = [aws_security_group.aurora[0].id]
  backup_retention_period     = 1
  skip_final_snapshot         = false
  final_snapshot_identifier   = "${var.project}-baseline-final"
  serverlessv2_scaling_configuration {
    min_capacity             = 0
    max_capacity             = var.aurora_max_acu
    seconds_until_auto_pause = 300
  }

}
resource "aws_rds_cluster_instance" "aurora" {
  count               = var.enable_aurora ? 1 : 0
  identifier          = "${var.project}-baseline-writer"
  cluster_identifier  = aws_rds_cluster.aurora[0].id
  instance_class      = "db.serverless"
  engine              = aws_rds_cluster.aurora[0].engine
  engine_version      = aws_rds_cluster.aurora[0].engine_version
  publicly_accessible = false
}
