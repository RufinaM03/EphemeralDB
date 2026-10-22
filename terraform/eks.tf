resource "aws_iam_role" "cluster" {
  name = "${var.project}-eks"
  assume_role_policy = jsonencode({
    Version = "2012-10-17", Statement = [{
      Effect = "Allow", Principal = {
        Service = "eks.amazonaws.com"
      }, Action = "sts:AssumeRole"
    }]
  })
}
resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}
resource "aws_eks_cluster" "lab" {
  name     = var.project
  role_arn = aws_iam_role.cluster.arn
  version  = var.kubernetes_version
  access_config {
    authentication_mode = "API"
  }
  vpc_config {
    subnet_ids              = aws_subnet.public[*].id
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = var.admin_cidrs
  }
  depends_on = [aws_iam_role_policy_attachment.cluster]
}
resource "aws_eks_access_entry" "admin" {
  cluster_name  = aws_eks_cluster.lab.name
  principal_arn = var.admin_principal_arn
  type          = "STANDARD"
}
resource "aws_eks_access_policy_association" "admin" {
  cluster_name  = aws_eks_cluster.lab.name
  principal_arn = aws_eks_access_entry.admin.principal_arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
  access_scope {
    type = "cluster"
  }

}
resource "aws_iam_role" "node" {
  name = "${var.project}-node"
  assume_role_policy = jsonencode({
    Version = "2012-10-17", Statement = [{
      Effect = "Allow", Principal = {
        Service = "ec2.amazonaws.com"
      }, Action = "sts:AssumeRole"
    }]
  })
}
resource "aws_iam_role_policy_attachment" "node" {
  for_each   = toset(["AmazonEKSWorkerNodePolicy", "AmazonEC2ContainerRegistryPullOnly", "AmazonEKS_CNI_Policy"])
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/${each.value}"
}
resource "aws_launch_template" "node" {
  name_prefix = "${var.project}-nvme-"
  user_data   = base64encode(file("${path.module}/nvme-user-data.mime"))
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }
  tag_specifications {
    resource_type = "instance"
    tags = {
      Project = var.project, Name = "${var.project}-spot-nvme"
    }
  }

}
resource "aws_eks_node_group" "spot" {
  cluster_name    = aws_eks_cluster.lab.name
  node_group_name = "spot-nvme"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = [aws_subnet.public[0].id]
  capacity_type   = "SPOT"
  ami_type        = "AL2023_x86_64_STANDARD"
  instance_types  = var.instance_types
  scaling_config {
    desired_size = 1
    min_size     = 1
    max_size     = 3
  }
  update_config {
    max_unavailable = 1
  }
  launch_template {
    id      = aws_launch_template.node.id
    version = aws_launch_template.node.latest_version
  }
  labels = {
    "ephemeraldb/storage" = "nvme"
  }
  depends_on = [aws_iam_role_policy_attachment.node, aws_route_table_association.public]
}
data "tls_certificate" "oidc" {
  url = aws_eks_cluster.lab.identity[0].oidc[0].issuer
}
resource "aws_iam_openid_connect_provider" "eks" {
  url             = aws_eks_cluster.lab.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.oidc.certificates[0].sha1_fingerprint]
}
resource "aws_iam_role" "runtime" {
  name = "${var.project}-runtime"
  assume_role_policy = jsonencode({
    Version = "2012-10-17", Statement = [{
      Effect = "Allow", Principal = {
        Federated = aws_iam_openid_connect_provider.eks.arn
        }, Action = "sts:AssumeRoleWithWebIdentity", Condition = {
        StringEquals = {
          "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:ephemeraldb:runtime", "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}
resource "aws_iam_role_policy" "runtime" {
  role = aws_iam_role.runtime.id
  policy = jsonencode({
    Version = "2012-10-17", Statement = [
      {
        Effect = "Allow", Action = ["s3express:CreateSession"], Resource = [aws_s3_directory_bucket.wal.arn]
      },
      {
        Effect = "Allow", Action = ["s3:ListBucket"], Resource = [aws_s3_bucket.checkpoints.arn], Condition = {
          StringLike = {
            "s3:prefix" = ["checkpoints/*"]
          }
        }
      },
      {
        Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject"], Resource = ["${aws_s3_bucket.checkpoints.arn}/checkpoints/*"]
    }]
  })
}
