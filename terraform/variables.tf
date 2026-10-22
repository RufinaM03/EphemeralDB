variable "region" {
  type    = string
  default = "ap-south-1"
}
variable "project" {
  type    = string
  default = "ephemeraldb"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,23}$", var.project))
    error_message = "Use a short lowercase project identifier."
  }

}
variable "wal_az_id" {
  type        = string
  description = "S3 Express supported AZ ID, verified in the target account/region."
}
variable "admin_principal_arn" {
  type        = string
  description = "IAM role/user ARN permitted to administer this benchmark EKS cluster."
}
variable "admin_cidrs" {
  type        = list(string)
  description = "Public EKS API source CIDRs, typically your current public IP /32."
  validation {
    condition     = length(var.admin_cidrs) > 0 && alltrue([for value in var.admin_cidrs : can(cidrhost(value, 0)) && value != "0.0.0.0/0"])
    error_message = "Supply restricted IPv4 source CIDRs."
  }

}
variable "kubernetes_version" {
  type        = string
  description = "A currently standard-supported EKS version in the target region."
}
variable "instance_types" {
  type    = list(string)
  default = ["r6id.large", "r6id.xlarge"]
}
variable "enable_aurora" {
  type    = bool
  default = false
}
variable "aurora_engine_version" {
  type        = string
  default     = "16.6"
  description = "Verify availability and Serverless v2 auto-pause support before planning."
}
variable "aurora_max_acu" {
  type    = number
  default = 4
}
