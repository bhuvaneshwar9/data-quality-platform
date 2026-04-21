variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "alert_email" {
  description = "Email to receive quality failure alerts"
  type        = string
  default     = "bhuvivangimalla9@gmail.com"
}
